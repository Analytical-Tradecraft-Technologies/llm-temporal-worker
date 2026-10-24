package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
	redisstore "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/redis"
	"github.com/google/uuid"
)

// budgetAuthorityRecoverer rebuilds lost Redis accounting behind a fence that
// keeps every reserve, claim and settlement unavailable until it commits.
type budgetAuthorityRecoverer interface {
	BeginRecovery(context.Context, budget.Initialization, bool) (bool, error)
	HoldRecoveryLease(context.Context, budget.Initialization, string, time.Duration, string) (string, bool, error)
	ReleaseRecoveryLease(context.Context, budget.Initialization, string) error
	RecoverOperation(context.Context, budget.Initialization, string, redisstore.BudgetRecoveryOperation) (bool, error)
	ApplyRecoveryFloors(context.Context, budget.Initialization, string) (int, error)
	CommitRecovery(context.Context, budget.Initialization, time.Time) error
}

// budgetRecoverySource enumerates the durable execution records that hold every
// Redis acceptance able to authorize paid work, with its claim and settlement.
type budgetRecoverySource interface {
	ListBudgetRecovery(context.Context, int, int, string, time.Time) (cloudstate.BudgetRecoveryPage, error)
}

type budgetRecovery struct {
	Recoverer budgetAuthorityRecoverer
	Source    budgetRecoverySource
	// Force fences and reconciles a ready marker, for suspected partial loss
	// or a failover that may have dropped acknowledged writes.
	Force bool
	// Horizon is how long settled spend of a finished request can still count.
	Horizon time.Duration
	Logger  *slog.Logger
	// Wait is the interval between checks while another process recovers.
	Wait     time.Duration
	LeaseTTL time.Duration
	PageSize int
	// MinimumWork is the least time left worth taking over the lease with.
	MinimumWork time.Duration
}

const (
	defaultBudgetRecoveryWait     = 2 * time.Second
	defaultBudgetRecoveryLeaseTTL = time.Minute
	defaultBudgetRecoveryPageSize = 250
	// budgetRecoveryMergeConcurrency bounds Redis merges in flight per shard.
	// All shards run at once, so up to eight times this many are in flight.
	budgetRecoveryMergeConcurrency = 16
	// minimumBudgetRecoveryWork is the least time left worth taking over a
	// lease with; less would only time out and restart the init container.
	minimumBudgetRecoveryWork = 30 * time.Second
	// minimumBudgetRecoveryHorizon keeps finished requests for windows that a
	// configuration reload removed and might restore under the same identity.
	minimumBudgetRecoveryHorizon = 31 * 24 * time.Hour
)

// budgetRecoveryHorizon bounds how long a finished request's settled spend can
// still count: its longest window and bucket, the start lease, and clock skew.
func budgetRecoveryHorizon(value config.Config) time.Duration {
	longest := time.Duration(0)
	for _, policy := range value.Budgets.Policies {
		for _, window := range policy.Windows {
			if total := time.Duration(window.Duration) + time.Duration(window.Bucket); total > longest {
				longest = total
			}
		}
	}
	longest += durable.BudgetStartLease + redisstore.BudgetRecoveryClockSkew + time.Hour
	if longest < minimumBudgetRecoveryHorizon {
		return minimumBudgetRecoveryHorizon
	}
	return longest
}

// budgetRecoveryProgress is a resumable cursor. Losing it only repeats
// idempotent merges from the first page. Shard and Token are the sequential
// format written by the first release; a cursor without Shards resumes from it.
type budgetRecoveryProgress struct {
	Shard    int                   `json:"shard"`
	Token    string                `json:"token,omitempty"`
	Scanned  int                   `json:"scanned"`
	Restored int                   `json:"restored"`
	Shards   []budgetRecoveryShard `json:"shards,omitempty"`
}

type budgetRecoveryShard struct {
	Token string `json:"token,omitempty"`
	Done  bool   `json:"done,omitempty"`
}

// errBudgetRecoveryInProgress reports a run that stopped without finishing;
// the fence and progress remain for the next start.
var errBudgetRecoveryInProgress = errors.New("budget recovery is still in progress")

func decodeBudgetRecoveryProgress(stored string) (budgetRecoveryProgress, bool) {
	var progress budgetRecoveryProgress
	if stored == "" || json.Unmarshal([]byte(stored), &progress) != nil || progress.Shard < 0 || progress.Shard > cloudstate.PendingShards ||
		(progress.Shards != nil && len(progress.Shards) != cloudstate.PendingShards) {
		return budgetRecoveryProgress{Shards: make([]budgetRecoveryShard, cloudstate.PendingShards)}, false
	}
	if progress.Shards == nil {
		progress.Shards = make([]budgetRecoveryShard, cloudstate.PendingShards)
		for shard := range progress.Shard {
			progress.Shards[shard].Done = true
		}
		if progress.Shard < cloudstate.PendingShards {
			progress.Shards[progress.Shard].Token = progress.Token
		}
	}
	progress.Shard, progress.Token = 0, ""
	return progress, true
}

func (progress budgetRecoveryProgress) shardsDone() int {
	done := 0
	for _, shard := range progress.Shards {
		if shard.Done {
			done++
		}
	}
	return done
}

func (recovery budgetRecovery) logger() *slog.Logger {
	if recovery.Logger != nil {
		return recovery.Logger
	}
	return slog.New(slog.DiscardHandler)
}

// recoverBudgetAuthority restores Redis accounting for a Ready receipt. One
// process holds the lease and merges every durable record; the others wait
// for the ready marker. An interrupted run leaves the fence in place and the
// next run resumes from the saved cursor. Readiness is published only after
// every shard has been merged.
func recoverBudgetAuthority(ctx context.Context, receipt budget.Initialization, recovery budgetRecovery, now func() time.Time) error {
	log := recovery.logger()
	required, err := recovery.Recoverer.BeginRecovery(ctx, receipt, recovery.Force)
	if err != nil {
		log.Error("budget recovery could not fence Redis budget authority", slog.String("reason", budgetRecoveryReason(err)),
			slog.String("action", "check that the Redis authority marker matches this receipt; never delete it or the receipt"))
		return err
	}
	if !required {
		return nil
	}
	if recovery.Wait <= 0 {
		recovery.Wait = defaultBudgetRecoveryWait
	}
	if recovery.LeaseTTL <= 0 {
		recovery.LeaseTTL = defaultBudgetRecoveryLeaseTTL
	}
	if recovery.PageSize <= 0 {
		recovery.PageSize = defaultBudgetRecoveryPageSize
	}
	if recovery.MinimumWork <= 0 {
		recovery.MinimumWork = minimumBudgetRecoveryWork
	}
	log.Warn("Redis budget authority is fenced for recovery; paid work stays unavailable until it completes", slog.Bool("forced", recovery.Force))
	token := uuid.NewString()
	waiting := false
	for {
		stored, ready, err := recovery.Recoverer.HoldRecoveryLease(ctx, receipt, token, recovery.LeaseTTL, "")
		if ready {
			log.Info("budget recovery completed in another process")
			return nil
		}
		if errors.Is(err, redisstore.ErrBudgetRecoveryBusy) {
			if !waiting {
				log.Info("budget recovery is running in another process; waiting for it to finish")
				waiting = true
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(recovery.Wait):
			}
			continue
		}
		if err != nil {
			log.Error("budget recovery could not acquire its lease", slog.String("reason", budgetRecoveryReason(err)))
			return err
		}
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < recovery.MinimumWork {
			releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			_ = recovery.Recoverer.ReleaseRecoveryLease(releaseCtx, receipt, token)
			cancel()
			log.Info("budget recovery is still in progress; too little time is left to continue it here, so the next start resumes it")
			return errBudgetRecoveryInProgress
		}
		err = recovery.run(ctx, receipt, token, stored, now)
		if errors.Is(err, redisstore.ErrBudgetRecoveryBusy) {
			log.Warn("budget recovery lease moved to another process; waiting for it to finish")
			waiting = true
			continue
		}
		if err != nil {
			// Leave the fence and progress for the next run; free the lease now.
			releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			_ = recovery.Recoverer.ReleaseRecoveryLease(releaseCtx, receipt, token)
			cancel()
		}
		return err
	}
}

func (recovery budgetRecovery) run(ctx context.Context, receipt budget.Initialization, token, stored string, now func() time.Time) error {
	log := recovery.logger()
	started := now()
	progress, resumed := decodeBudgetRecoveryProgress(stored)
	if resumed {
		log.Info("resuming interrupted budget recovery", slog.Int("shards_done", progress.shardsDone()), slog.Int("scanned", progress.Scanned), slog.Int("restored", progress.Restored))
	}
	terminalNotBefore := started.Add(-recovery.Horizon)
	// Every shard runs at once; merges are idempotent and independent, so
	// their order does not matter. The first failure stops the others.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	var wg sync.WaitGroup
	errs := make([]error, cloudstate.PendingShards)
	for shard := range cloudstate.PendingShards {
		mu.Lock()
		state := progress.Shards[shard]
		mu.Unlock()
		if state.Done {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := recovery.runShard(runCtx, receipt, token, shard, state.Token, terminalNotBefore, &mu, &progress); err != nil {
				errs[shard] = err
				cancel()
			}
		}()
	}
	wg.Wait()
	if err := firstBudgetRecoveryError(errs); err != nil {
		recovery.logShardError(ctx, err)
		if ctx.Err() != nil {
			return errBudgetRecoveryInProgress
		}
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	raised, err := recovery.Recoverer.ApplyRecoveryFloors(ctx, receipt, token)
	if err != nil {
		if !errors.Is(err, redisstore.ErrBudgetRecoveryBusy) {
			log.Error("budget recovery could not verify bucket balances", slog.String("reason", budgetRecoveryReason(err)))
		}
		return err
	}
	if raised > 0 {
		log.Warn("budget recovery raised bucket balances that held less than their recorded charges; Redis lost accounting keys behind surviving records",
			slog.Int("raised_buckets", raised))
	}
	if err := recovery.Recoverer.CommitRecovery(ctx, receipt, now()); err != nil {
		log.Error("budget recovery could not publish readiness", slog.String("reason", budgetRecoveryReason(err)))
		return err
	}
	log.Info("budget recovery completed; Redis budget authority is ready", slog.Int("scanned", progress.Scanned),
		slog.Int("restored", progress.Restored), slog.Duration("elapsed", now().Sub(started)))
	return nil
}

// runShard merges one discovery shard page by page. Its cursor advances only
// after every operation on a page has merged.
func (recovery budgetRecovery) runShard(ctx context.Context, receipt budget.Initialization, token string, shard int, cursor string, terminalNotBefore time.Time, mu *sync.Mutex, progress *budgetRecoveryProgress) error {
	log := recovery.logger()
	resumed := cursor != ""
	for {
		page, err := recovery.Source.ListBudgetRecovery(ctx, shard, recovery.PageSize, cursor, terminalNotBefore)
		if err != nil && resumed && ctx.Err() == nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			// A saved page token may have expired; restart this shard.
			log.Warn("budget recovery restarted a shard after its saved cursor was rejected", slog.Int("shard", shard))
			cursor, resumed = "", false
			continue
		}
		resumed = false
		if err != nil {
			return budgetRecoveryShardError{shard: shard, read: true, err: err}
		}
		restored, err := recovery.mergePage(ctx, receipt, token, page.Executions)
		if err != nil {
			return budgetRecoveryShardError{shard: shard, err: err}
		}
		cursor = page.NextPageToken
		mu.Lock()
		progress.Scanned += page.Scanned
		progress.Restored += restored
		progress.Shards[shard] = budgetRecoveryShard{Token: cursor, Done: cursor == ""}
		encoded, _ := json.Marshal(progress)
		done, scanned, total := progress.shardsDone(), progress.Scanned, progress.Restored
		mu.Unlock()
		if cursor == "" {
			log.Info("budget recovery merged a shard", slog.Int("shard", shard), slog.Int("shards_done", done), slog.Int("shards", cloudstate.PendingShards),
				slog.Int("scanned", scanned), slog.Int("restored", total))
		}
		if _, ready, err := recovery.Recoverer.HoldRecoveryLease(ctx, receipt, token, recovery.LeaseTTL, string(encoded)); err != nil || ready {
			if ready {
				return redisstore.ErrBudgetRecoveryBusy // finished elsewhere; the caller rechecks
			}
			return budgetRecoveryShardError{shard: shard, err: err}
		}
		if cursor == "" {
			return nil
		}
	}
}

// mergePage merges one page's operations with bounded concurrency and reports
// how many changed Redis.
func (recovery budgetRecovery) mergePage(ctx context.Context, receipt budget.Initialization, token string, executions []cloudstate.SavedProviderExecution) (int, error) {
	mergeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	slots := make(chan struct{}, budgetRecoveryMergeConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	restored := 0
	var first error
	for _, saved := range executions {
		wg.Add(1)
		slots <- struct{}{}
		go func() {
			defer func() { <-slots; wg.Done() }()
			changed, err := recovery.Recoverer.RecoverOperation(mergeCtx, receipt, token, budgetRecoveryOperation(saved))
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if first == nil || (errors.Is(first, context.Canceled) && !errors.Is(err, context.Canceled)) {
					first = err
				}
				cancel()
				return
			}
			if changed {
				restored++
			}
		}()
	}
	wg.Wait()
	return restored, first
}

type budgetRecoveryShardError struct {
	shard int
	read  bool
	err   error
}

func (err budgetRecoveryShardError) Error() string { return err.err.Error() }
func (err budgetRecoveryShardError) Unwrap() error { return err.err }

// firstBudgetRecoveryError returns the failure that stopped the shards,
// preferring a real cause over the cancellations it triggered in the others.
func firstBudgetRecoveryError(errs []error) error {
	var cause error
	for _, err := range errs {
		if err != nil && (cause == nil || (errors.Is(cause, context.Canceled) && !errors.Is(err, context.Canceled))) {
			cause = err
		}
	}
	return cause
}

func (recovery budgetRecovery) logShardError(ctx context.Context, err error) {
	log := recovery.logger()
	var shardErr budgetRecoveryShardError
	shard := -1
	if errors.As(err, &shardErr) {
		shard = shardErr.shard
	}
	reason := budgetRecoveryReason(err)
	switch {
	case errors.Is(err, redisstore.ErrBudgetRecoveryBusy):
	case reason == "timeout" || ctx.Err() != nil:
		log.Info("budget recovery is still in progress; this run's time ran out and the next start resumes from the saved cursor", slog.Int("shard", shard))
	case errors.Is(err, redisstore.ErrBudgetRecoveryConflict):
		log.Error("budget recovery found a Redis operation that disagrees with its durable record", slog.Int("shard", shard),
			slog.String("action", "investigate the Redis namespace and cloud records before retrying; never delete either to reopen spending"))
	case shardErr.read:
		log.Error("budget recovery could not read durable request records", slog.Int("shard", shard), slog.String("reason", reason),
			slog.String("action", "restore cloud request storage access and rerun budget-initialize --apply; progress is kept"))
	default:
		log.Error("budget recovery could not restore an operation", slog.Int("shard", shard), slog.String("reason", reason))
	}
}

// budgetRecoveryOperation converts a durable execution into the accounting it
// proves. A claim is assumed unless the record proves none applied: a claim
// is written to Redis before its execution record, so the absence of one
// there never proves the reservation was unused.
func budgetRecoveryOperation(saved cloudstate.SavedProviderExecution) redisstore.BudgetRecoveryOperation {
	execution := saved.Execution
	return redisstore.BudgetRecoveryOperation{
		Request:     saved.Plan.Reservation,
		Reservation: execution.Reservation,
		Claimed:     !execution.Unclaimed(saved.Plan),
		StartedAt:   execution.StartedAt,
		// Redis set the start deadline from its own clock at acceptance,
		// which precedes the execution record by at most the clock skew.
		StartBy:    execution.RecoverAfter.Add(redisstore.BudgetRecoveryClockSkew),
		Settlement: execution.Settlement,
	}
}

// budgetRecoveryReason classifies an error without SDK text or identifiers.
func budgetRecoveryReason(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "timeout"
	case errors.Is(err, redisstore.ErrBudgetRecoveryConflict):
		return "conflict"
	case errors.Is(err, redisstore.ErrBudgetAuthorityUnavailable):
		return "authority_unavailable"
	case errors.Is(err, redisstore.ErrUnavailable):
		return "redis_state_unavailable"
	case errors.Is(err, cloudstate.ErrCorrupt), errors.Is(err, cloudstate.ErrInvalid):
		return "durable_record_invalid"
	case errors.Is(err, contracts.ErrNotFound), errors.Is(err, contracts.ErrConflict):
		return "durable_record_unavailable"
	default:
		return "unavailable"
	}
}
