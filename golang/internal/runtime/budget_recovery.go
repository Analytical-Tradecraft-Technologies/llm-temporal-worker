package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
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
}

const (
	defaultBudgetRecoveryWait     = 2 * time.Second
	defaultBudgetRecoveryLeaseTTL = time.Minute
	defaultBudgetRecoveryPageSize = 100
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
// idempotent merges from the first shard.
type budgetRecoveryProgress struct {
	Shard    int    `json:"shard"`
	Token    string `json:"token,omitempty"`
	Scanned  int    `json:"scanned"`
	Restored int    `json:"restored"`
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
	var progress budgetRecoveryProgress
	if stored != "" {
		if json.Unmarshal([]byte(stored), &progress) != nil || progress.Shard < 0 || progress.Shard > cloudstate.PendingShards {
			progress = budgetRecoveryProgress{}
		} else {
			log.Info("resuming interrupted budget recovery", slog.Int("shard", progress.Shard), slog.Int("scanned", progress.Scanned), slog.Int("restored", progress.Restored))
		}
	}
	terminalNotBefore := started.Add(-recovery.Horizon)
	resumed := progress.Token != ""
	for progress.Shard < cloudstate.PendingShards {
		page, err := recovery.Source.ListBudgetRecovery(ctx, progress.Shard, recovery.PageSize, progress.Token, terminalNotBefore)
		if err != nil && resumed && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			// A saved page token may have expired; restart this shard.
			log.Warn("budget recovery restarted a shard after its saved cursor was rejected", slog.Int("shard", progress.Shard))
			progress.Token, resumed = "", false
			continue
		}
		resumed = false
		if err != nil {
			log.Error("budget recovery could not read durable request records", slog.Int("shard", progress.Shard), slog.String("reason", budgetRecoveryReason(err)),
				slog.String("action", "restore cloud request storage access and rerun budget-initialize --apply; progress is kept"))
			return err
		}
		for _, saved := range page.Executions {
			restored, err := recovery.Recoverer.RecoverOperation(ctx, receipt, token, budgetRecoveryOperation(saved))
			if err != nil {
				if errors.Is(err, redisstore.ErrBudgetRecoveryConflict) {
					log.Error("budget recovery found a Redis operation that disagrees with its durable record", slog.Int("shard", progress.Shard),
						slog.String("action", "investigate the Redis namespace and cloud records before retrying; never delete either to reopen spending"))
				} else if !errors.Is(err, redisstore.ErrBudgetRecoveryBusy) {
					log.Error("budget recovery could not restore an operation", slog.Int("shard", progress.Shard), slog.String("reason", budgetRecoveryReason(err)))
				}
				return err
			}
			if restored {
				progress.Restored++
			}
		}
		progress.Scanned += page.Scanned
		progress.Token = page.NextPageToken
		if progress.Token == "" {
			progress.Shard++
			log.Info("budget recovery merged a shard", slog.Int("shards_done", progress.Shard), slog.Int("shards", cloudstate.PendingShards),
				slog.Int("scanned", progress.Scanned), slog.Int("restored", progress.Restored))
		}
		encoded, _ := json.Marshal(progress)
		if _, ready, err := recovery.Recoverer.HoldRecoveryLease(ctx, receipt, token, recovery.LeaseTTL, string(encoded)); err != nil || ready {
			if ready {
				return nil
			}
			return err
		}
	}
	if err := recovery.Recoverer.CommitRecovery(ctx, receipt, now()); err != nil {
		log.Error("budget recovery could not publish readiness", slog.String("reason", budgetRecoveryReason(err)))
		return err
	}
	log.Info("budget recovery completed; Redis budget authority is ready", slog.Int("scanned", progress.Scanned),
		slog.Int("restored", progress.Restored), slog.Duration("elapsed", now().Sub(started)))
	return nil
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
