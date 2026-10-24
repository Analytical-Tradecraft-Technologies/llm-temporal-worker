package runtime

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
	redisstore "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/redis"
)

// recoveryTestRedis models the Redis fence, lease, progress and per-operation
// merge contract. Merges are idempotent per operation, like the Function.
type recoveryTestRedis struct {
	mu        sync.Mutex
	identity  budget.InitializationIdentity
	marker    string // "", "ready:<m>" or "recovering:<m>"
	lease     string
	leaseEnd  time.Time
	progress  string
	merged    map[durable.OperationID]int
	conflicts map[durable.OperationID]bool
	holders   map[string]bool
	commits   int
	floors    int
	raise     int
}

func newRecoveryTestRedis(identity budget.InitializationIdentity) *recoveryTestRedis {
	return &recoveryTestRedis{identity: identity, merged: map[durable.OperationID]int{}, conflicts: map[durable.OperationID]bool{}, holders: map[string]bool{}}
}

func (r *recoveryTestRedis) Identity() budget.InitializationIdentity { return r.identity }
func (r *recoveryTestRedis) Check(_ context.Context, value budget.Initialization) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.marker != "ready:"+value.Marker() {
		return redisstore.ErrBudgetAuthorityUnavailable
	}
	return nil
}
func (r *recoveryTestRedis) Initialize(context.Context, budget.InitializationPreparation) error {
	return errors.New("recovery must never initialize")
}
func (r *recoveryTestRedis) BeginRecovery(_ context.Context, value budget.Initialization, force bool) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch r.marker {
	case "recovering:" + value.Marker():
		return true, nil
	case "ready:" + value.Marker():
		if !force {
			return false, nil
		}
	case "":
	default:
		return false, redisstore.ErrBudgetAuthorityUnavailable
	}
	r.marker, r.progress = "recovering:"+value.Marker(), ""
	return true, nil
}
func (r *recoveryTestRedis) fence(value budget.Initialization, token string) (bool, error) {
	if r.marker == "ready:"+value.Marker() {
		return true, nil
	}
	if r.marker != "recovering:"+value.Marker() {
		return false, redisstore.ErrBudgetAuthorityUnavailable
	}
	if r.lease != "" && r.lease != token && time.Now().Before(r.leaseEnd) {
		return false, redisstore.ErrBudgetRecoveryBusy
	}
	return false, nil
}
func (r *recoveryTestRedis) HoldRecoveryLease(_ context.Context, value budget.Initialization, token string, ttl time.Duration, progress string) (string, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ready, err := r.fence(value, token); ready || err != nil {
		return "", ready, err
	}
	r.lease, r.leaseEnd, r.holders[token] = token, time.Now().Add(ttl), true
	if progress != "" {
		r.progress = progress
	}
	return r.progress, false, nil
}
func (r *recoveryTestRedis) ReleaseRecoveryLease(_ context.Context, value budget.Initialization, token string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lease == token {
		r.lease = ""
	}
	return nil
}
func (r *recoveryTestRedis) RecoverOperation(_ context.Context, value budget.Initialization, token string, operation redisstore.BudgetRecoveryOperation) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ready, err := r.fence(value, token); err != nil {
		return false, err
	} else if ready {
		return false, redisstore.ErrBudgetAuthorityUnavailable
	}
	id := operation.Request.OperationID
	if r.conflicts[id] {
		return false, redisstore.ErrBudgetRecoveryConflict
	}
	r.merged[id]++
	return r.merged[id] == 1, nil
}
func (r *recoveryTestRedis) ApplyRecoveryFloors(_ context.Context, value budget.Initialization, token string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ready, err := r.fence(value, token); err != nil {
		return 0, err
	} else if ready {
		return 0, redisstore.ErrBudgetAuthorityUnavailable
	}
	r.floors++
	return r.raise, nil
}
func (r *recoveryTestRedis) CommitRecovery(_ context.Context, value budget.Initialization, _ time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.marker == "ready:"+value.Marker() {
		return nil
	}
	if r.marker != "recovering:"+value.Marker() || r.floors == 0 {
		return redisstore.ErrBudgetAuthorityUnavailable
	}
	r.marker, r.progress = "ready:"+value.Marker(), ""
	r.commits++
	return nil
}

// recoveryTestSource serves one paid operation per shard, two pages per shard.
type recoveryTestSource struct {
	mu       sync.Mutex
	calls    []int
	failOnce map[int]bool
}

func (s *recoveryTestSource) ListBudgetRecovery(_ context.Context, shard, _ int, token string, _ time.Time) (cloudstate.BudgetRecoveryPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, shard)
	if s.failOnce[shard] {
		delete(s.failOnce, shard)
		return cloudstate.BudgetRecoveryPage{}, cloudstate.ErrCorrupt
	}
	if token == "" {
		return cloudstate.BudgetRecoveryPage{Scanned: 3, NextPageToken: "second"}, nil
	}
	saved := cloudstate.SavedProviderExecution{
		Plan:      cloudstate.BudgetPlan{Mode: cloudstate.BudgetReserved, Reservation: durable.ReserveRequest{OperationID: durable.OperationID("secret-operation-" + string(rune('a'+shard)))}},
		Execution: cloudstate.ProviderExecution{Stage: cloudstate.ExecutionSubmitting, StartedAt: time.Now(), RecoverAfter: time.Now().Add(durable.BudgetStartLease)},
	}
	return cloudstate.BudgetRecoveryPage{Executions: []cloudstate.SavedProviderExecution{saved}, Scanned: 2}, nil
}

func recoveryTestReceipt(t *testing.T) (budget.Initialization, *initializationTestStore) {
	t.Helper()
	identity := testBudgetIdentity(t, trustedTemporalTestConfig(t))
	receipt := testInitialization(identity, time.Now())
	receipt.Ready = true
	return receipt, &initializationTestStore{value: receipt}
}

func TestBudgetRecoveryRestoresCompletelyLostAuthority(t *testing.T) {
	receipt, store := recoveryTestReceipt(t)
	redis := newRecoveryTestRedis(receipt.Identity)
	redis.raise = 2
	source := &recoveryTestSource{}
	var logs bytes.Buffer
	recovery := budgetRecovery{Recoverer: redis, Source: source, Logger: slog.New(slog.NewJSONHandler(&logs, nil))}
	ctx := context.Background()
	report, err := establishBudgetAuthority(ctx, store, redis, recovery, false, time.Now)
	if err != nil || report.Status != "recovery_required" || redis.marker != "" || len(source.calls) != 0 {
		t.Fatalf("read-only = %#v, %v; marker %q", report, err, redis.marker)
	}
	report, err = establishBudgetAuthority(ctx, store, redis, recovery, true, time.Now)
	if err != nil || report != (budgetInitializationReport{Status: "ready", Applied: true, Recovered: true}) {
		t.Fatalf("recovery = %#v, %v", report, err)
	}
	if len(redis.merged) != cloudstate.PendingShards || redis.commits != 1 || store.value != receipt {
		t.Fatalf("merged %d operations, %d commits, receipt %#v", len(redis.merged), redis.commits, store.value)
	}
	if strings.Contains(logs.String(), "secret-operation") || !strings.Contains(logs.String(), "budget recovery completed") ||
		!strings.Contains(logs.String(), `"raised_buckets":2`) {
		t.Fatalf("recovery logs = %s", logs.String())
	}
	// A later startup finds a ready marker and does no work.
	report, err = establishBudgetAuthority(ctx, store, redis, recovery, true, time.Now)
	if err != nil || report.Status != "ready" || report.Recovered || redis.commits != 1 {
		t.Fatalf("replay = %#v, %v", report, err)
	}
}

func TestBudgetRecoveryResumesAfterInterruption(t *testing.T) {
	receipt, store := recoveryTestReceipt(t)
	redis := newRecoveryTestRedis(receipt.Identity)
	source := &recoveryTestSource{failOnce: map[int]bool{3: true}}
	var logs bytes.Buffer
	recovery := budgetRecovery{Recoverer: redis, Source: source, Logger: slog.New(slog.NewJSONHandler(&logs, nil))}
	ctx := context.Background()
	if _, err := establishBudgetAuthority(ctx, store, redis, recovery, true, time.Now); !errors.Is(err, cloudstate.ErrCorrupt) {
		t.Fatalf("interrupted recovery = %v", err)
	}
	if !strings.HasPrefix(redis.marker, "recovering:") || redis.lease != "" {
		t.Fatalf("interruption published readiness or kept the lease: %q %q", redis.marker, redis.lease)
	}
	// The other shards run alongside the failed one and keep their progress.
	progress, ok := decodeBudgetRecoveryProgress(redis.progress)
	if !ok || progress.Restored != cloudstate.PendingShards-1 || progress.Shards[3].Done || progress.shardsDone() != cloudstate.PendingShards-1 {
		t.Fatalf("progress = %q", redis.progress)
	}
	if !strings.Contains(logs.String(), "durable_record_invalid") || !strings.Contains(logs.String(), "progress is kept") {
		t.Fatalf("failure log is not actionable: %s", logs.String())
	}
	source.calls = nil
	report, err := establishBudgetAuthority(ctx, store, redis, recovery, true, time.Now)
	if err != nil || !report.Recovered {
		t.Fatalf("resume = %#v, %v", report, err)
	}
	for _, shard := range source.calls {
		if shard != 3 {
			t.Fatalf("resume reread finished shard %d", shard)
		}
	}
	for id, count := range redis.merged {
		if count != 1 {
			t.Fatalf("%s merged %d times", id, count)
		}
	}
	if len(redis.merged) != cloudstate.PendingShards {
		t.Fatalf("resume missed operations: %d", len(redis.merged))
	}
}

func TestBudgetRecoveryConcurrentStartupsWaitForOneRecoverer(t *testing.T) {
	receipt, store := recoveryTestReceipt(t)
	redis := newRecoveryTestRedis(receipt.Identity)
	source := &recoveryTestSource{}
	recovery := budgetRecovery{Recoverer: redis, Source: source, Wait: 5 * time.Millisecond, MinimumWork: time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 6)
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			report, err := establishBudgetAuthority(ctx, store, redis, recovery, true, time.Now)
			if err == nil && report.Status != "ready" {
				err = errors.New(report.Status)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if redis.commits != 1 || len(redis.holders) != 1 {
		t.Fatalf("%d commits by %d lease holders", redis.commits, len(redis.holders))
	}
	for id, count := range redis.merged {
		if count != 1 {
			t.Fatalf("%s merged %d times", id, count)
		}
	}
}

func TestBudgetRecoveryForcedAndConflictingMarkers(t *testing.T) {
	receipt, store := recoveryTestReceipt(t)
	redis := newRecoveryTestRedis(receipt.Identity)
	redis.marker = "ready:" + receipt.Marker()
	source := &recoveryTestSource{}
	ctx := context.Background()
	report, err := establishBudgetAuthority(ctx, store, redis, budgetRecovery{Recoverer: redis, Source: source}, true, time.Now)
	if err != nil || report.Recovered || len(source.calls) != 0 {
		t.Fatalf("unforced ready marker = %#v, %v", report, err)
	}
	report, err = establishBudgetAuthority(ctx, store, redis, budgetRecovery{Recoverer: redis, Source: source, Force: true}, true, time.Now)
	if err != nil || !report.Recovered || len(redis.merged) != cloudstate.PendingShards {
		t.Fatalf("forced recovery = %#v, %v", report, err)
	}

	redis = newRecoveryTestRedis(receipt.Identity)
	redis.conflicts["secret-operation-a"] = true
	var logs bytes.Buffer
	_, err = establishBudgetAuthority(ctx, store, redis, budgetRecovery{Recoverer: redis, Source: source, Logger: slog.New(slog.NewJSONHandler(&logs, nil))}, true, time.Now)
	if !errors.Is(err, redisstore.ErrBudgetRecoveryConflict) || !strings.HasPrefix(redis.marker, "recovering:") {
		t.Fatalf("conflict = %v, marker %q", err, redis.marker)
	}
	if !strings.Contains(logs.String(), "never delete either to reopen spending") || strings.Contains(logs.String(), "secret-operation") {
		t.Fatalf("conflict log = %s", logs.String())
	}

	redis = newRecoveryTestRedis(receipt.Identity)
	redis.marker = "preparing:" + receipt.Marker()
	if _, err := establishBudgetAuthority(ctx, store, redis, budgetRecovery{Recoverer: redis, Source: source}, true, time.Now); !errors.Is(err, redisstore.ErrBudgetAuthorityUnavailable) {
		t.Fatalf("preparing marker = %v", err)
	}
}

func TestBudgetRecoveryOperationNeverAssumesAnUnusedClaim(t *testing.T) {
	plan := cloudstate.BudgetPlan{Mode: cloudstate.BudgetReserved}
	started := time.Now()
	for _, stage := range []cloudstate.ExecutionStage{cloudstate.ExecutionClaiming, cloudstate.ExecutionUnknown, cloudstate.ExecutionSubmitting} {
		operation := budgetRecoveryOperation(cloudstate.SavedProviderExecution{Plan: plan, Execution: cloudstate.ProviderExecution{Stage: stage, StartedAt: started, RecoverAfter: started.Add(durable.BudgetStartLease)}})
		if !operation.Claimed || operation.StartBy.Before(started.Add(durable.BudgetStartLease)) {
			t.Fatalf("%s restored as unclaimed or with a shorter lease: %#v", stage, operation)
		}
	}
}

func TestBudgetRecoveryHorizonCoversLongestWindow(t *testing.T) {
	value := trustedTemporalTestConfig(t)
	if got := budgetRecoveryHorizon(value); got < minimumBudgetRecoveryHorizon {
		t.Fatalf("horizon %v below minimum", got)
	}
}

func TestBudgetRecoveryResumesTheSequentialProgressFormat(t *testing.T) {
	// Written by the first release, which merged one shard at a time.
	progress, ok := decodeBudgetRecoveryProgress(`{"shard":2,"token":"page-7","scanned":14100,"restored":5712}`)
	if !ok || progress.Scanned != 14100 || progress.Restored != 5712 || !progress.Shards[0].Done || !progress.Shards[1].Done ||
		progress.Shards[2] != (budgetRecoveryShard{Token: "page-7"}) || progress.Shards[3] != (budgetRecoveryShard{}) {
		t.Fatalf("sequential progress = %#v, %t", progress, ok)
	}
	if progress, ok := decodeBudgetRecoveryProgress(`{"shards":[{"done":true}]}`); ok || len(progress.Shards) != cloudstate.PendingShards {
		t.Fatalf("malformed progress accepted: %#v", progress)
	}
}

func TestBudgetRecoveryLateTakeoverNeedsTimeToWork(t *testing.T) {
	receipt, store := recoveryTestReceipt(t)
	redis := newRecoveryTestRedis(receipt.Identity)
	// Another process holds the lease until just before this one's deadline.
	redis.marker, redis.lease, redis.leaseEnd = "recovering:"+receipt.Marker(), "other", time.Now().Add(300*time.Millisecond)
	source := &recoveryTestSource{}
	var logs bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := establishBudgetAuthority(ctx, store, redis, budgetRecovery{Recoverer: redis, Source: source, Wait: 50 * time.Millisecond, Logger: slog.New(slog.NewJSONHandler(&logs, nil))}, true, time.Now)
	if !errors.Is(err, errBudgetRecoveryInProgress) || len(source.calls) != 0 || redis.lease != "" {
		t.Fatalf("late takeover = %v, %d reads, lease %q", err, len(source.calls), redis.lease)
	}
	if !strings.Contains(logs.String(), "next start resumes it") || strings.Contains(logs.String(), "ERROR") {
		t.Fatalf("late takeover log = %s", logs.String())
	}
}

func TestBudgetRecoveryRunsWithinTheDefaultCommandTimeout(t *testing.T) {
	receipt, store := recoveryTestReceipt(t)
	redis := newRecoveryTestRedis(receipt.Identity)
	// The CLI default is 30 seconds: shorter than MinimumWork, which must
	// not stop a process that got the lease without waiting.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	report, err := establishBudgetAuthority(ctx, store, redis, budgetRecovery{Recoverer: redis, Source: &recoveryTestSource{}}, true, time.Now)
	if err != nil || !report.Recovered {
		t.Fatalf("default-timeout recovery = %#v, %v", report, err)
	}
}

// orderedProgressRedis records the order progress snapshots reach Redis.
type orderedProgressRedis struct {
	*recoveryTestRedis
	saved []budgetRecoveryProgress
}

func (r *orderedProgressRedis) HoldRecoveryLease(ctx context.Context, value budget.Initialization, token string, ttl time.Duration, progress string) (string, bool, error) {
	if progress != "" {
		decoded, _ := decodeBudgetRecoveryProgress(progress)
		r.mu.Lock()
		r.saved = append(r.saved, decoded)
		r.mu.Unlock()
		time.Sleep(time.Millisecond) // widen any window between snapshot and save
	}
	return r.recoveryTestRedis.HoldRecoveryLease(ctx, value, token, ttl, progress)
}

func TestBudgetRecoveryCheckpointsNeverRewind(t *testing.T) {
	receipt, store := recoveryTestReceipt(t)
	redis := &orderedProgressRedis{recoveryTestRedis: newRecoveryTestRedis(receipt.Identity)}
	if _, err := establishBudgetAuthority(context.Background(), store, redis, budgetRecovery{Recoverer: redis, Source: &recoveryTestSource{}}, true, time.Now); err != nil {
		t.Fatal(err)
	}
	for index := 1; index < len(redis.saved); index++ {
		if redis.saved[index].Scanned < redis.saved[index-1].Scanned || redis.saved[index].shardsDone() < redis.saved[index-1].shardsDone() {
			t.Fatalf("checkpoint %d rewound progress: %#v after %#v", index, redis.saved[index], redis.saved[index-1])
		}
	}
	if len(redis.saved) != 2*cloudstate.PendingShards {
		t.Fatalf("saved %d checkpoints", len(redis.saved))
	}
}

func TestBudgetRecoveryTimeoutIsReportedAsProgressNotStorageFailure(t *testing.T) {
	receipt, store := recoveryTestReceipt(t)
	redis := newRecoveryTestRedis(receipt.Identity)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	source := &blockingRecoverySource{}
	var logs bytes.Buffer
	_, err := establishBudgetAuthority(ctx, store, redis, budgetRecovery{Recoverer: redis, Source: source, MinimumWork: time.Millisecond, Logger: slog.New(slog.NewJSONHandler(&logs, nil))}, true, time.Now)
	if !errors.Is(err, errBudgetRecoveryInProgress) || !strings.HasPrefix(redis.marker, "recovering:") {
		t.Fatalf("timed-out recovery = %v, marker %q", err, redis.marker)
	}
	if strings.Contains(logs.String(), "restore cloud request storage access") || !strings.Contains(logs.String(), "resumes from the saved cursor") {
		t.Fatalf("timeout log = %s", logs.String())
	}
}

// blockingRecoverySource never returns a page before its context ends.
type blockingRecoverySource struct{}

func (blockingRecoverySource) ListBudgetRecovery(ctx context.Context, _, _ int, _ string, _ time.Time) (cloudstate.BudgetRecoveryPage, error) {
	<-ctx.Done()
	return cloudstate.BudgetRecoveryPage{}, ctx.Err()
}
