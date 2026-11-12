package runtime

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
)

// Storage calls for one Prepare step, counted by the test doubles: KV gets,
// queries and writes, and blob opens and creates. Pinned so a regression
// shows up here, not only in a latency benchmark (#1782).
const (
	// A warm exact hit made 139 before #1782.
	cacheHitPrepareStorageBound = 75
	// A cached miss's first step is unchanged by #1782.
	cacheMissPrepareStorageBound = 70
)

func storageCalls(c executionStorageCounts) uint64 {
	return c.gets + c.queries + c.writes + c.opens + c.creates
}

// A warm hit is answered before the request has an attempt (#1782): it
// leaves no attempt, budget plan or fill behind, reports a zero-cost hit,
// replays its operation unchanged and stays within its storage budget.
func TestCloudExecutionRuntimeCacheHitWritesNoAttempt(t *testing.T) {
	f := boundedCloud(t, false)
	f.request.Cache = &llm.CachePolicyV1{}
	origin := f.finish(t)
	if origin.Generate.Cache.Disposition != "miss_populated" || f.submits.Load() != 1 {
		t.Fatalf("origin %q after %d submits", origin.Generate.Cache.Disposition, f.submits.Load())
	}
	ctx := context.Background()
	prepare := func(key string) (llm.ExecutionResultV1, uint64) {
		t.Helper()
		f.now = f.now.Add(time.Second)
		f.restart(t)
		f.request.OperationKey = key
		before := readExecutionStorageCounts(f.table, f.blobs)
		v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
		if err != nil {
			t.Fatal(err)
		}
		return v, storageCalls(readExecutionStorageCounts(f.table, f.blobs).minus(before))
	}
	hit, calls := prepare("warm-hit")
	if hit.State != llm.ExecutionCompleted || hit.Generate == nil || hit.Generate.Cache.Disposition != "hit" || f.submits.Load() != 1 {
		t.Fatalf("state %s result %+v after %d submits", hit.State, hit.Generate, f.submits.Load())
	}
	if cost := hit.Generate.Cost; cost.ActualCostUSD == nil || *cost.ActualCostUSD != "0" || cost.Status != "exact" {
		t.Fatalf("hit cost %+v", cost)
	}
	if calls > cacheHitPrepareStorageBound {
		t.Fatalf("a warm hit made %d storage calls, budget %d", calls, cacheHitPrepareStorageBound)
	}
	t.Logf("warm hit: %d storage calls", calls)
	scope, id := cloudstate.Scope{Tenant: "tenant", Project: "project"}, cloudstate.RequestID(hit.RequestID)
	// Attempts are child records, and each holds its budget plan and fill.
	if _, last, err := f.repository.LangfusePaidAttempts(ctx, scope, id); err != nil || last != "" {
		t.Fatalf("hit created attempt %q: %v", last, err)
	}
	record, err := f.repository.Read(ctx, scope, id)
	if err != nil || record.Status != cloudstate.StatusCompleted {
		t.Fatalf("hit record %s: %v", record.Status, err)
	}
	// Replaying the same operation returns the stored result unchanged.
	f.now = f.now.Add(time.Second)
	f.restart(t)
	replayed, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	if err != nil || !reflect.DeepEqual(replayed.Generate, hit.Generate) {
		t.Fatalf("replay changed the hit: %+v %v", replayed.Generate, err)
	}
	// Another variant misses. Its first step is no more expensive than
	// before, and still owns the fill under its own attempt.
	f.request.Cache = &llm.CachePolicyV1{Variant: 1}
	miss, calls := prepare("cold-miss")
	if miss.State != llm.ExecutionBudgetRequired {
		t.Fatalf("miss state %s", miss.State)
	}
	if calls > cacheMissPrepareStorageBound {
		t.Fatalf("a cached miss made %d storage calls, budget %d", calls, cacheMissPrepareStorageBound)
	}
	t.Logf("cached miss: %d storage calls", calls)
	if _, last, err := f.repository.LangfusePaidAttempts(ctx, scope, cloudstate.RequestID(miss.RequestID)); err != nil || last == "" {
		t.Fatalf("miss has no attempt: %v", err)
	}
	result := f.finish(t)
	if result.Generate.Cache.Disposition != "miss_populated" || f.submits.Load() != 2 {
		t.Fatalf("miss %q after %d submits", result.Generate.Cache.Disposition, f.submits.Load())
	}
}

// An expired entry is not answered before the attempt: the request pays for
// its own call and repopulates the cache.
func TestCloudExecutionRuntimeExpiredCacheEntryIsNotAnsweredEarly(t *testing.T) {
	f := boundedCloud(t, false)
	f.request.Cache = &llm.CachePolicyV1{MaxAgeSeconds: 60}
	if origin := f.finish(t); origin.Generate.Cache.Disposition != "miss_populated" {
		t.Fatal(origin.Generate.Cache)
	}
	f.now = f.now.Add(2 * time.Minute)
	f.restart(t)
	f.request.OperationKey = "after-expiry"
	again := f.finish(t)
	if again.Generate.Cache.Disposition != "miss_populated" || f.submits.Load() != 2 {
		t.Fatalf("expired entry answered: %q after %d submits", again.Generate.Cache.Disposition, f.submits.Load())
	}
}

// The resolve Activity's cache-only step answers a hit, and on a miss leaves
// the request without an attempt so that its Prepare step starts one (#1782).
func TestCloudExecutionRuntimeResolveCachedStartsNoAttempt(t *testing.T) {
	f := boundedCloud(t, false)
	f.request.Cache = &llm.CachePolicyV1{}
	f.finish(t)
	ctx := context.Background()
	scope := cloudstate.Scope{Tenant: "tenant", Project: "project"}
	resolve := func(key string, variant int32) (llm.ExecutionResultV1, bool, uint64) {
		t.Helper()
		f.now = f.now.Add(time.Second)
		f.restart(t)
		f.request.OperationKey, f.request.Cache = key, &llm.CachePolicyV1{Variant: variant}
		before := readExecutionStorageCounts(f.table, f.blobs)
		result, found, err := f.runtime.ResolveCachedV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
		if err != nil {
			t.Fatal(err)
		}
		return result, found, storageCalls(readExecutionStorageCounts(f.table, f.blobs).minus(before))
	}
	hit, found, _ := resolve("resolved-hit", 0)
	if !found || hit.State != llm.ExecutionCompleted || hit.Generate.Cache.Disposition != "hit" || hit.Capability == "" {
		t.Fatalf("hit not resolved: %v %+v", found, hit)
	}
	// Resolving the completed operation again returns the stored result.
	f.restart(t)
	again, found, err := f.runtime.ResolveCachedV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	if err != nil || !found || !reflect.DeepEqual(again.Generate, hit.Generate) {
		t.Fatalf("replay: %v %v", found, err)
	}
	miss, found, calls := resolve("resolved-miss", 1)
	if found || miss.State != "" {
		t.Fatalf("miss resolved: %+v", miss)
	}
	t.Logf("cache-only step for a miss: %d storage calls", calls)
	prepared, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	if err != nil || prepared.State != llm.ExecutionBudgetRequired {
		t.Fatalf("prepare after a miss: %s %v", prepared.State, err)
	}
	if _, last, err := f.repository.LangfusePaidAttempts(ctx, scope, cloudstate.RequestID(prepared.RequestID)); err != nil || last == "" {
		t.Fatalf("prepare did not start an attempt: %q %v", last, err)
	}
	if result := f.finish(t); result.Generate.Cache.Disposition != "miss_populated" || f.submits.Load() != 2 {
		t.Fatalf("miss %q after %d submits", result.Generate.Cache.Disposition, f.submits.Load())
	}
}
