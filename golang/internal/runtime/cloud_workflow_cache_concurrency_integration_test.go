//go:build cloudworkflowintegration

package runtime

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/workflows"
	"go.temporal.io/sdk/client"
)

func TestCloudWorkflowLiveCacheConcurrency(t *testing.T) {
	for _, compact := range []bool{false, true} {
		t.Run(fmt.Sprintf("compact-%t", compact), func(t *testing.T) { exerciseCloudCacheConcurrency(t, compact, false) })
	}
}

func TestCloudWorkflowAWSCacheConcurrency(t *testing.T) {
	if os.Getenv("LLMTW_CLOUD_TEST_AWS") != "1" {
		t.Skip("AWS resources are explicitly opt-in")
	}
	for _, compact := range []bool{false, true} {
		t.Run(fmt.Sprintf("compact-%t", compact), func(t *testing.T) { exerciseCloudCacheConcurrency(t, compact, true) })
	}
}

type cloudCacheWorkflowResult struct {
	key, operation string
	checkpoint     llm.CheckpointMetadata
	cache          llm.CacheDispositionV1
	cost           llm.CostV1
	diagnostics    []llm.Diagnostic
}

// The owner and waiters poll separate queues but share the same cloud stores
// and Redis authority. This forces both reconstructed workers to participate;
// merely starting two workers on one queue would not establish that fact.
func exerciseCloudCacheConcurrency(t *testing.T, compact, aws bool) {
	t.Helper()
	const callers = 100
	h := newLiveCloudWorkflow(t, true, aws)
	request := h.fixture.request
	var parent llm.CheckpointMetadata
	paid := 1
	if compact {
		// Create a real public-workflow source before the concurrent compactions.
		policy := json.RawMessage(`{"recent_turns":0}`)
		request.SettingsPatch.CompactionPolicy.Set = &policy
		h.complete.Store(true)
		stop := h.startWorker(t)
		parent = h.generate(t, request).Checkpoint
		stop()
		// No worker is running while these observation fields are reset.
		h.pending, h.observed = make(chan struct{}), sync.Once{}
		h.complete.Store(false)
		paid++
	}
	name := workflows.GenerateWorkflowName
	input := func(key string) any {
		v := request
		v.OperationKey, v.Cache = key, &llm.CachePolicyV1{}
		return v
	}
	read := func(run client.WorkflowRun) cloudCacheWorkflowResult {
		var v llm.GenerateResponseV1
		if err := run.Get(h.ctx, &v); err != nil {
			t.Fatal(err)
		}
		return cloudCacheWorkflowResult{v.OperationKey, v.OperationID, v.Checkpoint, v.Cache, v.Cost, v.Diagnostics}
	}
	if compact {
		name = workflows.CompactWorkflowName
		input = func(key string) any {
			return llm.CompactRequestV1{OperationKey: key, Context: request.Context, Parent: parent.Handle, Cache: &llm.CachePolicyV1{}}
		}
		read = func(run client.WorkflowRun) cloudCacheWorkflowResult {
			var v llm.CompactResponseV1
			if err := run.Get(h.ctx, &v); err != nil {
				t.Fatal(err)
			}
			return cloudCacheWorkflowResult{v.OperationKey, v.OperationID, v.Checkpoint, v.Cache, v.Cost, v.Diagnostics}
		}
	}
	var mu sync.Mutex
	waiters := make(map[string]struct{}, callers-1)
	allWaiting := make(chan struct{})
	h.cacheWaitObserver = func(id string) {
		mu.Lock()
		defer mu.Unlock()
		if _, exists := waiters[id]; exists {
			return
		}
		waiters[id] = struct{}{}
		if len(waiters) == callers-1 {
			close(allWaiting)
		}
	}
	h.startWorker(t)
	owner := h.start(t, name, input("cache-owner"))
	select {
	case <-h.pending:
	case <-h.ctx.Done():
		t.Fatal("owner did not reach durable pending state")
	}
	otherQueue := h.queue + "-waiters"
	h.startWorkerOnQueue(t, otherQueue)
	runs := make([]client.WorkflowRun, callers-1)
	for i := range runs {
		runs[i] = h.startOnQueue(t, otherQueue, name, input(fmt.Sprintf("cache-waiter-%d", i)))
	}
	select {
	case <-allWaiting:
	case <-h.ctx.Done():
		mu.Lock()
		count := len(waiters)
		mu.Unlock()
		t.Fatalf("only %d distinct requests reached cache wait", count)
	}
	if got := int(h.fixture.submits.Load()); got != paid {
		t.Fatalf("provider submissions before owner release = %d, want %d", got, paid)
	}
	h.complete.Store(true)
	original := read(owner)
	if original.key != "cache-owner" || original.operation == "" || original.checkpoint.Handle == "" || original.cache.Disposition != "miss_populated" || original.cost.Status != "exact" || original.cost.ActualCostUSD == nil || *original.cost.ActualCostUSD == "0" {
		t.Fatal("invalid paid owner response")
	}
	operations := map[string]bool{original.operation: true}
	checkpoints := map[string]bool{string(original.checkpoint.Handle): true}
	scope, err := h.fixture.options.ResolveScope(h.ctx, request.Context)
	if err != nil {
		t.Fatal(err)
	}
	var entryID string
	for i, run := range runs {
		v := read(run)
		key := fmt.Sprintf("cache-waiter-%d", i)
		if v.key != key || v.operation == "" || operations[v.operation] || v.checkpoint.Handle == "" || checkpoints[string(v.checkpoint.Handle)] || v.cache.Disposition != "hit" || v.cost.Status != "exact" || v.cost.ActualCostUSD == nil || *v.cost.ActualCostUSD != "0" {
			t.Fatalf("waiter %d lost its distinct zero-cost cache result", i)
		}
		operations[v.operation], checkpoints[string(v.checkpoint.Handle)] = true, true
		assertOriginCost(t, v.diagnostics, original.operation, *original.cost.ActualCostUSD)
		use, err := h.fixture.cap.Responses.ReadUse(h.ctx, scope, state.OperationID(v.operation))
		if err != nil || use.CheckpointID == "" || use.EntryID == "" || use.OperationID != state.OperationID(v.operation) {
			t.Fatalf("waiter %d has no valid consuming receipt: %v", i, err)
		}
		if entryID == "" {
			entryID = string(use.EntryID)
		} else if string(use.EntryID) != entryID {
			t.Fatalf("waiter %d consumed a different cache entry", i)
		}
		// Replay on the other worker must preserve publication/use identity.
		replayed := read(h.start(t, name, input(key)))
		if replayed.operation != v.operation || replayed.checkpoint.Handle != v.checkpoint.Handle || replayed.cost.ActualCostUSD == nil || *replayed.cost.ActualCostUSD != "0" {
			t.Fatalf("waiter %d replay changed identity or cost", i)
		}
		assertOriginCost(t, replayed.diagnostics, original.operation, *original.cost.ActualCostUSD)
		replayedUse, err := h.fixture.cap.Responses.ReadUse(h.ctx, scope, state.OperationID(v.operation))
		if err != nil || replayedUse != use {
			t.Fatalf("waiter %d replay changed its consuming receipt: %v", i, err)
		}
	}
	replayedOwner := read(h.startOnQueue(t, otherQueue, name, input("cache-owner")))
	if replayedOwner.operation != original.operation || replayedOwner.checkpoint.Handle != original.checkpoint.Handle || replayedOwner.cost.ActualCostUSD == nil || *replayedOwner.cost.ActualCostUSD != *original.cost.ActualCostUSD {
		t.Fatal("owner replay changed its original publication or paid receipt")
	}
	if got := int(h.fixture.submits.Load()); got != paid {
		t.Fatalf("provider submissions after all replays = %d, want %d", got, paid)
	}
	h.assertSettled(t, paid)
	for shard := range cloudstate.PendingShards {
		page, err := h.fixture.repository.ListPending(h.ctx, shard, callers+1, "")
		if err != nil || len(page.Requests) != 0 || page.NextPageToken != "" {
			t.Fatalf("cache completion left pending work in shard %d: %v", shard, err)
		}
	}
}
