package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

// A waiter created after the owner must not fail permanently when the
// abandoned owner's held lease expires while the waiter's own attempt is
// still live. It takes the fill over on its next step instead.
func TestCloudCacheWaiterTakesOverExpiredOwnerWithinOwnLease(t *testing.T) {
	f := boundedCloud(t, false)
	f.request.Cache = &llm.CachePolicyV1{}
	ctx := context.Background()
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	waiter := f.request
	waiter.OperationKey = "waiter"
	f.now = f.now.Add(time.Minute)
	v, err = f.runtime.GenerateStepV1(ctx, waiter)
	boundedState(t, v, err, llm.ExecutionCacheWait)
	// The owner is abandoned: its lease expires while the waiter's attempt
	// still has time left on its own lease.
	f.now = f.now.Add(14*time.Minute + 30*time.Second)
	v, err = f.runtime.GenerateStepV1(ctx, waiter)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	v, err = f.runtime.CompleteExecutionV1(ctx, llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: waiter.Context})
	boundedState(t, v, err, llm.ExecutionCompleted)
	if v.Generate.Cache.Disposition != "miss_populated" || f.submits.Load() != 1 {
		t.Fatalf("waiter after expired owner: disposition=%s submits=%d", v.Generate.Cache.Disposition, f.submits.Load())
	}
}
