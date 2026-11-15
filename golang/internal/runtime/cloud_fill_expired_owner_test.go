package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// A waiter created after the owner must not fail permanently when the
// abandoned owner's held lease expires while the waiter's own attempt is
// still live. It takes the fill over on its next step instead.
func TestCloudCacheWaiterTakesOverExpiredOwnerWithinOwnLease(t *testing.T) {
	f := boundedCloud(t, false)
	f.request.Cache = &llm.CachePolicyV1{}
	ctx := context.Background()
	v := f.prepareGenerate(t, ctx)
	waiter := f.request
	waiter.OperationKey = "waiter"
	f.now = f.now.Add(time.Minute)
	f.step(t, ctx, waiter, llm.ExecutionCacheWait)
	// The owner is abandoned: its lease expires while the waiter's attempt
	// still has time left on its own lease.
	f.now = f.now.Add(14*time.Minute + 30*time.Second)
	v = f.step(t, ctx, waiter, llm.ExecutionProviderCompleted)
	v = f.complete(t, ctx, executionRef(v, waiter.Context), llm.ExecutionCompleted)
	f.requireCacheOutcome(t, "waiter after expired owner", v, "miss_populated", 1)
}
