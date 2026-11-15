package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// A waiter's attempt predates the owner's completion. When the owner ends
// without publishing, the waiter must own the next fill at once instead of
// conflicting until its own attempt is renewed.
func TestCloudCacheWaiterOwnsFillAfterOwnerEndsUnpublished(t *testing.T) {
	f := boundedCloud(t, false)
	f.request.Cache = &llm.CachePolicyV1{}
	ctx := context.Background()
	v := f.prepareGenerate(t, ctx)
	waiter := f.request
	waiter.OperationKey = "waiter"
	f.now = f.now.Add(time.Second)
	f.step(t, ctx, waiter, llm.ExecutionCacheWait)
	original := f.adapter.invoke
	f.adapter.invoke = invokeFailing(&f.submits, deniedRejection())
	f.now = f.now.Add(time.Second)
	f.step(t, ctx, f.request, llm.ExecutionFailed)
	f.adapter.invoke = original
	f.now = f.now.Add(31 * time.Second)
	v = f.step(t, ctx, waiter, llm.ExecutionProviderCompleted)
	v = f.complete(t, ctx, executionRef(v, waiter.Context), llm.ExecutionCompleted)
	f.requireCacheOutcome(t, "waiter after failed owner", v, "miss_populated", 2)
}

// An owner workflow that ends abnormally after starting its fill leaves the
// fill started for good (#1455). An identical request waits on it until the
// fill is cache.StalledFillAfter old, then pays for its own uncached call. It
// never takes the fill over, so the owner's call is not dispatched again and
// the owner, if it ever resumes, still finishes and publishes its own fill.
func TestCloudCacheWaiterBypassesStalledStartedFill(t *testing.T) {
	f := boundedCloud(t, true)
	f.request.Cache = &llm.CachePolicyV1{}
	ctx, v := f.generateStep(t, llm.ExecutionPending)
	owner := f.ref(v)
	// The owner's workflow is terminated here: nothing polls it again.
	waiter := f.request
	waiter.OperationKey = "waiter"
	f.now = f.now.Add(cache.StalledFillAfter - time.Second)
	f.step(t, ctx, waiter, llm.ExecutionCacheWait)
	f.now = f.now.Add(time.Second)
	v = f.step(t, ctx, waiter, llm.ExecutionPending)
	ref := executionRef(v, waiter.Context)
	f.now = f.now.Add(2 * time.Second)
	v = f.poll(t, ctx, ref, llm.ExecutionProviderCompleted)
	ref.Capability = v.Capability
	v = f.complete(t, ctx, ref, llm.ExecutionCompleted)
	if v.Generate.Cache.Disposition != "miss_not_populated" || f.submits.Load() != 2 || f.polls.Load() != 1 {
		t.Fatalf("waiter on a stalled fill: disposition=%s submits=%d polls=%d", v.Generate.Cache.Disposition, f.submits.Load(), f.polls.Load())
	}
	// The owner's started fill is untouched: resuming it finishes the owner's
	// own dispatch and publishes its response, with no second submission.
	v = f.poll(t, ctx, owner, llm.ExecutionProviderCompleted)
	owner.Capability = v.Capability
	v = f.complete(t, ctx, owner, llm.ExecutionCompleted)
	f.requireCacheOutcome(t, "resumed owner", v, "miss_populated", 2)
	late := f.request
	late.OperationKey = "late"
	v = f.step(t, ctx, late, llm.ExecutionCompleted)
	f.requireCacheOutcome(t, "request after the owner published", v, "hit", 2)
}

type unstartedFills struct {
	cache.FillRepository
	failed bool
}

func (s *unstartedFills) Start(ctx context.Context, lease cache.FillLease, now time.Time) (bool, error) {
	if !s.failed {
		s.failed = true
		return false, errors.New("storage unavailable before start")
	}
	return s.FillRepository.Start(ctx, lease, now)
}

// An unknown attempt whose held fill expired, was taken over and was finished
// by another request has nothing left to finish. It must start a replacement
// attempt, which consumes the other request's published response. Its claim
// was never made, so recovery closes it as a definite non-dispatch (#1312).
func TestCloudUnknownAttemptContinuesAfterFillTakeover(t *testing.T) {
	for _, poll := range []bool{true, false} {
		t.Run(map[bool]string{true: "poll", false: "acquire"}[poll], func(t *testing.T) {
			f := boundedCloud(t, false)
			f.request.Cache = &llm.CachePolicyV1{}
			f.cap.ResponseFills = &unstartedFills{FillRepository: f.cap.ResponseFills}
			f.restart(t)
			ctx, v := f.generateStep(t, llm.ExecutionPending)
			ref := f.ref(v)
			requireCount(t, &f.submits, 0, "HTTP without a started fill")
			f.now = f.now.Add(16 * time.Minute)
			second := f.request
			second.OperationKey = "second"
			v = f.step(t, ctx, second, llm.ExecutionProviderCompleted)
			v = f.complete(t, ctx, executionRef(v, second.Context), llm.ExecutionCompleted)
			if poll {
				v = f.poll(t, ctx, ref, llm.ExecutionFailed)
				if !v.Retryable || v.Dispatch != string(provider.DispatchNotDispatched) {
					t.Fatalf("unclaimed attempt after takeover = %+v", v)
				}
			}
			v = f.acquire(t, ctx, ref, llm.ExecutionCompleted)
			f.requireCacheOutcome(t, "unknown attempt after takeover", v, "hit", 1)
		})
	}
}
