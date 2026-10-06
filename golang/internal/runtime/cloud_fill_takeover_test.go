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
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	waiter := f.request
	waiter.OperationKey = "waiter"
	f.now = f.now.Add(time.Second)
	v, err = f.runtime.GenerateStepV1(ctx, waiter)
	boundedState(t, v, err, llm.ExecutionCacheWait)
	original := f.adapter.invoke
	f.adapter.invoke = func(context.Context, provider.Call, provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		return provider.Result{}, provider.NewError(provider.CodePermissionDenied, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNever, "rejected")
	}
	f.now = f.now.Add(time.Second)
	v, err = f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionFailed)
	f.adapter.invoke = original
	f.now = f.now.Add(31 * time.Second)
	v, err = f.runtime.GenerateStepV1(ctx, waiter)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	v, err = f.runtime.CompleteExecutionV1(ctx, llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: waiter.Context})
	boundedState(t, v, err, llm.ExecutionCompleted)
	if v.Generate.Cache.Disposition != "miss_populated" || f.submits.Load() != 2 {
		t.Fatalf("waiter after failed owner: disposition=%s submits=%d", v.Generate.Cache.Disposition, f.submits.Load())
	}
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
// attempt, which consumes the other request's published response.
func TestCloudUnknownAttemptContinuesAfterFillTakeover(t *testing.T) {
	f := boundedCloud(t, false)
	f.request.Cache = &llm.CachePolicyV1{}
	f.cap.ResponseFills = &unstartedFills{FillRepository: f.cap.ResponseFills}
	f.restart(t)
	ctx := context.Background()
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionPending)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	if f.submits.Load() != 0 {
		t.Fatal("HTTP without a started fill")
	}
	f.now = f.now.Add(16 * time.Minute)
	second := f.request
	second.OperationKey = "second"
	v, err = f.runtime.GenerateStepV1(ctx, second)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	v, err = f.runtime.CompleteExecutionV1(ctx, llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: second.Context})
	boundedState(t, v, err, llm.ExecutionCompleted)
	v, err = f.runtime.PollExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionOutcomeUnknown)
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionCompleted)
	if v.Generate.Cache.Disposition != "hit" || f.submits.Load() != 1 {
		t.Fatalf("unknown attempt after takeover: disposition=%s submits=%d", v.Generate.Cache.Disposition, f.submits.Load())
	}
}
