package runtime

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
)

// lateStartFills delivers the fill start once the lease has expired, as a
// slow step would.
type lateStartFills struct {
	cache.FillRepository
	f *boundedCloudFixture
}

func (s lateStartFills) Start(ctx context.Context, lease cache.FillLease, _ time.Time) (bool, error) {
	s.f.now = lease.ExpiresAt
	return s.FillRepository.Start(ctx, lease, lease.ExpiresAt)
}

// An unused attempt is renewed before its lease expires, and the renewed
// attempt does not wait on the fill its predecessor still holds (#1360).
func TestCloudExecutionRuntimeRenewsAttemptBeforeLeaseExpiry(t *testing.T) {
	f := boundedCloud(t, false)
	f.request.Cache = &llm.CachePolicyV1{}
	ctx := context.Background()
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	scope := cloudstate.Scope{Tenant: f.request.Context.Tenant, Project: f.request.Context.Project}
	first, err := f.repository.LoadRequestAttempt(ctx, scope, cloudstate.RequestID(v.RequestID))
	if err != nil {
		t.Fatal(err)
	}
	f.now = first.CreatedAt.Add(cache.MaxFillLease - cloudstate.UnusedAttemptRenewMargin)
	v, err = f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	next, err := f.repository.LoadRequestAttempt(ctx, scope, cloudstate.RequestID(v.RequestID))
	if err != nil || next.ID == first.ID || len(next.PriorCandidates) != 0 {
		t.Fatalf("attempt not renewed before expiry: %+v, %v", next, err)
	}
	v, err = f.runtime.CompleteExecutionV1(ctx, llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context, Capability: v.Capability})
	boundedState(t, v, err, llm.ExecutionCompleted)
	if v.Generate.Cache.Disposition != "miss_populated" || f.submits.Load() != 1 {
		t.Fatalf("renewed attempt: disposition=%s submits=%d", v.Generate.Cache.Disposition, f.submits.Load())
	}
}

// A fill start refused because the lease expired mid-step is a definite
// non-dispatch: the attempt is closed as unclaimed, not as an unknown
// dispatch, its fill is not left started, and it renews at once (#1360).
func TestCloudExecutionRuntimeExpiredFillStartRenewsAttempt(t *testing.T) {
	f := boundedCloud(t, false)
	f.options.MaxAttempts = 1
	f.request.Cache = &llm.CachePolicyV1{}
	fills := f.cap.ResponseFills
	f.cap.ResponseFills = lateStartFills{FillRepository: fills, f: f}
	f.restart(t)
	ctx := context.Background()
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	v = boundedState(t, v, err, llm.ExecutionFailed)
	if !v.Retryable || v.Dispatch != string(provider.DispatchNotDispatched) || v.RetryAfterSeconds != 0 || f.submits.Load() != 0 {
		t.Fatalf("expired fill start result = %+v submits=%d", v, f.submits.Load())
	}
	scope := cloudstate.Scope{Tenant: f.request.Context.Tenant, Project: f.request.Context.Project}
	first, err := f.repository.LoadRequestAttempt(ctx, scope, cloudstate.RequestID(v.RequestID))
	if err != nil {
		t.Fatal(err)
	}
	saved, err := f.repository.LoadProviderExecution(ctx, scope, first.ID)
	if err != nil || !saved.Execution.Unclaimed(saved.Plan) || !saved.Execution.Settled {
		t.Fatalf("expired fill start = %+v, %v", saved.Execution, err)
	}
	f.cap.ResponseFills = fills
	f.restart(t)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context, Capability: v.Capability}
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionAcquired)
	next, err := f.repository.LoadRequestAttempt(ctx, scope, cloudstate.RequestID(v.RequestID))
	if err != nil || next.ID == first.ID || len(next.PriorCandidates) != 0 {
		t.Fatalf("expired attempt not renewed: %+v, %v", next, err)
	}
	// The renewed attempt owns the fill at once; it never waits on a fill
	// left started by the expired one.
	v, err = f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionCompleted)
	if f.submits.Load() != 1 {
		t.Fatal("renewed attempt not submitted once", f.submits.Load())
	}
}

// Sweep cached steps that start shortly before the lease expires, with
// storage latency injected on every blob read. However the step crosses the
// expiry, it never fails permanently and never records an unknown dispatch
// that sent nothing; the request then completes with one submission (#1360).
func TestCloudCachedStepAcrossLeaseExpiryStaysRetryable(t *testing.T) {
	for _, latency := range []time.Duration{5 * time.Millisecond, 5 * time.Second, 20 * time.Second} {
		for _, before := range []time.Duration{time.Millisecond, 50 * time.Millisecond, time.Second, 30 * time.Second, 59 * time.Second, 61 * time.Second, 90 * time.Second, 3 * time.Minute} {
			t.Run(fmt.Sprintf("latency=%s/before=%s", latency, before), func(t *testing.T) {
				f := boundedCloud(t, false)
				f.request.Cache = &llm.CachePolicyV1{}
				ctx := context.Background()
				v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
				boundedState(t, v, err, llm.ExecutionBudgetRequired)
				ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context, Capability: v.Capability}
				f.now = f.now.Add(cache.MaxFillLease - before)
				f.blobs.open = func(blob.BlobKey) error {
					f.now = f.now.Add(latency)
					return nil
				}
				v, err = f.runtime.GenerateStepV1(ctx, f.request)
				f.blobs.open = nil
				var mapped *provider.Error
				switch {
				case err != nil:
					if !errors.As(err, &mapped) || mapped.Code == provider.CodeConfiguration || mapped.Retry == provider.RetryNever {
						t.Fatalf("step error = %#v", err)
					}
				case v.State == llm.ExecutionFailed:
					if !v.Retryable || v.Dispatch != string(provider.DispatchNotDispatched) {
						t.Fatalf("step failure = %+v", v)
					}
				case v.State == llm.ExecutionPending || v.State == llm.ExecutionOutcomeUnknown:
					if f.submits.Load() == 0 {
						t.Fatalf("unknown dispatch with nothing sent: %+v", v)
					}
				}
				for range 4 {
					if err == nil && v.State == llm.ExecutionProviderCompleted {
						break
					}
					f.now = f.now.Add(time.Second)
					if err == nil && v.State == llm.ExecutionFailed {
						v, err = f.runtime.AcquireBudgetV1(ctx, ref)
						boundedState(t, v, err, llm.ExecutionAcquired)
					}
					v, err = f.runtime.GenerateStepV1(ctx, f.request)
				}
				boundedState(t, v, err, llm.ExecutionProviderCompleted)
				v, err = f.runtime.CompleteExecutionV1(ctx, ref)
				boundedState(t, v, err, llm.ExecutionCompleted)
				if f.submits.Load() != 1 {
					t.Fatal("request not submitted exactly once", f.submits.Load())
				}
			})
		}
	}
}
