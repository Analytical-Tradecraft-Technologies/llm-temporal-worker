package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// Every step may run on a different worker whose wall clock is slightly behind
// the one that stamped the previous step. That is latency, not corruption: the
// step must succeed as the next worker would after a moment, never fail the
// request permanently.
func TestCloudClockSkewBehindPreviousStepStillAdvances(t *testing.T) {
	const skew = 150 * time.Millisecond
	for _, cached := range []bool{false, true} {
		for _, async := range []bool{false, true} {
			name := map[bool]string{false: "uncached", true: "cached"}[cached] + "/" + map[bool]string{false: "sync", true: "async"}[async]
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				f := boundedCloud(t, async)
				if cached {
					f.request.Cache = &llm.CachePolicyV1{}
				}
				step := func(name string, run func() (llm.ExecutionResultV1, error), want llm.ExecutionStateV1) llm.ExecutionResultV1 {
					t.Helper()
					// The worker running this step is behind the one that ran the last.
					f.now = f.now.Add(-skew)
					f.restart(t)
					v, err := run()
					var failure *provider.Error
					if errors.As(err, &failure) {
						t.Fatalf("%s: code=%s retry=%s phase=%s: %v", name, failure.Code, failure.Retry, failure.Phase, err)
					}
					return boundedState(t, v, err, want)
				}
				v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
				boundedState(t, v, err, llm.ExecutionBudgetRequired)
				ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
				step("acquire", func() (llm.ExecutionResultV1, error) { return f.runtime.AcquireBudgetV1(ctx, ref) }, llm.ExecutionAcquired)
				if async {
					step("submit", func() (llm.ExecutionResultV1, error) { return f.runtime.GenerateStepV1(ctx, f.request) }, llm.ExecutionPending)
					f.now = f.now.Add(2 * time.Second)
					step("poll", func() (llm.ExecutionResultV1, error) { return f.runtime.PollExecutionV1(ctx, ref) }, llm.ExecutionProviderCompleted)
				} else {
					step("submit", func() (llm.ExecutionResultV1, error) { return f.runtime.GenerateStepV1(ctx, f.request) }, llm.ExecutionProviderCompleted)
				}
				step("complete", func() (llm.ExecutionResultV1, error) { return f.runtime.CompleteExecutionV1(ctx, ref) }, llm.ExecutionCompleted)
				if f.submits.Load() != 1 {
					t.Fatalf("submits=%d", f.submits.Load())
				}
			})
		}
	}
}

// Retiring a failed or unknown attempt and quoting its replacement compare the
// acquiring worker's clock with timestamps the failing worker wrote. A lagging
// clock must neither fail the request nor be mistaken for a conflict.
func TestCloudClockSkewBehindPreviousStepReplacesAttempt(t *testing.T) {
	const skew = 150 * time.Millisecond
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "retryable-failure", true: "unknown-outcome"}[unknown], func(t *testing.T) {
			ctx := context.Background()
			f := boundedCloud(t, false)
			f.request.Cache = &llm.CachePolicyV1{}
			original := f.adapter.invoke
			f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
				f.submits.Add(1)
				if err := o.BeforePossibleWrite(ctx); err != nil {
					return provider.Result{}, err
				}
				if unknown {
					return provider.Result{}, errors.New("lost paid response")
				}
				failure := provider.NewError(provider.CodeProviderRateLimited, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryAfter, "private")
				failure.RetryAfter = time.Second
				return provider.Result{}, failure
			}
			lag := func() { f.now = f.now.Add(-skew); f.restart(t) }
			v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
			boundedState(t, v, err, llm.ExecutionBudgetRequired)
			ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
			lag()
			v, err = f.runtime.AcquireBudgetV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionAcquired)
			lag()
			v, err = f.runtime.GenerateStepV1(ctx, f.request)
			if unknown {
				boundedState(t, v, err, llm.ExecutionPending)
				f.now = f.now.Add(16 * time.Minute)
				lag()
				v, err = f.runtime.PollExecutionV1(ctx, ref)
				boundedState(t, v, err, llm.ExecutionOutcomeUnknown)
			} else {
				boundedState(t, v, err, llm.ExecutionFailed)
				if !v.Retryable {
					t.Fatal("lost retry classification")
				}
				f.now = f.now.Add(2 * time.Second)
			}
			f.adapter.invoke = original
			lag()
			v, err = f.runtime.AcquireBudgetV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionAcquired)
			lag()
			v, err = f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionProviderCompleted)
			lag()
			v, err = f.runtime.CompleteExecutionV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionCompleted)
			if f.submits.Load() != 2 {
				t.Fatalf("submits=%d, want one per attempt", f.submits.Load())
			}
		})
	}
}
