package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"go.temporal.io/sdk/temporal"
)

// reload swaps the fixture to another configuration digest and returns a
// function that restores the original one.
func (f *boundedCloudFixture) reload(t *testing.T) func() {
	t.Helper()
	original := f.cap.ConfigDigest
	f.cap.ConfigDigest[0]++
	source := f.cap.Snapshot.(*planningSource)
	source.value.ConfigDigest = f.cap.ConfigDigest
	f.restart(t)
	return func() {
		f.cap.ConfigDigest = original
		source.value.ConfigDigest = original
		f.restart(t)
	}
}

func (f *boundedCloudFixture) rejectRetryably() {
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		return provider.Result{}, provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNextRoute, "unavailable")
	}
}

// A retryable failure found after a reload must wait for a compatible worker
// to start the next attempt, not hand the same failure back on every
// acquisition (#1162). Only a compatible worker applies the attempt limit.
func TestCloudReloadRetryableFailureWaitsForNextAttempt(t *testing.T) {
	for _, exhausted := range []bool{false, true} {
		name := "next-attempt"
		if exhausted {
			name = "exhausted"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			f := boundedCloud(t, false)
			f.options.MaxAttempts = 3
			if exhausted {
				f.options.MaxAttempts = 1
			}
			f.restart(t)
			f.rejectRetryably()
			v, err := f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionFailed)
			ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
			restore := f.reload(t)
			f.now = f.now.Add(time.Minute)
			for i := 0; i < 2; i++ {
				_, err = f.runtime.AcquireBudgetV1(ctx, ref)
				assertRecoveryError(t, err, provider.CodeStateUnavailable, provider.RetrySameOperation)
			}
			restore()
			v, err = f.runtime.AcquireBudgetV1(ctx, ref)
			if exhausted {
				// The request's own attempt limit applies once a compatible
				// worker serves it.
				boundedState(t, v, err, llm.ExecutionFailed)
				if v.Retryable {
					t.Fatal("exhausted request stayed retryable")
				}
				return
			}
			boundedState(t, v, err, llm.ExecutionAcquired)
			if f.submits.Load() != 1 {
				t.Fatal("reload dispatched with incompatible configuration")
			}
		})
	}
}

// Unpaid work prepared under a configuration that is never restored fails
// fast once the reload grace has passed, instead of waiting forever (#1161).
// The request is untouched, so restoring the configuration still resumes it.
func TestCloudReloadFailsUnpaidWorkAfterGrace(t *testing.T) {
	ctx := context.Background()
	f := boundedCloud(t, false)
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	f.now = f.now.Add(time.Second)
	restore := f.reload(t)
	f.now = f.now.Add(defaultCloudReloadGrace - time.Second)
	_, err = f.runtime.AcquireBudgetV1(ctx, ref)
	assertRecoveryError(t, err, provider.CodeStateUnavailable, provider.RetrySameOperation)
	f.now = f.now.Add(time.Second)
	for _, step := range []func() error{
		func() error { _, err := f.runtime.AcquireBudgetV1(ctx, ref); return err },
		func() error { _, err := f.runtime.GenerateStepV1(ctx, f.request); return err },
	} {
		err = step()
		assertRecoveryError(t, err, provider.CodeConfiguration, provider.RetryNever)
		var mapped *temporal.ApplicationError
		if !errors.As(activity.ToTemporalError(err), &mapped) || !mapped.NonRetryable() {
			t.Fatalf("retired configuration = %v, want a non-retryable Temporal failure", err)
		}
	}
	if f.submits.Load() != 0 {
		t.Fatal("retired configuration dispatched")
	}
	restore()
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionAcquired)
}

// A worker still on an older configuration may receive a request prepared by
// a newer one during a rolling deployment. It must keep waiting however long
// it has been running, so a compatible worker can take the request.
func TestCloudReloadOlderWorkerWaitsForNewerRequest(t *testing.T) {
	ctx := context.Background()
	f := boundedCloud(t, false)
	f.now = f.now.Add(time.Hour)
	restore := f.reload(t)
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	// The older worker started long before the request was prepared.
	f.now = f.now.Add(-time.Hour)
	restore()
	f.now = f.now.Add(time.Hour + defaultCloudReloadGrace)
	_, err = f.runtime.AcquireBudgetV1(ctx, ref)
	assertRecoveryError(t, err, provider.CodeStateUnavailable, provider.RetrySameOperation)
}
