package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"go.temporal.io/sdk/temporal"
)

func TestCloudReloadCompletesSavedSuccessAndWaitsForPendingRecovery(t *testing.T) {
	for _, pending := range []bool{false, true} {
		name := "saved-success"
		if pending {
			name = "pending"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			f := boundedCloud(t, pending)
			v, err := f.runtime.GenerateStepV1(ctx, f.request)
			want := llm.ExecutionProviderCompleted
			if pending {
				want = llm.ExecutionPending
			}
			boundedState(t, v, err, want)
			ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
			original := f.cap.ConfigDigest
			f.cap.ConfigDigest[0]++
			source := f.cap.Snapshot.(*planningSource)
			source.value.ConfigDigest = f.cap.ConfigDigest
			if !pending {
				f.cap.Adapters = planningRegistryFunc(func(context.Context, routing.Candidate) (provider.Adapter, error) {
					t.Fatal("saved success consulted current provider adapter")
					return nil, nil
				})
			}
			f.restart(t)
			f.now = f.now.Add(2 * time.Second)
			if pending {
				_, err = f.runtime.PollExecutionV1(ctx, ref)
				assertRecoveryError(t, err, provider.CodeStateUnavailable, provider.RetrySameOperation)
				var mapped *temporal.ApplicationError
				if !errors.As(activity.ToTemporalError(err), &mapped) || mapped.NonRetryable() || mapped.Type() != activity.ErrorTypeProviderTransient {
					t.Fatal("configuration wait became a permanent Temporal failure")
				}
				if f.submits.Load() != 1 || f.polls.Load() != 0 {
					t.Fatal("reload dispatched with incompatible configuration")
				}
				f.cap.ConfigDigest = original
				source.value.ConfigDigest = original
				f.restart(t)
				v, err = f.runtime.PollExecutionV1(ctx, ref)
				boundedState(t, v, err, llm.ExecutionProviderCompleted)
			}
			v, err = f.runtime.CompleteExecutionV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionCompleted)
			v, err = f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionCompleted)
			if f.submits.Load() != 1 {
				t.Fatal("recovery resubmitted paid work")
			}
		})
	}
}

func TestCloudReloadWaitsBeforeBudgetAdmission(t *testing.T) {
	ctx := context.Background()
	f := boundedCloud(t, false)
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	original := f.cap.ConfigDigest
	f.cap.ConfigDigest[0]++
	source := f.cap.Snapshot.(*planningSource)
	source.value.ConfigDigest = f.cap.ConfigDigest
	f.restart(t)
	_, err = f.runtime.AcquireBudgetV1(ctx, ref)
	assertRecoveryError(t, err, provider.CodeStateUnavailable, provider.RetrySameOperation)
	if f.submits.Load() != 0 {
		t.Fatal("reload dispatched before compatible admission")
	}
	f.cap.ConfigDigest = original
	source.value.ConfigDigest = original
	f.restart(t)
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionAcquired)
	v, err = f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionCompleted)
	if f.submits.Load() != 1 {
		t.Fatal("request failed to recover exactly once")
	}
}
