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

func bindReloadEndpoints(source *planningSource, digest [32]byte) {
	for _, model := range source.value.Routes.Models {
		for index := range model.Routes {
			model.Routes[index].EndpointDigest = digest
		}
	}
}

// A submitted job is paid work. A reload that leaves its route and endpoint
// alone must let the new configuration poll and publish it; a changed endpoint
// must wait rather than poll elsewhere or fail the request.
func TestCloudReloadPollsPendingPaidWorkOnlyOnUnchangedEndpoint(t *testing.T) {
	for _, endpointChanged := range []bool{false, true} {
		name := "unrelated-change"
		if endpointChanged {
			name = "endpoint-change"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			f := boundedCloud(t, true, func(b *budgetPlanningFixture) { bindReloadEndpoints(b.source, [32]byte{31}) })
			v, err := f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionPending)
			ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
			f.cap.ConfigDigest[0]++
			source := f.cap.Snapshot.(*planningSource)
			source.value.ConfigDigest = f.cap.ConfigDigest
			if endpointChanged {
				bindReloadEndpoints(source, [32]byte{32})
			}
			f.restart(t)
			f.now = f.now.Add(2 * time.Second)
			if endpointChanged {
				_, err = f.runtime.PollExecutionV1(ctx, ref)
				assertRecoveryError(t, err, provider.CodeStateUnavailable, provider.RetrySameOperation)
				var mapped *temporal.ApplicationError
				if !errors.As(activity.ToTemporalError(err), &mapped) || mapped.NonRetryable() {
					t.Fatal("changed endpoint became a permanent Temporal failure")
				}
				if f.polls.Load() != 0 {
					t.Fatal("polled a changed endpoint")
				}
				// Only the endpoint is restored; the configuration digest still differs.
				bindReloadEndpoints(source, [32]byte{31})
				f.restart(t)
			}
			v, err = f.runtime.PollExecutionV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionProviderCompleted)
			v, err = f.runtime.CompleteExecutionV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionCompleted)
			v, err = f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionCompleted)
			if f.submits.Load() != 1 || f.polls.Load() != 1 {
				t.Fatalf("submits=%d polls=%d, want one each", f.submits.Load(), f.polls.Load())
			}
		})
	}
}

// Dropping the bound route is not an unrelated change: the job stays saved and
// the request stays retryable instead of failing with a configuration error.
func TestCloudReloadWaitsWhenBoundRouteIsRemoved(t *testing.T) {
	ctx := context.Background()
	f := boundedCloud(t, true, func(b *budgetPlanningFixture) { bindReloadEndpoints(b.source, [32]byte{31}) })
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionPending)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	f.cap.ConfigDigest[0]++
	source := f.cap.Snapshot.(*planningSource)
	source.value.ConfigDigest = f.cap.ConfigDigest
	for _, model := range source.value.Routes.Models {
		for index := range model.Routes {
			model.Routes[index].ID += "-renamed"
		}
	}
	f.restart(t)
	f.now = f.now.Add(2 * time.Second)
	_, err = f.runtime.PollExecutionV1(ctx, ref)
	assertRecoveryError(t, err, provider.CodeStateUnavailable, provider.RetrySameOperation)
	if f.submits.Load() != 1 || f.polls.Load() != 0 {
		t.Fatal("reload dispatched on a changed route")
	}
}
