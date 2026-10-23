package runtime

import (
	"context"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

var _ activity.ExecutionRuntime = (*snapshotV1Runtime)(nil)

// callV1 holds the configuration lease for the whole bounded step. A reload
// can never close its Redis, storage or provider clients during dispatch.
// method is a method expression on the optional runtime interface T; a
// current runtime that does not implement T is unavailable.
func callV1[T, Q, R any](runtime *snapshotV1Runtime, ctx context.Context, request Q, method func(T, context.Context, Q) (R, error)) (R, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var result R
	err := runtime.with(ctx, func(current activity.V1Runtime) error {
		capable, ok := current.(T)
		if !ok {
			return snapshotV1RuntimeUnavailable()
		}
		var err error
		result, err = method(capable, ctx, request)
		return err
	})
	return result, err
}

func (runtime *snapshotV1Runtime) PrepareExecutionV1(ctx context.Context, request llm.PrepareExecutionV1) (llm.ExecutionResultV1, error) {
	return callV1(runtime, ctx, request, activity.ExecutionRuntime.PrepareExecutionV1)
}

func (runtime *snapshotV1Runtime) AcquireBudgetV1(ctx context.Context, request llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	return callV1(runtime, ctx, request, activity.ExecutionRuntime.AcquireBudgetV1)
}

func (runtime *snapshotV1Runtime) GenerateStepV1(ctx context.Context, request llm.GenerateRequestV1) (llm.ExecutionResultV1, error) {
	return callV1(runtime, ctx, request, activity.ExecutionRuntime.GenerateStepV1)
}

func (runtime *snapshotV1Runtime) CompactStepV1(ctx context.Context, request llm.CompactRequestV1) (llm.ExecutionResultV1, error) {
	return callV1(runtime, ctx, request, activity.ExecutionRuntime.CompactStepV1)
}

func (runtime *snapshotV1Runtime) PollExecutionV1(ctx context.Context, request llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	return callV1(runtime, ctx, request, activity.ExecutionRuntime.PollExecutionV1)
}

func (runtime *snapshotV1Runtime) CompleteExecutionV1(ctx context.Context, request llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	return callV1(runtime, ctx, request, activity.ExecutionRuntime.CompleteExecutionV1)
}

func (runtime *snapshotV1Runtime) PlanGenerationV1(ctx context.Context, request llm.GenerateRequestV1) (llm.GenerationPlanV1, error) {
	return callV1(runtime, ctx, request, activity.GenerationPlanningRuntime.PlanGenerationV1)
}

func (runtime *snapshotV1Runtime) ExportLangfuseV1(ctx context.Context, ref llm.ExecutionReferenceV1) error {
	return runtime.with(ctx, func(current activity.V1Runtime) error {
		if exporter, ok := current.(activity.LangfuseRuntime); ok {
			return exporter.ExportLangfuseV1(ctx, ref)
		}
		return nil
	})
}
