package runtime

import (
	"context"
	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/llm"
)

var _ activity.ExecutionRuntime = (*snapshotV1Runtime)(nil)

// withExecution holds the configuration lease for the whole bounded step. A
// reload can never close its Redis, storage or provider clients during dispatch.
func (runtime *snapshotV1Runtime) withExecution(ctx context.Context, dispatch func(activity.ExecutionRuntime) (llm.ExecutionResultV1, error)) (llm.ExecutionResultV1, error) {
	var result llm.ExecutionResultV1
	err := runtime.with(ctx, func(current activity.V1Runtime) error {
		bounded, ok := current.(activity.ExecutionRuntime)
		if !ok {
			return snapshotV1RuntimeUnavailable()
		}
		var err error
		result, err = dispatch(bounded)
		return err
	})
	return result, err
}

func (runtime *snapshotV1Runtime) PrepareExecutionV1(ctx context.Context, request llm.PrepareExecutionV1) (llm.ExecutionResultV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return runtime.withExecution(ctx, func(current activity.ExecutionRuntime) (llm.ExecutionResultV1, error) {
		return current.PrepareExecutionV1(ctx, request)
	})
}

func (runtime *snapshotV1Runtime) AcquireBudgetV1(ctx context.Context, request llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return runtime.withExecution(ctx, func(current activity.ExecutionRuntime) (llm.ExecutionResultV1, error) {
		return current.AcquireBudgetV1(ctx, request)
	})
}

func (runtime *snapshotV1Runtime) GenerateStepV1(ctx context.Context, request llm.GenerateRequestV1) (llm.ExecutionResultV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return runtime.withExecution(ctx, func(current activity.ExecutionRuntime) (llm.ExecutionResultV1, error) {
		return current.GenerateStepV1(ctx, request)
	})
}

func (runtime *snapshotV1Runtime) CompactStepV1(ctx context.Context, request llm.CompactRequestV1) (llm.ExecutionResultV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return runtime.withExecution(ctx, func(current activity.ExecutionRuntime) (llm.ExecutionResultV1, error) {
		return current.CompactStepV1(ctx, request)
	})
}

func (runtime *snapshotV1Runtime) PollExecutionV1(ctx context.Context, request llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return runtime.withExecution(ctx, func(current activity.ExecutionRuntime) (llm.ExecutionResultV1, error) {
		return current.PollExecutionV1(ctx, request)
	})
}

func (runtime *snapshotV1Runtime) CompleteExecutionV1(ctx context.Context, request llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return runtime.withExecution(ctx, func(current activity.ExecutionRuntime) (llm.ExecutionResultV1, error) {
		return current.CompleteExecutionV1(ctx, request)
	})
}

func (runtime *snapshotV1Runtime) PlanGenerationV1(ctx context.Context, request llm.GenerateRequestV1) (llm.GenerationPlanV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var result llm.GenerationPlanV1
	err := runtime.with(ctx, func(current activity.V1Runtime) error {
		planning, ok := current.(activity.GenerationPlanningRuntime)
		if !ok {
			return snapshotV1RuntimeUnavailable()
		}
		var err error
		result, err = planning.PlanGenerationV1(ctx, request)
		return err
	})
	return result, err
}
