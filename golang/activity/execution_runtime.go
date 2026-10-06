package activity

import (
	"context"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

const (
	PrepareActivityName       = "llm.request.prepare.v1"
	AcquireBudgetActivityName = "llm.budget.acquire.v1"
	PollActivityName          = "llm.poll.v1"
	CompleteActivityName      = "llm.complete.v1"
)

// ExecutionRuntime implements bounded durable steps. Every method must authorize
// the caller before reading saved state. Acquire tries once; Poll retrieves once;
// neither waits for capacity or completion. Repeated Generate/Compact calls must
// resume the saved attempt and must never submit an already dispatched job again.
// Provider and budget receipts remain private to the durable runtime.
type ExecutionRuntime interface {
	PrepareExecutionV1(context.Context, llm.PrepareExecutionV1) (llm.ExecutionResultV1, error)
	AcquireBudgetV1(context.Context, llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error)
	GenerateStepV1(context.Context, llm.GenerateRequestV1) (llm.ExecutionResultV1, error)
	CompactStepV1(context.Context, llm.CompactRequestV1) (llm.ExecutionResultV1, error)
	PollExecutionV1(context.Context, llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error)
	CompleteExecutionV1(context.Context, llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error)
}

func (activities *Activities) PrepareExecutionV1(ctx context.Context, request llm.PrepareExecutionV1) (*llm.ExecutionResultV1, error) {
	kind := "compact"
	if request.Generate != nil {
		kind = "generate"
	}
	return executionStep(ctx, activities, request, kind, "", func(ctx context.Context, runtime ExecutionRuntime) (llm.ExecutionResultV1, error) {
		return runtime.PrepareExecutionV1(ctx, request)
	})
}
func (activities *Activities) AcquireBudgetV1(ctx context.Context, request llm.ExecutionReferenceV1) (*llm.ExecutionResultV1, error) {
	return executionStep(ctx, activities, request, "", request.RequestID, func(ctx context.Context, runtime ExecutionRuntime) (llm.ExecutionResultV1, error) {
		return runtime.AcquireBudgetV1(ctx, request)
	})
}
func (activities *Activities) GenerateStepV1(ctx context.Context, request llm.GenerateRequestV1) (*llm.ExecutionResultV1, error) {
	return executionStep(ctx, activities, request, "generate", "", func(ctx context.Context, runtime ExecutionRuntime) (llm.ExecutionResultV1, error) {
		return runtime.GenerateStepV1(ctx, request)
	})
}
func (activities *Activities) CompactStepV1(ctx context.Context, request llm.CompactRequestV1) (*llm.ExecutionResultV1, error) {
	return executionStep(ctx, activities, request, "compact", "", func(ctx context.Context, runtime ExecutionRuntime) (llm.ExecutionResultV1, error) {
		return runtime.CompactStepV1(ctx, request)
	})
}
func (activities *Activities) PollExecutionV1(ctx context.Context, request llm.ExecutionReferenceV1) (*llm.ExecutionResultV1, error) {
	return executionStep(ctx, activities, request, "", request.RequestID, func(ctx context.Context, runtime ExecutionRuntime) (llm.ExecutionResultV1, error) {
		return runtime.PollExecutionV1(ctx, request)
	})
}
func (activities *Activities) CompleteExecutionV1(ctx context.Context, request llm.ExecutionReferenceV1) (*llm.ExecutionResultV1, error) {
	return executionStep(ctx, activities, request, "", request.RequestID, func(ctx context.Context, runtime ExecutionRuntime) (llm.ExecutionResultV1, error) {
		return runtime.CompleteExecutionV1(ctx, request)
	})
}

func executionStep[T any](ctx context.Context, activities *Activities, request T, kind, requestID string, dispatch func(context.Context, ExecutionRuntime) (llm.ExecutionResultV1, error)) (*llm.ExecutionResultV1, error) {
	marshal := func(value T, limits PayloadLimits) ([]byte, error) { return marshalBounded(value, limits) }
	if err := validateV1Request(ctx, marshal, request, activities); err != nil {
		return nil, err
	}
	if activities == nil {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhaseStateLoad))
	}
	runtime, ok := activities.V1Runtime.(ExecutionRuntime)
	if !ok {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhaseStateLoad))
	}
	var response llm.ExecutionResultV1
	err := activities.runV1(ctx, func(ctx context.Context) error {
		var err error
		response, err = dispatch(ctx, runtime)
		if err != nil {
			return err
		}
		if (kind != "" && response.Kind != kind) || (requestID != "" && response.RequestID != requestID) {
			return provider.NewError(provider.CodeInternal, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever, "execution result identity is invalid")
		}
		if !executionResponseMatchesRequest(request, response) {
			return provider.NewError(provider.CodeInternal, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever, "execution response does not match the request")
		}
		if _, err := marshalBounded(response, activities.payloadLimits()); err != nil {
			return provider.NewError(provider.CodeInternal, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever, "execution result is invalid or exceeds its limit")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func executionResponseMatchesRequest(request any, result llm.ExecutionResultV1) bool {
	if result.State != llm.ExecutionCompleted {
		return true
	}
	switch request := request.(type) {
	case llm.GenerateRequestV1:
		return result.Generate != nil && result.Generate.OperationKey == request.OperationKey
	case llm.CompactRequestV1:
		return result.Compact != nil && result.Compact.OperationKey == request.OperationKey
	case llm.PrepareExecutionV1:
		if request.Generate != nil {
			return executionResponseMatchesRequest(*request.Generate, result)
		}
		if request.Compact != nil {
			return executionResponseMatchesRequest(*request.Compact, result)
		}
		return false
	default:
		return true
	}
}
