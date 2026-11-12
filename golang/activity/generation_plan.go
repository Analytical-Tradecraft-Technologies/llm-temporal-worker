package activity

import (
	"context"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

const (
	PlanGenerationActivityName = "llm.generate.plan.v1"
	// ResolveGenerationActivityName plans a public Generate and, for a cached
	// request that needs no compaction, also runs the request workflow's
	// preparation step. A request that preparation finds complete, such as
	// an exact cache hit, returns with its plan, so the public workflow can
	// answer without starting the request workflow.
	ResolveGenerationActivityName = "llm.generate.resolve.v1"
)

type GenerationPlanningRuntime interface {
	PlanGenerationV1(context.Context, llm.GenerateRequestV1) (llm.GenerationPlanV1, error)
}

func (activities *Activities) PlanGenerationV1(ctx context.Context, input llm.GenerateRequestV1) (*llm.GenerationPlanV1, error) {
	resolution, err := activities.planGeneration(ctx, input, false)
	if err != nil {
		return nil, err
	}
	return &resolution.Plan, nil
}

// ResolveGenerationV1 is PlanGenerationV1 followed, when the plan allows it,
// by the preparation the request workflow would run first. Both run in one
// Activity attempt.
func (activities *Activities) ResolveGenerationV1(ctx context.Context, input llm.GenerateRequestV1) (*llm.GenerationResolutionV1, error) {
	return activities.planGeneration(ctx, input, true)
}

func (activities *Activities) planGeneration(ctx context.Context, input llm.GenerateRequestV1, resolve bool) (*llm.GenerationResolutionV1, error) {
	if err := validateV1Request(ctx, MarshalGenerateV1, input, activities); err != nil {
		return nil, err
	}
	if activities == nil {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhasePlan))
	}
	runtime, ok := activities.V1Runtime.(GenerationPlanningRuntime)
	if !ok {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhasePlan))
	}
	var resolution llm.GenerationResolutionV1
	err := activities.runV1(ctx, func(ctx context.Context) error {
		var err error
		resolution.Plan, err = runtime.PlanGenerationV1(ctx, input)
		if err == nil && resolve {
			resolution.Result = activities.resolvedGeneration(ctx, input, resolution.Plan)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &resolution, nil
}

// resolvedGeneration prepares the request exactly as the request workflow's
// first step would, and returns the result only when it is complete. Any
// other outcome, an error included, leaves the request to that workflow,
// which repeats the idempotent preparation and handles the outcome as before.
// A request without a cache policy skips the preparation: it can complete
// here only as a replay, so the probe would mostly duplicate a miss's work.
func (activities *Activities) resolvedGeneration(ctx context.Context, input llm.GenerateRequestV1, plan llm.GenerationPlanV1) *llm.ExecutionResultV1 {
	runtime, ok := activities.V1Runtime.(ExecutionRuntime)
	if !ok || input.Cache == nil || plan.CompactBeforeGenerate || plan.EffectiveParent != nil {
		return nil
	}
	request := llm.PrepareExecutionV1{Generate: &input, OriginalGenerate: &llm.GenerationOriginV1{Parent: input.Parent}}
	if _, err := marshalBounded(request, activities.payloadLimits()); err != nil {
		return nil
	}
	var result llm.ExecutionResultV1
	var err error
	if cached, ok := runtime.(CachedExecutionRuntime); ok {
		// A miss leaves no attempt, so the request workflow starts one once.
		var found bool
		result, found, err = cached.ResolveCachedV1(ctx, request)
		if !found {
			return nil
		}
	} else {
		result, err = runtime.PrepareExecutionV1(ctx, request)
	}
	if err != nil || result.State != llm.ExecutionCompleted || result.Kind != "generate" || result.Capability == "" || !executionResponseMatchesRequest(request, result) {
		return nil
	}
	if !FitsInlinePayload(activities.payloadLimits(), llm.GenerationResolutionV1{Plan: plan, Result: &result}) {
		return nil
	}
	return &result
}
