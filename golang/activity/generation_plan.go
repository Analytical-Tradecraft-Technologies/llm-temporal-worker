package activity

import (
	"context"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

const PlanGenerationActivityName = "llm.generate.plan.v1"

type GenerationPlanningRuntime interface {
	PlanGenerationV1(context.Context, llm.GenerateRequestV1) (llm.GenerationPlanV1, error)
}

func (activities *Activities) PlanGenerationV1(ctx context.Context, input llm.GenerateRequestV1) (*llm.GenerationPlanV1, error) {
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
	var result llm.GenerationPlanV1
	err := activities.runV1(ctx, func(ctx context.Context) error {
		var err error
		result, err = runtime.PlanGenerationV1(ctx, input)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}
