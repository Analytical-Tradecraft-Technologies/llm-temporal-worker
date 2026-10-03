package runtime

import (
	"context"
	"encoding/json"

	"github.com/mfow/llm-temporal-worker/golang/compaction"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// PlanGenerationV1 authorizes and materializes before inspecting content or
// routing. It has no write, admission, or provider I/O effects. The inherited
// parent's compaction policy governs this compaction; the Generate patch is
// applied after compaction and governs later turns.
func (r *CloudExecutionRuntime) PlanGenerationV1(ctx context.Context, request llm.GenerateRequestV1) (llm.GenerationPlanV1, error) {
	if r == nil {
		return llm.GenerationPlanV1{}, executionError(provider.CodeConfiguration)
	}
	replay, err := r.preparation.replay.Generate(ctx, request)
	if err != nil {
		return llm.GenerationPlanV1{}, err
	}
	input, err := PrepareGenerateInput(ctx, request, replay)
	if err != nil {
		return llm.GenerationPlanV1{}, err
	}
	if request.Parent == nil {
		return llm.GenerationPlanV1{}, nil
	}
	compact, err := PrepareCompactInput(ctx, llm.CompactRequestV1{OperationKey: "compaction-plan", Context: request.Context, Parent: *request.Parent}, durable.CompactReplay{State: replay.State})
	if err != nil {
		return llm.GenerationPlanV1{}, err
	}
	if compact.Request == nil {
		return llm.GenerationPlanV1{}, nil
	}
	encoded, err := json.Marshal(input.Request)
	if err != nil {
		return llm.GenerationPlanV1{}, executionError(provider.CodeInvalidArgument)
	}
	providers := r.execution.admission.planning.providers
	catalog, err := copyProviderCatalog(providers.catalog)
	if err != nil {
		return llm.GenerationPlanV1{}, executionError(provider.CodeConfiguration)
	}
	// A context-size rejection is the reason to compact, not a reason to skip
	// planning. Preserve every other eligibility constraint in a detached copy.
	for name, model := range catalog.Models {
		for i := range model.Routes {
			model.Routes[i].ContextBytes = 0
		}
		catalog.Models[name] = model
	}
	plan, err := providers.planner.Plan(ctx, routing.Input{Request: input.Request, Catalog: catalog, Health: copyProviderHealth(providers.health)})
	if err != nil || len(plan.Candidates) == 0 {
		return llm.GenerationPlanV1{}, executionError(provider.CodeNoRoute)
	}
	candidate := plan.Candidates[0]
	resolved := input.Request
	resolved.Model = candidate.Model
	resolved.ServiceClass = candidate.AttemptedClass
	resolved.ServiceClassFallbacks = nil
	tokens, err := r.capabilities.BudgetEstimator.CountInputTokens(resolved, candidate)
	if err != nil || tokens > int64(^uint(0)>>1) {
		return llm.GenerationPlanV1{}, executionError(provider.CodeInvalidArgument)
	}
	decision, err := compact.Policy.EvaluateTrigger(compaction.TriggerInput{ProjectedTokens: int(tokens), ProjectedBytes: int64(len(encoded)), ProjectedItems: len(input.Request.Input)})
	if err != nil {
		return llm.GenerationPlanV1{}, executionError(provider.CodeInvalidArgument)
	}
	for _, route := range providers.catalog.Models[input.Request.Model].Routes {
		if route.ID == candidate.RouteID && route.ContextBytes > 0 && len(encoded) >= route.ContextBytes {
			decision.ShouldCompact = true
		}
	}
	return llm.GenerationPlanV1{CompactBeforeGenerate: decision.ShouldCompact}, nil
}
