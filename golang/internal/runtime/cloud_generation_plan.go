package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/mfow/llm-temporal-worker/golang/budget"

	"github.com/mfow/llm-temporal-worker/golang/cache"
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
	providers := r.execution.admission.planning.providers
	input.Request, err = providers.normalizeRequest(input.Request)
	if err != nil {
		return llm.GenerationPlanV1{}, executionError(provider.CodeInvalidArgument)
	}
	encoded, err := json.Marshal(input.Request)
	if err != nil {
		return llm.GenerationPlanV1{}, executionError(provider.CodeInvalidArgument)
	}
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
	// Count the request compaction leaves behind as well: the selection's
	// retained window plus the items this Generate appends after the parent,
	// under the same instructions and tools. The summarizer cannot remove any
	// of it, so a policy trigger it already reaches is not worth a paid call.
	retained := resolved
	parentItems := len(compact.Selection.Prefix) + len(compact.Selection.Retained)
	if parentItems > len(input.Request.Input) {
		return llm.GenerationPlanV1{}, executionError(provider.CodeStateCorrupt)
	}
	retained.Input = append(append([]llm.Item(nil), compact.Selection.Retained...), input.Request.Input[parentItems:]...)
	retainedTokens, err := r.capabilities.BudgetEstimator.CountInputTokens(retained, candidate)
	if err != nil || retainedTokens > int64(^uint(0)>>1) {
		return llm.GenerationPlanV1{}, executionError(provider.CodeInvalidArgument)
	}
	decision, err := compact.Policy.EvaluateTrigger(compaction.TriggerInput{ProjectedTokens: int(tokens), RetainedTokens: int(retainedTokens), ProjectedBytes: int64(len(encoded)), ProjectedItems: len(input.Request.Input)})
	if err != nil {
		return llm.GenerationPlanV1{}, executionError(provider.CodeInvalidArgument)
	}
	// Selection skips a candidate that does not fit and uses the next one, so
	// a context limit is a reason to compact only when selection would end
	// without a route. Retry reordering in selectCall changes the order, not
	// this answer.
	limited := false
	for _, candidate := range plan.Candidates {
		var route routing.Route
		for _, configured := range providers.catalog.Models[input.Request.Model].Routes {
			if configured.ID == candidate.RouteID {
				route = configured
			}
		}
		if !route.SupportsOutputLimit(input.Request) {
			continue
		}
		// Compacting cannot make a blocked route selectable. The read is best
		// effort: selection reports a route status failure itself.
		if blocked, err := providers.routeBlocked(ctx, candidate); err == nil && blocked {
			continue
		}
		err := r.capabilities.BudgetEstimator.ValidateContext(input.Request, candidate)
		if err != nil && !errors.Is(err, budget.ErrContextLimit) {
			return llm.GenerationPlanV1{}, executionError(provider.CodeInvalidArgument)
		}
		// The byte check counts the text this candidate's family adds when
		// lowering failed tool results, as route planning does.
		if err != nil || (route.ContextBytes > 0 && len(encoded)+routing.ToolResultErrorOverheadBytes(input.Request, string(route.Family)) >= route.ContextBytes) {
			limited = true
		}
	}
	if limited {
		// A route that fits suppresses compaction only if admission would
		// really select it, so run the same selection (health, compilation,
		// price and budget-policy quote) with a throwaway attempt. Nothing is
		// reserved or dispatched; any failure leaves compaction requested.
		now := r.now()
		_, err := r.execution.admission.planning.Generate(ctx, input, BudgetAttempt{OperationID: "compaction-plan",
			GenerationID: r.options.BudgetGeneration, QuotedAt: now, ExpiresAt: now.Add(cache.MaxFillLease)})
		if ctx.Err() != nil {
			return llm.GenerationPlanV1{}, ctx.Err()
		}
		if err != nil {
			decision.ShouldCompact = true
		}
	}
	return llm.GenerationPlanV1{CompactBeforeGenerate: decision.ShouldCompact}, nil
}
