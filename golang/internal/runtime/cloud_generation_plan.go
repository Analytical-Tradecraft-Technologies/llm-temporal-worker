package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/compaction"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

// PlanGenerationV1 authorizes before loading the immutable public decision.
// New decisions are persisted before compaction, without admission or provider
// effects or creating a pending request. The inherited
// parent's compaction policy governs this compaction; the Generate patch is
// applied after compaction and governs later turns.
func (r *CloudExecutionRuntime) PlanGenerationV1(ctx context.Context, request llm.GenerateRequestV1) (llm.GenerationPlanV1, error) {
	if r == nil {
		return llm.GenerationPlanV1{}, executionError(provider.CodeConfiguration)
	}
	manifest, err := request.MarshalJSON()
	if err != nil {
		return llm.GenerationPlanV1{}, executionError(provider.CodeInvalidArgument)
	}
	if _, err := r.preparation.authorize(ctx, request.Context); err != nil {
		return llm.GenerationPlanV1{}, err
	}
	var index int64
	if request.Cache != nil {
		index = int64(request.Cache.Variant)
	}
	op := cloudstate.Operation{Scope: cloudstate.Scope{Tenant: request.Context.Tenant, Project: request.Context.Project}, Kind: "generate", Key: request.OperationKey, RequestIndex: index, Manifest: manifest, Now: r.now()}
	saved, err := r.store.LoadGenerationPlan(ctx, op)
	if err == nil {
		return r.publicGenerationPlan(ctx, op, request, saved)
	}
	if !errors.Is(err, contracts.ErrNotFound) || errors.Is(err, contracts.ErrOutcomeUnknown) {
		return llm.GenerationPlanV1{}, cloudRuntimeError(err, false)
	}
	// Historical operations with an unchanged public input already have a
	// durable manifest binding. Verify it before bypassing parent/routing reads.
	// A historical compacted input differs and must fail closed until its
	// original public binding can be proven; the child key alone is not proof.
	_, err = r.store.LookupOperation(ctx, op)
	var plan llm.GenerationPlanV1
	if err == nil {
		if _, err = r.store.BeginOperation(ctx, op); err != nil {
			return plan, cloudRuntimeError(err, false)
		}
	} else if errors.Is(err, contracts.ErrNotFound) && !errors.Is(err, contracts.ErrOutcomeUnknown) {
		plan, err = r.computeGenerationPlan(ctx, request)
		if err != nil {
			return plan, err
		}
	} else {
		return plan, cloudRuntimeError(err, false)
	}
	saved, err = r.store.SaveGenerationPlan(ctx, op, cloudstate.GenerationPlan{CompactBeforeGenerate: plan.CompactBeforeGenerate})
	if err != nil {
		return llm.GenerationPlanV1{}, cloudRuntimeError(err, false)
	}
	return r.publicGenerationPlan(ctx, op, request, saved)
}

func (r *CloudExecutionRuntime) computeGenerationPlan(ctx context.Context, request llm.GenerateRequestV1) (llm.GenerationPlanV1, error) {
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
	// Preparation refuses a Generate whose transcript would pass
	// MaxExtendableParentBytes or leave no item for its output, whatever the
	// policy says, so such a turn must compact first. Compaction is the remedy
	// only when the parent it leaves would fit: otherwise every retry would pay
	// for another summary that changes nothing, so fail before any of them.
	limits := lineageLimits(r.preparation.replay.limits)
	fits, err := generateFits(replay.State, input, input.Request.Input, limits)
	if err != nil {
		return llm.GenerationPlanV1{}, err
	}
	parentItems := len(compact.Selection.Prefix) + len(compact.Selection.Retained)
	if parentItems > len(input.Request.Input) {
		return llm.GenerationPlanV1{}, executionError(provider.CodeStateCorrupt)
	}
	if !fits {
		if compact.Request == nil {
			return llm.GenerationPlanV1{}, generateCannotFitError()
		}
		// The summary is projected at its output reserve, four bytes a token.
		summary := compaction.SummaryItem(strings.Repeat("x", 4*compact.Policy.OutputReserveTokens))
		compacted := append(append([]llm.Item{summary}, compact.Selection.Retained...), input.Request.Input[parentItems:]...)
		compactedFits, err := generateFits(replay.State, input, compacted, limits)
		if err != nil {
			return llm.GenerationPlanV1{}, err
		}
		if !compactedFits {
			return llm.GenerationPlanV1{}, generateCannotFitError()
		}
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
	// under the same instructions and tools, and the summary item it adds:
	// its heading and framing are counted here and its text at the output
	// reserve. The summarizer cannot remove any of it, so a policy trigger it
	// already reaches is not worth a paid call: the next turn would compact
	// again and summarize the summary.
	retained := resolved
	retained.Input = append(append([]llm.Item{compaction.SummaryItem("")}, compact.Selection.Retained...), input.Request.Input[parentItems:]...)
	retainedTokens, err := r.capabilities.BudgetEstimator.CountInputTokens(retained, candidate)
	retainedTokens += int64(compact.Policy.OutputReserveTokens)
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
		if err != nil || (route.ContextBytes > 0 && len(encoded)+routing.AdapterTextOverheadBytes(input.Request, string(route.Family)) >= route.ContextBytes) {
			limited = true
		}
	}
	if limited {
		// A route that fits suppresses compaction only if admission would
		// really select it, so run the same selection (health, compilation,
		// price and budget-policy quote) with a throwaway attempt. Nothing is
		// reserved or dispatched; any failure leaves compaction requested.
		// A failure here is the expected reason to compact, so it is not logged.
		now := r.now()
		_, err := r.execution.admission.planning.Generate(observability.WithLogger(ctx, nil), input, BudgetAttempt{OperationID: "compaction-plan",
			GenerationID: r.options.BudgetGeneration, QuotedAt: now, ExpiresAt: now.Add(cache.MaxFillLease)})
		if ctx.Err() != nil {
			return llm.GenerationPlanV1{}, ctx.Err()
		}
		if err != nil {
			decision.ShouldCompact = true
		}
	}
	// A turn preparation would refuse compacts first instead of failing; the
	// check above already proved that the compacted parent fits.
	if !fits {
		decision.ShouldCompact = true
	}
	return llm.GenerationPlanV1{CompactBeforeGenerate: decision.ShouldCompact}, nil
}

// generateFits reports whether preparation would extend parent with a
// Generate whose whole transcript is items: within MaxExtendableParentBytes
// and leaving room under the lineage item bound for one output item.
func generateFits(parent state.MaterializedState, input PreparedGenerateInput, items []llm.Item, limits state.MaterializeLimits) (bool, error) {
	if len(items)+1 > limits.MaxItems {
		return false, nil
	}
	probe := input
	probe.Request.Input = items
	return extendedParentFits(parent, probe)
}

// generateCannotFitError reports a turn that neither fits its parent nor
// would fit the parent compaction leaves. Nothing has been paid for; the
// caller must shorten the append or start a new lineage.
func generateCannotFitError() error {
	return provider.NewError(provider.CodeInvalidArgument, provider.PhasePlan, provider.DispatchNotDispatched, provider.RetryNever, "generate input cannot fit the parent checkpoint, even after compaction")
}
