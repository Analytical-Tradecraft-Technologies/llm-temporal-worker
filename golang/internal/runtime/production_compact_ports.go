package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	durablestore "github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// NewProductionCompactPortsFactory builds the distinct durable compaction
// phase. It uses the same immutable state identity and provider registry as
// Generate, but creates a materialized-snapshot checkpoint rather than
// aliasing compaction to an ordinary model turn.
func NewProductionCompactPortsFactory() CompactPortsFactory {
	return func(_ context.Context, capabilities V1RuntimeCapabilities) (durablestore.CompactPorts, error) {
		binding, err := newProductionPhaseBinding(capabilities)
		if err != nil {
			return durablestore.CompactPorts{}, err
		}
		return binding.compactPorts(), nil
	}
}

func (binding *productionPhaseBinding) compactPorts() durablestore.CompactPorts {
	return durablestore.CompactPorts{
		Replay:        binding.replayCompact,
		CacheLookup:   binding.compactCacheLookup,
		Route:         binding.routeCompact,
		Reserve:       binding.reserveCompact,
		Journal:       binding.journalCompact,
		Dispatch:      binding.dispatchCompact,
		Finalize:      binding.finalizeCompact,
		FinalizeCache: binding.finalizeCompactCache,
		Reconcile:     binding.reconcileCompact,
	}
}

func (binding *productionPhaseBinding) replayCompact(ctx context.Context, request llm.CompactRequestV1) (durablestore.CompactReplay, error) {
	manifest, digest, err := requestManifest(request)
	if err != nil {
		return durablestore.CompactReplay{}, err
	}
	_, begun, err := binding.beginOperation(ctx, "compact", llm.CompactAPIVersion, request.OperationKey, request.Context, manifest, digest)
	if err != nil {
		return durablestore.CompactReplay{}, err
	}
	scopeID, err := binding.cap.ResolveScope(ctx, request.Context)
	if err != nil {
		return durablestore.CompactReplay{}, err
	}
	materialized, err := binding.cap.Checkpoints.Materializer.MaterializeHandle(ctx, scopeID, string(request.Parent), state.MaterializeLimits{MaxItems: 100000, MaxRows: 100000, MaxBytes: binding.cap.MaxRequestBytes, MaxDepth: 100000})
	if err != nil {
		return durablestore.CompactReplay{}, err
	}
	replay := durablestore.CompactReplay{State: materialized}
	if begun.Existing {
		switch begun.Operation.State {
		case admission.StateCompleted:
			response, err := binding.composition.Results.Get(ctx, begun.Operation.ID)
			if err != nil {
				return durablestore.CompactReplay{}, err
			}
			mapped, err := compactResponseFromNormalized(response, request.Parent, "miss_populated")
			if err != nil {
				return durablestore.CompactReplay{}, err
			}
			route, err := binding.routeCompact(ctx, request, replay)
			if err != nil {
				return durablestore.CompactReplay{}, fmt.Errorf("reconstruct completed compact route: %w", err)
			}
			reservation, err := binding.reserveCompact(ctx, request, route)
			if err != nil {
				return durablestore.CompactReplay{}, fmt.Errorf("recover completed compact reservation: %w", err)
			}
			if err := binding.reconcileCompact(ctx, request, route, reservation, durablestore.CompactFinalization{Response: mapped}); err != nil {
				return durablestore.CompactReplay{}, fmt.Errorf("reconcile completed compact operation: %w", err)
			}
			replay.Completed = &mapped
			return replay, nil
		case admission.StateAmbiguous, admission.StateDispatching, admission.StateProviderPending:
			return durablestore.CompactReplay{}, errors.New("durable compact provider outcome requires operator-safe recovery")
		case admission.StateDefiniteFailed:
			return durablestore.CompactReplay{}, errors.New("durable compact operation previously failed")
		}
	}
	return replay, nil
}

func (binding *productionPhaseBinding) compactCacheLookup(_ context.Context, request llm.CompactRequestV1, _ durablestore.CompactReplay) (durablestore.CompactCacheDecision, error) {
	if request.Cache != nil {
		return durablestore.CompactCacheDecision{}, errors.New("durable compact cache is not configured for this snapshot")
	}
	return durablestore.CompactCacheDecision{Disposition: durablestore.CacheDisabled}, nil
}

func compactProviderRequest(request llm.CompactRequestV1, replay durablestore.CompactReplay) (llm.Request, error) {
	settings := replay.State.Settings
	if err := settings.Validate(); err != nil {
		return llm.Request{}, err
	}
	instructions := append([]llm.Instruction(nil), settings.Instructions...)
	instructions = append(instructions, llm.Instruction{Level: llm.InstructionLevelPolicy, Kind: llm.InstructionKindText, Text: "Produce a faithful compact summary of the supplied conversation. Return text only; do not call tools."})
	value := llm.Request{APIVersion: llm.APIVersion, OperationKey: request.OperationKey, Context: request.Context, Model: settings.Model, ServiceClass: settings.ServiceClass, ServiceClassFallbacks: append([]llm.ServiceClass(nil), settings.ServiceClassFallbacks...), Portability: llm.PortabilityStrict, Instructions: instructions, Input: append([]llm.Item(nil), replay.State.Items...), Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindText}}, Sampling: &llm.SamplingSpec{Temperature: settings.Temperature}, Reasoning: &llm.ReasoningSpec{Effort: settings.ReasoningEffort, Summary: settings.ReasoningSummary}}
	return llm.NormalizeRequest(value)
}

func (binding *productionPhaseBinding) routeCompact(ctx context.Context, request llm.CompactRequestV1, replay durablestore.CompactReplay) (durablestore.RoutePlan, error) {
	normalized, err := compactProviderRequest(request, replay)
	if err != nil {
		return durablestore.RoutePlan{}, err
	}
	snapshot, err := binding.cap.Snapshot.Current(ctx)
	if err != nil {
		return durablestore.RoutePlan{}, err
	}
	if snapshot.Prices == nil {
		return durablestore.RoutePlan{}, errors.New("pricing resolver is unavailable")
	}
	now := binding.now()
	plan, err := binding.cap.Planner.Plan(ctx, routing.Input{Request: normalized, Catalog: snapshot.Routes, Health: snapshot.Health, Now: now})
	if err != nil {
		return durablestore.RoutePlan{}, err
	}
	_, digest, err := requestManifest(request)
	if err != nil {
		return durablestore.RoutePlan{}, err
	}
	operationID := operationIdentity("compact", request.OperationKey, digest)
	for _, candidate := range plan.Candidates {
		matches := budget.MatchPolicies(snapshot.BudgetPolicies, budget.ContextFor(normalized, candidate, snapshot.Environment))
		if snapshot.RequireBudgetMatch && len(matches) == 0 {
			continue
		}
		quote, quoteErr := snapshot.Prices.Resolve(pricing.Query{Provider: candidate.Provider, Family: candidate.Family, EndpointID: candidate.EndpointID, Region: candidate.Region, Model: candidate.Model, ProviderTier: candidate.ProviderTier, At: now})
		if quoteErr != nil {
			continue
		}
		entry := quote.Entry
		if entry.Version == "" {
			entry.Version = quote.CatalogVersion
		}
		estimate, estimateErr := binding.cap.Estimator.EstimateCandidate(normalized, candidate, entry)
		if estimateErr != nil {
			continue
		}
		reservations := make([]admission.WindowReservation, 0, len(matches))
		for _, match := range matches {
			_, bucket := match.Window.Range(now)
			reservations = append(reservations, admission.WindowReservation{PolicyID: match.PolicyID, WindowID: match.Window.ID, Bucket: bucket, Amount: estimate.MicroUSD, Limit: match.Window.Limit, AmountUSD: estimate.CostUSD, LimitUSD: match.Window.LimitUSD, BucketNanos: match.Window.Bucket.Nanoseconds(), DurationNanos: match.Window.Duration.Nanoseconds()})
		}
		return durablestore.RoutePlan{OperationID: operationID, GenerationID: binding.cap.BudgetGenerationID, RouteID: candidate.RouteID, EndpointID: candidate.EndpointID, Provider: candidate.Provider, Model: candidate.Model, PriceVersion: entry.Version, Execution: &durablestore.RouteExecution{Request: normalized, Candidate: candidate, Price: entry, Reservations: reservations, EstimatedUSD: estimate.CostUSD}}, nil
	}
	return durablestore.RoutePlan{}, errors.New("no eligible priced compact route")
}

func (binding *productionPhaseBinding) reserveCompact(ctx context.Context, _ llm.CompactRequestV1, route durablestore.RoutePlan) (durablestore.ReserveResult, error) {
	if route.Execution == nil {
		return durablestore.ReserveResult{}, errors.New("compact route execution is unavailable")
	}
	return binding.composition.Materializer.Accept(ctx, durablestore.ReserveRequest{OperationID: route.OperationID, GenerationID: route.GenerationID, Reservations: route.Execution.Reservations, ExpiresAt: binding.now().Add(binding.cap.OperationRetention)})
}

func (binding *productionPhaseBinding) journalCompact(ctx context.Context, _ llm.CompactRequestV1, route durablestore.RoutePlan, reservation durablestore.ReserveResult) (durablestore.JournalReceipt, error) {
	for _, event := range reservation.Events {
		if _, err := binding.composition.Journal.AppendReservation(ctx, event); err != nil {
			return durablestore.JournalReceipt{}, err
		}
	}
	return durablestore.JournalReceipt{OperationID: route.OperationID, GenerationID: route.GenerationID}, nil
}

func (binding *productionPhaseBinding) dispatchCompact(ctx context.Context, request llm.CompactRequestV1, replay durablestore.CompactReplay, route durablestore.RoutePlan, receipt durablestore.JournalReceipt) (durablestore.CompactDispatchResult, error) {
	generated, err := binding.dispatchGenerate(ctx, llm.GenerateRequestV1{}, durablestore.GenerateReplay{State: replay.State}, route, receipt)
	if err != nil {
		return durablestore.CompactDispatchResult{}, err
	}
	for _, item := range generated.Response.Output {
		message, ok := item.(llm.Message)
		if !ok || message.Actor != llm.ActorModel {
			return durablestore.CompactDispatchResult{}, errors.New("compact provider returned non-message output")
		}
		for _, part := range message.Content {
			if _, ok := part.(llm.TextPart); !ok {
				return durablestore.CompactDispatchResult{}, errors.New("compact provider returned non-text content")
			}
		}
	}
	_ = request
	return durablestore.CompactDispatchResult{Response: generated.Response}, nil
}

func (binding *productionPhaseBinding) finalizeCompact(ctx context.Context, request llm.CompactRequestV1, replay durablestore.CompactReplay, route durablestore.RoutePlan, reservation durablestore.ReserveResult, dispatch durablestore.CompactDispatchResult) (durablestore.CompactFinalization, error) {
	scopeID, err := binding.cap.ResolveScope(ctx, request.Context)
	if err != nil {
		return durablestore.CompactFinalization{}, err
	}
	parent := request.Parent
	checkpoint, handle, err := binding.publishCheckpoint(ctx, scopeID, request.Context.Tenant, &parent, route.OperationID, state.CheckpointCompaction, durablestore.GenerateReplay{State: replay.State}, nil, llm.SettingsPatchV1{}, dispatch.Response.Output)
	if err != nil {
		return durablestore.CompactFinalization{}, err
	}
	dispatch.Response.Continuation = &llm.Continuation{Handle: handle}
	resultRef, err := binding.composition.Results.Put(ctx, string(route.OperationID), dispatch.Response)
	if err != nil {
		return durablestore.CompactFinalization{}, err
	}
	actualMicro := pricing.MicroUSD(0)
	if dispatch.Response.Cost.ActualCostUSD != nil {
		actualMicro, err = pricing.CeilMicroFromUSD(*dispatch.Response.Cost.ActualCostUSD)
		if err != nil {
			return durablestore.CompactFinalization{}, err
		}
	}
	operation, err := binding.composition.Operations.Get(ctx, string(route.OperationID))
	if err != nil {
		return durablestore.CompactFinalization{}, err
	}
	actualUSD, unknownReason := operationCompletionCost(dispatch.Response.Cost)
	if err := binding.composition.Operations.Complete(ctx, admission.CompleteRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Actual: actualMicro, ActualCostUSD: actualUSD, ResultRef: &resultRef, Attempt: admission.AttemptFacts{RouteID: route.RouteID, EndpointID: route.EndpointID, Provider: route.Provider, ResolvedModel: route.Model, Dispatch: admission.Accepted, AttemptNumber: 1}, CostStatus: costStatus(dispatch.Response.Cost), CostMethod: dispatch.Response.Cost.Method, UnknownReason: unknownReason}); err != nil {
		return durablestore.CompactFinalization{}, err
	}
	mapped, err := compactResponseFromNormalized(dispatch.Response, request.Parent, "miss_populated")
	if err != nil {
		return durablestore.CompactFinalization{}, err
	}
	mapped.Checkpoint.Depth = checkpoint.Depth
	_ = reservation
	return durablestore.CompactFinalization{Response: mapped}, nil
}

func compactResponseFromNormalized(response llm.Response, parent llm.CheckpointHandle, cacheDisposition string) (llm.CompactResponseV1, error) {
	if response.Continuation == nil || response.Continuation.Handle == "" {
		return llm.CompactResponseV1{}, errors.New("stored compact response has no checkpoint handle")
	}
	handle := llm.CheckpointHandle(response.Continuation.Handle)
	cost := llm.CostV1{Status: "unknown", UnknownReason: "provider_did_not_report_cost"}
	if response.Cost.ActualCostUSD != nil {
		value := response.Cost.ActualCostUSD.String()
		cost = llm.CostV1{Status: "exact", ActualCostUSD: &value, Method: response.Cost.Method, CatalogVersion: response.Cost.CatalogVersion}
	}
	provenance, err := json.Marshal(map[string]any{"route": response.Route, "provider": response.Provider})
	if err != nil {
		return llm.CompactResponseV1{}, err
	}
	return llm.CompactResponseV1{APIVersion: llm.CompactAPIVersion, OperationKey: response.OperationKey, OperationID: response.OperationID, Checkpoint: llm.CheckpointMetadata{Handle: handle, Parent: &parent, Kind: "compaction"}, Cache: llm.CacheDispositionV1{Disposition: cacheDisposition}, Provenance: provenance, Usage: &response.Usage, Cost: cost, Diagnostics: response.Diagnostics}, nil
}

func (binding *productionPhaseBinding) finalizeCompactCache(context.Context, llm.CompactRequestV1, durablestore.CompactReplay, durablestore.CompactCacheDecision) (durablestore.CompactFinalization, error) {
	return durablestore.CompactFinalization{}, errors.New("compact cache finalization cannot run when durable cache is disabled")
}

func (binding *productionPhaseBinding) reconcileCompact(ctx context.Context, _ llm.CompactRequestV1, route durablestore.RoutePlan, reservation durablestore.ReserveResult, finalization durablestore.CompactFinalization) error {
	events, err := completionEvents(route, reservation, finalization.Response.Cost, binding.now())
	if err != nil {
		return err
	}
	for _, event := range events {
		if _, err := binding.composition.Journal.AppendCompletion(ctx, event); err != nil {
			return err
		}
	}
	return binding.composition.Materializer.Reconcile(ctx, durablestore.ReconcileRequest{OperationID: route.OperationID, GenerationID: route.GenerationID, IncarnationID: reservation.IncarnationID, Events: events})
}
