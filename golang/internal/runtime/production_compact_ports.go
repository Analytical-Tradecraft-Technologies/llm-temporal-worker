package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/compaction"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	durablestore "github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

const (
	compactInvalidProviderOutputReason   = "compact_invalid_provider_output"
	compactPostResponseValidationReason  = "compact_post_response_validation"
	compactResultStoreFailureReason      = "compact_result_store_failed"
	compactFinalizationInterruptedReason = "compact_finalization_interrupted"
)

var (
	errCompactInvalidProviderOutput        = errors.New("compact provider returned an invalid compacted summary")
	errCompactOperationPreviouslyFailed    = errors.New("durable compact operation previously failed")
	errCompactProviderOutcomeNeedsRecovery = errors.New("durable compact provider outcome requires operator-safe recovery")
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
		Abort:         binding.abortCompact,
		FinalizeCache: binding.finalizeCompactCache,
		Reconcile:     binding.reconcileCompact,
	}
}

func compactRouteFromReplayFacts(raw []byte, operationID durablestore.OperationID, request llm.Request) (durablestore.RoutePlan, durablestore.ReserveResult, error) {
	if err := operationID.Validate(); err != nil {
		return durablestore.RoutePlan{}, durablestore.ReserveResult{}, err
	}
	route, err := routeFromPersistedFacts(raw, request)
	if err != nil {
		return durablestore.RoutePlan{}, durablestore.ReserveResult{}, err
	}
	if route.OperationID != operationID {
		return durablestore.RoutePlan{}, durablestore.ReserveResult{}, errors.New("compact immutable reservation operation identity does not match replay")
	}
	if route.Reservation == nil {
		return durablestore.RoutePlan{}, durablestore.ReserveResult{}, errors.New("compact immutable reservation is unavailable")
	}
	return route, *route.Reservation, nil
}

func compactTerminalFailure(operation admission.Operation, cause error) error {
	if _, ok := deterministicNoDispatchClassification(operation.FailureReason); ok {
		return deterministicNoDispatchFailure(operation.ID, operation.FailureReason, cause)
	}
	dispatch := provider.DispatchNotDispatched
	switch operation.Attempt.Dispatch {
	case admission.Rejected:
		dispatch = provider.DispatchRejected
	case admission.Accepted:
		dispatch = provider.DispatchAccepted
	case admission.Ambiguous:
		dispatch = provider.DispatchAmbiguous
	}
	code, phase, message, marker := provider.CodeOperationConflict, provider.PhaseAdmission, "compact operation is already terminal", errCompactOperationPreviouslyFailed
	switch {
	case operation.State == admission.StateCanceled:
		code, phase, message = provider.CodeCanceled, provider.PhaseFinalize, "compact operation was canceled"
	case operation.State == admission.StateAmbiguous:
		code, phase, dispatch, message, marker = provider.CodeAmbiguousDispatch, provider.PhaseDispatch, provider.DispatchAmbiguous, "compact provider outcome is ambiguous", errCompactProviderOutcomeNeedsRecovery
	case operation.FailureReason == providerCanceledPreDispatchReason:
		code, phase, message = provider.CodeCanceled, provider.PhaseDispatch, "provider request canceled before dispatch"
	case operation.FailureReason == providerDeadlinePreDispatchReason:
		code, phase, message = provider.CodeDeadlineExceeded, provider.PhaseDispatch, "provider request deadline exceeded before dispatch"
	case operation.FailureReason == compactInvalidProviderOutputReason:
		code, phase, dispatch, message, marker = provider.CodeProviderInvalidResponse, provider.PhaseLift, provider.DispatchAccepted, "compact provider response is invalid", errCompactInvalidProviderOutput
	case operation.FailureReason == compactPostResponseValidationReason:
		code, phase, dispatch, message = provider.CodeProviderInvalidResponse, provider.PhaseFinalize, provider.DispatchAccepted, "compact provider receipt violates admitted bounds"
	case operation.FailureReason == "checkpoint_publication_failed" || operation.FailureReason == compactResultStoreFailureReason || operation.FailureReason == compactFinalizationInterruptedReason:
		code, phase, dispatch, message = provider.CodeStateUnavailable, provider.PhaseContinuationWrite, provider.DispatchAccepted, "compact result finalization failed"
	}
	mapped := provider.NewError(code, phase, dispatch, provider.RetryNever, message)
	mapped.OperationID = operation.ID
	if cause == nil {
		cause = marker
	} else {
		cause = errors.Join(marker, cause)
	}
	mapped.Cause = cause
	return mapped
}

func (binding *productionPhaseBinding) reconcileCompactTerminal(ctx context.Context, replay durablestore.CompactReplay, operation admission.Operation, cause error) error {
	beforeReservation := operation.FailureReason == callerCanceledPreReservationReason || operation.FailureReason == callerDeadlinePreReservationReason
	if len(replay.ImmutableFacts) != 0 && !beforeReservation {
		route, reservation, err := compactRouteFromReplayFacts(replay.ImmutableFacts, replay.OperationID, llm.Request{})
		if err != nil {
			return err
		}
		if err := binding.finalizeFailedBudget(context.WithoutCancel(ctx), route, reservation, operation); err != nil {
			return err
		}
	}
	return compactTerminalFailure(operation, cause)
}

func (binding *productionPhaseBinding) persistAndReconcileCompactNoDispatch(ctx context.Context, replay durablestore.CompactReplay, operation admission.Operation, attemptNumber int, reason string, cause error) error {
	terminalErr := binding.persistDeterministicNoDispatch(ctx, operation, attemptNumber, reason, cause)
	var mapped *provider.Error
	if !errors.As(terminalErr, &mapped) {
		return terminalErr
	}
	terminal, err := binding.composition.Operations.Get(context.WithoutCancel(ctx), operation.ID)
	if err != nil {
		return fmt.Errorf("reload compact terminal no-dispatch outcome: %w", err)
	}
	if len(replay.ImmutableFacts) != 0 {
		route, reservation, err := compactRouteFromReplayFacts(replay.ImmutableFacts, replay.OperationID, llm.Request{})
		if err != nil {
			return err
		}
		if err := binding.finalizeFailedBudget(context.WithoutCancel(ctx), route, reservation, terminal); err != nil {
			return fmt.Errorf("reconcile compact terminal no-dispatch outcome: %w", err)
		}
	}
	return terminalErr
}

func (binding *productionPhaseBinding) abortCompact(ctx context.Context, request llm.CompactRequestV1, replay durablestore.CompactReplay, route durablestore.RoutePlan, reservation *durablestore.ReserveResult, cause error) error {
	reason := callerCanceledFailureReason
	if reservation == nil && route.OperationID != "" {
		reason = callerCanceledPreReservationReason
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		reason = callerDeadlineFailureReason
		if reservation == nil && route.OperationID != "" {
			reason = callerDeadlinePreReservationReason
		}
	}
	operation, err := binding.composition.Operations.Get(context.WithoutCancel(ctx), string(replay.OperationID))
	if err != nil {
		return fmt.Errorf("load Compact operation for caller termination: %w", err)
	}
	if operation.State.Terminal() {
		if reservation != nil {
			if err := binding.finalizeFailedBudget(context.WithoutCancel(ctx), route, *reservation, operation); err != nil {
				return fmt.Errorf("reconcile Compact caller termination: %w", err)
			}
		}
		// Provider dispatch may already have persisted a more precise typed
		// cancellation/deadline receipt. Preserve that classification.
		return cause
	}
	terminalErr := binding.persistDeterministicNoDispatch(ctx, operation, gatewayAttemptNumber(request.CostAdmission), reason, cause)
	var mapped *provider.Error
	if !errors.As(terminalErr, &mapped) {
		return terminalErr
	}
	if reservation == nil {
		return terminalErr
	}
	terminal, err := binding.composition.Operations.Get(context.WithoutCancel(ctx), operation.ID)
	if err != nil {
		return fmt.Errorf("reload Compact caller termination: %w", err)
	}
	if err := binding.finalizeFailedBudget(context.WithoutCancel(ctx), route, *reservation, terminal); err != nil {
		return fmt.Errorf("reconcile Compact caller termination: %w", err)
	}
	return terminalErr
}

func (binding *productionPhaseBinding) replayCompact(ctx context.Context, request llm.CompactRequestV1) (durablestore.CompactReplay, error) {
	manifest, payload, digest, err := requestManifest(request)
	if err != nil {
		return durablestore.CompactReplay{}, err
	}
	_, begun, err := binding.beginOperation(ctx, "compact", llm.CompactAPIVersion, request.OperationKey, request.Context, manifest, payload, digest)
	if err != nil {
		return durablestore.CompactReplay{}, err
	}
	replay := durablestore.CompactReplay{
		OperationID:        durablestore.OperationID(begun.Operation.ID),
		ImmutableFacts:     append([]byte(nil), begun.Operation.ImmutableFacts...),
		OperationExpiresAt: begun.Operation.ExpiresAt,
	}
	if begun.Existing {
		switch begun.Operation.State {
		case admission.StateCompleted:
			response, err := binding.composition.Results.Get(context.WithoutCancel(ctx), begun.Operation.ID)
			if err != nil {
				return durablestore.CompactReplay{}, err
			}
			mapped, err := compactResponseFromNormalized(response, request.Parent, "miss_populated")
			if err != nil {
				return durablestore.CompactReplay{}, err
			}
			if request.CostAdmission != nil {
				admission := *request.CostAdmission
				mapped.CostAdmission = &admission
			}
			route, reservation, err := compactRouteFromReplayFacts(replay.ImmutableFacts, replay.OperationID, llm.Request{})
			if err != nil {
				return durablestore.CompactReplay{}, err
			}
			if err := binding.reconcileCompact(ctx, request, route, reservation, durablestore.CompactFinalization{Response: mapped}); err != nil {
				return durablestore.CompactReplay{}, err
			}
			replay.Completed = &mapped
			return replay, nil
		case admission.StateDefiniteFailed, admission.StateAmbiguous, admission.StateCanceled:
			return durablestore.CompactReplay{}, binding.reconcileCompactTerminal(ctx, replay, begun.Operation, nil)
		case admission.StateDispatching, admission.StateProviderPending:
			// A durable result proves that provider dispatch returned and the
			// worker was interrupted during storage finalization. Terminalize
			// that chargeable receipt instead of leaving an ambiguous lease.
			response, resultErr := binding.composition.Results.Get(context.WithoutCancel(ctx), begun.Operation.ID)
			if resultErr != nil {
				return durablestore.CompactReplay{}, errCompactProviderOutcomeNeedsRecovery
			}
			route, reservation, routeErr := compactRouteFromReplayFacts(replay.ImmutableFacts, replay.OperationID, llm.Request{})
			if routeErr != nil {
				return durablestore.CompactReplay{}, routeErr
			}
			cause := binding.failPostResponse(ctx, route, reservation, response, compactFinalizationInterruptedReason, errCompactOperationPreviouslyFailed)
			return durablestore.CompactReplay{}, cause
		}
	}
	scopeID, err := binding.cap.ResolveScope(ctx, request.Context)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return durablestore.CompactReplay{}, binding.persistAndReconcileCompactNoDispatch(ctx, replay, begun.Operation, gatewayAttemptNumber(request.CostAdmission), callerCanceledFailureReason, err)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return durablestore.CompactReplay{}, binding.persistAndReconcileCompactNoDispatch(ctx, replay, begun.Operation, gatewayAttemptNumber(request.CostAdmission), callerDeadlineFailureReason, err)
		}
		return durablestore.CompactReplay{}, err
	}
	materialized, err := binding.cap.Checkpoints.Materializer.MaterializeHandle(ctx, scopeID, string(request.Parent), binding.cap.CheckpointLimits)
	if err != nil {
		switch {
		case errors.Is(err, context.Canceled):
			return durablestore.CompactReplay{}, binding.persistAndReconcileCompactNoDispatch(ctx, replay, begun.Operation, gatewayAttemptNumber(request.CostAdmission), callerCanceledFailureReason, err)
		case errors.Is(err, context.DeadlineExceeded):
			return durablestore.CompactReplay{}, binding.persistAndReconcileCompactNoDispatch(ctx, replay, begun.Operation, gatewayAttemptNumber(request.CostAdmission), callerDeadlineFailureReason, err)
		case errors.Is(err, state.ErrMaterializeLimit):
			return durablestore.CompactReplay{}, binding.persistAndReconcileCompactNoDispatch(ctx, replay, begun.Operation, gatewayAttemptNumber(request.CostAdmission), checkpointBoundsFailureReason, err)
		case errors.Is(err, state.ErrNotFound), errors.Is(err, state.ErrExpired), errors.Is(err, state.ErrTenantMismatch), errors.Is(err, state.ErrInvalidHandle), errors.Is(err, state.ErrInvalidCheckpoint):
			return durablestore.CompactReplay{}, binding.persistAndReconcileCompactNoDispatch(ctx, replay, begun.Operation, gatewayAttemptNumber(request.CostAdmission), checkpointInvalidFailureReason, err)
		default:
			return durablestore.CompactReplay{}, err
		}
	}
	// MaterializeHandle authenticates the opaque storage scope resolved from
	// this request. Restore the caller-facing scope labels expected by the
	// provider-neutral replay contract; the checkpoint row intentionally does
	// not persist plaintext tenant or project names.
	materialized.Tenant = request.Context.Tenant
	materialized.Project = request.Context.Project
	replay.State = materialized
	if err := validateCheckpointBounds(replay.State, true, 0, binding.cap.CheckpointLimits); err != nil {
		return durablestore.CompactReplay{}, binding.persistAndReconcileCompactNoDispatch(ctx, replay, begun.Operation, gatewayAttemptNumber(request.CostAdmission), checkpointBoundsFailureReason, err)
	}
	normalized, err := compactProviderRequest(request, replay)
	if err != nil {
		return durablestore.CompactReplay{}, binding.persistAndReconcileCompactNoDispatch(ctx, replay, begun.Operation, gatewayAttemptNumber(request.CostAdmission), checkpointInvalidFailureReason, err)
	}
	if err := validateProviderPayloadBounds(normalized, binding.cap.MaxRequestBytes); err != nil {
		return durablestore.CompactReplay{}, binding.persistAndReconcileCompactNoDispatch(ctx, replay, begun.Operation, gatewayAttemptNumber(request.CostAdmission), payloadBoundsFailureReason, err)
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
	if err := replay.OperationID.Validate(); err != nil {
		return durablestore.RoutePlan{}, err
	}
	normalized, err := compactProviderRequest(request, replay)
	if err != nil {
		return durablestore.RoutePlan{}, err
	}
	if len(replay.ImmutableFacts) != 0 {
		route, _, err := compactRouteFromReplayFacts(replay.ImmutableFacts, replay.OperationID, normalized)
		if err != nil {
			return durablestore.RoutePlan{}, err
		}
		dispatchRequest, err := prepareDurableDispatchRequest(normalized, route.Execution.Candidate)
		if err != nil {
			return durablestore.RoutePlan{}, err
		}
		route.Execution.Request = dispatchRequest
		return route, nil
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
	operationID := replay.OperationID
	var tokenLimitErr error
	for _, candidate := range plan.Candidates {
		dispatchRequest, policyErr := prepareDurableDispatchRequest(normalized, candidate)
		if policyErr != nil {
			return durablestore.RoutePlan{}, policyErr
		}
		matches := budget.MatchPolicies(snapshot.BudgetPolicies, budget.ContextFor(normalized, candidate, snapshot.Environment))
		if snapshot.RequireBudgetMatch && len(matches) == 0 {
			continue
		}
		quote, quoteErr := snapshot.Prices.Resolve(pricing.Query{Provider: candidate.Provider, Family: candidate.Family, EndpointID: candidate.EndpointID, Region: candidate.Region, Model: candidate.Model, ProviderTier: candidate.ProviderTier, At: now})
		if quoteErr != nil {
			if request.CostAdmission != nil {
				return durablestore.RoutePlan{}, fmt.Errorf("compact cost admission route %q has no immutable price: %w", candidate.RouteID, quoteErr)
			}
			continue
		}
		if request.CostAdmission != nil &&
			(quote.CatalogVersion != request.CostAdmission.PricingGenerationID ||
				quote.CatalogDigest != request.CostAdmission.PricingManifestSHA256) {
			return durablestore.RoutePlan{}, fmt.Errorf("compact cost admission pricing snapshot does not match route %q", candidate.RouteID)
		}
		entry := quote.Entry
		if entry.Version == "" {
			entry.Version = quote.CatalogVersion
		}
		estimate, estimateErr := binding.cap.Estimator.EstimateCandidate(dispatchRequest, candidate, entry)
		if estimateErr != nil {
			if errors.Is(estimateErr, budget.ErrTokenLimit) {
				tokenLimitErr = estimateErr
				continue
			}
			if request.CostAdmission != nil {
				return durablestore.RoutePlan{}, fmt.Errorf("compact cost admission route %q cannot be safely priced: %w", candidate.RouteID, estimateErr)
			}
			continue
		}
		reservations := make([]admission.WindowReservation, 0, len(matches))
		for _, match := range matches {
			_, bucket := match.Window.Range(now)
			reservations = append(reservations, admission.WindowReservation{PolicyID: match.PolicyID, WindowID: match.Window.ID, Bucket: bucket, Amount: estimate.MicroUSD, Limit: match.Window.Limit, AmountUSD: estimate.CostUSD, LimitUSD: match.Window.LimitUSD, BucketNanos: match.Window.Bucket.Nanoseconds(), DurationNanos: match.Window.Duration.Nanoseconds()})
		}
		route := durablestore.RoutePlan{OperationID: operationID, GenerationID: binding.cap.BudgetGenerationID, RouteID: candidate.RouteID, EndpointID: candidate.EndpointID, Provider: candidate.Provider, Model: candidate.Model, PriceVersion: entry.Version, PricingGenerationID: quote.CatalogVersion, PricingManifestSHA256: quote.CatalogDigest, ResourceCapacityGenerationID: binding.cap.ResourceCapacity.GenerationID, ResourceCapacityManifestSHA256: binding.cap.ResourceCapacity.ManifestSHA256, Execution: &durablestore.RouteExecution{Request: dispatchRequest, Candidate: candidate, Price: entry, Reservations: reservations, EstimatedUSD: estimate.CostUSD}}
		return binding.prepareReservationRoute(ctx, "compact", llm.CompactAPIVersion, request.OperationKey, request.Context, request, route)
	}
	if tokenLimitErr != nil {
		operation, loadErr := binding.composition.Operations.Get(ctx, string(replay.OperationID))
		if loadErr != nil {
			return durablestore.RoutePlan{}, fmt.Errorf("reload compact token-bound operation: %w", loadErr)
		}
		return durablestore.RoutePlan{}, binding.persistAndReconcileCompactNoDispatch(ctx, replay, operation, gatewayAttemptNumber(request.CostAdmission), tokenBoundsFailureReason, tokenLimitErr)
	}
	return durablestore.RoutePlan{}, errors.New("no eligible priced compact route")
}

func (binding *productionPhaseBinding) reserveCompact(ctx context.Context, _ llm.CompactRequestV1, route durablestore.RoutePlan) (durablestore.ReserveResult, error) {
	return binding.reserveGenerate(ctx, llm.GenerateRequestV1{}, route)
}

func (binding *productionPhaseBinding) journalCompact(ctx context.Context, request llm.CompactRequestV1, route durablestore.RoutePlan, reservation durablestore.ReserveResult) (durablestore.JournalReceipt, error) {
	reserveRequest, err := reservationRequest(route)
	if err != nil {
		return durablestore.JournalReceipt{}, err
	}
	if err := durablestore.ValidatePlannedReserveResult(reserveRequest, reservation); err != nil {
		return durablestore.JournalReceipt{}, err
	}
	for _, event := range reservation.Events {
		if _, err := binding.composition.Journal.AppendReservation(ctx, event); err != nil {
			return durablestore.JournalReceipt{}, err
		}
	}
	return durablestore.JournalReceipt{OperationID: route.OperationID, GenerationID: route.GenerationID}, nil
}

func validateCompactProviderResponse(response llm.Response, maxBytes int) error {
	if _, err := compaction.PlainTextSummary(response, maxBytes); err != nil {
		return errCompactInvalidProviderOutput
	}
	return nil
}

func (binding *productionPhaseBinding) failCompactProviderOutput(ctx context.Context, route durablestore.RoutePlan, response llm.Response, cause error) error {
	if route.Reservation == nil {
		return errors.New("compact immutable reservation is unavailable")
	}
	return binding.failPostResponse(ctx, route, *route.Reservation, response, compactInvalidProviderOutputReason, cause)
}

func (binding *productionPhaseBinding) dispatchCompact(ctx context.Context, request llm.CompactRequestV1, replay durablestore.CompactReplay, route durablestore.RoutePlan, receipt durablestore.JournalReceipt) (durablestore.CompactDispatchResult, error) {
	generated, err := binding.dispatchGenerate(ctx, llm.GenerateRequestV1{CostAdmission: request.CostAdmission}, durablestore.GenerateReplay{State: replay.State}, route, receipt)
	if err != nil {
		return durablestore.CompactDispatchResult{}, err
	}
	if err := validateCompactProviderResponse(generated.Response, int(binding.cap.CheckpointLimits.MaxBytes)); err != nil {
		return durablestore.CompactDispatchResult{}, binding.failCompactProviderOutput(ctx, route, generated.Response, err)
	}
	return durablestore.CompactDispatchResult{Response: generated.Response}, nil
}

func (binding *productionPhaseBinding) finalizeCompact(ctx context.Context, request llm.CompactRequestV1, replay durablestore.CompactReplay, route durablestore.RoutePlan, reservation durablestore.ReserveResult, dispatch durablestore.CompactDispatchResult) (durablestore.CompactFinalization, error) {
	scopeID, err := binding.cap.ResolveScope(ctx, request.Context)
	if err != nil {
		return durablestore.CompactFinalization{}, binding.failPostResponseCheckpoint(ctx, route, reservation, dispatch.Response, err)
	}
	if request.CostAdmission != nil {
		if dispatch.Response.Cost.ActualCostUSD == nil {
			return durablestore.CompactFinalization{}, binding.failPostResponse(ctx, route, reservation, dispatch.Response, compactPostResponseValidationReason, errors.New("compact cost admission requires exact provider receipt"))
		}
		allowance, allowanceErr := pricing.USDFromMicro(pricing.MicroUSD(request.CostAdmission.RemainingMaxCostMicrounits))
		if allowanceErr != nil {
			return durablestore.CompactFinalization{}, binding.failPostResponse(ctx, route, reservation, dispatch.Response, compactPostResponseValidationReason, fmt.Errorf("compact cost admission allowance: %w", allowanceErr))
		}
		actual := *dispatch.Response.Cost.ActualCostUSD
		if actual.Cmp(route.Execution.EstimatedUSD) > 0 || actual.Cmp(allowance) > 0 {
			return durablestore.CompactFinalization{}, binding.failPostResponse(ctx, route, reservation, dispatch.Response, compactPostResponseValidationReason, errors.New("exact compact provider cost exceeds admitted reservation"))
		}
		dispatch.Response.Cost.CatalogVersion = route.PricingGenerationID
	}
	parent := request.Parent
	checkpoint, handle, checkpointObjects, err := binding.publishCheckpoint(ctx, scopeID, &parent, route.OperationID, state.CheckpointCompaction, durablestore.GenerateReplay{State: replay.State}, nil, llm.SettingsPatchV1{}, dispatch.Response.Output)
	if err != nil {
		return durablestore.CompactFinalization{}, binding.failPostResponseCheckpoint(ctx, route, reservation, dispatch.Response, err)
	}
	dispatch.Response.Continuation = &llm.Continuation{Handle: handle}
	resultRef, err := binding.composition.Results.Put(context.WithoutCancel(ctx), string(route.OperationID), dispatch.Response)
	if err != nil {
		return durablestore.CompactFinalization{}, binding.failPostResponse(ctx, route, reservation, dispatch.Response, compactResultStoreFailureReason, err)
	}
	actualMicro := pricing.MicroUSD(0)
	if dispatch.Response.Cost.ActualCostUSD != nil {
		// The exact decimal receipt is authoritative. Keep the legacy integer
		// compatibility field at zero when it cannot represent the receipt.
		if compatible, compatibilityErr := pricing.CeilMicroFromUSD(*dispatch.Response.Cost.ActualCostUSD); compatibilityErr == nil {
			actualMicro = compatible
		}
	}
	operation, err := binding.composition.Operations.Get(context.WithoutCancel(ctx), string(route.OperationID))
	if err != nil {
		return durablestore.CompactFinalization{}, err
	}
	actualUSD, unknownReason := operationCompletionCost(dispatch.Response.Cost)
	checkpoint, err = binding.composition.Finalizer.Finalize(ctx, admission.AtomicFinalization{
		ScopeID: scopeID, Checkpoint: checkpoint, CheckpointObjects: checkpointObjects,
		Complete: admission.CompleteRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Actual: actualMicro, ActualCostUSD: actualUSD, ResultRef: &resultRef, Attempt: admission.AttemptFacts{RouteID: route.RouteID, EndpointID: route.EndpointID, Provider: route.Provider, ResolvedModel: route.Model, ProviderRequestID: dispatch.Response.Provider.RequestID, ServiceClass: string(route.Execution.Candidate.AttemptedClass), Dispatch: admission.Accepted, AttemptNumber: operation.Attempt.AttemptNumber}, CostStatus: costStatus(dispatch.Response.Cost), CostMethod: dispatch.Response.Cost.Method, CostCatalogVersion: dispatch.Response.Cost.CatalogVersion, UnknownReason: unknownReason},
	})
	if err != nil {
		return durablestore.CompactFinalization{}, binding.failPostResponseCheckpoint(ctx, route, reservation, dispatch.Response, err)
	}
	mapped, err := compactResponseFromNormalized(dispatch.Response, request.Parent, "miss_populated")
	if err != nil {
		return durablestore.CompactFinalization{}, err
	}
	if request.CostAdmission != nil {
		admission := *request.CostAdmission
		mapped.CostAdmission = &admission
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
	provenance, err := json.Marshal(map[string]any{"route": response.Route, "service": response.Service, "provider": response.Provider})
	if err != nil {
		return llm.CompactResponseV1{}, err
	}
	return llm.CompactResponseV1{APIVersion: llm.CompactAPIVersion, OperationKey: response.OperationKey, OperationID: response.OperationID, Checkpoint: llm.CheckpointMetadata{Handle: handle, Parent: &parent, Kind: "compaction"}, Cache: llm.CacheDispositionV1{Disposition: cacheDisposition}, Provenance: provenance, Usage: &response.Usage, Cost: cost, Diagnostics: response.Diagnostics}, nil
}

func (binding *productionPhaseBinding) finalizeCompactCache(context.Context, llm.CompactRequestV1, durablestore.CompactReplay, durablestore.CompactCacheDecision) (durablestore.CompactFinalization, error) {
	return durablestore.CompactFinalization{}, errors.New("compact cache finalization cannot run when durable cache is disabled")
}

func (binding *productionPhaseBinding) reconcileCompact(ctx context.Context, _ llm.CompactRequestV1, route durablestore.RoutePlan, reservation durablestore.ReserveResult, finalization durablestore.CompactFinalization) error {
	operation, err := binding.composition.Operations.Get(context.WithoutCancel(ctx), string(route.OperationID))
	if err != nil {
		return err
	}
	if operation.CompletedAt.IsZero() {
		return errors.New("persisted compact completion timestamp is unavailable")
	}
	events, err := completionEvents(route, reservation, finalization.Response.Cost, operation.CompletedAt)
	if err != nil {
		return err
	}
	for _, event := range events {
		if _, err := binding.composition.Journal.AppendCompletion(context.WithoutCancel(ctx), event); err != nil {
			return err
		}
	}
	return binding.composition.Materializer.Reconcile(context.WithoutCancel(ctx), durablestore.ReconcileRequest{OperationID: route.OperationID, GenerationID: route.GenerationID, IncarnationID: reservation.IncarnationID, Events: events})
}
