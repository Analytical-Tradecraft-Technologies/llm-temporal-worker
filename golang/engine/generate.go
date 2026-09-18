package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

const (
	operationDomain        = "llmtw/operation/v1\x00"
	providerHandleMedia    = "application/vnd.llmtw.provider-handle"
	defaultLease           = time.Minute
	defaultRetention       = 24 * time.Hour
	defaultContinuationTTL = 24 * time.Hour
)

type quotedCandidate struct {
	candidate    routing.Candidate
	entry        *pricing.Entry
	estimate     budget.Estimate
	reservations []admission.WindowReservation
}

func (candidate quotedCandidate) priceKnown() bool { return candidate.entry != nil }

func (candidate quotedCandidate) priceVersion() string {
	if candidate.entry == nil {
		return ""
	}
	return candidate.entry.Version
}

type quotedPlan struct {
	candidates []quotedCandidate
	maximum    pricing.MicroUSD
	// maximumUSD carries the authoritative exact reservation alongside the
	// legacy microUSD compatibility amount used by Redis admission stores.
	// Keeping both values lets exact PostgreSQL operation records avoid a
	// lossy conversion while older stores continue to enforce their bounded
	// integer representation.
	maximumUSD pricing.USD
}

func (engine *Engine) Generate(ctx context.Context, request llm.Request) (response llm.Response, resultErr error) {
	if engine == nil {
		return llm.Response{}, engineError(provider.CodeInternal, provider.PhaseNormalize, provider.DispatchNotDispatched, provider.RetryNever, "engine is nil", nil)
	}
	if err := ctx.Err(); err != nil {
		return llm.Response{}, engineError(provider.CodeCanceled, provider.PhaseNormalize, provider.DispatchNotDispatched, provider.RetryNever, "request canceled", err)
	}
	ctx, requestSpan := engine.startTrace(ctx, "llmtw.generate", requestTraceAttrs(request)...)
	defer func() {
		if resultErr != nil {
			engine.recordTraceError(ctx, requestSpan, resultErr)
		}
		requestSpan.End()
	}()
	normalizeCtx, normalizeSpan := engine.startTrace(ctx, "llmtw.normalize", requestTraceAttrs(request)...)
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		engine.recordTraceError(normalizeCtx, normalizeSpan, err)
		normalizeSpan.End()
		return llm.Response{}, engineError(provider.CodeInvalidArgument, provider.PhaseNormalize, provider.DispatchNotDispatched, provider.RetryNever, "request normalization failed", err)
	}
	digest, err := llm.RequestDigest(normalized)
	if err != nil {
		engine.recordTraceError(normalizeCtx, normalizeSpan, err)
		normalizeSpan.End()
		return llm.Response{}, engineError(provider.CodeInvalidArgument, provider.PhaseNormalize, provider.DispatchNotDispatched, provider.RetryNever, "request digest failed", err)
	}
	normalizeSpan.End()
	ctx = normalizeCtx
	planCtx, planSpan := engine.startTrace(ctx, "llmtw.planning", requestTraceAttrs(normalized)...)
	snapshot, err := engine.dependencies.Snapshots.Current(planCtx)
	if err != nil {
		engine.recordTraceError(planCtx, planSpan, err)
		planSpan.End()
		return llm.Response{}, engineError(provider.CodeConfiguration, provider.PhasePlan, provider.DispatchNotDispatched, provider.RetryNever, "configuration snapshot unavailable", err)
	}
	now := engine.dependencies.Clock()
	stateCtx, stateSpan := engine.startTrace(planCtx, "llmtw.state.load", requestTraceAttrs(normalized)...)
	providerRequest, constraints, parent, err := engine.loadContinuation(stateCtx, normalized, now)
	if err != nil {
		engine.recordTraceError(stateCtx, stateSpan, err)
		stateSpan.End()
		planSpan.End()
		return llm.Response{}, err
	}
	stateSpan.End()
	if err := engine.beat(planCtx, Progress{Phase: "planning", At: now}); err != nil {
		engine.recordTraceError(planCtx, planSpan, err)
		planSpan.End()
		return llm.Response{}, err
	}
	plan, err := engine.dependencies.Planner.Plan(planCtx, routingInput(normalized, snapshot, constraints, now))
	if err != nil {
		engine.recordTraceError(planCtx, planSpan, err)
		planSpan.End()
		return llm.Response{}, engineError(provider.CodeNoRoute, provider.PhasePlan, provider.DispatchNotDispatched, provider.RetryNever, "no eligible route", err)
	}
	quoted, err := engine.quotePlan(planCtx, normalized, plan, snapshot, now)
	if err != nil {
		engine.recordTraceError(planCtx, planSpan, err)
		planSpan.End()
		return llm.Response{}, err
	}
	planSpan.End()
	ctx = planCtx
	operationID, scopeKey := operationIdentity(normalized, digest)
	admissionCtx, admissionSpan := engine.startTrace(ctx, "llmtw.admission", requestTraceAttrs(normalized)...)
	operation, existing, err := engine.beginOrResume(admissionCtx, normalized, snapshot, operationID, scopeKey, digest, quoted, now)
	if err != nil {
		engine.recordTraceError(admissionCtx, admissionSpan, err)
		admissionSpan.End()
		return llm.Response{}, err
	}
	admissionSpan.End()
	ctx = admissionCtx
	if existing {
		response, resumed, err := engine.resolveExisting(ctx, operation, now)
		if err != nil {
			return llm.Response{}, err
		}
		if response != nil {
			return *response, nil
		}
		if !resumed {
			return llm.Response{}, engineError(provider.CodeOperationConflict, provider.PhaseAdmission, provider.DispatchNotDispatched, provider.RetrySameOperation, "operation is already in progress", nil)
		}
	}
	if err := engine.beat(ctx, Progress{OperationID: operation.ID, Phase: "admission", At: now}); err != nil {
		return llm.Response{}, err
	}
	return engine.dispatchPlan(ctx, normalized, providerRequest, snapshot, quoted, operation, parent)
}

func (engine *Engine) loadContinuation(ctx context.Context, request llm.Request, now time.Time) (llm.Request, state.Constraints, *state.Continuation, error) {
	if request.Continuation == nil {
		return request, state.Constraints{}, nil, nil
	}
	if engine.dependencies.Continuations == nil {
		return llm.Request{}, state.Constraints{}, nil, engineError(provider.CodeStateUnavailable, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetrySameOperation, "continuation store is unavailable", nil)
	}
	handle := state.Handle(request.Continuation.Handle)
	if handle == "" {
		return llm.Request{}, state.Constraints{}, nil, engineError(provider.CodeInvalidArgument, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever, "continuation handle is empty", nil)
	}
	if request.Context.Tenant == "" {
		return llm.Request{}, state.Constraints{}, nil, engineError(provider.CodeInvalidArgument, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever, "continuation tenant is required", state.ErrTenantMismatch)
	}
	continuation, err := engine.dependencies.Continuations.GetForTenant(ctx, request.Context.Tenant, handle)
	if err != nil {
		code := provider.CodeStateUnavailable
		if errors.Is(err, state.ErrInvalidHandle) || errors.Is(err, state.ErrNotFound) || errors.Is(err, state.ErrTenantMismatch) || errors.Is(err, state.ErrExpired) {
			code = provider.CodeInvalidArgument
		}
		return llm.Request{}, state.Constraints{}, nil, engineError(code, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever, "continuation could not be loaded", err)
	}
	if err := continuation.Validate(now); err != nil {
		return llm.Request{}, state.Constraints{}, nil, engineError(provider.CodeStateCorrupt, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever, "continuation is invalid", err)
	}
	if metrics := observability.MetricsFromContext(ctx); metrics != nil {
		metrics.RecordContinuation("reused")
	}
	providerValue := toProviderContinuation(continuation)
	request.Continuation = providerValue
	return request, continuation.Constraints(request.Portability), continuationPointer(continuation), nil
}

func continuationPointer(value state.Continuation) *state.Continuation {
	clone := value.Clone()
	return &clone
}

func toProviderContinuation(continuation state.Continuation) *llm.Continuation {
	result := &llm.Continuation{EndpointID: continuation.Pinning.EndpointID, Model: continuation.Pinning.ModelLineage, Pinned: !continuation.Pinning.Empty()}
	for _, value := range continuation.ProviderState {
		if value.Media == providerHandleMedia {
			result.Handle = string(value.Data)
			continue
		}
		result.ProviderStates = append(result.ProviderStates, llm.ProviderState{Provider: value.Provider, EndpointFamily: value.Family, MediaType: value.Media, Opaque: append([]byte(nil), value.Data...)})
	}
	return result
}

func (engine *Engine) quotePlan(ctx context.Context, request llm.Request, plan routing.Plan, snapshot Snapshot, now time.Time) (quotedPlan, error) {
	if snapshot.Prices == nil {
		return quotedPlan{}, engineError(provider.CodeConfiguration, provider.PhasePrice, provider.DispatchNotDispatched, provider.RetryNever, "pricing resolver is unavailable", nil)
	}
	quoted := quotedPlan{candidates: make([]quotedCandidate, 0, len(plan.Candidates))}
	skippedForBudgetMatch := false
	skippedForPrice := false
	for _, candidate := range plan.Candidates {
		if err := ctx.Err(); err != nil {
			return quotedPlan{}, engineError(provider.CodeCanceled, provider.PhasePrice, provider.DispatchNotDispatched, provider.RetryNever, "pricing canceled", err)
		}
		matches := budget.MatchPolicies(snapshot.BudgetPolicies, budget.ContextFor(request, candidate, snapshot.Environment))
		if snapshot.RequireBudgetMatch && len(matches) == 0 {
			skippedForBudgetMatch = true
			continue
		}
		quote, err := snapshot.Prices.Resolve(pricing.Query{
			Provider: candidate.Provider, Family: candidate.Family, EndpointID: candidate.EndpointID,
			Region: candidate.Region, Model: candidate.Model, ProviderTier: candidate.ProviderTier, At: now,
		})
		if err != nil {
			if errors.Is(err, pricing.ErrNoActivePrice) {
				if len(matches) > 0 || !snapshot.RequirePriceWhenBudgeted {
					skippedForPrice = true
					continue
				}
				quoted.candidates = append(quoted.candidates, quotedCandidate{candidate: candidate})
				continue
			}
			return quotedPlan{}, engineError(provider.CodeConfiguration, provider.PhasePrice, provider.DispatchNotDispatched, provider.RetryNever, "candidate has no active price", err)
		}
		entry := quote.Entry
		if entry.Version == "" {
			entry.Version = quote.CatalogVersion
		}
		estimate, err := engine.dependencies.Estimator.EstimateCandidate(request, candidate, entry)
		if err != nil {
			if errors.Is(err, budget.ErrUnusablePrice) {
				// A partial entry or a compatibility-boundary overflow is not a
				// safe reservation for this candidate. Unlike a missing quote,
				// an active partial entry must never be dispatched as unknown:
				// its presence cannot authorize an unpriced operation. Continue
				// to the next ordered fallback instead.
				skippedForPrice = true
				continue
			}
			return quotedPlan{}, engineError(provider.CodeInvalidArgument, provider.PhasePrice, provider.DispatchNotDispatched, provider.RetryNever, "candidate cost estimate failed", err)
		}
		reservations := reservations(matches, estimate.MicroUSD, estimate.CostUSD, now)
		quoted.candidates = append(quoted.candidates, quotedCandidate{candidate: candidate, entry: &entry, estimate: estimate, reservations: reservations})
		if estimate.MicroUSD > quoted.maximum {
			quoted.maximum = estimate.MicroUSD
		}
		if estimate.CostUSD.Cmp(quoted.maximumUSD) > 0 {
			quoted.maximumUSD = estimate.CostUSD
		}
	}
	if len(quoted.candidates) == 0 {
		reason, message := "no_eligible_price", "no candidate has a usable price"
		if skippedForBudgetMatch && !skippedForPrice {
			reason, message = "no_matching_budget_policy", "no candidate matches a budget policy"
		}
		failure := engineError(provider.CodeNoRoute, provider.PhasePrice, provider.DispatchNotDispatched, provider.RetryNever, message, nil)
		failure.SafeDetails = map[string]string{"reason": reason}
		return quotedPlan{}, failure
	}
	return quoted, nil
}

func reservations(matches []budget.MatchedWindow, amount pricing.MicroUSD, amountUSD pricing.USD, now time.Time) []admission.WindowReservation {
	result := make([]admission.WindowReservation, 0, len(matches))
	for _, value := range matches {
		_, bucket := value.Window.Range(now)
		result = append(result, admission.WindowReservation{
			PolicyID: value.PolicyID, WindowID: value.Window.ID, Bucket: bucket, Amount: amount,
			Limit: value.Window.Limit, AmountUSD: amountUSD, LimitUSD: value.Window.LimitUSD,
			BucketNanos: value.Window.Bucket.Nanoseconds(), DurationNanos: value.Window.Duration.Nanoseconds(),
		})
	}
	return result
}

func (engine *Engine) beginOrResume(ctx context.Context, request llm.Request, snapshot Snapshot, operationID, scopeKey string, digest [32]byte, quoted quotedPlan, now time.Time) (admission.Operation, bool, error) {
	lease := snapshot.ReservationLease
	if lease <= 0 {
		lease = defaultLease
	}
	retention := snapshot.OperationRetention
	if retention <= 0 {
		retention = defaultRetention
	}
	reservations := aggregateReservations(quoted.candidates, quoted.maximumUSD)
	result, err := engine.dependencies.Admission.Begin(ctx, admission.BeginRequest{
		ID: operationID, ScopeKey: scopeKey, RequestDigest: digest, Reservation: quoted.maximum,
		ReservationUSD: quoted.maximumUSD,
		Reservations:   reservations, ConfigVersion: snapshot.Version, PriceVersion: priceVersion(quoted.candidates),
		LeaseUntil: now.Add(lease), ExpiresAt: now.Add(retention),
	})
	if err != nil {
		if errors.Is(err, admission.ErrOperationConflict) {
			return admission.Operation{}, false, engineError(provider.CodeOperationConflict, provider.PhaseAdmission, provider.DispatchNotDispatched, provider.RetryNever, "operation key is already bound to another request", err)
		}
		return admission.Operation{}, false, engineError(provider.CodeStateUnavailable, provider.PhaseAdmission, provider.DispatchNotDispatched, provider.RetrySameOperation, "admission failed", err)
	}
	if result.Denied != nil {
		if metrics := observability.MetricsFromContext(ctx); metrics != nil {
			metrics.RecordBudgetAdmission(result.Denied.PolicyID, "denied")
		}
		mapped := engineError(provider.CodeBudgetDenied, provider.PhaseAdmission, provider.DispatchNotDispatched, provider.RetryAfter, "budget reservation denied", nil)
		mapped.RetryAfter = result.Denied.RetryAfter
		mapped.SafeDetails = map[string]string{"policy_id": result.Denied.PolicyID, "window_id": result.Denied.WindowID}
		return admission.Operation{}, false, mapped
	}
	if !result.Existing {
		recordOperationState(ctx, result.Operation.State)
		recordAdmission(ctx, result.Operation.Reservations, "accepted")
	}
	return result.Operation, result.Existing, nil
}

func (engine *Engine) resolveExisting(ctx context.Context, operation admission.Operation, now time.Time) (*llm.Response, bool, error) {
	switch operation.State {
	case admission.StateCompleted:
		response, err := engine.dependencies.Results.Get(ctx, operation.ID)
		if err != nil {
			return nil, false, engineError(provider.CodeStateCorrupt, provider.PhaseFinalize, provider.DispatchAccepted, provider.RetryNever, "completed operation result is unavailable", err)
		}
		return &response, false, nil
	case admission.StateDispatching:
		response, err := engine.dependencies.Results.Get(ctx, operation.ID)
		if err == nil {
			actual := pricing.MicroUSD(0)
			if response.Cost.ActualCostUSD != nil {
				actual, err = compatibilityActualMicroUSD(*response.Cost.ActualCostUSD)
				if err != nil {
					return nil, false, engineError(provider.CodeStateCorrupt, provider.PhaseFinalize, provider.DispatchAccepted, provider.RetryNever, "stored result cost is invalid", err)
				}
			}
			if !actual.Valid() {
				return nil, false, engineError(provider.CodeStateCorrupt, provider.PhaseFinalize, provider.DispatchAccepted, provider.RetryNever, "stored result cost is invalid", nil)
			}
			finalCtx, cancel := engine.finalizationContext(ctx)
			defer cancel()
			if completeErr := engine.dependencies.Admission.Complete(finalCtx, admission.CompleteRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Actual: actual, Attempt: operation.Attempt}); completeErr != nil {
				return nil, false, engineError(provider.CodeStateUnavailable, provider.PhaseFinalize, provider.DispatchAccepted, provider.RetrySameOperation, "operation finalization failed", completeErr)
			}
			recordCompletion(ctx, response)
			return &response, false, nil
		}
		if !errors.Is(err, ErrResultNotFound) {
			return nil, false, engineError(provider.CodeStateUnavailable, provider.PhaseFinalize, provider.DispatchAccepted, provider.RetrySameOperation, "in-flight result lookup failed", err)
		}
		// A resumable adapter may recover the accepted operation by the
		// deterministic idempotency key. dispatchPlan pins the recorded route
		// and invokes that lookup; one-shot adapters fail closed there without
		// issuing a second provider request.
		return nil, true, nil
	case admission.StateAmbiguous:
		return nil, false, engineError(provider.CodeAmbiguousDispatch, provider.PhaseAdmission, provider.DispatchAmbiguous, provider.RetryNever, "operation may have reached the provider", nil)
	case admission.StateProviderPending:
		// The provider id is authoritative durable state. dispatchPlan will
		// load it through the optional ProviderPendingStore and poll only; it
		// must never submit a second operation on an Activity retry.
		return nil, true, nil
	case admission.StateReserved:
		if !operation.LeaseUntil.IsZero() && now.Before(operation.LeaseUntil) {
			return nil, false, nil
		}
		return nil, true, nil
	case admission.StateDefiniteFailed, admission.StateCanceled:
		return nil, false, engineError(provider.CodeOperationConflict, provider.PhaseAdmission, provider.DispatchNotDispatched, provider.RetryNever, "operation is already terminal", nil)
	default:
		return nil, false, engineError(provider.CodeStateCorrupt, provider.PhaseAdmission, provider.DispatchNotDispatched, provider.RetryNever, "operation state is unknown", nil)
	}
}

func compatibilityActualMicroUSD(exact pricing.USD) (pricing.MicroUSD, error) {
	return pricing.CeilMicroFromUSD(exact)
}

func operationIdentity(request llm.Request, digest [32]byte) (string, string) {
	scope := request.Context.Tenant + "\x00" + request.OperationKey
	prefixLen := len(operationDomain) + len(digest)
	if len(scope) > math.MaxInt-prefixLen {
		identity := sha256.Sum256(digest[:])
		return hex.EncodeToString(identity[:]), scope
	}
	data := make([]byte, 0, prefixLen+len(scope))
	data = append(data, operationDomain...)
	data = append(data, scope...)
	data = append(data, digest[:]...)
	identity := sha256.Sum256(data)
	return hex.EncodeToString(identity[:]), scope
}

func priceVersion(candidates []quotedCandidate) string {
	first := ""
	for _, candidate := range candidates {
		if !candidate.priceKnown() {
			continue
		}
		version := candidate.priceVersion()
		if first == "" {
			first = version
			continue
		}
		if version != first {
			return "mixed"
		}
	}
	return first
}

func aggregateReservations(candidates []quotedCandidate, expectedUSD pricing.USD) []admission.WindowReservation {
	type key struct{ policy, window string }
	byKey := make(map[key]admission.WindowReservation)
	for _, candidate := range candidates {
		for _, reservation := range candidate.reservations {
			id := key{policy: reservation.PolicyID, window: reservation.WindowID}
			current, ok := byKey[id]
			if !ok || reservation.Amount > current.Amount || (reservation.Amount == current.Amount && reservation.AmountUSD.Cmp(current.AmountUSD) > 0) {
				byKey[id] = reservation
			}
		}
	}
	result := make([]admission.WindowReservation, 0, len(byKey))
	for _, reservation := range byKey {
		// The scalar reservation is the maximum exact estimate across all
		// eligible candidates. Every union-window reservation must carry that
		// same exact amount so the admission envelope remains self-consistent;
		// the legacy MicroUSD amount is still selected conservatively per
		// window above.
		if !expectedUSD.IsZero() {
			reservation.AmountUSD = expectedUSD
		}
		result = append(result, reservation)
	}
	// Map iteration must not affect admission payloads or digests.
	sortReservations(result)
	return result
}

func sortReservations(values []admission.WindowReservation) {
	for i := 1; i < len(values); i++ {
		value := values[i]
		j := i - 1
		for j >= 0 && (values[j].PolicyID > value.PolicyID || (values[j].PolicyID == value.PolicyID && values[j].WindowID > value.WindowID)) {
			values[j+1] = values[j]
			j--
		}
		values[j+1] = value
	}
}

func engineError(code provider.Code, phase provider.Phase, dispatch provider.DispatchCertainty, retry provider.RetryDisposition, message string, cause error) *provider.Error {
	result := provider.NewError(code, phase, dispatch, retry, message)
	result.Cause = cause
	return result
}
