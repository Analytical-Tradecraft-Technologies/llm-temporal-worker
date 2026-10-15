package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

// Budget planners quote the selected route and resolve its configured windows.
// They must use snapshot-owned configuration and preserve the complete request
// (including ExpiresAt) when retrying uncertain acceptance of the same operation.
// The Redis leaser owns the 15-minute start deadline; planners do not renew it.
type GenerateBudgetPlanner func(context.Context, llm.GenerateRequestV1, durable.RoutePlan) (durable.ReserveRequest, error)
type CompactBudgetPlanner func(context.Context, llm.CompactRequestV1, durable.RoutePlan) (durable.ReserveRequest, error)

// BudgetAdmission supplies the Reserve and Claim ports for both execution paths.
// It holds no per-request state: Redis is authoritative across workers/retries.
// Settlement remains in the durable finalization path, after result persistence.
type BudgetAdmission struct {
	boundary durable.BudgetBoundary
	generate GenerateBudgetPlanner
	compact  CompactBudgetPlanner
}

// NewBudgetAdmission captures the composition already validated by the complete
// runtime builder. It never invokes a composition factory, creates clients, or
// falls back to a separate leaser supplied in another capability field.
func (capabilities V1RuntimeCapabilities) NewBudgetAdmission(generate GenerateBudgetPlanner, compact CompactBudgetPlanner) (*BudgetAdmission, error) {
	composition, ok := capabilities.DurableComposition()
	if !ok {
		return nil, errors.New("budget admission requires a bound durable composition")
	}
	if err := capabilities.validateDurableComposition(composition); err != nil {
		return nil, err
	}
	if generate == nil || compact == nil {
		return nil, errors.New("Generate and Compact budget planners are required")
	}
	return &BudgetAdmission{boundary: composition.BudgetBoundary(), generate: generate, compact: compact}, nil
}

// ReserveGenerate returns acquired or wait immediately; workflow timers own any
// waiting. No provider submission is performed by this callback.
func (admission *BudgetAdmission) ReserveGenerate(ctx context.Context, request llm.GenerateRequestV1, route durable.RoutePlan) (durable.ReserveResult, error) {
	_, requestErr := request.MarshalJSON()
	if err := admission.validate(ctx, route, requestErr); err != nil {
		return durable.ReserveResult{}, err
	}
	planned, err := admission.generate(ctx, request, route)
	return admission.reserve(ctx, route, planned, err)
}

func (admission *BudgetAdmission) ReserveCompact(ctx context.Context, request llm.CompactRequestV1, route durable.RoutePlan) (durable.ReserveResult, error) {
	_, requestErr := request.MarshalJSON()
	if err := admission.validate(ctx, route, requestErr); err != nil {
		return durable.ReserveResult{}, err
	}
	planned, err := admission.compact(ctx, request, route)
	return admission.reserve(ctx, route, planned, err)
}

// ClaimGenerate consumes the reservation's single-use permission to start. It
// must be called immediately before submission. An uncertain/duplicate claim
// never grants permission to dispatch, refund, or reuse the same paid attempt.
func (admission *BudgetAdmission) ClaimGenerate(ctx context.Context, request llm.GenerateRequestV1, route durable.RoutePlan, reservation durable.ReserveResult) (durable.ClaimReceipt, error) {
	_, requestErr := request.MarshalJSON()
	if err := admission.validate(ctx, route, requestErr); err != nil {
		return durable.ClaimReceipt{}, err
	}
	return admission.claim(ctx, route, reservation)
}

func (admission *BudgetAdmission) ClaimCompact(ctx context.Context, request llm.CompactRequestV1, route durable.RoutePlan, reservation durable.ReserveResult) (durable.ClaimReceipt, error) {
	_, requestErr := request.MarshalJSON()
	if err := admission.validate(ctx, route, requestErr); err != nil {
		return durable.ClaimReceipt{}, err
	}
	return admission.claim(ctx, route, reservation)
}

func (admission *BudgetAdmission) validate(ctx context.Context, route durable.RoutePlan, requestErr error) error {
	if ctx == nil || admission == nil || admission.generate == nil || admission.compact == nil || admission.boundary.Validate() != nil {
		return budgetAdmissionError(provider.CodeConfiguration, provider.DispatchNotDispatched, provider.RetryNever)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if requestErr != nil || route.Validate() != nil {
		return budgetAdmissionError(provider.CodeInvalidArgument, provider.DispatchNotDispatched, provider.RetryNever)
	}
	return nil
}

func (admission *BudgetAdmission) reserve(ctx context.Context, route durable.RoutePlan, request durable.ReserveRequest, planErr error) (durable.ReserveResult, error) {
	if err := ctx.Err(); err != nil {
		return durable.ReserveResult{}, err
	}
	if planErr != nil || request.OperationID != route.OperationID || request.GenerationID != route.GenerationID || len(request.Reservations) == 0 {
		return durable.ReserveResult{}, budgetAdmissionError(provider.CodeConfiguration, provider.DispatchNotDispatched, provider.RetryNever)
	}
	var lifecycle durable.Lifecycle
	if err := lifecycle.Advance(durable.PhaseOperationReplay); err != nil {
		return durable.ReserveResult{}, err
	}
	reservation, err := admission.boundary.Reserve(ctx, &lifecycle, request)
	if err != nil {
		code, retry := provider.CodeStateUnavailable, provider.RetrySameOperation
		if errors.Is(err, durable.ErrLeaseExpired) {
			code, retry = provider.CodeBudgetDenied, provider.RetryAfter
		} else if errors.Is(err, durable.ErrBudgetBoundaryInvalid) {
			code, retry = provider.CodeStateCorrupt, provider.RetryNever
		}
		// Acceptance may have committed even if its reply was lost. Retrying
		// identical admission is safe; releasing it or changing the quote is not.
		return durable.ReserveResult{}, budgetAdmissionError(code, provider.DispatchNotDispatched, retry)
	}
	if !reservation.Replayed {
		// A replay (Submit after Acquire, or a retried Acquire) repeats an
		// earlier decision; count each admission decision once.
		recordCloudBudgetAdmission(ctx, request, reservation.Result)
	}
	return reservation.Result, nil
}

func (admission *BudgetAdmission) claim(ctx context.Context, route durable.RoutePlan, result durable.ReserveResult) (durable.ClaimReceipt, error) {
	return admission.claimWith(ctx, route, result, false)
}

// claimFenced is claim for the holder of the attempt's durable execution
// fence, the only caller that ever claims this reservation, so Redis's answer
// is its own (#1312). A claim that succeeded is kept even if the context ended
// afterwards, and an uncertain reply is resolved by replaying the claim once:
// already_claimed, on that replay or on a client resend of the first request,
// proves the claim applied. It is also how recovery probes an attempt that
// never recorded the outcome of its claim.
func (admission *BudgetAdmission) claimFenced(ctx context.Context, route durable.RoutePlan, result durable.ReserveResult) (durable.ClaimReceipt, error) {
	return admission.claimWith(ctx, route, result, true)
}

// claimConfirmTimeout bounds the replay that resolves an uncertain claim. It
// is detached from the caller, whose context may be what ended the first try.
const claimConfirmTimeout = 5 * time.Second

func (admission *BudgetAdmission) claimWith(ctx context.Context, route durable.RoutePlan, result durable.ReserveResult, fenced bool) (durable.ClaimReceipt, error) {
	request := durable.ReserveRequest{OperationID: route.OperationID, GenerationID: route.GenerationID}
	if !result.Accepted || result.Validate(request) != nil {
		return durable.ClaimReceipt{}, budgetAdmissionError(provider.CodeInvalidArgument, provider.DispatchNotDispatched, provider.RetryNever)
	}
	seen := make(map[string]bool, len(result.Events))
	for _, event := range result.Events {
		if seen[event.EventID] {
			return durable.ClaimReceipt{}, budgetAdmissionError(provider.CodeInvalidArgument, provider.DispatchNotDispatched, provider.RetryNever)
		}
		seen[event.EventID] = true
	}
	var lifecycle durable.Lifecycle
	for _, phase := range []durable.Phase{durable.PhaseOperationReplay, durable.PhaseRedisAccepted} {
		if err := lifecycle.Advance(phase); err != nil {
			return durable.ClaimReceipt{}, err
		}
	}
	reservation := durable.BudgetReservation{Result: result}
	if err := ctx.Err(); err != nil {
		return durable.ClaimReceipt{}, err
	}
	receipt, err := admission.boundary.Claim(ctx, &lifecycle, &reservation)
	if err != nil && fenced && !errors.Is(err, durable.ErrLeaseExpired) {
		receipt, err = admission.confirmClaim(ctx, &lifecycle, &reservation, err)
	}
	if err != nil {
		if errors.Is(err, durable.ErrLeaseExpired) {
			// durable_claim reports already_claimed before it checks the start
			// deadline, so expired proves nothing was claimed or dispatched.
			return durable.ClaimReceipt{}, budgetAdmissionError(provider.CodeBudgetDenied, provider.DispatchNotDispatched, provider.RetryAfter)
		}
		// Includes lost replies, duplicate claims, and malformed receipts. The
		// consumed lease stays charged; a new paid attempt needs fresh budget.
		return durable.ClaimReceipt{}, budgetAdmissionError(provider.CodeAmbiguousDispatch, provider.DispatchAmbiguous, provider.RetryNever)
	}
	if !fenced && ctx.Err() != nil {
		return durable.ClaimReceipt{}, budgetAdmissionError(provider.CodeAmbiguousDispatch, provider.DispatchAmbiguous, provider.RetryNever)
	}
	return receipt, nil
}

// confirmClaim resolves a fenced claim that failed without a definite answer.
// A replay answers claimed or already_claimed when the claim applied, either
// first time or now, and expired when it never did and no longer can.
func (admission *BudgetAdmission) confirmClaim(ctx context.Context, lifecycle *durable.Lifecycle, reservation *durable.BudgetReservation, cause error) (durable.ClaimReceipt, error) {
	if !errors.Is(cause, durable.ErrAlreadyClaimed) {
		replayCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), claimConfirmTimeout)
		defer cancel()
		receipt, err := admission.boundary.Claim(replayCtx, lifecycle, reservation)
		if !errors.Is(err, durable.ErrAlreadyClaimed) {
			return receipt, err
		}
	}
	result := reservation.Result
	receipt := durable.ClaimReceipt{OperationID: result.OperationID, GenerationID: result.GenerationID, IncarnationID: result.IncarnationID}
	if err := receipt.Validate(result); err != nil {
		return durable.ClaimReceipt{}, err
	}
	return receipt, nil
}

func budgetAdmissionError(code provider.Code, dispatch provider.DispatchCertainty, retry provider.RetryDisposition) error {
	return provider.NewError(code, provider.PhaseAdmission, dispatch, retry, "budget admission failed")
}
