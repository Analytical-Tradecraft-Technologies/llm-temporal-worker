package durable

import (
	"context"
	"errors"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func validatePendingDispatch(p *llm.PendingOperationV1, route RoutePlan, kind string) error {
	if p == nil {
		return errors.New("pending handle is required")
	}
	if err := p.Validate(); err != nil {
		return err
	}
	if p.OperationID != string(route.OperationID) || p.Kind != kind || p.EndpointID != route.EndpointID || p.Provider != route.Provider {
		return errors.New("pending handle does not match reserved route")
	}
	return nil
}

// PollFinalization is immutable, including its timestamp, across retries.
type PollFinalization struct {
	Response    llm.PollResponseV1
	FinalizedAt time.Time
}

// PollReplay is loaded from authoritative operation state. The implementation
// must authorize scope before disclosing a handle, result, or contacting a
// provider. Finalize persists the immutable outcome, returning the stored winner on
// concurrent finalization. The runner journals deterministic completion events
// for disaster recovery, then settles through Redis atomic deduplication.
// A retry after a lost Redis acknowledgement repeats Reconcile with the same
// event IDs and payload, never repeats submission or creates new charges.
type PollReplay struct {
	Context     llm.RequestContext
	Pending     llm.PendingOperationV1
	Completed   *PollFinalization
	Call        provider.Call
	Adapter     provider.ResumableAdapter
	Observer    provider.Observer
	Finalize    func(context.Context, provider.ResumableResult) (PollFinalization, error)
	Reservation ReserveResult
	Budget      BudgetMaterializer
	Journal     Journal
}
type PollPorts struct {
	Load func(context.Context, llm.PollRequestV1) (PollReplay, error)
}

func (p PollPorts) Validate() error { return requiredPort("poll load", p.Load) }

func PollV1(ctx context.Context, request llm.PollRequestV1, ports PollPorts) (llm.PollResponseV1, error) {
	var zero llm.PollResponseV1
	if ctx == nil {
		return zero, errors.New("poll context is nil")
	}
	if err := request.Validate(); err != nil {
		return zero, err
	}
	if err := ports.Validate(); err != nil {
		return zero, err
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	replay, err := ports.Load(ctx, request)
	if err != nil {
		return zero, err
	}
	if replay.Pending != request.Pending || replay.Context.Tenant != request.Context.Tenant || replay.Context.Project != request.Context.Project {
		return zero, errors.New("poll operation scope or identity mismatch")
	}
	if isNilPort(replay.Budget) || isNilPort(replay.Journal) || string(replay.Reservation.OperationID) != request.Pending.OperationID {
		return zero, errors.New("poll settlement port is required")
	}
	finish := func(finalization PollFinalization) (llm.PollResponseV1, error) {
		result := finalization.Response
		if result.Pending != request.Pending || result.Status == "pending" {
			return zero, errors.New("invalid finalized poll result")
		}
		if err := result.Validate(); err != nil {
			return zero, err
		}
		var actual *pricing.USD
		var cost llm.CostV1
		if result.Generate != nil {
			cost = result.Generate.Cost
		}
		if result.Compact != nil {
			cost = result.Compact.Cost
		}
		if cost.Status == "exact" && cost.ActualCostUSD != nil {
			value, err := pricing.ParseUSD(*cost.ActualCostUSD)
			if err != nil {
				return zero, err
			}
			actual = &value
		}
		settlement, err := PollSettlement(replay.Reservation, actual, finalization.FinalizedAt)
		if err != nil {
			return zero, err
		}
		for _, event := range settlement.Events {
			if _, err := replay.Journal.AppendCompletion(ctx, event); err != nil {
				return zero, generateReconciliationError(RoutePlan{OperationID: replay.Reservation.OperationID}, err)
			}
		}
		if err := replay.Budget.Reconcile(ctx, settlement); err != nil {
			return zero, generateReconciliationError(RoutePlan{OperationID: replay.Reservation.OperationID}, err)
		}
		return result, nil
	}
	if replay.Completed != nil {
		return finish(*replay.Completed)
	}
	if isNilPort(replay.Adapter) || replay.Finalize == nil {
		return zero, errors.New("poll provider and finalization ports are required")
	}
	if replay.Call.EndpointID != request.Pending.EndpointID || replay.Call.OperationKey == "" {
		return zero, errors.New("poll call identity mismatch")
	}
	// Exactly one status check. No sleeps, Submit, admission or local retry loop.
	outcome, err := replay.Adapter.Poll(ctx, replay.Call, request.Pending.ProviderOperationID, replay.Observer)
	if err != nil {
		return zero, err
	}
	if err = outcome.ValidateForCall(replay.Call); err != nil {
		return zero, err
	}
	if outcome.ProviderOperationID != "" && outcome.ProviderOperationID != request.Pending.ProviderOperationID {
		return zero, errors.New("poll provider identity changed")
	}
	switch outcome.State {
	case provider.ResumablePending:
		return llm.PollResponseV1{Status: "pending", Pending: request.Pending}, nil
	case provider.ResumableNotFound:
		// Missing/expired results are terminal unknown-cost failures. Finalize must
		// persist that ambiguity; settlement retains the reservation.
		outcome.State = provider.ResumableFailed
		outcome.ProviderOperationID = request.Pending.ProviderOperationID
		outcome.Failure = provider.NewError(provider.CodeAmbiguousDispatch, provider.PhasePoll, provider.DispatchAmbiguous, provider.RetryNever, "provider operation unavailable")
		fallthrough
	case provider.ResumableCompleted, provider.ResumableFailed:
		result, err := replay.Finalize(ctx, outcome)
		if err != nil {
			return zero, err
		}
		if (outcome.State == provider.ResumableFailed && result.Response.Status != "failed") || (outcome.State == provider.ResumableCompleted && result.Response.Status != "completed") {
			return zero, errors.New("finalization changed provider outcome")
		}
		return finish(result)
	default:
		return zero, errors.New("unexpected poll outcome")
	}
}

// DispatchProvider submits only for explicitly resumable adapters. Callers
// must journal admission and arm the dispatch observer before invoking it,
// and durably persist a pending outcome before returning an Activity result.
func DispatchProvider(ctx context.Context, adapter provider.Adapter, call provider.Call, observer provider.Observer) (provider.ResumableResult, error) {
	if isNilPort(adapter) {
		return provider.ResumableResult{}, errors.New("provider is required")
	}
	if resumable, ok := adapter.(provider.ResumableAdapter); ok {
		result, err := resumable.Submit(ctx, call, observer)
		if err != nil {
			return result, err
		}
		return result, result.ValidateForCall(call)
	}
	result, err := adapter.Invoke(ctx, call, observer)
	if err != nil {
		return provider.ResumableResult{}, err
	}
	outcome := provider.ResumableResult{State: provider.ResumableCompleted, Dispatch: provider.DispatchAccepted, Result: result}
	return outcome, outcome.ValidateForCall(call)
}
