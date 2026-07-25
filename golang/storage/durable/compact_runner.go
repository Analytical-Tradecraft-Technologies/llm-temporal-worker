package durable

// This file contains the storage-neutral orchestration for the v1 Compact
// Activity. Compact is deliberately a separate runner from Generate: it
// creates a compaction checkpoint, never returns a normal answer, and has a
// fixed cache variant of zero. The concrete Redis, PostgreSQL, checkpoint,
// and provider adapters are supplied by the snapshot-owned composition.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

// CompactReplay is the bounded state view produced by replay/materialization.
// A completed response is returned before cache lookup or any admission work
// so Temporal retries cannot create a second child for one operation key.
type CompactReplay struct {
	State     state.MaterializedState
	Completed *llm.CompactResponseV1
	// ReconciliationPending is populated when finalization committed but the
	// Redis completion event did not. A retry must run Reconcile before it can
	// return the completed response.
	ReconciliationPending *CompactReconciliation
}

// CompactReconciliation carries the durable identities required to retry the
// post-finalization Redis completion without dispatching the summarizer again.
type CompactReconciliation struct {
	Route        RoutePlan
	Reservation  ReserveResult
	Finalization CompactFinalization
}

// CompactCacheDecision is the route-isolated exact-cache result for Compact.
// A cache hit is a template only: the cache finalization port must create a
// new compaction child with the current operation identity and parent handle.
type CompactCacheDecision struct {
	Disposition CacheDisposition
	Response    *llm.CompactResponseV1
}

func (decision CompactCacheDecision) Validate() error {
	switch decision.Disposition {
	case CacheDisabled, CacheMiss:
		if decision.Response != nil {
			return errors.New("compact cache miss/disabled decision must not include a response")
		}
	case CacheHit:
		if decision.Response == nil {
			return errors.New("compact cache hit must include a response template")
		}
		if decision.Response.Cache.Variant != 0 {
			return errors.New("compact cache hit must use variant zero")
		}
		if _, err := json.Marshal(decision.Response); err != nil {
			return fmt.Errorf("compact cache response template: %w", err)
		}
	default:
		return fmt.Errorf("unknown compact cache disposition %d", decision.Disposition)
	}
	return nil
}

// CompactDispatchResult is the provider-neutral one-shot result passed to
// compaction finalization. Provider adapters must reject tool calls,
// structured output, and non-text content before returning this response.
type CompactDispatchResult struct {
	Response llm.Response
}

// CompactFinalization is the bounded response returned after the durable
// checkpoint/cache/cost finalization phase.
type CompactFinalization struct {
	Response llm.CompactResponseV1
}

// CompactPorts are the snapshot-bound production composition ports for one
// Compact operation. The order is intentionally distinct from Generate:
// replay -> exact cache -> route -> Redis reservation -> PostgreSQL journal
// -> one-shot summarizer dispatch -> PostgreSQL finalization -> reconciliation.
// Every callback must be idempotent across Temporal Activity retries.
type CompactPorts struct {
	Replay        func(context.Context, llm.CompactRequestV1) (CompactReplay, error)
	CacheLookup   func(context.Context, llm.CompactRequestV1, CompactReplay) (CompactCacheDecision, error)
	Route         func(context.Context, llm.CompactRequestV1, CompactReplay) (RoutePlan, error)
	Reserve       func(context.Context, llm.CompactRequestV1, RoutePlan) (ReserveResult, error)
	Journal       func(context.Context, llm.CompactRequestV1, RoutePlan, ReserveResult) (JournalReceipt, error)
	Dispatch      func(context.Context, llm.CompactRequestV1, CompactReplay, RoutePlan, JournalReceipt) (CompactDispatchResult, error)
	Finalize      func(context.Context, llm.CompactRequestV1, CompactReplay, RoutePlan, ReserveResult, CompactDispatchResult) (CompactFinalization, error)
	FinalizeCache func(context.Context, llm.CompactRequestV1, CompactReplay, CompactCacheDecision) (CompactFinalization, error)
	Reconcile     func(context.Context, llm.CompactRequestV1, RoutePlan, ReserveResult, CompactFinalization) error
}

func (ports CompactPorts) validate() error {
	fields := []struct {
		name string
		fn   any
	}{
		{"replay", ports.Replay}, {"cache lookup", ports.CacheLookup},
		{"route", ports.Route}, {"reserve", ports.Reserve},
		{"journal", ports.Journal}, {"dispatch", ports.Dispatch},
		{"finalize", ports.Finalize}, {"finalize cache", ports.FinalizeCache},
		{"reconcile", ports.Reconcile},
	}
	for _, field := range fields {
		if field.fn == nil {
			return fmt.Errorf("%w: %s port is required", ErrV1PortsInvalid, field.name)
		}
	}
	return nil
}

// CompactV1 executes one complete durable Compact composition. A cache hit
// still goes through FinalizeCache so it creates a distinct child checkpoint;
// it never returns the origin cache operation or parent checkpoint directly.
// A reconciliation failure is retryable after finalization and must not cause
// the provider dispatch to run again on the next Activity attempt.
func CompactV1(ctx context.Context, request llm.CompactRequestV1, ports CompactPorts) (llm.CompactResponseV1, error) {
	if ctx == nil {
		return llm.CompactResponseV1{}, fmt.Errorf("%w: context is nil", ErrV1PortsInvalid)
	}
	if err := ports.validate(); err != nil {
		return llm.CompactResponseV1{}, err
	}
	if _, err := json.Marshal(request); err != nil {
		return llm.CompactResponseV1{}, fmt.Errorf("%w: request is invalid: %v", ErrV1Stage, err)
	}
	if err := ctx.Err(); err != nil {
		return llm.CompactResponseV1{}, err
	}

	replay, err := ports.Replay(ctx, request)
	if err != nil {
		return llm.CompactResponseV1{}, stageError("compact replay", err)
	}
	if replay.ReconciliationPending != nil {
		pending := replay.ReconciliationPending
		if err := validateCompactPendingReconciliation(request, *pending); err != nil {
			return llm.CompactResponseV1{}, stageError("compact replay", err)
		}
		if replay.Completed != nil && replay.Completed.OperationID != pending.Finalization.Response.OperationID {
			return llm.CompactResponseV1{}, stageError("compact replay", errors.New("completed response does not match pending reconciliation"))
		}
		if err := ports.Reconcile(ctx, request, pending.Route, pending.Reservation, pending.Finalization); err != nil {
			return llm.CompactResponseV1{}, compactReconciliationError(pending.Route, err)
		}
		return pending.Finalization.Response, nil
	}
	if replay.Completed != nil {
		if err := validateCompactResponse(request, "", *replay.Completed); err != nil {
			return llm.CompactResponseV1{}, stageError("compact replay", err)
		}
		return *replay.Completed, nil
	}
	if err := validateCompactReplay(request, replay); err != nil {
		return llm.CompactResponseV1{}, stageError("compact replay", err)
	}

	cache, err := ports.CacheLookup(ctx, request, replay)
	if err != nil {
		return llm.CompactResponseV1{}, stageError("compact cache lookup", err)
	}
	if err := cache.Validate(); err != nil {
		return llm.CompactResponseV1{}, stageError("compact cache decision", err)
	}
	if cache.Disposition == CacheHit {
		finalization, err := ports.FinalizeCache(ctx, request, replay, cache)
		if err != nil {
			return llm.CompactResponseV1{}, stageError("compact cache finalization", err)
		}
		if err := validateCompactResponse(request, "", finalization.Response); err != nil {
			return llm.CompactResponseV1{}, stageError("compact cache finalization", err)
		}
		if finalization.Response.OperationID == cache.Response.OperationID || finalization.Response.Checkpoint.Handle == cache.Response.Checkpoint.Handle {
			return llm.CompactResponseV1{}, stageError("compact cache finalization", errors.New("cache hit must create a distinct operation and checkpoint"))
		}
		if finalization.Response.Cache.Disposition != "hit" {
			return llm.CompactResponseV1{}, stageError("compact cache finalization", errors.New("cache hit must produce hit disposition"))
		}
		if !compactZeroCost(finalization.Response.Cost) {
			return llm.CompactResponseV1{}, stageError("compact cache finalization", errors.New("cache hit must have exact zero cost"))
		}
		if err := validateWorkerCacheProvenance(finalization.Response.Provenance); err != nil {
			return llm.CompactResponseV1{}, stageError("compact cache finalization", err)
		}
		return finalization.Response, nil
	}

	route, err := ports.Route(ctx, request, replay)
	if err != nil {
		return llm.CompactResponseV1{}, stageError("compact route", err)
	}
	if err := route.Validate(); err != nil {
		return llm.CompactResponseV1{}, stageError("compact route", err)
	}
	reservation, err := ports.Reserve(ctx, request, route)
	if err != nil {
		return llm.CompactResponseV1{}, stageError("compact Redis reservation", err)
	}
	if err := validateCompactReservation(request, route, reservation); err != nil {
		return llm.CompactResponseV1{}, stageError("compact Redis reservation", err)
	}
	if !reservation.Accepted {
		mapped := provider.NewError(provider.CodeBudgetDenied, provider.PhaseAdmission, provider.DispatchNotDispatched, provider.RetryAfter, "budget reservation denied")
		mapped.OperationID = string(route.OperationID)
		mapped.RetryAfter = reservation.RetryAfter
		return llm.CompactResponseV1{}, fmt.Errorf("%w: %w", ErrReservationDenied, mapped)
	}

	journal, err := ports.Journal(ctx, request, route, reservation)
	if err != nil {
		return llm.CompactResponseV1{}, stageError("compact PostgreSQL journal", err)
	}
	if err := validateCompactJournal(reservation, journal); err != nil {
		return llm.CompactResponseV1{}, stageError("compact PostgreSQL journal", err)
	}
	dispatch, err := ports.Dispatch(ctx, request, replay, route, journal)
	if err != nil {
		return llm.CompactResponseV1{}, stageError("compact provider dispatch", err)
	}
	finalization, err := ports.Finalize(ctx, request, replay, route, reservation, dispatch)
	if err != nil {
		return llm.CompactResponseV1{}, stageError("compact PostgreSQL finalization", err)
	}
	if err := validateCompactResponse(request, route.OperationID, finalization.Response); err != nil {
		return llm.CompactResponseV1{}, stageError("compact PostgreSQL finalization", err)
	}
	if err := ports.Reconcile(ctx, request, route, reservation, finalization); err != nil {
		return llm.CompactResponseV1{}, compactReconciliationError(route, err)
	}
	return finalization.Response, nil
}

func validateCompactReservation(_ llm.CompactRequestV1, route RoutePlan, reservation ReserveResult) error {
	if err := reservation.Validate(ReserveRequest{OperationID: route.OperationID, GenerationID: route.GenerationID}); err != nil {
		return err
	}
	if reservation.OperationID != route.OperationID {
		return errors.New("compact reservation operation identity does not match route plan")
	}
	if reservation.GenerationID != route.GenerationID {
		return errors.New("compact reservation generation identity does not match route plan")
	}
	return nil
}

func validateCompactReplay(request llm.CompactRequestV1, replay CompactReplay) error {
	if replay.State.Handle == "" || string(replay.State.Handle) != string(request.Parent) {
		return errors.New("materialized compact parent does not match request")
	}
	if replay.State.Tenant != request.Context.Tenant {
		return errors.New("materialized compact parent tenant does not match request")
	}
	if replay.State.Project != request.Context.Project {
		return errors.New("materialized compact parent project does not match request")
	}
	return nil
}

func validateCompactPendingReconciliation(request llm.CompactRequestV1, pending CompactReconciliation) error {
	if err := pending.Route.Validate(); err != nil {
		return err
	}
	if err := validateCompactReservation(request, pending.Route, pending.Reservation); err != nil {
		return err
	}
	return validateCompactResponse(request, pending.Route.OperationID, pending.Finalization.Response)
}

func compactReconciliationError(route RoutePlan, cause error) error {
	mapped := provider.NewError(provider.CodeStateUnavailable, provider.PhaseFinalize, provider.DispatchAccepted, provider.RetrySameOperation, "Redis reconciliation is pending")
	mapped.OperationID = string(route.OperationID)
	mapped.Cause = cause
	return fmt.Errorf("%w: %w", ErrReconcilePending, mapped)
}

func validateCompactJournal(reservation ReserveResult, journal JournalReceipt) error {
	if journal.OperationID == "" || journal.GenerationID == "" {
		return errors.New("compact journal receipt identities are required")
	}
	return journal.Validate(reservation)
}

func validateCompactResponse(request llm.CompactRequestV1, expectedOperationID OperationID, response llm.CompactResponseV1) error {
	if response.OperationKey != request.OperationKey {
		return errors.New("compact response operation key does not match request")
	}
	if response.OperationID == "" {
		return errors.New("compact response operation ID is required")
	}
	if expectedOperationID != "" && response.OperationID != string(expectedOperationID) {
		return errors.New("compact response operation ID does not match reserved operation")
	}
	if response.Checkpoint.Parent == nil || string(*response.Checkpoint.Parent) != string(request.Parent) {
		return errors.New("compact response parent checkpoint does not match request")
	}
	if response.Cache.Variant != 0 {
		return errors.New("compact response cache variant must be zero")
	}
	if _, err := json.Marshal(response); err != nil {
		return err
	}
	return nil
}

func compactZeroCost(cost llm.CostV1) bool {
	if cost.Status != "exact" || cost.ActualCostUSD == nil {
		return false
	}
	return strings.Trim(*cost.ActualCostUSD, "0.") == ""
}

func validateWorkerCacheProvenance(raw json.RawMessage) error {
	if len(raw) == 0 {
		return errors.New("compact cache hit must include worker-cache provenance")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return errors.New("compact cache provenance must be an object")
	}
	var source string
	if err := json.Unmarshal(fields["source"], &source); err != nil || source != "worker_cache" {
		return errors.New("compact cache provenance source must be worker_cache")
	}
	return nil
}
