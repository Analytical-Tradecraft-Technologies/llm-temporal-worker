package durable

// This file contains the storage-neutral orchestration for the v1 Generate
// Activity.  It is deliberately a set of explicit, typed ports rather than an
// adapter around engine.Engine: the legacy engine does not own checkpoint
// replay, v1 cache semantics, or Compact/Query contracts.  Runtime composition
// supplies the ports from one immutable configuration snapshot.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

var (
	ErrV1PortsInvalid    = errors.New("v1 durable runtime ports are invalid")
	ErrV1Stage           = errors.New("v1 durable runtime stage failed")
	ErrReservationDenied = errors.New("durable Redis reservation denied")
)

// GenerateReplay is the bounded state view produced by the replay/materialize
// phase.  It contains no ancestor transcript in the Activity payload; the
// materialized view is held only in the worker process.
type GenerateReplay struct {
	State     state.MaterializedState
	Completed *llm.GenerateResponseV1
	// ReconciliationPending is populated when PostgreSQL finalization has
	// committed but the Redis completion event did not. A Temporal retry must
	// reconcile the committed identities before returning the response; it
	// must not dispatch the provider a second time.
	ReconciliationPending *GenerateReconciliation
}

// GenerateReconciliation carries the durable identities needed to retry a
// post-finalization Redis reconciliation. The response is already
// authoritative in PostgreSQL; this value is only a bounded retry handoff and
// must never be treated as permission to run provider work again.
type GenerateReconciliation struct {
	Route        RoutePlan
	Reservation  ReserveResult
	Finalization GenerateFinalization
}

// CacheDisposition identifies the result of the route-isolated cache lookup.
// A hit must provide a complete v1 response and short-circuits provider work.
type CacheDisposition uint8

const (
	CacheDisabled CacheDisposition = iota
	CacheMiss
	CacheHit
)

// CacheDecision is returned by the cache phase.  The cache implementation is
// responsible for route isolation, freshness, fill ownership, and replay-child
// publication; this runner only prevents a hit from reaching inference.
type CacheDecision struct {
	Disposition CacheDisposition
	Response    *llm.GenerateResponseV1
}

func (decision CacheDecision) Validate() error {
	switch decision.Disposition {
	case CacheDisabled, CacheMiss:
		if decision.Response != nil {
			return errors.New("cache miss/disabled decision must not include a response")
		}
	case CacheHit:
		if decision.Response == nil {
			return errors.New("cache hit must include a response")
		}
	default:
		return fmt.Errorf("unknown cache disposition %d", decision.Disposition)
	}
	return nil
}

// CompactionDecision keeps automatic compaction as an explicit phase.  The
// compaction child is dispatched by the runtime's route/provider ports; the
// Generate runner never aliases Compact to a normal Generate call.
type CompactionDecision struct {
	Required bool
}

// RoutePlan is an opaque, snapshot-bound route selection.  Implementations may
// attach route identity and pricing in an unexported wrapper while the runner
// only carries the immutable plan between typed ports.
type RoutePlan struct {
	OperationID  OperationID
	GenerationID GenerationID
	RouteID      string
	EndpointID   string
	Provider     string
	Model        string
	PriceVersion string
}

func (plan RoutePlan) Validate() error {
	if err := plan.OperationID.Validate(); err != nil {
		return err
	}
	if err := plan.GenerationID.Validate(); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"route": plan.RouteID, "endpoint": plan.EndpointID,
		"provider": plan.Provider, "model": plan.Model,
	} {
		if value == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	return nil
}

// JournalReceipt proves that the PostgreSQL write-ahead journal committed
// before provider dispatch.  Keeping operation and generation IDs typed and
// bound here prevents a stage implementation from accidentally journaling a
// different reservation.
type JournalReceipt struct {
	OperationID  OperationID
	GenerationID GenerationID
}

func (receipt JournalReceipt) Validate(reservation ReserveResult) error {
	if err := receipt.OperationID.Validate(); err != nil {
		return err
	}
	if err := receipt.GenerationID.Validate(); err != nil {
		return err
	}
	if receipt.OperationID != reservation.OperationID {
		return errors.New("journal operation identity does not match reservation")
	}
	if receipt.GenerationID != reservation.GenerationID {
		return errors.New("journal generation identity does not match reservation")
	}
	return nil
}

// DispatchResult is the provider-neutral one-shot result passed to finalization.
// Provider adapters own wire decoding and must return a normalized response;
// no token/event stream is exposed by this contract.
type DispatchResult struct {
	Response llm.Response
}

// GenerateFinalization is the bounded response returned after the PostgreSQL
// checkpoint/cache/cost finalization phase.
type GenerateFinalization struct {
	Response llm.GenerateResponseV1
}

// GeneratePorts are the production composition ports for one snapshot.  The
// callbacks are intentionally explicit and ordered by the runner below:
// replay -> route-isolated cache -> compaction decision -> route/affinity ->
// Redis reservation -> PostgreSQL journal -> provider state machine ->
// PostgreSQL finalization -> Redis reconciliation.
//
// Every callback must be idempotent for Temporal Activity retries.  The runner
// does not log or serialize values passed between phases.
type GeneratePorts struct {
	Replay             func(context.Context, llm.GenerateRequestV1) (GenerateReplay, error)
	CacheLookup        func(context.Context, llm.GenerateRequestV1, GenerateReplay) (CacheDecision, error)
	CompactionDecision func(context.Context, llm.GenerateRequestV1, GenerateReplay, CacheDecision) (CompactionDecision, error)
	// Compact is required only when CompactionDecision.Required is true. It
	// dispatches the distinct Compact child and rematerializes its checkpoint;
	// it is never an alias for Generate or provider dispatch.
	Compact       func(context.Context, llm.GenerateRequestV1, GenerateReplay) (GenerateReplay, error)
	Route         func(context.Context, llm.GenerateRequestV1, GenerateReplay, CompactionDecision) (RoutePlan, error)
	Reserve       func(context.Context, llm.GenerateRequestV1, RoutePlan) (ReserveResult, error)
	Journal       func(context.Context, llm.GenerateRequestV1, RoutePlan, ReserveResult) (JournalReceipt, error)
	Dispatch      func(context.Context, llm.GenerateRequestV1, GenerateReplay, RoutePlan, JournalReceipt) (DispatchResult, error)
	Finalize      func(context.Context, llm.GenerateRequestV1, GenerateReplay, RoutePlan, ReserveResult, DispatchResult) (GenerateFinalization, error)
	FinalizeCache func(context.Context, llm.GenerateRequestV1, GenerateReplay, CacheDecision) (GenerateFinalization, error)
	Reconcile     func(context.Context, llm.GenerateRequestV1, RoutePlan, ReserveResult, GenerateFinalization) error
}

func (ports GeneratePorts) validate() error {
	fields := []struct {
		name string
		fn   any
	}{
		{"replay", ports.Replay}, {"cache lookup", ports.CacheLookup},
		{"compaction decision", ports.CompactionDecision}, {"route", ports.Route},
		{"reserve", ports.Reserve}, {"journal", ports.Journal},
		{"dispatch", ports.Dispatch}, {"finalize", ports.Finalize},
		{"finalize cache", ports.FinalizeCache}, {"reconcile", ports.Reconcile},
	}
	for _, field := range fields {
		if field.fn == nil {
			return fmt.Errorf("%w: %s port is required", ErrV1PortsInvalid, field.name)
		}
	}
	return nil
}

// GenerateV1 executes one complete durable Generate composition.  It performs
// no provider work if replay, cache, route, Redis reservation, or PostgreSQL
// journaling fails.  Reconciliation is deliberately last: after finalization
// it returns ErrReconcilePending so Temporal retries the reconciliation path
// without re-running provider dispatch.
func GenerateV1(ctx context.Context, request llm.GenerateRequestV1, ports GeneratePorts) (llm.GenerateResponseV1, error) {
	if ctx == nil {
		return llm.GenerateResponseV1{}, fmt.Errorf("%w: context is nil", ErrV1PortsInvalid)
	}
	if err := ports.validate(); err != nil {
		return llm.GenerateResponseV1{}, err
	}
	if _, err := json.Marshal(request); err != nil {
		return llm.GenerateResponseV1{}, fmt.Errorf("%w: request is invalid: %v", ErrV1Stage, err)
	}
	if err := ctx.Err(); err != nil {
		return llm.GenerateResponseV1{}, err
	}

	replay, err := ports.Replay(ctx, request)
	if err != nil {
		return llm.GenerateResponseV1{}, stageError("replay", err)
	}
	if replay.ReconciliationPending != nil {
		pending := replay.ReconciliationPending
		if err := validateGeneratePendingReconciliation(request, *pending); err != nil {
			return llm.GenerateResponseV1{}, stageError("replay", err)
		}
		// A replay implementation should return either a completed response or
		// the pending handoff. If it returns both, require them to refer to the
		// same operation before reconciling, rather than trusting an ambiguous
		// result from a mixed snapshot.
		if replay.Completed != nil && replay.Completed.OperationID != pending.Finalization.Response.OperationID {
			return llm.GenerateResponseV1{}, stageError("replay", errors.New("completed response does not match pending reconciliation"))
		}
		if err := ports.Reconcile(ctx, request, pending.Route, pending.Reservation, pending.Finalization); err != nil {
			return llm.GenerateResponseV1{}, generateReconciliationError(pending.Route, err)
		}
		return pending.Finalization.Response, nil
	}
	if replay.Completed != nil {
		if err := validateGenerateResponse(request, "", *replay.Completed); err != nil {
			return llm.GenerateResponseV1{}, stageError("replay", err)
		}
		return *replay.Completed, nil
	}
	decision, err := ports.CacheLookup(ctx, request, replay)
	if err != nil {
		return llm.GenerateResponseV1{}, stageError("cache lookup", err)
	}
	if err := decision.Validate(); err != nil {
		return llm.GenerateResponseV1{}, stageError("cache decision", err)
	}
	if decision.Disposition == CacheHit {
		finalization, err := ports.FinalizeCache(ctx, request, replay, decision)
		if err != nil {
			return llm.GenerateResponseV1{}, stageError("cache finalization", err)
		}
		if err := validateGenerateFinalization(request, "", finalization); err != nil {
			return llm.GenerateResponseV1{}, stageError("cache finalization", err)
		}
		return finalization.Response, nil
	}

	compaction, err := ports.CompactionDecision(ctx, request, replay, decision)
	if err != nil {
		return llm.GenerateResponseV1{}, stageError("compaction decision", err)
	}
	if compaction.Required {
		if ports.Compact == nil {
			return llm.GenerateResponseV1{}, stageError("compaction", fmt.Errorf("%w: compact port is required", ErrV1PortsInvalid))
		}
		replay, err = ports.Compact(ctx, request, replay)
		if err != nil {
			return llm.GenerateResponseV1{}, stageError("compaction", err)
		}
	}
	route, err := ports.Route(ctx, request, replay, compaction)
	if err != nil {
		return llm.GenerateResponseV1{}, stageError("route", err)
	}
	if err := route.Validate(); err != nil {
		return llm.GenerateResponseV1{}, stageError("route", err)
	}
	reservation, err := ports.Reserve(ctx, request, route)
	if err != nil {
		return llm.GenerateResponseV1{}, stageError("Redis reservation", err)
	}
	if err := validateReservationForRequest(request, route, reservation); err != nil {
		return llm.GenerateResponseV1{}, stageError("Redis reservation", err)
	}
	if !reservation.Accepted {
		mapped := provider.NewError(provider.CodeBudgetDenied, provider.PhaseAdmission, provider.DispatchNotDispatched, provider.RetryAfter, "budget reservation denied")
		mapped.OperationID = string(route.OperationID)
		mapped.RetryAfter = reservation.RetryAfter
		return llm.GenerateResponseV1{}, fmt.Errorf("%w: %w", ErrReservationDenied, mapped)
	}
	journal, err := ports.Journal(ctx, request, route, reservation)
	if err != nil {
		return llm.GenerateResponseV1{}, stageError("PostgreSQL journal", err)
	}
	if err := validateJournalForRequest(request, reservation, journal); err != nil {
		return llm.GenerateResponseV1{}, stageError("PostgreSQL journal", err)
	}
	dispatch, err := ports.Dispatch(ctx, request, replay, route, journal)
	if err != nil {
		return llm.GenerateResponseV1{}, stageError("provider dispatch", err)
	}
	finalization, err := ports.Finalize(ctx, request, replay, route, reservation, dispatch)
	if err != nil {
		return llm.GenerateResponseV1{}, stageError("PostgreSQL finalization", err)
	}
	if err := validateGenerateFinalization(request, route.OperationID, finalization); err != nil {
		return llm.GenerateResponseV1{}, stageError("PostgreSQL finalization", err)
	}
	if err := ports.Reconcile(ctx, request, route, reservation, finalization); err != nil {
		return llm.GenerateResponseV1{}, generateReconciliationError(route, err)
	}
	return finalization.Response, nil
}

func validateReservationForRequest(request llm.GenerateRequestV1, route RoutePlan, reservation ReserveResult) error {
	if err := reservation.Validate(ReserveRequest{OperationID: route.OperationID, GenerationID: route.GenerationID}); err != nil {
		return err
	}
	if reservation.Accepted && reservation.GenerationID == "" {
		return errors.New("accepted reservation has no generation identity")
	}
	if reservation.Accepted {
		if err := reservation.IncarnationID.Validate(); err != nil {
			return err
		}
		if len(reservation.Events) == 0 {
			return errors.New("accepted reservation has no journal events")
		}
	}
	if reservation.OperationID != route.OperationID {
		return errors.New("reservation operation identity does not match route plan")
	}
	if reservation.GenerationID != route.GenerationID {
		return errors.New("reservation generation identity does not match route plan")
	}
	if request.OperationKey == "" {
		return errors.New("operation key is required")
	}
	return nil
}

func validateJournalForRequest(_ llm.GenerateRequestV1, reservation ReserveResult, journal JournalReceipt) error {
	if journal.OperationID == "" || journal.GenerationID == "" {
		return errors.New("journal receipt identities are required")
	}
	if err := journal.Validate(reservation); err != nil {
		return err
	}
	return nil
}

func validateGenerateFinalization(request llm.GenerateRequestV1, expectedOperationID OperationID, finalization GenerateFinalization) error {
	return validateGenerateResponse(request, expectedOperationID, finalization.Response)
}

func validateGeneratePendingReconciliation(request llm.GenerateRequestV1, pending GenerateReconciliation) error {
	if err := pending.Route.Validate(); err != nil {
		return err
	}
	if err := validateReservationForRequest(request, pending.Route, pending.Reservation); err != nil {
		return err
	}
	return validateGenerateFinalization(request, pending.Route.OperationID, pending.Finalization)
}

func validateGenerateResponse(request llm.GenerateRequestV1, expectedOperationID OperationID, response llm.GenerateResponseV1) error {
	if response.OperationKey != request.OperationKey {
		return errors.New("finalized response operation key does not match request")
	}
	if response.OperationID == "" {
		return errors.New("finalized response operation ID is required")
	}
	if expectedOperationID != "" && response.OperationID != string(expectedOperationID) {
		return errors.New("finalized response operation ID does not match reserved operation")
	}
	if _, err := json.Marshal(response); err != nil {
		return err
	}
	return nil
}

func stageError(stage string, err error) error {
	if err == nil {
		return fmt.Errorf("%w: %s", ErrV1Stage, stage)
	}
	return fmt.Errorf("%w: %s: %w", ErrV1Stage, stage, err)
}

func generateReconciliationError(route RoutePlan, cause error) error {
	mapped := provider.NewError(provider.CodeStateUnavailable, provider.PhaseFinalize, provider.DispatchAccepted, provider.RetrySameOperation, "Redis reconciliation is pending")
	mapped.OperationID = string(route.OperationID)
	mapped.Cause = cause
	return fmt.Errorf("%w: %w", ErrReconcilePending, mapped)
}
