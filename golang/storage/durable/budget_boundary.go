package durable

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	postgresstore "github.com/mfow/llm-temporal-worker/golang/storage/postgres"
)

var (
	// ErrBudgetBoundaryInvalid identifies a boundary that cannot safely own
	// both snapshot-scoped stores.
	ErrBudgetBoundaryInvalid = errors.New("durable budget boundary is invalid")
	// ErrJournalPending means Redis accepted a reservation, but PostgreSQL did
	// not yet record every reservation event. The caller must persist the
	// cleanup/recovery metadata and must not dispatch a provider call from the
	// partial result.
	ErrJournalPending = errors.New("postgres budget journal is pending")
)

// BudgetBoundary is the narrow Redis/PostgreSQL budget handoff used by a
// future durable Generate or Compact composition. It deliberately owns no
// provider, operation, checkpoint, cache, or legacy engine state.
//
// The boundary is immutable after snapshot construction. Its identity binds
// the Redis generation namespace and PostgreSQL worker namespace to the same
// configuration snapshot; callers must construct a new boundary on reload.
type BudgetBoundary struct {
	Identity     StateIdentity
	Materializer BudgetMaterializer
	Journal      Journal
}

// Validate checks the complete snapshot-owned budget handoff. In particular,
// typed-nil interfaces are rejected before an Activity can begin a Redis or
// PostgreSQL side effect.
func (boundary BudgetBoundary) Validate() error {
	if err := boundary.Identity.Validate(); err != nil {
		return fmt.Errorf("%w: identity: %v", ErrBudgetBoundaryInvalid, err)
	}
	if isNilBudgetPort(boundary.Materializer) {
		return fmt.Errorf("%w: Redis budget materializer is required", ErrBudgetBoundaryInvalid)
	}
	if isNilBudgetPort(boundary.Journal) {
		return fmt.Errorf("%w: PostgreSQL budget journal is required", ErrBudgetBoundaryInvalid)
	}
	return nil
}

func isNilBudgetPort(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

// BudgetReservation contains the Redis decision and the PostgreSQL records
// that prove every reservation event was journaled. DispatchReady is false on
// denial, validation failure, cancellation, or a partial journal write.
type BudgetReservation struct {
	Result         ReserveResult
	JournalRecords []postgresstore.JournalRecord
	// ReleaseEvents is bounded cleanup metadata for an accepted reservation
	// whose PostgreSQL journal could not be completed. ReleasePending is true
	// only when the best-effort Redis cleanup also failed.
	ReleaseEvents  []budget.CompletionEvent
	ReleasePending bool
	// PostgresRecoveryRequired is true when one or more reservation journal
	// appends succeeded before a later append failed. Recovery must reconcile
	// that append-only gap before this operation can be retried for dispatch.
	PostgresRecoveryRequired bool
}

func (reservation BudgetReservation) DispatchReady() bool {
	return reservation.Result.Accepted && len(reservation.Result.Events) > 0 &&
		len(reservation.JournalRecords) == len(reservation.Result.Events) && !reservation.ReleasePending
}

// Reserve executes the only safe pre-dispatch order: Redis accepts first,
// then PostgreSQL records every returned reservation event. A journal error
// aborts this lifecycle after best-effort cleanup; this boundary never permits
// dispatch from a partial result.
// The returned partial reservation is never dispatch-ready.
func (boundary BudgetBoundary) Reserve(ctx context.Context, lifecycle *Lifecycle, request ReserveRequest) (BudgetReservation, error) {
	var result BudgetReservation
	if ctx == nil {
		return result, fmt.Errorf("%w: context is nil", ErrBudgetBoundaryInvalid)
	}
	if err := boundary.Validate(); err != nil {
		return result, err
	}
	if lifecycle == nil {
		return result, ErrInvalidPhase
	}
	current, ok := lifecycle.Current()
	if !ok || (current != PhaseOperationReplay && current != PhaseRedisAccepted) {
		return result, fmt.Errorf("%w: reserve requires operation replay or a retry after Redis acceptance", ErrInvalidPhase)
	}
	if lifecycle.reservationAborted {
		return result, fmt.Errorf("%w: reservation cleanup was attempted; recover before retry", ErrInvalidPhase)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	accepted, err := boundary.Materializer.Accept(ctx, request)
	if err != nil {
		return result, err
	}
	if err := accepted.Validate(request); err != nil {
		return result, fmt.Errorf("%w: Redis acceptance: %v", ErrBudgetBoundaryInvalid, err)
	}
	if accepted.Accepted {
		if err := lifecycle.bindReservationIdentity(accepted); err != nil {
			return result, err
		}
	}
	if current == PhaseOperationReplay {
		if err := lifecycle.Advance(PhaseRedisAccepted); err != nil {
			return result, err
		}
	}
	result.Result = accepted
	if !accepted.Accepted {
		// A denial is a complete Redis decision and must not create a journal
		// record or provider work.
		return result, nil
	}
	result.JournalRecords = make([]postgresstore.JournalRecord, 0, len(accepted.Events))
	seen := make(map[string]struct{}, len(accepted.Events))
	for index, event := range accepted.Events {
		if _, exists := seen[event.EventID]; exists {
			return boundary.reservationJournalFailure(ctx, lifecycle, result, accepted, fmt.Errorf("duplicate reservation event %d", index))
		}
		seen[event.EventID] = struct{}{}
	}
	for index, event := range accepted.Events {
		if err := ctx.Err(); err != nil {
			return boundary.reservationJournalFailure(ctx, lifecycle, result, accepted, fmt.Errorf("reservation journal canceled: %v", err))
		}
		record, err := boundary.Journal.AppendReservation(ctx, event)
		if err != nil {
			return boundary.reservationJournalFailure(ctx, lifecycle, result, accepted, fmt.Errorf("reservation event %d: %v", index, err))
		}
		result.JournalRecords = append(result.JournalRecords, record)
	}
	if err := lifecycle.Advance(PhasePostgresJournaled); err != nil {
		return result, err
	}
	return result, nil
}

// Finalize records all completion events in PostgreSQL before reconciling
// Redis. PostgreSQL remains authoritative if reconciliation fails; callers
// must retry the reconciliation handoff and must not submit the provider a
// second time.
func (boundary BudgetBoundary) Finalize(ctx context.Context, lifecycle *Lifecycle, reservation BudgetReservation, events []budget.CompletionEvent) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrBudgetBoundaryInvalid)
	}
	if err := boundary.Validate(); err != nil {
		return err
	}
	if lifecycle == nil {
		return ErrInvalidPhase
	}
	current, ok := lifecycle.Current()
	if !ok || (current != PhaseDispatched && current != PhasePostgresFinalized) {
		return fmt.Errorf("%w: finalize requires dispatch or a reconciliation retry", ErrInvalidPhase)
	}
	if !reservation.DispatchReady() {
		return fmt.Errorf("%w: reservation is not journaled", ErrJournalRequired)
	}
	if len(events) == 0 {
		return fmt.Errorf("%w: completion events are required", ErrBudgetBoundaryInvalid)
	}
	for index, event := range events {
		if err := event.Validate(); err != nil {
			return fmt.Errorf("%w: completion event %d: %v", ErrBudgetBoundaryInvalid, index, err)
		}
		if event.OperationID != string(reservation.Result.OperationID) || event.GenerationID != string(reservation.Result.GenerationID) {
			return fmt.Errorf("%w: completion event %d identity does not match reservation", ErrBudgetBoundaryInvalid, index)
		}
		matched := false
		for _, reserved := range reservation.Result.Events {
			if event.WindowID == reserved.WindowID && event.BucketStart.Equal(reserved.BucketStart) {
				if event.ReservationRevision <= reserved.ReservationRevision {
					return fmt.Errorf("%w: completion event %d revision does not advance reservation", ErrBudgetBoundaryInvalid, index)
				}
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%w: completion event %d window and bucket do not match reservation", ErrBudgetBoundaryInvalid, index)
		}
	}
	seenCompletionIDs := make(map[string]struct{}, len(events))
	for index, event := range events {
		if _, exists := seenCompletionIDs[event.EventID]; exists {
			return fmt.Errorf("%w: duplicate completion event %d", ErrBudgetBoundaryInvalid, index)
		}
		seenCompletionIDs[event.EventID] = struct{}{}
	}
	if err := lifecycle.bindCompletionDigest(completionDigest(events)); err != nil {
		return err
	}
	if current == PhaseDispatched {
		for index, event := range events {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("%w: completion journal canceled: %v", ErrJournalPending, err)
			}
			if _, err := boundary.Journal.AppendCompletion(ctx, event); err != nil {
				return fmt.Errorf("%w: completion event %d: %v", ErrJournalPending, index, err)
			}
		}
		if err := lifecycle.Advance(PhasePostgresFinalized); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	reconcile := ReconcileRequest{
		OperationID:   reservation.Result.OperationID,
		GenerationID:  reservation.Result.GenerationID,
		IncarnationID: reservation.Result.IncarnationID,
		Events:        append([]budget.CompletionEvent(nil), events...),
	}
	if err := reconcile.Validate(); err != nil {
		return fmt.Errorf("%w: reconciliation: %v", ErrBudgetBoundaryInvalid, err)
	}
	if err := boundary.Materializer.Reconcile(ctx, reconcile); err != nil {
		return lifecycle.ReconcileFailure(err)
	}
	if err := lifecycle.Advance(PhaseRedisReconciled); err != nil {
		return err
	}
	return nil
}

func (boundary BudgetBoundary) reservationJournalFailure(ctx context.Context, lifecycle *Lifecycle, result BudgetReservation, accepted ReserveResult, cause error) (BudgetReservation, error) {
	result.PostgresRecoveryRequired = len(result.JournalRecords) > 0
	// A best-effort release aborts this lifecycle even when it succeeds: a
	// subsequent Reserve could otherwise replay an already released Redis
	// reservation while PostgreSQL still has a partial append-only journal.
	// The caller must persist ReleaseEvents and begin a fresh operation after
	// recovery confirms the state is safe.
	if lifecycle != nil {
		lifecycle.markReservationAborted()
	}
	releaseEvents := reservationReleaseEvents(accepted)
	result.ReleaseEvents = releaseEvents
	releaseErr := boundary.Materializer.Reconcile(ctx, ReconcileRequest{
		OperationID: accepted.OperationID, GenerationID: accepted.GenerationID, IncarnationID: accepted.IncarnationID,
		Events: releaseEvents,
	})
	if releaseErr != nil {
		result.ReleasePending = true
		return result, fmt.Errorf("%w: %v; Redis release cleanup pending: %v", ErrJournalPending, cause, releaseErr)
	}
	return result, fmt.Errorf("%w: %v", ErrJournalPending, cause)
}

func reservationReleaseEvents(result ReserveResult) []budget.CompletionEvent {
	released := make([]budget.CompletionEvent, 0, len(result.Events))
	zero := pricing.USD{}
	for _, event := range result.Events {
		cost := zero
		released = append(released, budget.CompletionEvent{
			EventID:      fmt.Sprintf("%x", sha256.Sum256([]byte("release:"+event.EventID))),
			GenerationID: event.GenerationID, OperationID: event.OperationID, WindowID: event.WindowID,
			BucketStart: event.BucketStart, ReservationRevision: event.ReservationRevision + 1,
			Kind: budget.JournalRelease, ReservedDecreaseUSD: event.AmountUSD,
			ActualCostUSD: &cost, CostStatus: budget.CostExact, OccurredAt: event.OccurredAt,
		})
	}
	return released
}

func completionDigest(events []budget.CompletionEvent) [32]byte {
	// CompletionEvent's fields are deterministic JSON (including exact USD
	// strings), so the digest is a stable receipt for reconciliation retries.
	data, _ := json.Marshal(events)
	return sha256.Sum256(data)
}
