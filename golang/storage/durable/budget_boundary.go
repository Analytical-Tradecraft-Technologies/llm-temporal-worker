package durable

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/budget"
)

var ErrBudgetBoundaryInvalid = errors.New("durable budget boundary is invalid")

// BudgetBoundary owns only Redis budget reservations, dispatch claims and
// settlement. Operation/checkpoint persistence belongs to the caller.
type BudgetBoundary struct {
	Identity     StateIdentity
	Materializer BudgetLeaser
}

func (boundary BudgetBoundary) Validate() error {
	if err := boundary.Identity.ValidateBudget(); err != nil {
		return fmt.Errorf("%w: %v", ErrBudgetBoundaryInvalid, err)
	}
	if isNilPort(boundary.Materializer) {
		return fmt.Errorf("%w: Redis budget leaser is required", ErrBudgetBoundaryInvalid)
	}
	return nil
}

type BudgetReservation struct {
	Result  ReserveResult
	claimed bool
}

func (reservation BudgetReservation) DispatchReady() bool {
	return reservation.Result.Accepted && reservation.claimed
}

// Reserve returns immediately with acquired or wait. An unsuccessful acquisition
// is not remembered as a permanent denial; workflow timers may retry it.
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
		return result, ErrInvalidPhase
	}
	accepted, err := boundary.Materializer.Accept(ctx, request)
	if err != nil {
		return result, err
	}
	if err := accepted.Validate(request); err != nil {
		return result, fmt.Errorf("%w: %v", ErrBudgetBoundaryInvalid, err)
	}
	seen := make(map[string]bool)
	for _, event := range accepted.Events {
		if seen[event.EventID] {
			return result, ErrBudgetBoundaryInvalid
		}
		seen[event.EventID] = true
	}
	if accepted.Accepted {
		if err := lifecycle.bindReservationIdentity(accepted); err != nil {
			return result, err
		}
		if current == PhaseOperationReplay {
			if err := lifecycle.Advance(PhaseRedisAccepted); err != nil {
				return result, err
			}
		}
	}
	result.Result = accepted
	return result, nil
}

// Claim must succeed immediately before submission. A lost reply never grants
// permission to resubmit: the reservation remains charged and requires recovery.
func (boundary BudgetBoundary) Claim(ctx context.Context, lifecycle *Lifecycle, reservation *BudgetReservation) (ClaimReceipt, error) {
	if ctx == nil || lifecycle == nil || reservation == nil {
		return ClaimReceipt{}, ErrBudgetBoundaryInvalid
	}
	if err := boundary.Validate(); err != nil {
		return ClaimReceipt{}, err
	}
	current, ok := lifecycle.Current()
	if !ok || current != PhaseRedisAccepted || !reservation.Result.Accepted {
		return ClaimReceipt{}, ErrInvalidPhase
	}
	result := reservation.Result
	if err := lifecycle.bindReservationIdentity(result); err != nil {
		return ClaimReceipt{}, err
	}
	receipt, err := boundary.Materializer.Claim(ctx, ClaimRequest{result.OperationID, result.GenerationID, result.IncarnationID})
	if err != nil {
		return ClaimReceipt{}, err
	}
	if err := receipt.Validate(result); err != nil {
		return ClaimReceipt{}, err
	}
	if err := lifecycle.Advance(PhaseRedisClaimed); err != nil {
		return ClaimReceipt{}, err
	}
	reservation.claimed = true
	return receipt, nil
}

// Finalize atomically settles the full reservation in Redis. Callers retry
// the same completion batch after an uncertain reply without dispatching again.
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
	if !ok || (current != PhaseDispatched && current != PhaseResultFinalized) {
		return fmt.Errorf("%w: finalize requires dispatch or a reconciliation retry", ErrInvalidPhase)
	}
	if !reservation.DispatchReady() {
		return fmt.Errorf("%w: reservation is not claimed", ErrClaimRequired)
	}
	if len(events) == 0 {
		return fmt.Errorf("%w: completion events are required", ErrBudgetBoundaryInvalid)
	}
	type reservationKey struct {
		windowID      string
		bucketSeconds int64
		bucketNanos   int
	}
	reservedByKey := make(map[reservationKey]budget.ReservationEvent, len(reservation.Result.Events))
	for index, reserved := range reservation.Result.Events {
		key := reservationKey{
			windowID:      reserved.WindowID,
			bucketSeconds: reserved.BucketStart.Unix(),
			bucketNanos:   reserved.BucketStart.Nanosecond(),
		}
		if _, exists := reservedByKey[key]; exists {
			return fmt.Errorf("%w: duplicate reservation window and bucket %d", ErrBudgetBoundaryInvalid, index)
		}
		reservedByKey[key] = reserved
	}
	completedKeys := make(map[reservationKey]struct{}, len(events))
	for index, event := range events {
		if err := event.Validate(); err != nil {
			return fmt.Errorf("%w: completion event %d: %v", ErrBudgetBoundaryInvalid, index, err)
		}
		if event.OperationID != string(reservation.Result.OperationID) || event.GenerationID != string(reservation.Result.GenerationID) {
			return fmt.Errorf("%w: completion event %d identity does not match reservation", ErrBudgetBoundaryInvalid, index)
		}
		key := reservationKey{
			windowID:      event.WindowID,
			bucketSeconds: event.BucketStart.Unix(),
			bucketNanos:   event.BucketStart.Nanosecond(),
		}
		reserved, matched := reservedByKey[key]
		if !matched || !event.BucketStart.Equal(reserved.BucketStart) {
			return fmt.Errorf("%w: completion event %d window and bucket do not match reservation", ErrBudgetBoundaryInvalid, index)
		}
		if _, exists := completedKeys[key]; exists {
			return fmt.Errorf("%w: completion event %d duplicates a reservation window and bucket", ErrBudgetBoundaryInvalid, index)
		}
		if event.ReservationRevision <= reserved.ReservationRevision {
			return fmt.Errorf("%w: completion event %d revision does not advance reservation", ErrBudgetBoundaryInvalid, index)
		}
		completedKeys[key] = struct{}{}
	}
	if len(completedKeys) != len(reservedByKey) {
		return fmt.Errorf("%w: completion events do not cover every reservation window and bucket", ErrBudgetBoundaryInvalid)
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
		if err := lifecycle.Advance(PhaseResultFinalized); err != nil {
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

func completionDigest(events []budget.CompletionEvent) [32]byte {
	// CompletionEvent's fields are deterministic JSON (including exact USD
	// strings), so the digest is a stable receipt for reconciliation retries.
	data, _ := json.Marshal(events)
	return sha256.Sum256(data)
}
