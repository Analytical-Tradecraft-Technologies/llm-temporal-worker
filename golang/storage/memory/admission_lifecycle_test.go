package memory

import (
	"context"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

func memoryReservation(amount, limit pricing.MicroUSD) admission.WindowReservation {
	return admission.WindowReservation{
		PolicyID:      "policy",
		WindowID:      "window",
		Bucket:        100,
		Amount:        amount,
		Limit:         limit,
		BucketNanos:   int64(time.Second),
		DurationNanos: int64(10 * time.Second),
	}
}

// assertNextBeginSeesActive begins a reservation of 8 against a limit of 10
// and requires it to be denied with active spend still at active.
func assertNextBeginSeesActive(t *testing.T, store *AdmissionStore, now time.Time, id string, active pricing.MicroUSD) {
	t.Helper()
	denied, err := store.Begin(context.Background(), admission.BeginRequest{
		ID: id, ScopeKey: "tenant/" + id, RequestDigest: admission.Digest([]byte(id)),
		Reservation: 8, Reservations: []admission.WindowReservation{memoryReservation(8, 10)}, ExpiresAt: now.Add(time.Hour),
	})
	if err != nil || denied.Denied == nil || denied.Denied.Active != active {
		t.Fatalf("%s budget = %#v, %v", id, denied, err)
	}
}

func beginMemoryOperation(t *testing.T, store *AdmissionStore, now time.Time, id string, reservation admission.WindowReservation) admission.Operation {
	t.Helper()
	result, err := store.Begin(context.Background(), admission.BeginRequest{
		ID:            id,
		ScopeKey:      "tenant/" + id,
		RequestDigest: admission.Digest([]byte(id)),
		Reservation:   reservation.Amount,
		Reservations:  []admission.WindowReservation{reservation},
		LeaseUntil:    now.Add(time.Minute),
		ExpiresAt:     now.Add(time.Hour),
	})
	if err != nil || result.Denied != nil {
		t.Fatalf("begin %q = %#v, %v", id, result, err)
	}
	return result.Operation
}

func dispatchMemoryOperation(t *testing.T, store *AdmissionStore, now time.Time, operation admission.Operation, route string) admission.Operation {
	t.Helper()
	attempt := admission.AttemptFacts{RouteID: route, AttemptNumber: 1}
	if err := store.MarkDispatching(context.Background(), admission.DispatchRequest{
		OperationID: operation.ID, DispatchToken: operation.DispatchToken, Attempt: attempt, LeaseUntil: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("mark dispatching %q: %v", operation.ID, err)
	}
	operation.State = admission.StateDispatching
	operation.Attempt = attempt
	operation.Attempt.AttemptNumber++
	return operation
}

// newDispatchedMemoryOperation begins id with a reservation of amount against
// a limit of 10, on a store whose clock is fixed at time.Unix(100, 0), and
// marks it dispatching on route "initial".
func newDispatchedMemoryOperation(t *testing.T, id string, amount pricing.MicroUSD) (*AdmissionStore, time.Time, admission.Operation) {
	t.Helper()
	now := time.Unix(100, 0)
	store := NewAdmissionStore(AdmissionOptions{Clock: func() time.Time { return now }})
	operation := beginMemoryOperation(t, store, now, id, memoryReservation(amount, 10))
	return store, now, dispatchMemoryOperation(t, store, now, operation, "initial")
}

func TestAdmissionCompleteReconcilesReservationAndClonesResult(t *testing.T) {
	store, now, operation := newDispatchedMemoryOperation(t, "complete", 10)

	ref := &state.BlobRef{Digest: [32]byte{1}, Size: 3, Media: "application/octet-stream"}
	testassert.NoError(t, store.Complete(context.Background(), admission.CompleteRequest{
		OperationID:   operation.ID,
		DispatchToken: operation.DispatchToken,
		Actual:        3,
		ResultRef:     ref,
		Attempt:       admission.AttemptFacts{RouteID: "initial"},
	}))
	ref.Media = "mutated-input"

	got, err := store.Get(context.Background(), operation.ID)
	testassert.NoError(t, err)
	if got.State != admission.StateCompleted || got.ReservedMicroUSD != 0 || got.IncurredMicroUSD != 3 || got.FinalMicroUSD != 3 {
		t.Fatalf("completed operation = %#v", got)
	}
	if got.Attempt.Dispatch != admission.Accepted || got.ResultRef == nil || got.ResultRef.Media != "application/octet-stream" {
		t.Fatalf("completion facts = %#v", got)
	}

	got.ResultRef.Media = "mutated-read"
	again, err := store.Get(context.Background(), operation.ID)
	if err != nil || again.ResultRef == nil || again.ResultRef.Media != "application/octet-stream" {
		t.Fatalf("result ref was not cloned: %#v, %v", again, err)
	}

	testassert.NoError(t, store.Complete(context.Background(), admission.CompleteRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Actual: 99}), "completed replay")
	assertNextBeginSeesActive(t, store, now, "after-complete", 3)
}

func TestAdmissionProviderPendingRetainsPollGuidance(t *testing.T) {
	store, now, operation := newDispatchedMemoryOperation(t, "pending-guidance", 1)
	pollAfter := now.Add(5 * time.Second)
	testassert.NoError(t, store.MarkProviderPending(context.Background(), admission.ProviderPendingRequest{
		OperationID: operation.ID, DispatchToken: operation.DispatchToken,
		ProviderOperationID: "provider-operation", EndpointID: "endpoint", PollAfter: pollAfter,
	}))
	schedule, ok := any(store).(admission.ProviderPendingSchedule)
	if !ok {
		t.Fatal("memory admission store does not expose provider poll schedule")
	}
	got, err := schedule.ProviderPollAfter(context.Background(), operation.ID)
	testassert.NoError(t, err)
	if !got.Equal(pollAfter) {
		t.Fatalf("poll schedule = %s, want %s", got, pollAfter)
	}
}

func TestAdmissionDefiniteFailureReconcilesIncurredCost(t *testing.T) {
	store, now, operation := newDispatchedMemoryOperation(t, "failed", 10)

	testassert.NoError(t, store.Fail(context.Background(), admission.FailRequest{
		OperationID:   operation.ID,
		DispatchToken: operation.DispatchToken,
		Certainty:     admission.Rejected,
		Incurred:      3,
		Attempt:       admission.AttemptFacts{RouteID: "initial"},
	}))
	got, err := store.Get(context.Background(), operation.ID)
	testassert.NoError(t, err)
	if got.State != admission.StateDefiniteFailed || got.ReservedMicroUSD != 0 || got.IncurredMicroUSD != 3 || got.FinalMicroUSD != 3 || got.Attempt.Dispatch != admission.Rejected {
		t.Fatalf("failed operation = %#v", got)
	}

	assertNextBeginSeesActive(t, store, now, "after-failure", 3)
}

func TestAdmissionContinueReconcilesAndRotatesDispatchToken(t *testing.T) {
	store, now, operation := newDispatchedMemoryOperation(t, "continue", 10)

	next := memoryReservation(7, 10)
	continued, err := continueAfterRejection(store, now, operation, "fallback", 7, next)
	if err != nil || continued.Denied != nil {
		t.Fatalf("continue = %#v, %v", continued, err)
	}
	if continued.Operation.State != admission.StateReserved || continued.Operation.ReservedMicroUSD != 7 || continued.Operation.DispatchToken == operation.DispatchToken || continued.Operation.Attempt.Dispatch != admission.NotDispatched || continued.Operation.Attempt.RouteID != "fallback" {
		t.Fatalf("continued operation = %#v", continued.Operation)
	}
	testassert.ErrorIs(t, store.MarkDispatching(context.Background(), admission.DispatchRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Attempt: continued.Operation.Attempt}), admission.ErrInvalidToken, "old dispatch token")
	testassert.NoError(t, store.MarkDispatching(context.Background(), admission.DispatchRequest{OperationID: operation.ID, DispatchToken: continued.Operation.DispatchToken, Attempt: continued.Operation.Attempt}))
	testassert.NoError(t, store.Complete(context.Background(), admission.CompleteRequest{OperationID: operation.ID, DispatchToken: continued.Operation.DispatchToken, Actual: 7, Attempt: admission.AttemptFacts{RouteID: "fallback", AttemptNumber: 2}}))
	completed, err := store.Get(context.Background(), operation.ID)
	if err != nil || completed.State != admission.StateCompleted || completed.FinalMicroUSD != 7 || completed.Attempt.AttemptNumber != 2 {
		t.Fatalf("completed fallback = %#v, %v", completed, err)
	}
}

func TestAdmissionContinueDenialFinalizesWithoutGhostReservation(t *testing.T) {
	store, now, operation := newDispatchedMemoryOperation(t, "continue-denied", 10)

	deniedReservation := memoryReservation(8, 10)
	continued, err := continueAfterRejection(store, now, operation, "initial", 8, deniedReservation)
	if err != nil || continued.Denied == nil || continued.Denied.Active != 3 {
		t.Fatalf("denied continue = %#v, %v", continued, err)
	}
	if continued.Operation.State != admission.StateDefiniteFailed || continued.Operation.ReservedMicroUSD != 0 || continued.Operation.FinalMicroUSD != 3 {
		t.Fatalf("denied continuation operation = %#v", continued.Operation)
	}

	remainingCapacity := memoryReservation(7, 10)
	accepted, err := store.Begin(context.Background(), admission.BeginRequest{
		ID: "after-denied-continue", ScopeKey: "tenant/after-denied-continue", RequestDigest: admission.Digest([]byte("after-denied-continue")),
		Reservation: 7, Reservations: []admission.WindowReservation{remainingCapacity}, ExpiresAt: now.Add(time.Hour),
	})
	if err != nil || accepted.Denied != nil {
		t.Fatalf("ghost reservation after denied continue = %#v, %v", accepted, err)
	}
}

// continueAfterRejection continues operation after its first attempt on
// route was rejected having incurred 3, reserving reservation for the
// remaining budget.
func continueAfterRejection(store *AdmissionStore, now time.Time, operation admission.Operation, route string, remaining pricing.MicroUSD, reservation admission.WindowReservation) (admission.ContinueResult, error) {
	return store.Continue(context.Background(), admission.ContinueRequest{
		OperationID:   operation.ID,
		DispatchToken: operation.DispatchToken,
		Outcome: admission.AttemptOutcome{
			Certainty: admission.Rejected,
			Incurred:  3,
			Attempt:   admission.AttemptFacts{RouteID: route, AttemptNumber: 1},
		},
		Remaining:    remaining,
		Reservations: []admission.WindowReservation{reservation},
		LeaseUntil:   now.Add(time.Minute),
		ExpiresAt:    now.Add(time.Hour),
	})
}
