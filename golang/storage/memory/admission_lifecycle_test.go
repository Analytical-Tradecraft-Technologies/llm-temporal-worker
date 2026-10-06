package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
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

func TestAdmissionCompleteReconcilesReservationAndClonesResult(t *testing.T) {
	now := time.Unix(100, 0)
	store := NewAdmissionStore(AdmissionOptions{Clock: func() time.Time { return now }})
	reservation := memoryReservation(10, 10)
	operation := beginMemoryOperation(t, store, now, "complete", reservation)
	operation = dispatchMemoryOperation(t, store, now, operation, "initial")

	ref := &state.BlobRef{Digest: [32]byte{1}, Size: 3, Media: "application/octet-stream"}
	if err := store.Complete(context.Background(), admission.CompleteRequest{
		OperationID:   operation.ID,
		DispatchToken: operation.DispatchToken,
		Actual:        3,
		ResultRef:     ref,
		Attempt:       admission.AttemptFacts{RouteID: "initial"},
	}); err != nil {
		t.Fatal(err)
	}
	ref.Media = "mutated-input"

	got, err := store.Get(context.Background(), operation.ID)
	if err != nil {
		t.Fatal(err)
	}
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

	if err := store.Complete(context.Background(), admission.CompleteRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Actual: 99}); err != nil {
		t.Fatalf("completed replay = %v", err)
	}
	next := memoryReservation(8, 10)
	denied, err := store.Begin(context.Background(), admission.BeginRequest{
		ID: "after-complete", ScopeKey: "tenant/after-complete", RequestDigest: admission.Digest([]byte("after-complete")),
		Reservation: 8, Reservations: []admission.WindowReservation{next}, ExpiresAt: now.Add(time.Hour),
	})
	if err != nil || denied.Denied == nil || denied.Denied.Active != 3 {
		t.Fatalf("post-completion budget = %#v, %v", denied, err)
	}
}

func TestAdmissionProviderPendingRetainsPollGuidance(t *testing.T) {
	now := time.Unix(100, 0)
	store := NewAdmissionStore(AdmissionOptions{Clock: func() time.Time { return now }})
	operation := beginMemoryOperation(t, store, now, "pending-guidance", memoryReservation(1, 10))
	operation = dispatchMemoryOperation(t, store, now, operation, "initial")
	pollAfter := now.Add(5 * time.Second)
	if err := store.MarkProviderPending(context.Background(), admission.ProviderPendingRequest{
		OperationID: operation.ID, DispatchToken: operation.DispatchToken,
		ProviderOperationID: "provider-operation", EndpointID: "endpoint", PollAfter: pollAfter,
	}); err != nil {
		t.Fatal(err)
	}
	schedule, ok := any(store).(admission.ProviderPendingSchedule)
	if !ok {
		t.Fatal("memory admission store does not expose provider poll schedule")
	}
	got, err := schedule.ProviderPollAfter(context.Background(), operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(pollAfter) {
		t.Fatalf("poll schedule = %s, want %s", got, pollAfter)
	}
}

func TestAdmissionDefiniteFailureReconcilesIncurredCost(t *testing.T) {
	now := time.Unix(100, 0)
	store := NewAdmissionStore(AdmissionOptions{Clock: func() time.Time { return now }})
	operation := beginMemoryOperation(t, store, now, "failed", memoryReservation(10, 10))
	operation = dispatchMemoryOperation(t, store, now, operation, "initial")

	if err := store.Fail(context.Background(), admission.FailRequest{
		OperationID:   operation.ID,
		DispatchToken: operation.DispatchToken,
		Certainty:     admission.Rejected,
		Incurred:      3,
		Attempt:       admission.AttemptFacts{RouteID: "initial"},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(context.Background(), operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != admission.StateDefiniteFailed || got.ReservedMicroUSD != 0 || got.IncurredMicroUSD != 3 || got.FinalMicroUSD != 3 || got.Attempt.Dispatch != admission.Rejected {
		t.Fatalf("failed operation = %#v", got)
	}

	next := memoryReservation(8, 10)
	denied, err := store.Begin(context.Background(), admission.BeginRequest{
		ID: "after-failure", ScopeKey: "tenant/after-failure", RequestDigest: admission.Digest([]byte("after-failure")),
		Reservation: 8, Reservations: []admission.WindowReservation{next}, ExpiresAt: now.Add(time.Hour),
	})
	if err != nil || denied.Denied == nil || denied.Denied.Active != 3 {
		t.Fatalf("post-failure budget = %#v, %v", denied, err)
	}
}

func TestAdmissionContinueReconcilesAndRotatesDispatchToken(t *testing.T) {
	now := time.Unix(100, 0)
	store := NewAdmissionStore(AdmissionOptions{Clock: func() time.Time { return now }})
	operation := beginMemoryOperation(t, store, now, "continue", memoryReservation(10, 10))
	operation = dispatchMemoryOperation(t, store, now, operation, "initial")

	next := memoryReservation(7, 10)
	continued, err := store.Continue(context.Background(), admission.ContinueRequest{
		OperationID:   operation.ID,
		DispatchToken: operation.DispatchToken,
		Outcome: admission.AttemptOutcome{
			Certainty: admission.Rejected,
			Incurred:  3,
			Attempt:   admission.AttemptFacts{RouteID: "fallback", AttemptNumber: 1},
		},
		Remaining:    7,
		Reservations: []admission.WindowReservation{next},
		LeaseUntil:   now.Add(time.Minute),
		ExpiresAt:    now.Add(time.Hour),
	})
	if err != nil || continued.Denied != nil {
		t.Fatalf("continue = %#v, %v", continued, err)
	}
	if continued.Operation.State != admission.StateReserved || continued.Operation.ReservedMicroUSD != 7 || continued.Operation.DispatchToken == operation.DispatchToken || continued.Operation.Attempt.Dispatch != admission.NotDispatched || continued.Operation.Attempt.RouteID != "fallback" {
		t.Fatalf("continued operation = %#v", continued.Operation)
	}
	if err := store.MarkDispatching(context.Background(), admission.DispatchRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Attempt: continued.Operation.Attempt}); !errors.Is(err, admission.ErrInvalidToken) {
		t.Fatalf("old dispatch token error = %v", err)
	}
	if err := store.MarkDispatching(context.Background(), admission.DispatchRequest{OperationID: operation.ID, DispatchToken: continued.Operation.DispatchToken, Attempt: continued.Operation.Attempt}); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(context.Background(), admission.CompleteRequest{OperationID: operation.ID, DispatchToken: continued.Operation.DispatchToken, Actual: 7, Attempt: admission.AttemptFacts{RouteID: "fallback", AttemptNumber: 2}}); err != nil {
		t.Fatal(err)
	}
	completed, err := store.Get(context.Background(), operation.ID)
	if err != nil || completed.State != admission.StateCompleted || completed.FinalMicroUSD != 7 || completed.Attempt.AttemptNumber != 2 {
		t.Fatalf("completed fallback = %#v, %v", completed, err)
	}
}

func TestAdmissionContinueDenialFinalizesWithoutGhostReservation(t *testing.T) {
	now := time.Unix(100, 0)
	store := NewAdmissionStore(AdmissionOptions{Clock: func() time.Time { return now }})
	operation := beginMemoryOperation(t, store, now, "continue-denied", memoryReservation(10, 10))
	operation = dispatchMemoryOperation(t, store, now, operation, "initial")

	deniedReservation := memoryReservation(8, 10)
	continued, err := store.Continue(context.Background(), admission.ContinueRequest{
		OperationID:   operation.ID,
		DispatchToken: operation.DispatchToken,
		Outcome: admission.AttemptOutcome{
			Certainty: admission.Rejected,
			Incurred:  3,
			Attempt:   admission.AttemptFacts{RouteID: "initial", AttemptNumber: 1},
		},
		Remaining:    8,
		Reservations: []admission.WindowReservation{deniedReservation},
		LeaseUntil:   now.Add(time.Minute),
		ExpiresAt:    now.Add(time.Hour),
	})
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
