package memory

import (
	"context"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func TestAdmissionBeginIdempotencyAndAmbiguity(t *testing.T) {
	now := time.Unix(100, 0)
	store := NewAdmissionStore(AdmissionOptions{Clock: func() time.Time { return now }})
	reservation := admission.WindowReservation{PolicyID: "p", WindowID: "w", Bucket: 100, Amount: 10, Limit: 100, BucketNanos: int64(time.Second), DurationNanos: int64(10 * time.Second)}
	request := admission.BeginRequest{ID: "op", ScopeKey: "tenant/op", RequestDigest: admission.Digest([]byte("request")), Reservation: 10, Reservations: []admission.WindowReservation{reservation}, LeaseUntil: now.Add(time.Minute), ExpiresAt: now.Add(time.Hour)}
	first, err := store.Begin(context.Background(), request)
	if err != nil || first.Existing {
		t.Fatalf("first begin = %#v %v", first, err)
	}
	replay, err := store.Begin(context.Background(), request)
	if err != nil || !replay.Existing || replay.Operation.ID != "op" {
		t.Fatalf("replay = %#v %v", replay, err)
	}
	request.RequestDigest = admission.Digest([]byte("different"))
	if _, err := store.Begin(context.Background(), request); err != admission.ErrOperationConflict {
		t.Fatalf("conflict error = %v", err)
	}
	request.RequestDigest = first.Operation.RequestDigest
	if err := store.MarkDispatching(context.Background(), admission.DispatchRequest{OperationID: "op", DispatchToken: first.Operation.DispatchToken, Attempt: admission.AttemptFacts{RouteID: "r"}, LeaseUntil: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if err := store.Fail(context.Background(), admission.FailRequest{OperationID: "op", DispatchToken: first.Operation.DispatchToken, Certainty: admission.Ambiguous, Incurred: 0}); err != nil {
		t.Fatal(err)
	}
	operation, err := store.Get(context.Background(), "op")
	if err != nil || operation.State != admission.StateAmbiguous || operation.ReservedMicroUSD != 10 {
		t.Fatalf("ambiguous operation = %#v %v", operation, err)
	}
}

func TestAdmissionDeniesOverlappingWindow(t *testing.T) {
	now := time.Unix(100, 0)
	store := NewAdmissionStore(AdmissionOptions{Clock: func() time.Time { return now }})
	reservation := admission.WindowReservation{PolicyID: "p", WindowID: "w", Bucket: 100, Amount: 6, Limit: 10, BucketNanos: int64(time.Second), DurationNanos: int64(10 * time.Second)}
	_, err := store.Begin(context.Background(), admission.BeginRequest{ID: "one", ScopeKey: "one", RequestDigest: admission.Digest([]byte("one")), Reservation: 6, Reservations: []admission.WindowReservation{reservation}, LeaseUntil: now.Add(time.Minute), ExpiresAt: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	reservation.Amount = 5
	result, err := store.Begin(context.Background(), admission.BeginRequest{ID: "two", ScopeKey: "two", RequestDigest: admission.Digest([]byte("two")), Reservation: 5, Reservations: []admission.WindowReservation{reservation}, LeaseUntil: now.Add(time.Minute), ExpiresAt: now.Add(time.Hour)})
	if err != nil || result.Denied == nil {
		t.Fatalf("second begin = %#v %v", result, err)
	}
}

func TestAdmissionRejectsInvalidReservationEnvelopeBeforeMutation(t *testing.T) {
	now := time.Unix(100, 0)
	base := admission.WindowReservation{PolicyID: "p", WindowID: "w", Bucket: 100, Amount: 6, Limit: 10, BucketNanos: int64(time.Second), DurationNanos: int64(10 * time.Second)}
	for _, test := range []struct {
		name         string
		reservation  pricing.MicroUSD
		reservations []admission.WindowReservation
	}{
		{name: "mismatched amount", reservation: 5, reservations: []admission.WindowReservation{base}},
		{name: "duplicate identity", reservation: 6, reservations: []admission.WindowReservation{base, base}},
		{name: "missing vector", reservation: 6},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := NewAdmissionStore(AdmissionOptions{Clock: func() time.Time { return now }})
			_, err := store.Begin(context.Background(), admission.BeginRequest{ID: "invalid", ScopeKey: "invalid", Reservation: test.reservation, Reservations: test.reservations, ExpiresAt: now.Add(time.Hour)})
			if err == nil {
				t.Fatal("invalid reservation envelope accepted")
			}
			valid := base
			valid.Amount = 5
			result, err := store.Begin(context.Background(), admission.BeginRequest{ID: "valid", ScopeKey: "valid", Reservation: 5, Reservations: []admission.WindowReservation{valid}, ExpiresAt: now.Add(time.Hour)})
			if err != nil || result.Denied != nil {
				t.Fatalf("valid reservation after rejected envelope = %#v, %v", result, err)
			}
		})
	}
}
