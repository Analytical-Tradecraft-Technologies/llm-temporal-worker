package durable

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

func TestReferenceMaterializerIsIdempotentAndAtomic(t *testing.T) {
	now := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	m := newReferenceMaterializer(t, func() time.Time { return now })
	reservation := referenceTestReservation(now, 0, "0.75")
	request := referenceRequest("op-1", reservation)

	first, err := m.Accept(context.Background(), request)
	if err != nil || !first.Accepted {
		t.Fatalf("first acceptance = %#v, %v", first, err)
	}
	replay, err := m.Accept(context.Background(), request)
	if err != nil || replay.Accepted != first.Accepted || len(replay.Events) != len(first.Events) {
		t.Fatalf("idempotent replay = %#v, %v", replay, err)
	}
	conflict := request
	conflict.Reservations = []admission.WindowReservation{referenceTestReservation(now, 0, "0.74")}
	if _, err := m.Accept(context.Background(), conflict); !errors.Is(err, ErrReferenceMaterializerConflict) {
		t.Fatalf("changed replay error = %v, want conflict", err)
	}

	// A denied multi-window request must not consume the capacity of its first
	// window before discovering that its second window is full.
	denied := ReserveRequest{OperationID: "op-denied", GenerationID: "gen-1", Reservations: []admission.WindowReservation{
		referenceTestReservation(now, 1, "0.10"),
		referenceTestReservation(now, 0, "0.30"),
	}}
	denied.Reservations[0].WindowID = "other-window"
	assertReferenceAccept(t, m, denied, false, "multi-window denial =")
	followup := referenceRequest("op-followup", referenceTestReservation(now, 1, "0.10"))
	followup.Reservations[0].WindowID = "other-window"
	assertReferenceAccept(t, m, followup, true, "atomic denial consumed capacity:")
}

func TestReferenceMaterializerReconcilesByWindowAndBucket(t *testing.T) {
	now := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	m := newReferenceMaterializer(t, func() time.Time { return now })
	first := referenceTestReservation(now, 0, "0.40")
	second := referenceTestReservation(now, 1, "0.40")
	request := referenceRequest("op-multi", first, second)
	assertReferenceAccept(t, m, request, true, "acceptance =")
	claimReference(t, m, request)
	completion := exactCompletion("completion-second", request.OperationID, request.GenerationID, second, 2, "0.10")
	reconcile := referenceReconcile(request, completion)
	if err := m.Reconcile(context.Background(), reconcile); err != nil {
		t.Fatalf("reconcile = %v", err)
	}
	if err := m.Reconcile(context.Background(), reconcile); err != nil {
		t.Fatalf("idempotent reconcile = %v", err)
	}
	changed := completion
	changed.ActualCostUSD = ptr(pricing.MustUSD("0.11"))
	changed.AccountedIncreaseUSD = pricing.MustUSD("0.11")
	if err := m.Reconcile(context.Background(), referenceReconcile(request, changed)); !errors.Is(err, ErrReferenceMaterializerConflict) {
		t.Fatalf("changed completion error = %v, want conflict", err)
	}

	// The completion above targeted only bucket 1. Bucket 0 remains at 0.40,
	// while bucket 1 has only 0.10 accounted cost.
	assertReferenceAccept(t, m, referenceRequest("op-first-followup", referenceTestReservation(now, 0, "0.60")), false, "bucket 0 was reconciled accidentally:")
	assertReferenceAccept(t, m, referenceRequest("op-second-followup", referenceTestReservation(now, 1, "0.50")), true, "bucket 1 did not reconcile independently:")
}

func TestReferenceMaterializerExpiryRemovesReservedAndAccounted(t *testing.T) {
	now := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	m := newReferenceMaterializer(t, func() time.Time { return now })
	reservation := referenceTestReservation(now, 0, "0.40")
	request := ReserveRequest{OperationID: "op-expiring", GenerationID: "gen-1", ExpiresAt: now.Add(time.Minute), Reservations: []admission.WindowReservation{reservation}}
	assertReferenceAccept(t, m, request, true, "acceptance =")
	claimReference(t, m, request)
	completion := exactCompletion("completion-expiring", request.OperationID, request.GenerationID, reservation, 2, "0.20")
	if err := m.Reconcile(context.Background(), referenceReconcile(request, completion)); err != nil {
		t.Fatalf("reconcile = %v", err)
	}
	now = now.Add(2 * time.Minute)
	assertReferenceAccept(t, m, referenceRequest("still-accounted", referenceTestReservation(now, 0, "1.00")), false, "start deadline refunded accounted cost:")
	now = now.Add(3 * time.Hour)
	assertReferenceAccept(t, m, referenceRequest("op-after-expiry", referenceTestReservation(now, 0, "1.00")), true, "expired reserved/accounted values still consume capacity:")
}

func newReferenceMaterializer(t *testing.T, now func() time.Time) *ReferenceBudgetMaterializer {
	t.Helper()
	m, err := NewReferenceBudgetMaterializer("gen-1", "inc-1", now)
	if err != nil {
		t.Fatalf("new reference materializer: %v", err)
	}
	return m
}

// referenceRequest reserves reservations for operation id in "gen-1".
func referenceRequest(id OperationID, reservations ...admission.WindowReservation) ReserveRequest {
	return ReserveRequest{OperationID: id, GenerationID: "gen-1", Reservations: reservations}
}

// assertReferenceAccept requires request to be accepted (want) or denied
// without error, failing with "<label> <result>, <err>" otherwise.
func assertReferenceAccept(t *testing.T, m *ReferenceBudgetMaterializer, request ReserveRequest, want bool, label string) {
	t.Helper()
	if result, err := m.Accept(context.Background(), request); err != nil || result.Accepted != want {
		t.Fatalf("%s %#v, %v", label, result, err)
	}
}

func claimReference(t *testing.T, m *ReferenceBudgetMaterializer, request ReserveRequest) {
	t.Helper()
	if _, err := m.Claim(context.Background(), ClaimRequest{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: "inc-1"}); err != nil {
		t.Fatal(err)
	}
}

// referenceReconcile settles request's "inc-1" claim with events.
func referenceReconcile(request ReserveRequest, events ...budget.CompletionEvent) ReconcileRequest {
	return ReconcileRequest{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: "inc-1", Events: events}
}

func referenceTestReservation(now time.Time, bucketOffset int64, amount string) admission.WindowReservation {
	bucketNanos := int64(time.Hour)
	bucket := now.UnixNano()/bucketNanos + bucketOffset
	return admission.WindowReservation{
		PolicyID:      "policy",
		WindowID:      "window",
		Bucket:        bucket,
		AmountUSD:     pricing.MustUSD(amount),
		LimitUSD:      pricing.MustUSD("1.00"),
		BucketNanos:   bucketNanos,
		DurationNanos: int64(2 * time.Hour),
	}
}

func exactCompletion(eventID string, operationID OperationID, generationID GenerationID, reservation admission.WindowReservation, revision int, actual string) budget.CompletionEvent {
	cost := pricing.MustUSD(actual)
	amount := pricing.MustUSD(reservation.AmountUSD.String())
	return budget.CompletionEvent{
		EventID:              eventID,
		GenerationID:         string(generationID),
		OperationID:          string(operationID),
		WindowID:             reservation.WindowID,
		BucketStart:          time.Unix(0, reservation.Bucket*reservation.BucketNanos).UTC(),
		ReservationRevision:  revision,
		Kind:                 budget.JournalFinalizeExact,
		ReservedDecreaseUSD:  amount,
		AccountedIncreaseUSD: cost,
		ActualCostUSD:        &cost,
		CostStatus:           budget.CostExact,
		OccurredAt:           time.Now().UTC(),
	}
}
