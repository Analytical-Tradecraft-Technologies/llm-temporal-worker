package durable

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func TestReferenceMaterializerIsIdempotentAndAtomic(t *testing.T) {
	now := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	m := newReferenceMaterializer(t, func() time.Time { return now })
	reservation := referenceTestReservation(now, 0, "0.75")
	request := ReserveRequest{OperationID: "op-1", GenerationID: "gen-1", Reservations: []admission.WindowReservation{reservation}}

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
	result, err := m.Accept(context.Background(), denied)
	if err != nil || result.Accepted {
		t.Fatalf("multi-window denial = %#v, %v", result, err)
	}
	followup := ReserveRequest{OperationID: "op-followup", GenerationID: "gen-1", Reservations: []admission.WindowReservation{referenceTestReservation(now, 1, "0.10")}}
	followup.Reservations[0].WindowID = "other-window"
	if result, err := m.Accept(context.Background(), followup); err != nil || !result.Accepted {
		t.Fatalf("atomic denial consumed capacity: %#v, %v", result, err)
	}
}

func TestReferenceMaterializerReconcilesByWindowAndBucket(t *testing.T) {
	now := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	m := newReferenceMaterializer(t, func() time.Time { return now })
	first := referenceTestReservation(now, 0, "0.60")
	second := referenceTestReservation(now, 1, "0.60")
	request := ReserveRequest{OperationID: "op-multi", GenerationID: "gen-1", Reservations: []admission.WindowReservation{first, second}}
	accepted, err := m.Accept(context.Background(), request)
	if err != nil || !accepted.Accepted {
		t.Fatalf("acceptance = %#v, %v", accepted, err)
	}

	completion := exactCompletion("completion-second", request.OperationID, request.GenerationID, second, 2, "0.10")
	reconcile := ReconcileRequest{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: "inc-1", Events: []budget.CompletionEvent{completion}}
	if err := m.Reconcile(context.Background(), reconcile); err != nil {
		t.Fatalf("reconcile = %v", err)
	}
	if err := m.Reconcile(context.Background(), reconcile); err != nil {
		t.Fatalf("idempotent reconcile = %v", err)
	}
	changed := completion
	changed.ActualCostUSD = ptr(pricing.MustUSD("0.11"))
	changed.AccountedIncreaseUSD = pricing.MustUSD("0.11")
	if err := m.Reconcile(context.Background(), ReconcileRequest{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: "inc-1", Events: []budget.CompletionEvent{changed}}); !errors.Is(err, ErrReferenceMaterializerConflict) {
		t.Fatalf("changed completion error = %v, want conflict", err)
	}

	// The completion above targeted only bucket 1. Bucket 0 remains at 0.60,
	// while bucket 1 has only 0.10 accounted cost.
	firstFollowup := ReserveRequest{OperationID: "op-first-followup", GenerationID: "gen-1", Reservations: []admission.WindowReservation{referenceTestReservation(now, 0, "0.50")}}
	if result, err := m.Accept(context.Background(), firstFollowup); err != nil || result.Accepted {
		t.Fatalf("bucket 0 was reconciled accidentally: %#v, %v", result, err)
	}
	secondFollowup := ReserveRequest{OperationID: "op-second-followup", GenerationID: "gen-1", Reservations: []admission.WindowReservation{referenceTestReservation(now, 1, "0.50")}}
	if result, err := m.Accept(context.Background(), secondFollowup); err != nil || !result.Accepted {
		t.Fatalf("bucket 1 did not reconcile independently: %#v, %v", result, err)
	}
}

func TestReferenceMaterializerExpiryRemovesReservedAndAccounted(t *testing.T) {
	now := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	m := newReferenceMaterializer(t, func() time.Time { return now })
	reservation := referenceTestReservation(now, 0, "0.40")
	request := ReserveRequest{OperationID: "op-expiring", GenerationID: "gen-1", ExpiresAt: now.Add(time.Minute), Reservations: []admission.WindowReservation{reservation}}
	if result, err := m.Accept(context.Background(), request); err != nil || !result.Accepted {
		t.Fatalf("acceptance = %#v, %v", result, err)
	}
	completion := exactCompletion("completion-expiring", request.OperationID, request.GenerationID, reservation, 2, "0.20")
	if err := m.Reconcile(context.Background(), ReconcileRequest{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: "inc-1", Events: []budget.CompletionEvent{completion}}); err != nil {
		t.Fatalf("reconcile = %v", err)
	}
	now = now.Add(2 * time.Minute)
	newRequest := ReserveRequest{OperationID: "op-after-expiry", GenerationID: "gen-1", Reservations: []admission.WindowReservation{referenceTestReservation(now, 0, "1.00")}}
	if result, err := m.Accept(context.Background(), newRequest); err != nil || !result.Accepted {
		t.Fatalf("expired reserved/accounted values still consume capacity: %#v, %v", result, err)
	}
}

func newReferenceMaterializer(t *testing.T, now func() time.Time) *ReferenceBudgetMaterializer {
	t.Helper()
	m, err := NewReferenceBudgetMaterializer("gen-1", "inc-1", now)
	if err != nil {
		t.Fatalf("new reference materializer: %v", err)
	}
	return m
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
