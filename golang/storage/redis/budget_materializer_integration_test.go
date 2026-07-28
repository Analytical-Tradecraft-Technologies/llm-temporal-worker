//go:build integration

package redis

import (
	"context"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	durable "github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// TestLiveRedisBudgetMaterializerContract exercises the active durable budget
// port against the same provisioned Redis Function used by production. It
// covers acceptance, idempotent denial, and duplicate completion reconciliation
// without depending on process-local state.
func TestLiveRedisBudgetMaterializerContract(t *testing.T) {
	client := openLiveRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	keys := liveKeyOptions("durable-materializer")
	cleanupLivePrefix(t, client, keys.Prefix)

	now := time.Now().UTC().Truncate(time.Second)
	materializer, err := NewRedisBudgetMaterializer(RedisBudgetMaterializerOptions{
		Client: client, Mode: AdmissionModeFunction, Keys: keys,
		GenerationID:  durable.GenerationID("generation-live"),
		IncarnationID: durable.IncarnationID("incarnation-live"),
		Clock:         func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	reservation := admission.WindowReservation{
		PolicyID: "durable-policy", WindowID: "hour", Bucket: now.Unix() / 3600,
		AmountUSD: pricing.MustUSD("0.01"), LimitUSD: pricing.MustUSD("0.02"),
		BucketNanos: int64(time.Hour), DurationNanos: int64(24 * time.Hour),
	}
	request := durable.ReserveRequest{
		OperationID: "durable-operation-accepted", GenerationID: "generation-live",
		ExpiresAt: now.Add(time.Hour), Reservations: []admission.WindowReservation{reservation},
	}
	accepted, err := materializer.Accept(ctx, request)
	if err != nil {
		t.Fatalf("accepted reservation = %v", err)
	}
	if !accepted.Accepted || len(accepted.Events) != 1 {
		t.Fatalf("accepted reservation = %#v", accepted)
	}

	deniedRequest := request
	deniedRequest.OperationID = "durable-operation-denied"
	denied, err := materializer.Accept(ctx, deniedRequest)
	if err != nil {
		t.Fatalf("denied reservation = %v", err)
	}
	if denied.Accepted || denied.Denial == nil {
		t.Fatalf("denied reservation = %#v", denied)
	}
	deniedReplay, err := materializer.Accept(ctx, deniedRequest)
	if err != nil {
		t.Fatalf("denied replay = %v", err)
	}
	if deniedReplay.Accepted || deniedReplay.Denial == nil || deniedReplay.Denial.PolicyID != denied.Denial.PolicyID {
		t.Fatalf("denied replay = %#v", deniedReplay)
	}

	reservationEvent := accepted.Events[0]
	completion := budget.CompletionEvent{
		EventID: "durable-completion-1", GenerationID: string(request.GenerationID),
		OperationID: string(request.OperationID), WindowID: reservationEvent.WindowID,
		BucketStart: reservationEvent.BucketStart, ReservationRevision: reservationEvent.ReservationRevision + 1,
		Kind: budget.JournalFinalizeExact, ReservedDecreaseUSD: reservationEvent.AmountUSD,
		AccountedIncreaseUSD: reservationEvent.AmountUSD, ActualCostUSD: ptrUSD(reservationEvent.AmountUSD),
		CostStatus: budget.CostExact, OccurredAt: now,
	}
	reconcile := durable.ReconcileRequest{
		OperationID: request.OperationID, GenerationID: request.GenerationID,
		IncarnationID: "incarnation-live", Events: []budget.CompletionEvent{completion},
	}
	if err := materializer.Reconcile(ctx, reconcile); err != nil {
		t.Fatalf("completion reconciliation = %v", err)
	}
	if err := materializer.Reconcile(ctx, reconcile); err != nil {
		t.Fatalf("duplicate completion reconciliation = %v", err)
	}
}
