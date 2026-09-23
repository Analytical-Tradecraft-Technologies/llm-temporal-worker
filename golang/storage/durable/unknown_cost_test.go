package durable

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

type unknownCostMaterializer struct {
	err   error
	calls int
}

func (materializer *unknownCostMaterializer) Accept(context.Context, ReserveRequest) (ReserveResult, error) {
	return ReserveResult{}, errors.New("not used by unknown-cost resolution")
}

func (materializer *unknownCostMaterializer) Reconcile(context.Context, ReconcileRequest) error {
	materializer.calls++
	return materializer.err
}

func unknownCostBoundary() UnknownCostBoundary {
	return UnknownCostBoundary{
		Identity:     validIdentity(),
		Materializer: &unknownCostMaterializer{},
	}
}

func unknownCostResolution(amount string) UnknownCostResolution {
	now := time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC)
	actual := pricing.MustUSD(amount)
	return UnknownCostResolution{
		OperationID:   OperationID("operation-unknown"),
		GenerationID:  GenerationID("generation-1"),
		IncarnationID: IncarnationID("incarnation-1"),
		Events: []budget.CompletionEvent{{
			EventID:              "resolve-1",
			GenerationID:         "generation-1",
			OperationID:          "operation-unknown",
			WindowID:             "window-1",
			BucketStart:          now.Add(-time.Hour),
			ReservationRevision:  3,
			Kind:                 budget.JournalResolveUnknownExact,
			AccountedDecreaseUSD: pricing.MustUSD("1.25"),
			AccountedIncreaseUSD: actual,
			ActualCostUSD:        &actual,
			CostStatus:           budget.CostExact,
			OccurredAt:           now,
		}},
	}
}

func TestUnknownCostBoundaryRedisFailureReturnsAuthoritativeReceipt(t *testing.T) {
	boundary := unknownCostBoundary()
	materializer := &unknownCostMaterializer{err: errors.New("redis unavailable")}
	boundary.Materializer = materializer
	result, err := boundary.Resolve(context.Background(), unknownCostResolution("0.25"))
	if !errors.Is(err, ErrReconcilePending) {
		t.Fatalf("Redis error = %v, want ErrReconcilePending", err)
	}
	if !result.ReconcilePending || result.Reconciled {
		t.Fatalf("pending result = %+v, want receipt and pending reconciliation", result)
	}
	if materializer.calls != 1 {
		t.Fatalf("Redis reconciliation calls = %d, want 1", materializer.calls)
	}
}

func TestUnknownCostBoundaryRetriesIdempotentlyAfterRedisRecovery(t *testing.T) {
	boundary := unknownCostBoundary()
	materializer := boundary.Materializer.(*unknownCostMaterializer)
	resolution := unknownCostResolution("0.25")
	first, err := boundary.Resolve(context.Background(), resolution)
	if err != nil || !first.Reconciled {
		t.Fatalf("first resolution = %+v, %v", first, err)
	}
	second, err := boundary.Resolve(context.Background(), resolution)
	if err != nil || !second.Reconciled {
		t.Fatalf("idempotent replay = %+v, %v", second, err)
	}
	if materializer.calls != 2 {
		t.Fatalf("Redis reconciliation calls = %d, want one per safe retry", materializer.calls)
	}

}

func TestUnknownCostResolutionRejectsIdentityAndAmountDrift(t *testing.T) {
	resolution := unknownCostResolution("0.25")
	resolution.OperationID = OperationID("other-operation")
	if err := resolution.Validate(); err == nil {
		t.Fatal("operation identity drift was accepted")
	}
	resolution = unknownCostResolution("0.25")
	second := resolution.Events[0]
	amount := pricing.MustUSD("0.26")
	second.EventID = "resolve-2"
	second.ActualCostUSD = &amount
	second.AccountedIncreaseUSD = amount
	resolution.Events = append(resolution.Events, second)
	if err := resolution.Validate(); err == nil {
		t.Fatal("mixed exact amounts were accepted")
	}
	resolution = unknownCostResolution("0.25")
	resolution.Events[0].Kind = budget.JournalFinalizeExact
	if err := resolution.Validate(); err == nil {
		t.Fatal("non-resolution event kind was accepted")
	}
}

func TestUnknownCostBoundaryRejectsTypedNilPorts(t *testing.T) {
	var materializer *unknownCostMaterializer
	boundary := UnknownCostBoundary{Identity: validIdentity(), Materializer: materializer}
	if err := boundary.Validate(); err == nil {
		t.Fatal("typed nil materializer accepted")
	}
}
