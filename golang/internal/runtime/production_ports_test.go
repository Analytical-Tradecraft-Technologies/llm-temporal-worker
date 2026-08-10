package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	durablestore "github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

func TestProductionPhaseFactoriesRejectIncompleteSnapshotBeforeCallbacks(t *testing.T) {
	if _, err := NewProductionGeneratePortsFactory()(context.Background(), V1RuntimeCapabilities{}); err == nil {
		t.Fatal("Generate factory accepted an incomplete durable snapshot")
	}
	if _, err := NewProductionCompactPortsFactory()(context.Background(), V1RuntimeCapabilities{}); err == nil {
		t.Fatal("Compact factory accepted an incomplete durable snapshot")
	}
}

func TestProductionOperationIdentitySeparatesKindsAndRequestDigests(t *testing.T) {
	first := operationIdentity("generate", "operation", [32]byte{1})
	if first != operationIdentity("generate", "operation", [32]byte{1}) {
		t.Fatal("operation identity is not deterministic")
	}
	if first == operationIdentity("compact", "operation", [32]byte{1}) {
		t.Fatal("Generate and Compact operation identities collided")
	}
	if first == operationIdentity("generate", "operation", [32]byte{2}) {
		t.Fatal("distinct request digests collided")
	}
	if err := first.Validate(); err != nil {
		t.Fatalf("operation identity is invalid: %v", err)
	}
}

func TestCompletionEventsPreserveExactUSDForEveryBudgetWindow(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	actual := "0.125"
	reserved := pricing.MustUSD("0.2")
	actualUSD := pricing.MustUSD(actual)
	route := durablestore.RoutePlan{OperationID: "operation-1", GenerationID: "generation-1", Execution: &durablestore.RouteExecution{EstimatedUSD: reserved}}
	reservation := durablestore.ReserveResult{Events: []budget.ReservationEvent{(budgetReservationFixture{}).event("event-a", "window-a", reserved, now), (budgetReservationFixture{}).event("event-b", "window-b", reserved, now)}}
	events, err := completionEvents(route, reservation, llm.CostV1{Status: "exact", ActualCostUSD: &actual, Method: "provider_reported"}, now.Add(time.Second))
	if err != nil {
		t.Fatalf("completionEvents() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("completion event count = %d, want 2", len(events))
	}
	for _, event := range events {
		if event.Kind != budget.JournalFinalizeExact || event.ActualCostUSD == nil || event.ActualCostUSD.Cmp(actualUSD) != 0 || event.AccountedIncreaseUSD.Cmp(actualUSD) != 0 || event.ReservedDecreaseUSD.Cmp(reserved) != 0 {
			t.Fatalf("completion event lost exact accounting: %#v", event)
		}
		if err := event.Validate(); err != nil {
			t.Fatalf("completion event invalid: %v", err)
		}
	}
}

type budgetReservationFixture struct{}

func (budgetReservationFixture) event(id, window string, amount pricing.USD, at time.Time) budget.ReservationEvent {
	return budget.ReservationEvent{EventID: id, GenerationID: "generation-1", OperationID: "operation-1", WindowID: window, BucketStart: at, ReservationRevision: 1, AmountUSD: amount, OccurredAt: at}
}
