package postgres

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func validUnknownResolutionEvent(operationID, eventID string, amount pricing.USD) budget.CompletionEvent {
	return budget.CompletionEvent{
		EventID: eventID, GenerationID: uuid.NewString(), OperationID: operationID, WindowID: uuid.NewString(),
		BucketStart: time.Unix(1, 0).UTC(), ReservationRevision: 2,
		Kind: budget.JournalResolveUnknownExact, ReservedDecreaseUSD: pricing.MustUSD("1"),
		AccountedIncreaseUSD: amount, ActualCostUSD: &amount, CostStatus: budget.CostExact,
		OccurredAt: time.Unix(2, 0).UTC(),
	}
}

func TestValidateUnknownResolutionEventsRequiresOneOperationAndAmount(t *testing.T) {
	operationID := uuid.NewString()
	amount := pricing.MustUSD("0.75")
	first := validUnknownResolutionEvent(operationID, uuid.NewString(), amount)
	if gotID, gotAmount, err := validateUnknownResolutionEvents([]budget.CompletionEvent{first}); err != nil || gotID.String() != operationID || gotAmount.Cmp(amount) != 0 {
		t.Fatalf("valid resolution = %s/%s/%v", gotID, gotAmount.String(), err)
	}
	for name, events := range map[string][]budget.CompletionEvent{
		"mixed operations": func() []budget.CompletionEvent {
			second := validUnknownResolutionEvent(uuid.NewString(), uuid.NewString(), amount)
			return []budget.CompletionEvent{first, second}
		}(),
		"mixed amounts": func() []budget.CompletionEvent {
			second := validUnknownResolutionEvent(operationID, uuid.NewString(), pricing.MustUSD("0.76"))
			return []budget.CompletionEvent{first, second}
		}(),
		"duplicate event": []budget.CompletionEvent{first, first},
		"wrong kind": func() []budget.CompletionEvent {
			wrong := first
			wrong.Kind = budget.JournalFinalizeExact
			return []budget.CompletionEvent{wrong}
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := validateUnknownResolutionEvents(events); err == nil {
				t.Fatal("invalid resolution was accepted")
			}
		})
	}
}

func TestUnknownCostOperationUpdateIsScopedAndClearsUnknownReason(t *testing.T) {
	query := unknownCostOperationUpdateSQL(`"llm_worker"."operations"`)
	for _, required := range []string{
		`UPDATE "llm_worker"."operations"`,
		"actual_cost_usd=$2",
		"cost_status='exact'",
		"cost_unknown_reason_code=NULL",
		"WHERE operation_id=$1 AND cost_status='unknown'",
	} {
		if !strings.Contains(query, required) {
			t.Fatalf("unknown-cost update missing %q: %s", required, query)
		}
	}
	if strings.Contains(strings.ToUpper(query), "SELECT") || strings.Contains(strings.ToUpper(query), "REDIS") {
		t.Fatalf("unknown-cost operation update should be one scoped SQL write: %s", query)
	}
}

func TestResolveUnknownExactRejectsWithoutPoolBeforeEventWork(t *testing.T) {
	repository := &BudgetJournalRepository{}
	if _, err := repository.ResolveUnknownExact(t.Context(), []budget.CompletionEvent{{}}); err == nil || !strings.Contains(err.Error(), "budget journal PostgreSQL pool") {
		t.Fatalf("nil pool validation = %v", err)
	}
}
