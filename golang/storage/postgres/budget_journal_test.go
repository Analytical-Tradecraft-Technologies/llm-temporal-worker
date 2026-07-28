package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func TestBudgetJournalRejectsInvalidEventsBeforePoolAccess(t *testing.T) {
	namespace, err := NewNamespace("worker", "llm_worker", "")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	repository := &BudgetJournalRepository{Namespace: namespace}
	event := budget.ReservationEvent{EventID: "not-a-uuid", GenerationID: "generation", OperationID: "operation", WindowID: "window", BucketStart: time.Unix(1, 0), OccurredAt: time.Unix(2, 0), AmountUSD: pricing.MustUSD("1")}
	if _, err := repository.AppendReservation(context.Background(), event); err == nil || !strings.Contains(err.Error(), "budget journal PostgreSQL pool") {
		t.Fatalf("nil pool validation = %v", err)
	}
	repository.Pool = nil
	if _, err := repository.AppendReservation(context.Background(), event); err == nil {
		t.Fatal("invalid repository unexpectedly accepted")
	}
}

func TestBudgetJournalCompletionProjectionMatchesSchemaStates(t *testing.T) {
	for _, test := range []struct {
		kind  budget.JournalEventKind
		state string
		basis string
	}{
		{budget.JournalRetainAmbiguous, "retained_ambiguous", "retained_bound"},
		{budget.JournalFinalizeUnknown, "finalized", "retained_bound"},
		{budget.JournalRelease, "released", "released"},
		{budget.JournalFinalizeExact, "finalized", "exact_actual"},
		{budget.JournalResolveUnknownExact, "finalized", "exact_actual"},
	} {
		state, basis, _ := completionProjection(journalInput{kind: test.kind, occurredAt: time.Unix(1, 0)})
		if state != test.state || basis != test.basis {
			t.Fatalf("projection(%s) = %q/%q, want %q/%q", test.kind, state, basis, test.state, test.basis)
		}
	}
}

func TestBudgetJournalOnlyRendersValidatedRelations(t *testing.T) {
	namespace, err := NewNamespace("worker", "llm_worker", "tenant_")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	for _, logical := range []string{"budget_journal_events", "budget_buckets", "operation_budget_reservations"} {
		relation, err := namespace.Render(logical)
		if err != nil {
			t.Fatalf("render %s: %v", logical, err)
		}
		if !strings.Contains(relation, `"llm_worker"`) || !strings.Contains(relation, `"tenant_`) {
			t.Fatalf("relation %q is not schema/prefix qualified", relation)
		}
	}
}

func TestBudgetJournalAppendSQLIsWriteOnly(t *testing.T) {
	for name, query := range map[string]string{
		"journal append":         journalAppendSQL(`"llm_worker"."budget_journal_events"`),
		"bucket projection":      budgetBucketUpsertSQL(`"llm_worker"."budget_buckets"`),
		"reservation projection": reservationAppendSQL(`"llm_worker"."operation_budget_reservations"`),
		"completion projection":  reservationCompletionSQL(`"llm_worker"."operation_budget_reservations"`, budget.JournalFinalizeExact),
	} {
		if got := classifyBudgetJournalSQL(query); got != budgetJournalSQLWriteOnly {
			t.Fatalf("%s classification = %s, want %s: %s", name, got, budgetJournalSQLWriteOnly, query)
		}
	}
}

func TestBudgetJournalRetryRejectsChangedPayload(t *testing.T) {
	query := journalAppendSQL(`"llm_worker"."budget_journal_events"`)
	for _, required := range []string{
		"ON CONFLICT (operation_id, window_id, reservation_revision)",
		"redis_generation_id = EXCLUDED.redis_generation_id",
		"event_kind = EXCLUDED.event_kind",
		"reserved_increase_usd = EXCLUDED.reserved_increase_usd",
		"actual_cost_usd IS NOT DISTINCT FROM EXCLUDED.actual_cost_usd",
		"actual_cost_unknown_reason_code IS NOT DISTINCT FROM EXCLUDED.actual_cost_unknown_reason_code",
		"occurred_at = EXCLUDED.occurred_at",
	} {
		if !strings.Contains(query, required) {
			t.Fatalf("journal retry predicate missing %q: %s", required, query)
		}
	}
}

func TestBudgetCompletionProjectionGuardsStateAndRevision(t *testing.T) {
	query := reservationCompletionSQL(`"llm_worker"."operation_budget_reservations"`, budget.JournalFinalizeExact)
	for _, required := range []string{"reservation_revision < $9", "state IN ('reserved')", "$7::numeric IS NULL"} {
		if !strings.Contains(query, required) {
			t.Fatalf("completion update missing %q: %s", required, query)
		}
	}
	resolve := reservationCompletionSQL(`"llm_worker"."operation_budget_reservations"`, budget.JournalResolveUnknownExact)
	for _, required := range []string{
		"'retained_ambiguous'",
		"'finalized'",
		"state <> 'finalized' OR actual_cost_status = 'unknown'",
	} {
		if !strings.Contains(resolve, required) {
			t.Fatalf("unknown-cost resolution missing state fence %q: %s", required, resolve)
		}
	}
}

func TestBudgetBucketProjectionKeepsNewestJournalID(t *testing.T) {
	query := budgetBucketUpsertSQL(`"llm_worker"."budget_buckets"`)
	if !strings.Contains(query, "last_journal_id = GREATEST(") {
		t.Fatalf("bucket upsert does not retain the greatest journal ID: %s", query)
	}
}

func TestNullableBudgetJournalReason(t *testing.T) {
	if got := nullableReason(""); got != nil {
		t.Fatalf("empty reason = %#v, want nil", got)
	}
	if got := nullableReason("provider_timeout"); got != "provider_timeout" {
		t.Fatalf("non-empty reason = %#v", got)
	}
}

type budgetJournalSQLClass string

const (
	budgetJournalSQLRead      budgetJournalSQLClass = "read"
	budgetJournalSQLWriteOnly budgetJournalSQLClass = "write-only"
)

func classifyBudgetJournalSQL(query string) budgetJournalSQLClass {
	for _, token := range strings.FieldsFunc(strings.ToUpper(query), func(r rune) bool {
		return r < 'A' || r > 'Z'
	}) {
		if token == "SELECT" {
			return budgetJournalSQLRead
		}
	}
	return budgetJournalSQLWriteOnly
}
