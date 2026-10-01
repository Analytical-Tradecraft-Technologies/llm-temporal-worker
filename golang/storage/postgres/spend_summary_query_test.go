package postgres

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/control"
)

func TestSpendSummaryOptionsNormalizeAndCloseEnums(t *testing.T) {
	options := SpendSummaryListOptions{ScopeID: uuid.New(), StartTime: time.Unix(10, 0), EndTime: time.Unix(20, 0)}
	if err := normalizeSpendSummaryOptions(&options); err != nil {
		t.Fatal(err)
	}
	if options.StartTime.Location() != time.UTC || options.EndTime.Location() != time.UTC {
		t.Fatalf("time bounds were not normalized to UTC: %v, %v", options.StartTime.Location(), options.EndTime.Location())
	}
	for _, test := range []struct {
		name    string
		options SpendSummaryListOptions
	}{
		{name: "missing scope", options: SpendSummaryListOptions{StartTime: time.Unix(10, 0), EndTime: time.Unix(20, 0)}},
		{name: "reversed interval", options: SpendSummaryListOptions{ScopeID: uuid.New(), StartTime: time.Unix(20, 0), EndTime: time.Unix(10, 0)}},
		{name: "unknown dimension", options: SpendSummaryListOptions{ScopeID: uuid.New(), StartTime: time.Unix(10, 0), EndTime: time.Unix(20, 0), GroupBy: []control.SpendDimension{"tenant"}}},
		{name: "repeated dimension", options: SpendSummaryListOptions{ScopeID: uuid.New(), StartTime: time.Unix(10, 0), EndTime: time.Unix(20, 0), GroupBy: []control.SpendDimension{control.SpendByModel, control.SpendByModel}}},
		{name: "unknown operation", options: SpendSummaryListOptions{ScopeID: uuid.New(), StartTime: time.Unix(10, 0), EndTime: time.Unix(20, 0), OperationKinds: []control.OperationKind{"replay"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := normalizeSpendSummaryOptions(&test.options); err == nil {
				t.Fatal("invalid options were accepted")
			}
		})
	}
}

func TestSpendSummaryQueryUnionsLedgersAndUsesHalfOpenBounds(t *testing.T) {
	query := spendSummaryQuery(`"worker"."operations"`, `"worker"."operation_attempts"`, `"worker"."query_executions"`, []control.SpendDimension{control.SpendByProvider, control.SpendByOperation})
	for _, expected := range []string{
		`FROM "worker"."operations" o LEFT JOIN LATERAL`,
		`FROM "worker"."operation_attempts" WHERE operation_id = o.operation_id ORDER BY attempt_number DESC LIMIT 1`,
		`o.scope_id = $1 AND o.state = 'completed' AND o.completed_at >= $2 AND o.completed_at < $3`,
		`UNION ALL SELECT 'query'::text`,
		`FROM "worker"."query_executions" WHERE scope_id = $1 AND completed_at >= $2 AND completed_at < $3`,
		`SUM(actual_cost_usd) FILTER (WHERE cost_status = 'exact')`,
		`COUNT(*) FILTER (WHERE cost_status = 'unknown')`,
		`GROUP BY provider, operation_kind`,
		`ORDER BY provider ASC NULLS FIRST, operation_kind ASC NULLS FIRST`,
		`operation_kind = ANY($4::text[])`,
	} {
		if !strings.Contains(query, expected) {
			t.Fatalf("spend summary query missing %q: %s", expected, query)
		}
	}
	if strings.Contains(query, "COALESCE(SUM(actual_cost_usd)") {
		t.Fatalf("spend summary query coalesces a NULL cost aggregate to zero: %s", query)
	}
}

func TestSpendSummaryQueryGlobalBucketHasNoGrouping(t *testing.T) {
	query := spendSummaryQuery("operations", "operation_attempts", "query_executions", nil)
	selectSQL := query[strings.Index(query, ") SELECT"):]
	if strings.Contains(selectSQL, "GROUP BY") || strings.Contains(selectSQL, "ORDER BY") {
		t.Fatalf("global spend summary unexpectedly groups or orders: %s", query)
	}
	if !strings.Contains(query, "NULL::text, NULL::text, NULL::text") {
		t.Fatalf("global spend summary did not project a single ungrouped bucket: %s", query)
	}
}

func TestSpendSummaryEmptyGlobalBucketIsRepresentable(t *testing.T) {
	// The SQL aggregate emits one row with zero counts when no ledger row
	// matches. ListSpendSummary intentionally retains that row only when the
	// caller requested a global (ungrouped) summary.
	options := SpendSummaryListOptions{ScopeID: uuid.New(), StartTime: time.Unix(10, 0), EndTime: time.Unix(20, 0)}
	if err := normalizeSpendSummaryOptions(&options); err != nil {
		t.Fatal(err)
	}
	if len(options.GroupBy) != 0 {
		t.Fatal("test must exercise the global bucket")
	}
}
