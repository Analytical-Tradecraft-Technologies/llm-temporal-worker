package postgres

import (
	"strings"
	"testing"
)

func TestMigrationIndexesRemainExplicit(t *testing.T) {
	data, err := schemaFiles.ReadFile("schema/000001_worker_state.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(data)
	for _, index := range []string{
		"operations_completed_brin_idx",
		"operations_exact_terminal_expiry_idx",
		"operations_scope_spend_idx",
		"budget_journal_time_brin_idx",
		"provider_status_event_brin_idx",
		"provider_route_query_idx",
		"provider_route_credit_query_idx",
		"response_cache_reusable_key_uidx",
		"operations_provider_operation_uidx",
		"query_executions_unknown_cost_idx",
		"query_executions_scope_time_idx",
		"provider_inventory_latest_account_idx",
	} {
		if !strings.Contains(sql, "CREATE ") || !strings.Contains(sql, index) {
			t.Errorf("migration missing index %q", index)
		}
	}
	if strings.Count(sql, "CREATE INDEX ") < 20 {
		t.Fatalf("migration contains too few indexes: %d", strings.Count(sql, "CREATE INDEX "))
	}
	if !strings.Contains(sql, "INCLUDE (") || !strings.Contains(sql, "WHERE state IN") {
		t.Fatal("covering or partial index contract missing")
	}
	for _, fragment := range []string{
		"(config_digest, provider, endpoint_id, observed_at DESC, route_id DESC)",
		"INCLUDE (credit_state, billing_state, credit_confirmed_at, last_event_id)",
	} {
		if !strings.Contains(sql, fragment) {
			t.Errorf("credit-status index contract missing %q", fragment)
		}
	}
}
