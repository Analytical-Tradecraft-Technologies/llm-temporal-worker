package postgres

import (
	"strings"
	"testing"
)

func TestRenderRoleGrantsUsesLeastPrivilegeRuntimeCatalog(t *testing.T) {
	namespace, err := NewNamespace("llm_worker", "private", "tenant_")
	if err != nil {
		t.Fatal(err)
	}
	sql, err := RenderRoleGrants(namespace)
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(sql, "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES") {
		t.Fatal("role ACL must not grant all table privileges")
	}
	for _, required := range []string{
		`REVOKE ALL ON TABLE "private"."tenant_operations" FROM llmtw_runtime;`,
		`GRANT SELECT, INSERT ON TABLE "private"."tenant_conversation_checkpoints" TO llmtw_runtime;`,
		`GRANT SELECT, INSERT ON TABLE "private"."tenant_checkpoint_provider_state" TO llmtw_runtime;`,
		`GRANT SELECT, INSERT ON TABLE "private"."tenant_checkpoint_provider_affinities" TO llmtw_runtime;`,
		`GRANT UPDATE (expires_at) ON TABLE "private"."tenant_blobs" TO llmtw_runtime;`,
		`GRANT SELECT, INSERT, UPDATE ON TABLE "private"."tenant_operations" TO llmtw_runtime;`,
		`GRANT SELECT (journal_id, event_id, redis_generation_id, operation_id, window_id, bucket_start, reservation_revision, event_kind, reserved_increase_usd, reserved_decrease_usd, accounted_increase_usd, accounted_decrease_usd, actual_cost_usd, actual_cost_status, actual_cost_unknown_reason_code, occurred_at), INSERT, UPDATE (event_id) ON TABLE "private"."tenant_budget_journal_events" TO llmtw_runtime;`,
		`GRANT SELECT (reserved_cost_usd, accounted_cost_usd, last_journal_id), INSERT, UPDATE ON TABLE "private"."tenant_budget_buckets" TO llmtw_runtime;`,
		`GRANT SELECT (operation_id, window_id, state, reserved_cost_usd, reservation_revision), INSERT, UPDATE ON TABLE "private"."tenant_operation_budget_reservations" TO llmtw_runtime;`,
		`GRANT UPDATE (response_digest) ON TABLE "private"."tenant_query_executions" TO llmtw_runtime;`,
		`GRANT SELECT ON TABLE "private"."tenant_conversation_checkpoints" TO llmtw_maintenance;`,
		`GRANT INSERT, UPDATE, DELETE ON TABLE "private"."tenant_conversation_checkpoints" TO llmtw_maintenance;`,
		`GRANT USAGE ON SEQUENCE "private"."tenant_budget_journal_events_journal_id_seq" TO llmtw_runtime;`,
	} {
		if !strings.Contains(sql, required) {
			t.Errorf("rendered role grants missing %q", required)
		}
	}
	if strings.Contains(sql, "ON ALL TABLES") || strings.Contains(sql, "ON ALL SEQUENCES") {
		t.Fatal("shared-schema role ACL must not sweep unrelated relations")
	}
	if strings.Contains(sql, `GRANT USAGE ON SCHEMA "private"`) {
		t.Fatal("shared-schema role ACL must leave schema privileges to the operator")
	}
	owned, err := renderRoleGrants(namespace, true)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(owned, `GRANT USAGE ON SCHEMA "private" TO llmtw_runtime;`) {
		t.Fatal("dedicated schema grants omitted runtime schema usage")
	}

	// Immutable checkpoint/provider records must never acquire destructive or
	// mutable runtime privileges through a future broad-grant regression.
	for _, table := range []string{
		"tenant_conversation_checkpoints",
		"tenant_checkpoint_provider_state",
		"tenant_checkpoint_provider_affinities",
		"tenant_provider_status_events",
		"tenant_provider_inventory_snapshots",
		"tenant_provider_inventory_models",
	} {
		for _, line := range strings.Split(sql, "\n") {
			if !strings.Contains(line, "TO llmtw_runtime;") || !strings.Contains(line, table) {
				continue
			}
			if strings.Contains(line, "UPDATE") || strings.Contains(line, "DELETE") {
				t.Errorf("runtime received forbidden privilege: %s", strings.TrimSpace(line))
			}
		}
	}
}

func TestRenderRoleGrantsRejectsInvalidNamespace(t *testing.T) {
	if _, err := RenderRoleGrants(Namespace{Database: "worker", Schema: "private.bad"}); err == nil {
		t.Fatal("invalid schema was accepted")
	}
}
