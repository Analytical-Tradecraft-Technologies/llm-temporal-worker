package postgres

import (
	"strings"
	"testing"
)

func TestNamespaceValidationMatrix(t *testing.T) {
	valid := []struct{ database, schema, prefix string }{
		{"llm_worker", "llm_worker", ""},
		{"worker_1", "private_2", "tenant_"},
		{"a", "a", "a_"},
	}
	for _, tc := range valid {
		if _, err := NewNamespace(tc.database, tc.schema, tc.prefix); err != nil {
			t.Errorf("valid namespace %#v rejected: %v", tc, err)
		}
	}
	invalid := []struct{ database, schema, prefix string }{
		{"LLM_WORKER", "llm_worker", ""}, {"worker-db", "llm_worker", ""},
		{"worker", "llm_worker.public", ""}, {"worker", "llm_worker", "Tenant_"},
		{"worker", "llm_worker", "x"}, {"worker", "llm_worker", strings.Repeat("x", 24) + "_"},
		{"worker", "llm_worker", "worker;drop_"}, {"worker", "llm_worker", "worker\n_"},
	}
	for _, tc := range invalid {
		if _, err := NewNamespace(tc.database, tc.schema, tc.prefix); err == nil {
			t.Errorf("invalid namespace %#v accepted", tc)
		}
	}
}

func TestNamespaceIdentifiersAreQuotedAndBounded(t *testing.T) {
	ns, err := NewNamespace("llm_worker", "private", "tenant_")
	if err != nil {
		t.Fatal(err)
	}
	id, err := ns.Relation("operations")
	if err != nil {
		t.Fatal(err)
	}
	if got := id.Sanitize(); got != `"private"."tenant_operations"` {
		t.Fatalf("unexpected relation identifier %q", got)
	}
	if _, err := ns.Relation(strings.Repeat("a", 64)); err == nil {
		t.Fatal("overlength relation was accepted")
	}
	if _, err := ns.Relation("operations;drop"); err == nil {
		t.Fatal("injected relation was accepted")
	}
}

func TestMigrationTemplateContract(t *testing.T) {
	data, err := schemaFiles.ReadFile("schema/000001_worker_state.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(data)
	for _, expected := range []string{
		"__SCHEMA__.__PREFIX__operations",
		"__SCHEMA__.__PREFIX__response_cache_entries",
		"__SCHEMA__.__PREFIX__budget_buckets",
		"__SCHEMA__.__PREFIX__query_executions",
		"CREATE INDEX __PREFIX__operations_completed_brin_idx",
		"fillfactor = 80",
		"numeric(38,18)",
	} {
		if !strings.Contains(sql, expected) {
			t.Errorf("migration missing %q", expected)
		}
	}
	if strings.Contains(sql, "WITH inserted") || strings.Contains(sql, "search_path") {
		t.Fatal("migration contains runtime DML or search_path")
	}
	if strings.Contains(sql, "CREATE SCHEMA") || strings.Contains(sql, "REVOKE ALL ON SCHEMA") {
		t.Fatal("worker object migration must not mutate a pre-existing shared schema ACL")
	}
	ns, err := NewNamespace("llm_worker", "private", "tenant_")
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := RenderMigration(ns)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(rendered, "__SCHEMA__") || strings.Contains(rendered, "__PREFIX__") {
		t.Fatal("rendered migration retained a placeholder")
	}
	if !strings.Contains(rendered, `CREATE TABLE private.tenant_operations`) {
		t.Fatal("rendered migration did not apply namespace")
	}
}

func TestResponseCacheStorageIdentityIncludesRoute(t *testing.T) {
	data, err := schemaFiles.ReadFile("schema/000001_worker_state.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(data)
	indexStart := strings.Index(sql, "CREATE UNIQUE INDEX __PREFIX__response_cache_reusable_key_uidx")
	if indexStart < 0 {
		t.Fatal("response cache reusable index is missing")
	}
	indexEnd := strings.Index(sql[indexStart:], "WHERE state = 'ready';")
	if indexEnd < 0 || !strings.Contains(sql[indexStart:indexStart+indexEnd], "cache_route_identity_hmac") {
		t.Fatal("response cache reusable identity does not include route identity")
	}
	fillsStart := strings.Index(sql, "CREATE TABLE __SCHEMA__.__PREFIX__response_cache_fills")
	if fillsStart < 0 {
		t.Fatal("response cache fills table is missing")
	}
	fillsEnd := strings.Index(sql[fillsStart:], "CHECK (state <> 'completed' OR cache_entry_id IS NOT NULL)")
	if fillsEnd < 0 || !strings.Contains(sql[fillsStart:fillsStart+fillsEnd], "cache_route_identity_hmac") {
		t.Fatal("response cache fill identity does not include route identity")
	}
}

func TestMigrationObjectNamesCoverTablesAndIndexes(t *testing.T) {
	namespace, err := NewNamespace("llm_worker", "private", "tenant_")
	if err != nil {
		t.Fatal(err)
	}
	migration, err := RenderMigration(namespace)
	if err != nil {
		t.Fatal(err)
	}
	tables, indexes := migrationObjectNames(migration, namespace)
	if len(tables) != 26 {
		t.Fatalf("migration table count = %d, want 26: %v", len(tables), tables)
	}
	if len(indexes) != 71 {
		t.Fatalf("migration index count = %d, want 71", len(indexes))
	}
	if got := migrationConstraintIndexCount(migration); got != 46 {
		t.Fatalf("constraint-backed index count = %d, want 46", got)
	}
	withoutUnique := strings.Replace(migration, "UNIQUE (tenant_hmac, project_hmac)", "", 1)
	if got := migrationConstraintIndexCount(withoutUnique); got != 45 {
		t.Fatalf("dropping one unique constraint changed expected count to %d, want 45", got)
	}
	for _, name := range []string{
		"tenant_schema_contract", "tenant_operations", "tenant_query_executions",
	} {
		if !containsString(tables, name) {
			t.Fatalf("required table %q was not derived from migration", name)
		}
	}
	for _, name := range []string{
		"tenant_operations_poll_due_idx", "tenant_response_cache_reusable_key_uidx",
		"tenant_query_executions_unknown_cost_idx",
	} {
		if !containsString(indexes, name) {
			t.Fatalf("required index %q was not derived from migration", name)
		}
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
