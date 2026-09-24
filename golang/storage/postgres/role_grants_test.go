package postgres

import (
	"context"
	"testing"
)

func TestRenderRoleGrantsUsesLeastPrivilegeRuntimeCatalog(t *testing.T) {
	repository, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()
	tx, err := repository.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	// The role and ACL changes are transactional, including when a local
	// integration database has not provisioned the runtime role yet.
	if _, err := tx.Exec(ctx, `DO $$ BEGIN
		IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'llmtw_runtime') THEN
			CREATE ROLE llmtw_runtime NOLOGIN;
		END IF;
	END $$`); err != nil {
		t.Fatal(err)
	}
	if err := grantRuntimeRoles(ctx, tx, repository.Namespace, true); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "SET LOCAL ROLE llmtw_runtime"); err != nil {
		t.Fatal(err)
	}
	// Check effective PostgreSQL authorization, not the renderer's SQL text.
	// These reads are needed by RETURNING, ON CONFLICT and reconciliation;
	// the terminal attempt write is required even when its value is NULL.
	for _, check := range []struct {
		table, column, privilege string
		allowed                  bool
	}{
		{"budget_journal_events", "xmax", "SELECT", true},
		{"budget_buckets", "window_id", "SELECT", true},
		{"budget_buckets", "bucket_start", "SELECT", true},
		{"operation_budget_reservations", "actual_cost_status", "SELECT", true},
		{"operation_attempts", "cost_catalog_version", "UPDATE", true},
		{"budget_journal_events", "actual_cost_usd", "UPDATE", false},
	} {
		relation, err := repository.Namespace.Render(check.table)
		if err != nil {
			t.Fatal(err)
		}
		var allowed bool
		if err := tx.QueryRow(ctx, "SELECT has_column_privilege(current_user, $1, $2, $3)", relation, check.column, check.privilege).Scan(&allowed); err != nil {
			t.Fatal(err)
		}
		if allowed != check.allowed {
			t.Errorf("%s on %s.%s: allowed=%t, want %t", check.privilege, check.table, check.column, allowed, check.allowed)
		}
	}
	for _, table := range []string{
		"conversation_checkpoints", "checkpoint_provider_state",
		"checkpoint_provider_affinities", "provider_status_events",
		"provider_inventory_snapshots", "provider_inventory_models",
	} {
		relation, err := repository.Namespace.Render(table)
		if err != nil {
			t.Fatal(err)
		}
		var destructive bool
		if err := tx.QueryRow(ctx, "SELECT has_table_privilege(current_user, $1, 'UPDATE,DELETE')", relation).Scan(&destructive); err != nil {
			t.Fatal(err)
		}
		if destructive {
			t.Errorf("runtime can alter immutable relation %s", table)
		}
	}
}

func TestRenderRoleGrantsRejectsInvalidNamespace(t *testing.T) {
	if _, err := RenderRoleGrants(Namespace{Database: "worker", Schema: "private.bad"}); err == nil {
		t.Fatal("invalid schema was accepted")
	}
}
