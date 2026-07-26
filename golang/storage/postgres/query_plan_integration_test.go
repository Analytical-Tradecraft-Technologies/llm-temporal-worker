package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mfow/llm-temporal-worker/golang/control"
)

// TestInventoryQueryPlansUseTheLatestIndex exercises the production inventory
// query at enough rows for PostgreSQL to cost an index path meaningfully.  The
// fixture deliberately spreads one configuration across many endpoint routes;
// the requested route is selective, while its endpoint account epochs still
// exercise DISTINCT ON's latest-per-account grouping.
//
// This is an index eligibility contract, not a latency or SLO measurement. It
// uses the normal planner settings and fails if the query regresses to a
// sequential scan despite the checked-in latest-per-account snapshot index.
func TestInventoryQueryPlansUseTheLatestIndex(t *testing.T) {
	ctx, namespace, pool, cleanup := providerControlIntegrationPool(t)
	t.Cleanup(cleanup)

	configDigest := sha256.Sum256([]byte("inventory-query-plan-" + uuid.NewString()))
	configs, err := namespace.Render("configuration_snapshots")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO "+configs+" (config_digest, config_version, source_digest, sanitized_config) VALUES ($1,$2,$1,'{}'::jsonb)", configDigest[:], "inventory-query-plan"); err != nil {
		t.Fatal(err)
	}

	snapshots, err := namespace.Relation("provider_inventory_snapshots")
	if err != nil {
		t.Fatal(err)
	}
	snapshotsSQL := snapshots.Sanitize()
	models, err := namespace.Relation("provider_inventory_models")
	if err != nil {
		t.Fatal(err)
	}
	modelsSQL := models.Sanitize()
	const (
		rowCount       = 10_000
		endpointCount  = 100
		accountEpochs  = 4
		targetProvider = "provider-07"
		targetEndpoint = "endpoint-47"
	)
	base := time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Microsecond)
	snapshotRows := make([][]any, 0, rowCount)
	modelRows := make([][]any, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("inventory-query-plan-%d", i)))
		provider := fmt.Sprintf("provider-%02d", i%10)
		endpoint := fmt.Sprintf("endpoint-%02d", i%endpointCount)
		account := sha256.Sum256([]byte(fmt.Sprintf("inventory-query-plan-account-%d", i%accountEpochs)))
		observed := base.Add(time.Duration(i) * time.Second)
		inventoryDigest := sha256.Sum256([]byte(id.String()))
		snapshotRows = append(snapshotRows, []any{
			id, configDigest[:], provider, endpoint, account[:], "chat", "test-region", "provider_api", observed,
			true, nil, nil, inventoryDigest[:], observed.Add(time.Hour),
		})
		capabilityDigest := sha256.Sum256([]byte("capability-" + id.String()))
		modelRows = append(modelRows, []any{
			id, "model-" + id.String(), "query-plan model", "fixture", observed, "available", capabilityDigest[:], []byte(`{}`),
		})
	}
	if _, err := pool.CopyFrom(ctx, snapshots, []string{
		"inventory_snapshot_id", "config_digest", "provider", "endpoint_id", "endpoint_account_hmac", "endpoint_family", "region", "source", "observed_at", "complete", "next_cursor_ciphertext", "next_cursor_key_id", "inventory_digest", "expires_at",
	}, pgx.CopyFromRows(snapshotRows)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.CopyFrom(ctx, models, []string{
		"inventory_snapshot_id", "provider_model_id", "display_name", "owned_by", "created_at_provider", "lifecycle_state", "capability_digest", "safe_metadata",
	}, pgx.CopyFromRows(modelRows)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM "+modelsSQL+" WHERE inventory_snapshot_id IN (SELECT inventory_snapshot_id FROM "+snapshotsSQL+" WHERE config_digest=$1)", configDigest[:])
		_, _ = pool.Exec(ctx, "DELETE FROM "+snapshotsSQL+" WHERE config_digest=$1", configDigest[:])
		_, _ = pool.Exec(ctx, "DELETE FROM "+configs+" WHERE config_digest=$1", configDigest[:])
	})
	if _, err := pool.Exec(ctx, "ANALYZE "+snapshotsSQL); err != nil {
		t.Fatal(err)
	}

	wantIndex, err := namespace.PrefixName("provider_inventory_latest_account_idx")
	if err != nil {
		t.Fatal(err)
	}
	args := []any{configDigest[:], targetProvider, targetEndpoint}
	horizonPlan := explainJSONPlan(t, ctx, pool, latestInventoryHorizonQuery(snapshots.Sanitize()), args...)
	assertPlanUsesIndex(t, horizonPlan, wantIndex)

	// The model-page query has the same selective snapshot predicate and joins
	// through the immutable snapshot identity. Its plan must retain the same
	// latest-snapshot access path as the horizon query.
	modelsPlan := explainJSONPlan(t, ctx, pool, latestInventoryModelsQuery(snapshots.Sanitize(), models.Sanitize()),
		configDigest[:], targetProvider, targetEndpoint, base.Add(time.Duration(rowCount)*time.Second), "", "", "", "", uuid.Nil, "", 2)
	assertPlanUsesIndex(t, modelsPlan, wantIndex)

	// An endpoint-only lookup uses the less-specific latest snapshot index. This
	// guards the query shape used when callers do not provide a provider filter.
	endpointIndex, err := namespace.PrefixName("provider_inventory_latest_idx")
	if err != nil {
		t.Fatal(err)
	}
	endpointPlan := explainJSONPlan(t, ctx, pool, latestInventoryHorizonQuery(snapshots.Sanitize()),
		configDigest[:], "", targetEndpoint)
	assertPlanUsesIndex(t, endpointPlan, endpointIndex)
}

// TestProviderQueryPlansUseProjectionIndexes exercises the two provider
// control-plane query shapes at representative cardinality. The fixture keeps
// one configuration with 10,000 route projections and status events, then
// requires the route page and DISTINCT-ON credit page to use their dedicated
// covering indexes. This is an index eligibility contract, not a latency SLO.
func TestProviderQueryPlansUseProjectionIndexes(t *testing.T) {
	ctx, namespace, pool, cleanup := providerControlIntegrationPool(t)
	t.Cleanup(cleanup)

	configDigest := sha256.Sum256([]byte("provider-query-plan-" + uuid.NewString()))
	configs, err := namespace.Render("configuration_snapshots")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO "+configs+" (config_digest, config_version, source_digest, sanitized_config) VALUES ($1,$2,$1,'{}'::jsonb)", configDigest[:], "provider-query-plan"); err != nil {
		t.Fatal(err)
	}
	events, err := namespace.Render("provider_status_events")
	if err != nil {
		t.Fatal(err)
	}
	routes, err := namespace.Render("provider_route_status")
	if err != nil {
		t.Fatal(err)
	}
	const rowCount = 10_000
	now := time.Now().UTC().Truncate(time.Microsecond)
	insertEvents := "INSERT INTO " + events + " (event_digest, config_digest, route_id, endpoint_id, endpoint_account_hmac, provider, endpoint_family, observed_at, source, availability, credit_state, billing_state, safe_error_code, provider_code, evidence_digest, config_epoch, expires_at) " +
		"SELECT decode(md5('provider-plan-event-'||i::text),'hex'), $1, 'route-'||i::text, 'endpoint-'||(i % 100)::text, decode(md5('provider-plan-account-'||(i % 250)::text),'hex'), 'provider-'||(i % 20)::text, 'chat', $2::timestamptz - (i || ' seconds')::interval, 'management_api', CASE WHEN i % 11 = 0 THEN 'unavailable' ELSE 'available' END, CASE WHEN i % 13 = 0 THEN 'exhausted' ELSE 'ok' END, CASE WHEN i % 17 = 0 THEN 'issue' ELSE 'ok' END, CASE WHEN i % 13 = 0 THEN 'quota_exhausted' ELSE NULL END, CASE WHEN i % 13 = 0 THEN 'provider_quota' ELSE NULL END, decode(md5('provider-plan-evidence-'||i::text),'hex'), 'epoch-1', $2::timestamptz + interval '1 day' FROM generate_series(1," + fmt.Sprint(rowCount) + ") AS series(i)"
	if _, err := pool.Exec(ctx, insertEvents, configDigest[:], now); err != nil {
		t.Fatalf("insert provider status events: %v", err)
	}
	insertRoutes := "INSERT INTO " + routes + " (config_digest, route_id, endpoint_id, endpoint_account_hmac, provider, endpoint_family, config_epoch, availability, credit_state, billing_state, circuit_state, consecutive_definite_failures, last_event_id, last_event_digest, observed_at, stale_after, projection_version) " +
		"SELECT $1, 'route-'||series.i::text, 'endpoint-'||(series.i % 100)::text, decode(md5('provider-plan-account-'||(series.i % 250)::text),'hex'), 'provider-'||(series.i % 20)::text, 'chat', 'epoch-1', CASE WHEN series.i % 11 = 0 THEN 'unavailable' ELSE 'available' END, CASE WHEN series.i % 13 = 0 THEN 'exhausted' ELSE 'ok' END, CASE WHEN series.i % 17 = 0 THEN 'issue' ELSE 'ok' END, 'closed', 0, event.event_id, event.event_digest, event.observed_at, $2::timestamptz + interval '1 day', 1 FROM generate_series(1," + fmt.Sprint(rowCount) + ") AS series(i) JOIN " + events + " event ON event.config_digest = $1 AND event.route_id = 'route-'||series.i::text"
	if _, err := pool.Exec(ctx, insertRoutes, configDigest[:], now); err != nil {
		t.Fatalf("insert provider route projections: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM "+routes+" WHERE config_digest=$1", configDigest[:])
		_, _ = pool.Exec(ctx, "DELETE FROM "+events+" WHERE config_digest=$1", configDigest[:])
		_, _ = pool.Exec(ctx, "DELETE FROM "+configs+" WHERE config_digest=$1", configDigest[:])
	})
	if _, err := pool.Exec(ctx, "ANALYZE "+routes); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "ANALYZE "+events); err != nil {
		t.Fatal(err)
	}

	routeIndex, err := namespace.PrefixName("provider_route_query_idx")
	if err != nil {
		t.Fatal(err)
	}
	routeQuery := "SELECT config_digest, config_epoch, route_id, endpoint_id, endpoint_account_hmac, provider, endpoint_family, availability, credit_state, billing_state, circuit_state, consecutive_definite_failures, last_event_digest, observed_at, stale_after, credit_confirmed_at FROM " + routes + " WHERE config_digest = $1 AND ($2 = '' OR provider = $2) AND ($3 = '' OR endpoint_id = $3) AND ($4 = '' OR availability = $4) AND ($5 OR availability <> 'available' OR credit_state <> 'ok' OR billing_state <> 'ok' OR circuit_state <> 'closed') AND ($6::timestamptz IS NULL OR observed_at <= $6) AND ($7 = '' OR route_id > $7) ORDER BY route_id LIMIT $8"
	routePlan := explainJSONPlan(t, ctx, pool, routeQuery, configDigest[:], "provider-7", "endpoint-47", "", false, nil, "", 101)
	assertPlanUsesIndex(t, routePlan, routeIndex)

	creditIndex, err := namespace.PrefixName("provider_route_credit_query_idx")
	if err != nil {
		t.Fatal(err)
	}
	creditPlan := explainJSONPlan(t, ctx, pool, creditStatusListQuery(routes, events), configDigest[:], "", "", nil, false, "", "", 101)
	assertPlanUsesIndex(t, creditPlan, creditIndex)
}

// TestSpendSummaryQueryPlanUsesLedgerIndexes checks both durable cost ledgers
// in the UNION ALL spend-summary query. Ten thousand completed operation rows
// and query-execution rows make the scoped time indexes materially cheaper than
// sequential scans under PostgreSQL's normal planner settings.
func TestSpendSummaryQueryPlanUsesLedgerIndexes(t *testing.T) {
	ctx, namespace, pool, cleanup := providerControlIntegrationPool(t)
	t.Cleanup(cleanup)

	configDigest := sha256.Sum256([]byte("spend-query-plan-" + uuid.NewString()))
	configs, err := namespace.Render("configuration_snapshots")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO "+configs+" (config_digest, config_version, source_digest, sanitized_config) VALUES ($1,$2,$1,'{}'::jsonb)", configDigest[:], "spend-query-plan"); err != nil {
		t.Fatal(err)
	}
	key := []byte("01234567890123456789012345678901")
	scopes := DefaultScopeRepository(pool, namespace, ScopeKeyring{ActiveVersion: "scope-v1", Keys: map[string][]byte{"scope-v1": key}})
	scope, err := scopes.Ensure(ctx, "query-plan", "spend")
	if err != nil {
		t.Fatal(err)
	}
	operations, err := namespace.Render("operations")
	if err != nil {
		t.Fatal(err)
	}
	attempts, err := namespace.Render("operation_attempts")
	if err != nil {
		t.Fatal(err)
	}
	executions, err := namespace.Render("query_executions")
	if err != nil {
		t.Fatal(err)
	}
	const rowCount = 10_000
	now := time.Now().UTC().Truncate(time.Microsecond)
	insertOperations := "INSERT INTO " + operations + " (operation_id, scope_id, operation_kind, api_version, operation_key_hmac, request_fingerprint_hmac, request_digest, request_schema_version, request_manifest_jsonb, request_inline_ciphertext, request_key_id, config_digest, state, result_inline_ciphertext, result_key_id, result_digest, result_byte_length, result_media_type, route_id, endpoint_id, provider, endpoint_family, resolved_model, operation_expires_at, reserved_cost_usd, incurred_cost_usd, actual_cost_usd, cost_status, cost_method, created_at, updated_at, completed_at, retention_expires_at) " +
		"SELECT md5('spend-plan-operation-'||i::text)::uuid, $1, 'generate', 'llm.temporal/v1', decode(md5('spend-plan-key-'||i::text),'hex'), decode(md5('spend-plan-fingerprint-'||i::text),'hex'), decode(md5('spend-plan-request-'||i::text),'hex'), 1, '{}'::jsonb, decode('01','hex'), 'query-plan-key', $2, 'completed', decode('02','hex'), 'query-plan-result-key', decode(md5('spend-plan-result-'||i::text),'hex'), 1, 'application/json', 'route-'||(i % 100)::text, 'endpoint-'||(i % 100)::text, 'provider-'||(i % 20)::text, 'chat', 'model-'||(i % 50)::text, $3::timestamptz + interval '1 day', 1.0, 1.0, 1.0, 'exact', 'provider_reported', $3::timestamptz, $3::timestamptz, $3::timestamptz + (i || ' microseconds')::interval, $3::timestamptz + interval '1 day' FROM generate_series(1," + fmt.Sprint(rowCount) + ") AS series(i)"
	if _, err := pool.Exec(ctx, insertOperations, scope.ID, configDigest[:], now); err != nil {
		t.Fatalf("insert spend operations: %v", err)
	}
	insertExecutions := "INSERT INTO " + executions + " (query_execution_id, scope_id, api_version, operation_key_hmac, request_fingerprint_hmac, query_kind, request_jsonb, response_jsonb, response_digest, source, actual_cost_usd, cost_status, cost_method, started_at, completed_at, retention_expires_at) " +
		"SELECT md5('spend-plan-query-'||i::text)::uuid, $1, 'llm.temporal/query/v1', decode(md5('spend-plan-query-key-'||i::text),'hex'), decode(md5('spend-plan-query-fingerprint-'||i::text),'hex'), 'spend_summary', '{}'::jsonb, '{}'::jsonb, decode(md5('spend-plan-query-response-'||i::text),'hex'), 'worker', 0, 'exact', 'control_query_zero', $2::timestamptz, $2::timestamptz + (i || ' microseconds')::interval, $2::timestamptz + interval '1 day' FROM generate_series(1," + fmt.Sprint(rowCount) + ") AS series(i)"
	if _, err := pool.Exec(ctx, insertExecutions, scope.ID, now); err != nil {
		t.Fatalf("insert spend query executions: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM "+executions+" WHERE scope_id=$1", scope.ID)
		_, _ = pool.Exec(ctx, "DELETE FROM "+operations+" WHERE scope_id=$1", scope.ID)
		_, _ = pool.Exec(ctx, "DELETE FROM "+configs+" WHERE config_digest=$1", configDigest[:])
	})
	if _, err := pool.Exec(ctx, "ANALYZE "+operations); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "ANALYZE "+executions); err != nil {
		t.Fatal(err)
	}

	operationIndex, err := namespace.PrefixName("operations_scope_spend_idx")
	if err != nil {
		t.Fatal(err)
	}
	executionIndex, err := namespace.PrefixName("query_executions_scope_time_idx")
	if err != nil {
		t.Fatal(err)
	}
	query := spendSummaryQuery(operations, attempts, executions, []control.SpendDimension{control.SpendByOperation})
	plan := explainJSONPlan(t, ctx, pool, query, scope.ID, now.Add(-time.Minute), now.Add(time.Hour), nil)
	assertPlanUsesIndex(t, plan, operationIndex)
	assertPlanUsesIndex(t, plan, executionIndex)
}

func explainJSONPlan(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) any {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(ctx, "EXPLAIN (FORMAT JSON, COSTS OFF) "+query, args...).Scan(&raw); err != nil {
		t.Fatalf("explain query plan: %v", err)
	}
	var plan any
	if err := json.Unmarshal(raw, &plan); err != nil {
		t.Fatalf("decode query plan: %v", err)
	}
	return plan
}

func assertPlanUsesIndex(t *testing.T, plan any, wantIndex string) {
	t.Helper()
	var walk func(any) bool
	walk = func(value any) bool {
		switch node := value.(type) {
		case map[string]any:
			if index, ok := node["Index Name"].(string); ok && index == wantIndex {
				return true
			}
			for _, child := range node {
				if walk(child) {
					return true
				}
			}
		case []any:
			for _, child := range node {
				if walk(child) {
					return true
				}
			}
		}
		return false
	}
	if !walk(plan) {
		encoded, _ := json.Marshal(plan)
		t.Fatalf("query plan did not use %q: %s", wantIndex, encoded)
	}
}
