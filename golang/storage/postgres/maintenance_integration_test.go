package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/control"
	"github.com/mfow/llm-temporal-worker/golang/maintenance"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func maintenanceIntegrationRepository(t *testing.T) (MaintenanceRepository, context.Context, func()) {
	t.Helper()
	if os.Getenv("LLMTW_POSTGRES_ADDR") == "" {
		t.Skip("LLMTW_POSTGRES_ADDR is not configured; set it for PostgreSQL maintenance tests")
	}
	ns, err := NewNamespace(valueOr("LLMTW_POSTGRES_DATABASE", "llm_worker"), valueOr("LLMTW_POSTGRES_SCHEMA", "llm_worker"), os.Getenv("LLMTW_POSTGRES_TABLE_PREFIX"))
	if err != nil {
		t.Fatal(err)
	}
	pool, err := NewPool(context.Background(), PoolOptions{Namespace: ns, Addresses: []string{os.Getenv("LLMTW_POSTGRES_ADDR")}, Username: valueOr("LLMTW_POSTGRES_USER", "llmtw"), Password: valueOr("LLMTW_POSTGRES_PASSWORD", "llmtw"), MaxConnections: 4, MinConnections: 1, DialTimeout: 5 * time.Second, StatementTimeout: 5 * time.Second, LockTimeout: time.Second, IdleTxTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := Install(ctx, pool, ns); err != nil {
		cancel()
		pool.Close()
		t.Fatal(err)
	}
	return MaintenanceRepository{Pool: pool, Namespace: ns}, ctx, func() { cancel(); pool.Close() }
}

func TestMaintenanceOutboxFencesReclaimedLeaseAndDeduplicatesCompletion(t *testing.T) {
	repository, ctx, cleanup := maintenanceIntegrationRepository(t)
	defer cleanup()
	now := time.Now().UTC().Truncate(time.Microsecond)
	event, err := maintenance.NewDeleteBlobEvent(uuid.New().String(), uuid.New().String(), now.Add(-time.Minute), now.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.PublishOutbox(ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := repository.PublishOutbox(ctx, event); err != nil {
		t.Fatalf("duplicate enqueue was not idempotent: %v", err)
	}
	conflict := event
	conflict.ID = uuid.New().String()
	conflict.AggregateID = uuid.New().String()
	conflict.SafePayload = []byte(`{"blob_id":"` + conflict.AggregateID + `"}`)
	if err := repository.PublishOutbox(ctx, conflict); !errors.Is(err, ErrMaintenanceOutboxConflict) {
		t.Fatalf("conflicting dedupe enqueue was accepted: %v", err)
	}
	typeConflict := event
	typeConflict.ID = uuid.New().String()
	typeConflict.AggregateType = "provider_state"
	if err := repository.PublishOutbox(ctx, typeConflict); err == nil || !strings.Contains(err.Error(), "aggregate type") {
		t.Fatalf("invalid aggregate-type payload was accepted: %v", err)
	}
	first, err := repository.ClaimOutbox(ctx, maintenance.ClaimOptions{Now: now, Limit: 1, Lease: 10 * time.Minute})
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim=%+v err=%v", first, err)
	}
	outboxTable, err := repository.Namespace.Render("maintenance_outbox")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Pool.Exec(ctx, "UPDATE "+outboxTable+" SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE outbox_id = $1", uuid.MustParse(event.ID)); err != nil {
		t.Fatal(err)
	}
	second, err := repository.ClaimOutbox(ctx, maintenance.ClaimOptions{Now: now.Add(time.Second), Limit: 1, Lease: 10 * time.Minute})
	if err != nil || len(second) != 1 {
		t.Fatalf("reclaim=%+v err=%v", second, err)
	}
	if first[0].LeaseToken == second[0].LeaseToken {
		t.Fatal("reclaim reused the old lease fence")
	}
	if err := repository.CompleteOutbox(ctx, event.ID, first[0].LeaseToken, time.Now().UTC()); !errors.Is(err, maintenance.ErrOutboxNotClaimed) {
		t.Fatalf("stale claimant completed reclaimed row: %v", err)
	}
	// Lease ownership is evaluated against the caller's explicit maintenance
	// clock, just like the storage-neutral outbox contract. Advancing that
	// clock beyond the lease must fence the claimant even when the database
	// wall clock has not advanced by the same amount.
	if err := repository.CompleteOutbox(ctx, event.ID, second[0].LeaseToken, now.Add(11*time.Minute)); !errors.Is(err, maintenance.ErrOutboxNotClaimed) {
		t.Fatalf("completion past explicit lease horizon was accepted: %v", err)
	}
	if err := repository.CompleteOutbox(ctx, event.ID, second[0].LeaseToken, time.Now().UTC()); err != nil {
		t.Fatalf("current claimant completion failed: %v", err)
	}
	if err := repository.CompleteOutbox(ctx, event.ID, second[0].LeaseToken, time.Now().UTC()); err != nil {
		t.Fatalf("duplicate completion was not idempotent: %v", err)
	}
}

func TestMaintenanceRetentionPreservesCurrentProviderState(t *testing.T) {
	repository, ctx, cleanup := maintenanceIntegrationRepository(t)
	defer cleanup()
	now := time.Now().UTC().Truncate(time.Microsecond)
	configDigest := sha256.Sum256([]byte("maintenance-retention-" + now.Format(time.RFC3339Nano)))
	configs, err := repository.Namespace.Render("configuration_snapshots")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Pool.Exec(ctx, "INSERT INTO "+configs+" (config_digest, config_version, source_digest, sanitized_config) VALUES ($1,$2,$1,'{}'::jsonb)", configDigest[:], "maintenance-retention-"+now.Format("150405.000000")); err != nil {
		t.Fatal(err)
	}

	statusRepository := DefaultProviderStatusRepository(repository.Pool, repository.Namespace)
	statusBase := control.StatusObservation{
		ConfigDigest:        configDigest,
		RouteID:             "maintenance-retention-route",
		EndpointID:          "maintenance-retention-endpoint",
		EndpointAccountHMAC: sha256.Sum256([]byte("maintenance-retention-account")),
		Provider:            "maintenance-provider",
		EndpointFamily:      "chat",
		Source:              control.SourceInference,
		Availability:        control.AvailabilityAvailable,
		Credit:              control.CreditOK,
		Billing:             control.BillingOK,
		EvidenceDigest:      sha256.Sum256([]byte("maintenance-retention-evidence")),
		ConfigEpoch:         "maintenance-retention-epoch",
	}
	oldObservation := statusBase
	oldObservation.ObservedAt = now.Add(-3 * time.Hour)
	oldObservation.ExpiresAt = now.Add(-2 * time.Hour)
	oldEvent, err := control.NewStatusEvent(oldObservation)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := statusRepository.PersistStatusEvent(ctx, oldEvent); err != nil || !applied {
		t.Fatalf("persist old status event: applied=%v err=%v", applied, err)
	}
	latestObservation := statusBase
	latestObservation.ObservedAt = now.Add(-90 * time.Minute)
	// The current projection may still reference an expired event. Retention
	// must preserve it until the projection advances, so the foreign key remains
	// valid even when the event is past its expiry cutoff.
	latestObservation.ExpiresAt = now.Add(-70 * time.Minute)
	latestObservation.EvidenceDigest = sha256.Sum256([]byte("maintenance-retention-latest"))
	latestEvent, err := control.NewStatusEvent(latestObservation)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := statusRepository.PersistStatusEvent(ctx, latestEvent); err != nil || !applied {
		t.Fatalf("persist latest status event: applied=%v err=%v", applied, err)
	}

	inventoryRepository := DefaultInventoryRepository(repository.Pool, repository.Namespace, Keyring{Active: "maintenance-retention-v1", Keys: map[string][]byte{"maintenance-retention-v1": []byte("01234567890123456789012345678901")}})
	oldInventory := inventoryQuerySnapshot(configDigest, "maintenance-provider", "maintenance-retention-endpoint", now.Add(-3*time.Hour), "maintenance-old-model")
	oldInventory.ExpiresAt = now.Add(-2 * time.Hour)
	if _, err := inventoryRepository.PersistSnapshot(ctx, oldInventory); err != nil {
		t.Fatalf("persist old inventory snapshot: %v", err)
	}
	latestInventory := inventoryQuerySnapshot(configDigest, "maintenance-provider", "maintenance-retention-endpoint", now.Add(-30*time.Minute), "maintenance-latest-model")
	latestInventory.ExpiresAt = now.Add(time.Hour)
	if _, err := inventoryRepository.PersistSnapshot(ctx, latestInventory); err != nil {
		t.Fatalf("persist latest inventory snapshot: %v", err)
	}
	// The provider is part of the query's latest-snapshot identity. A newer
	// snapshot from another provider must not protect this provider's history.
	singletonInventory := inventoryQuerySnapshot(configDigest, "maintenance-other-provider", "maintenance-retention-endpoint", now.Add(-150*time.Minute), "maintenance-singleton-model")
	singletonInventory.ExpiresAt = now.Add(-2 * time.Hour)
	if _, err := inventoryRepository.PersistSnapshot(ctx, singletonInventory); err != nil {
		t.Fatalf("persist singleton inventory snapshot: %v", err)
	}

	cutoff := now.Add(-time.Hour)
	statusResult, err := repository.PruneExpiredProviderStatus(ctx, now, cutoff, 10)
	if err != nil {
		t.Fatalf("prune provider status: %v", err)
	}
	if statusResult.Examined != 1 || statusResult.Deleted != 1 {
		t.Fatalf("provider status retention result = %+v", statusResult)
	}
	inventoryResult, err := repository.PruneExpiredInventory(ctx, now, cutoff, 10)
	if err != nil {
		t.Fatalf("prune provider inventory: %v", err)
	}
	if inventoryResult.Examined != 1 || inventoryResult.Deleted != 1 {
		t.Fatalf("provider inventory retention result = %+v", inventoryResult)
	}

	statusEvents, err := repository.Namespace.Render("provider_status_events")
	if err != nil {
		t.Fatal(err)
	}
	var statusCount, currentStatusCount int
	if err := repository.Pool.QueryRow(ctx, "SELECT count(*) FROM "+statusEvents+" WHERE config_digest = $1 AND route_id = $2", configDigest[:], statusBase.RouteID).Scan(&statusCount); err != nil {
		t.Fatal(err)
	}
	routes, err := repository.Namespace.Render("provider_route_status")
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Pool.QueryRow(ctx, "SELECT count(*) FROM "+statusEvents+" e JOIN "+routes+" r ON r.last_event_id = e.event_id WHERE r.config_digest = $1 AND r.route_id = $2", configDigest[:], statusBase.RouteID).Scan(&currentStatusCount); err != nil {
		t.Fatal(err)
	}
	if statusCount != 1 || currentStatusCount != 1 {
		t.Fatalf("status history/current projection counts = %d/%d, want 1/1", statusCount, currentStatusCount)
	}

	snapshots, err := repository.Namespace.Render("provider_inventory_snapshots")
	if err != nil {
		t.Fatal(err)
	}
	models, err := repository.Namespace.Render("provider_inventory_models")
	if err != nil {
		t.Fatal(err)
	}
	var snapshotCount, modelCount int
	if err := repository.Pool.QueryRow(ctx, "SELECT count(*) FROM "+snapshots+" WHERE config_digest = $1", configDigest[:]).Scan(&snapshotCount); err != nil {
		t.Fatal(err)
	}
	if err := repository.Pool.QueryRow(ctx, "SELECT count(*) FROM "+models+" m JOIN "+snapshots+" s ON s.inventory_snapshot_id = m.inventory_snapshot_id WHERE s.config_digest = $1", configDigest[:]).Scan(&modelCount); err != nil {
		t.Fatal(err)
	}
	if snapshotCount != 2 || modelCount != 2 {
		t.Fatalf("inventory snapshot/model counts = %d/%d, want 2/2", snapshotCount, modelCount)
	}
}

func TestMaintenanceRetentionPrunesQueryAuditInBoundedBatches(t *testing.T) {
	repository, ctx, cleanup := maintenanceIntegrationRepository(t)
	defer cleanup()
	now := time.Now().UTC().Truncate(time.Microsecond)
	keys := Keyring{Active: "maintenance-query-v1", Keys: map[string][]byte{"maintenance-query-v1": []byte("01234567890123456789012345678901")}}
	scopeKeys := ScopeKeyring{ActiveVersion: "maintenance-scope-v1", Keys: map[string][]byte{"maintenance-scope-v1": []byte("abcdefghijklmnopqrstuvwxyz123456")}}
	queries := DefaultQueryExecutionRepository(repository.Pool, repository.Namespace, keys, DefaultScopeRepository(repository.Pool, repository.Namespace, scopeKeys))
	queries.Now = func() time.Time { return now }

	oldIDs := make([]uuid.UUID, 0, 2)
	for _, key := range []string{"old-a", "old-b"} {
		request := validQueryExecutionRequest(now)
		request.Tenant = "maintenance-query-" + key
		request.OperationKey = key
		request.StartedAt = now.Add(-3 * time.Hour)
		request.CompletedAt = now.Add(-2 * time.Hour)
		request.RetentionExpiresAt = now.Add(-time.Hour)
		record, err := queries.Record(ctx, request)
		if err != nil {
			t.Fatalf("record expired query execution %q: %v", key, err)
		}
		oldIDs = append(oldIDs, record.ID)
	}
	fresh := validQueryExecutionRequest(now)
	fresh.Tenant = "maintenance-query-fresh"
	fresh.OperationKey = "fresh"
	fresh.CompletedAt = now.Add(-time.Minute)
	fresh.StartedAt = now.Add(-2 * time.Minute)
	fresh.RetentionExpiresAt = now.Add(time.Hour)
	freshRecord, err := queries.Record(ctx, fresh)
	if err != nil {
		t.Fatalf("record fresh query execution: %v", err)
	}

	cutoff := now.Add(-10 * time.Minute)
	first, err := repository.PruneExpiredQueryExecutions(ctx, now, cutoff, 1)
	if err != nil {
		t.Fatalf("first query execution retention pass: %v", err)
	}
	if first.Examined != 1 || first.Deleted != 1 {
		t.Fatalf("first query execution retention result = %+v", first)
	}
	second, err := repository.PruneExpiredQueryExecutions(ctx, now, cutoff, 10)
	if err != nil {
		t.Fatalf("second query execution retention pass: %v", err)
	}
	if second.Examined != 1 || second.Deleted != 1 {
		t.Fatalf("second query execution retention result = %+v", second)
	}

	executions, err := repository.Namespace.Render("query_executions")
	if err != nil {
		t.Fatal(err)
	}
	var remaining int
	if err := repository.Pool.QueryRow(ctx, "SELECT count(*) FROM "+executions+" WHERE query_execution_id = ANY($1::uuid[])", oldIDs).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("expired query executions remaining = %d", remaining)
	}
	var freshRemaining int
	if err := repository.Pool.QueryRow(ctx, "SELECT count(*) FROM "+executions+" WHERE query_execution_id = $1", freshRecord.ID).Scan(&freshRemaining); err != nil {
		t.Fatal(err)
	}
	if freshRemaining != 1 {
		t.Fatal("unexpired query execution was removed")
	}
}

func TestMaintenanceRetentionPreservesCacheUsedByActiveOperation(t *testing.T) {
	fixture := newResponseCacheFixture(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	key := testCacheKey()
	key.ScopeID = fixture.scope.ID
	fill := CacheFillRequest{Key: key, OperationID: fixture.originID, Lease: time.Minute}
	if acquired, err := fixture.repository.BeginFill(fixture.ctx, fill); err != nil || acquired.Status != CacheFillAcquired {
		t.Fatalf("begin cache fill=%#v err=%v", acquired, err)
	}
	entry, err := fixture.repository.Publish(fixture.ctx, CachePublishRequest{
		Fill:                   fill,
		CanonicalRequestJSON:   []byte(`{"model":"maintenance-retention"}`),
		SemanticProfileVersion: "maintenance-retention",
		CacheEpoch:             "maintenance-retention",
		OriginOperationID:      fixture.originID,
		OriginCheckpointID:     fixture.checkpointID,
		OriginProvider:         "fixture",
		OriginEndpointID:       "endpoint-fixture",
		OriginResolvedModel:    "model-fixture",
		Response:               []byte(`{"output":"cached"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	// The producer is terminal, while the consumer remains active. The
	// operation's cache_entry_id is intentionally not populated by Lookup;
	// response_cache_uses is the authoritative use ledger for this path.
	operations, err := fixture.operations.Namespace.Render("operations")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.operations.Pool.Exec(fixture.ctx, "UPDATE "+operations+" SET state='canceled', retention_expires_at=$2 WHERE operation_id=$1", operationUUID(fixture.originID), now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	consumerID := "cache-retention-active-consumer-" + uuid.NewString()
	if _, err := fixture.operations.Begin(fixture.ctx, admission.BeginRequest{
		ID: consumerID, ScopeKey: "cache-integration-tenant/cache-integration-project",
		RequestDigest: admission.Digest([]byte(consumerID)), ReservationUSD: pricing.MustUSD("0"),
		ExpiresAt: now.Add(time.Hour), RequestManifest: []byte(`{"model":"maintenance-retention"}`),
	}); err != nil {
		t.Fatal(err)
	}
	if hit, err := fixture.repository.Lookup(fixture.ctx, CacheLookupRequest{Key: key, OperationID: consumerID, MaxAge: time.Hour}); err != nil || !hit.Hit {
		t.Fatalf("active consumer cache hit=%#v err=%v", hit, err)
	}
	entries, err := fixture.operations.Namespace.Render("response_cache_entries")
	if err != nil {
		t.Fatal(err)
	}
	old := now.Add(-2 * time.Hour)
	if _, err := fixture.operations.Pool.Exec(fixture.ctx, "UPDATE "+entries+" SET created_at=$2, completed_at=$2, last_used_at=$2 WHERE cache_entry_id=$1", entry.ID, old); err != nil {
		t.Fatal(err)
	}

	result, err := (MaintenanceRepository{Pool: fixture.operations.Pool, Namespace: fixture.operations.Namespace}).PruneExpiredCache(fixture.ctx, now, now.Add(-time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Examined != 0 || result.Tombstoned != 0 {
		t.Fatalf("active cache use was pruned: result=%+v", result)
	}

	// Once the consumer is terminal, the same old entry is eligible. This
	// second pass proves that the new fence is specific to active uses rather
	// than disabling cache retention altogether.
	if _, err := fixture.operations.Pool.Exec(fixture.ctx, "UPDATE "+operations+" SET state='canceled', retention_expires_at=$2 WHERE operation_id=$1", operationUUID(consumerID), old); err != nil {
		t.Fatal(err)
	}
	result, err = (MaintenanceRepository{Pool: fixture.operations.Pool, Namespace: fixture.operations.Namespace}).PruneExpiredCache(fixture.ctx, now, now.Add(-time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Examined != 1 || result.Tombstoned != 1 {
		t.Fatalf("terminal cache use did not become eligible: result=%+v", result)
	}
}

func TestMaintenanceRetentionPrunesExpiredCheckpointAfterOriginTerminal(t *testing.T) {
	fixture := newResponseCacheFixture(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	checkpoints, err := fixture.operations.Namespace.Render("conversation_checkpoints")
	if err != nil {
		t.Fatal(err)
	}
	operations, err := fixture.operations.Namespace.Render("operations")
	if err != nil {
		t.Fatal(err)
	}
	old := now.Add(-time.Hour)
	if _, err := fixture.operations.Pool.Exec(fixture.ctx, "UPDATE "+checkpoints+" SET expires_at=$2 WHERE checkpoint_id=$1", fixture.checkpointID, old); err != nil {
		t.Fatal(err)
	}
	var checkpointBlob uuid.UUID
	if err := fixture.operations.Pool.QueryRow(fixture.ctx, "SELECT delta_blob_id FROM "+checkpoints+" WHERE checkpoint_id=$1", fixture.checkpointID).Scan(&checkpointBlob); err != nil {
		t.Fatal(err)
	}
	providerState, err := fixture.operations.Namespace.Render("checkpoint_provider_state")
	if err != nil {
		t.Fatal(err)
	}
	providerAffinities, err := fixture.operations.Namespace.Render("checkpoint_provider_affinities")
	if err != nil {
		t.Fatal(err)
	}
	accountDigest := sha256.Sum256([]byte("maintenance-checkpoint-account"))
	stateDigest := sha256.Sum256([]byte("maintenance-checkpoint-state"))
	if _, err := fixture.operations.Pool.Exec(fixture.ctx, "INSERT INTO "+providerState+" (checkpoint_id, ordinal, provider, endpoint_id, endpoint_account_hmac, region, endpoint_family, model_lineage, state_kind, state_blob_id, state_digest, required, immutable_fork_safe, expires_at) VALUES ($1,0,'fixture','endpoint',$2,'region','family','lineage','opaque',$3,$4,true,true,$5)", fixture.checkpointID, accountDigest[:], checkpointBlob, stateDigest[:], old); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.operations.Pool.Exec(fixture.ctx, "INSERT INTO "+providerAffinities+" (checkpoint_id, affinity_rank, provider, route_id, endpoint_id, endpoint_account_hmac, region, endpoint_family, model_lineage, route_model_revision, cache_epoch, hard_pinned, last_success_at, expires_at) VALUES ($1,0,'fixture','route','endpoint',$2,'region','family','lineage','revision','epoch',true,$3,$4)", fixture.checkpointID, accountDigest[:], old, old); err != nil {
		t.Fatal(err)
	}
	// An expired checkpoint whose origin operation is still active remains
	// protected from maintenance.
	result, err := (MaintenanceRepository{Pool: fixture.operations.Pool, Namespace: fixture.operations.Namespace}).PruneExpiredCheckpoints(fixture.ctx, now, now.Add(-time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Examined != 0 || result.Deleted != 0 {
		t.Fatalf("active origin checkpoint was pruned: result=%+v", result)
	}
	if _, err := fixture.operations.Pool.Exec(fixture.ctx, "UPDATE "+operations+" SET state='canceled', retention_expires_at=$2, updated_at=$2 WHERE operation_id=$1", operationUUID(fixture.originID), old); err != nil {
		t.Fatal(err)
	}
	result, err = (MaintenanceRepository{Pool: fixture.operations.Pool, Namespace: fixture.operations.Namespace}).PruneExpiredCheckpoints(fixture.ctx, now, now.Add(-time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Examined != 1 || result.Deleted != 1 {
		t.Fatalf("expired checkpoint retention result=%+v", result)
	}
	var remaining int
	if err := fixture.operations.Pool.QueryRow(fixture.ctx, "SELECT count(*) FROM "+checkpoints+" WHERE checkpoint_id=$1", fixture.checkpointID).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("expired checkpoint remains after retention: %d", remaining)
	}
	var providerStateCount, providerAffinityCount int
	if err := fixture.operations.Pool.QueryRow(fixture.ctx, "SELECT count(*) FROM "+providerState+" WHERE checkpoint_id=$1", fixture.checkpointID).Scan(&providerStateCount); err != nil {
		t.Fatal(err)
	}
	if err := fixture.operations.Pool.QueryRow(fixture.ctx, "SELECT count(*) FROM "+providerAffinities+" WHERE checkpoint_id=$1", fixture.checkpointID).Scan(&providerAffinityCount); err != nil {
		t.Fatal(err)
	}
	if providerStateCount != 0 || providerAffinityCount != 0 {
		t.Fatalf("checkpoint provider children remain after retention: state=%d affinity=%d", providerStateCount, providerAffinityCount)
	}
}
