package postgres

// PostgreSQL maintenance is intentionally kept out of runtime repositories.
// Every mutating method is bounded, uses the dedicated llmtw_maintenance role,
// and either locks candidates with SKIP LOCKED or updates one claimed outbox
// row. External object-store work is never performed while a SQL transaction
// is open.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mfow/llm-temporal-worker/golang/maintenance"
)

const maxMaintenanceBatch = 10_000

var (
	ErrMaintenanceOutboxNotClaimed = maintenance.ErrOutboxNotClaimed
	ErrMaintenanceOutboxConflict   = maintenance.ErrOutboxConflict
	ErrMaintenanceInvalidPayload   = errors.New("maintenance outbox payload is invalid")
)

// MaintenanceRepository owns only maintenance-role operations. It is not
// used by the worker runtime, which must not receive DELETE privileges.
type MaintenanceRepository struct {
	Pool      *pgxpool.Pool
	Namespace Namespace
	// Observer is optional. When set, bounded retention/blob passes report
	// progress and PostgreSQL pool state after each repository boundary.
	Observer      MaintenanceObserver
	NewID         func() (uuid.UUID, error)
	NewLeaseToken func() (maintenance.LeaseToken, error)
}

func (repository MaintenanceRepository) validate() error {
	if repository.Pool == nil {
		return errors.New("maintenance PostgreSQL pool is nil")
	}
	return repository.Namespace.Validate()
}

func validateBatch(now time.Time, limit int) error {
	if now.IsZero() {
		return errors.New("maintenance time is required")
	}
	if limit <= 0 || limit > maxMaintenanceBatch {
		return fmt.Errorf("maintenance batch limit must be between 1 and %d", maxMaintenanceBatch)
	}
	return nil
}

// newDeleteBlobOutboxEvent is the single constructor used by SQL retention
// paths. Keeping the payload construction in the storage-neutral maintenance
// contract prevents a cache retention change from silently drifting away
// from the outbox validator.
func newDeleteBlobOutboxEvent(eventID, blobID uuid.UUID, now time.Time) (maintenance.Event, error) {
	if eventID == uuid.Nil || blobID == uuid.Nil {
		return maintenance.Event{}, errors.New("cache deletion outbox IDs are required")
	}
	return maintenance.NewDeleteBlobEvent(eventID.String(), blobID.String(), now, now)
}

// PruneExpiredCache tombstones an unused cache entry and publishes a durable
// blob-delete intent in the same transaction. Reference/fill checks are
// repeated in the locked candidate query; a concurrent reference therefore
// prevents tombstoning rather than racing a blob deletion.
func (repository MaintenanceRepository) PruneExpiredCache(ctx context.Context, now, unusedBefore time.Time, limit int) (result maintenance.RetentionResult, returnErr error) {
	started := time.Now()
	defer func() { repository.observeMaintenance(ctx, "cache", started, result, returnErr) }()
	if err := repository.validate(); err != nil {
		return result, err
	}
	if err := validateBatch(now, limit); err != nil {
		return result, err
	}
	if unusedBefore.IsZero() || unusedBefore.After(now) {
		return result, errors.New("cache unused cutoff must not be after maintenance time")
	}
	cacheTable, err := repository.Namespace.Render("response_cache_entries")
	if err != nil {
		return result, err
	}
	fillsTable, err := repository.Namespace.Render("response_cache_fills")
	if err != nil {
		return result, err
	}
	operationsTable, err := repository.Namespace.Render("operations")
	if err != nil {
		return result, err
	}
	usesTable, err := repository.Namespace.Render("response_cache_uses")
	if err != nil {
		return result, err
	}
	checkpointsTable, err := repository.Namespace.Render("conversation_checkpoints")
	if err != nil {
		return result, err
	}
	outboxTable, err := repository.Namespace.Render("maintenance_outbox")
	if err != nil {
		return result, err
	}
	newID := repository.NewID
	if newID == nil {
		newID = uuid.NewRandom
	}
	err = WithTransaction(ctx, repository.Pool, func(ctx context.Context, tx pgx.Tx) error {
		query := "WITH candidates AS (" +
			" SELECT c.cache_entry_id, c.response_blob_id" +
			" FROM " + cacheTable + " c" +
			" WHERE c.state = 'ready' AND c.last_used_at < $1" +
			" AND NOT EXISTS (SELECT 1 FROM " + fillsTable + " f" +
			" WHERE f.cache_entry_id = c.cache_entry_id AND f.state = 'filling' AND f.lease_expires_at > $2)" +
			" AND NOT EXISTS (SELECT 1 FROM " + operationsTable + " o" +
			" WHERE o.cache_entry_id = c.cache_entry_id AND o.state NOT IN ('completed', 'definite_failed', 'canceled'))" +
			" AND NOT EXISTS (SELECT 1 FROM " + usesTable + " u" +
			" JOIN " + operationsTable + " o ON o.operation_id = u.operation_id" +
			" WHERE u.cache_entry_id = c.cache_entry_id AND o.state NOT IN ('completed', 'definite_failed', 'canceled'))" +
			" AND NOT EXISTS (SELECT 1 FROM " + checkpointsTable + " cp" +
			" WHERE cp.origin_cache_entry_id = c.cache_entry_id AND cp.expires_at > $2)" +
			" ORDER BY c.last_used_at, c.cache_entry_id LIMIT $3 FOR UPDATE SKIP LOCKED" +
			"), tombstoned AS (" +
			" UPDATE " + cacheTable + " c SET state = 'tombstoned'" +
			" FROM candidates WHERE c.cache_entry_id = candidates.cache_entry_id" +
			" RETURNING c.cache_entry_id, c.response_blob_id" +
			") SELECT cache_entry_id, response_blob_id FROM tombstoned"
		rows, err := tx.Query(ctx, query, unusedBefore, now, limit)
		if err != nil {
			return redactPostgresError(fmt.Errorf("claim expired response cache entries: %w", err))
		}
		defer rows.Close()
		for rows.Next() {
			var cacheID uuid.UUID
			var blobID *uuid.UUID
			if err := rows.Scan(&cacheID, &blobID); err != nil {
				return fmt.Errorf("scan expired response cache entry: %w", err)
			}
			result.Examined++
			result.Tombstoned++
			if blobID == nil {
				continue
			}
			eventID, err := newID()
			if err != nil {
				return fmt.Errorf("generate cache deletion outbox ID: %w", err)
			}
			event, err := newDeleteBlobOutboxEvent(eventID, *blobID, now)
			if err != nil {
				return fmt.Errorf("build cache deletion outbox event: %w", err)
			}
			if _, err := tx.Exec(ctx, "INSERT INTO "+outboxTable+
				" (outbox_id, event_kind, aggregate_type, aggregate_id, dedupe_key, safe_payload, state, attempt_count, available_at, created_at)"+
				" VALUES ($1, 'delete_blob', 'blob', $2, $3, $4, 'pending', 0, $5, $5)"+
				" ON CONFLICT (event_kind, dedupe_key) DO NOTHING", eventID, *blobID, event.DedupeKey[:], event.SafePayload, now); err != nil {
				return redactPostgresError(fmt.Errorf("publish cache deletion outbox: %w", err))
			}
			_ = cacheID // returned for deterministic row accounting and future metrics.
		}
		if err := rows.Err(); err != nil {
			return redactPostgresError(fmt.Errorf("iterate expired response cache entries: %w", err))
		}
		return nil
	})
	if err != nil {
		return maintenance.RetentionResult{}, err
	}
	return result, nil
}

// PruneExpiredProviderStatus removes bounded, expired status-history rows
// while preserving the row referenced by every current route projection. The
// candidate rows are locked with SKIP LOCKED before deletion; the foreign-key
// reference therefore cannot be deleted underneath a concurrent projection
// update. Current projections remain available for status queries.
func (repository MaintenanceRepository) PruneExpiredProviderStatus(ctx context.Context, now, expiresBefore time.Time, limit int) (result maintenance.RetentionResult, returnErr error) {
	started := time.Now()
	defer func() { repository.observeMaintenance(ctx, "status", started, result, returnErr) }()
	if err := repository.validate(); err != nil {
		return result, err
	}
	if err := validateBatch(now, limit); err != nil {
		return result, err
	}
	if expiresBefore.IsZero() || expiresBefore.After(now) {
		return result, errors.New("provider status expiry cutoff must not be after maintenance time")
	}
	eventsTable, err := repository.Namespace.Render("provider_status_events")
	if err != nil {
		return result, err
	}
	routesTable, err := repository.Namespace.Render("provider_route_status")
	if err != nil {
		return result, err
	}
	err = WithTransaction(ctx, repository.Pool, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, "SELECT e.event_id FROM "+eventsTable+" e WHERE e.expires_at < $1 AND NOT EXISTS (SELECT 1 FROM "+routesTable+" r WHERE r.last_event_id = e.event_id) ORDER BY e.expires_at, e.event_id LIMIT $2 FOR UPDATE OF e SKIP LOCKED", expiresBefore, limit)
		if err != nil {
			return redactPostgresError(fmt.Errorf("claim expired provider status events: %w", err))
		}
		defer rows.Close()
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				return fmt.Errorf("scan expired provider status event: %w", err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return redactPostgresError(fmt.Errorf("iterate expired provider status events: %w", err))
		}
		rows.Close()
		result.Examined = len(ids)
		if len(ids) == 0 {
			return nil
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+eventsTable+" WHERE event_id = ANY($1::bigint[])", ids); err != nil {
			return redactPostgresError(fmt.Errorf("delete expired provider status events: %w", err))
		}
		result.Deleted = len(ids)
		return nil
	})
	if err != nil {
		return maintenance.RetentionResult{}, err
	}
	return result, nil
}

// PruneExpiredInventory removes bounded, expired inventory history and their
// model rows, but never removes the latest snapshot for an endpoint account
// epoch (even if that snapshot is itself expired). Parent rows are locked
// before children are deleted so a concurrent model insert cannot race the
// foreign-key cleanup.
func (repository MaintenanceRepository) PruneExpiredInventory(ctx context.Context, now, expiresBefore time.Time, limit int) (result maintenance.RetentionResult, returnErr error) {
	started := time.Now()
	defer func() { repository.observeMaintenance(ctx, "inventory", started, result, returnErr) }()
	if err := repository.validate(); err != nil {
		return result, err
	}
	if err := validateBatch(now, limit); err != nil {
		return result, err
	}
	if expiresBefore.IsZero() || expiresBefore.After(now) {
		return result, errors.New("provider inventory expiry cutoff must not be after maintenance time")
	}
	snapshotsTable, err := repository.Namespace.Render("provider_inventory_snapshots")
	if err != nil {
		return result, err
	}
	modelsTable, err := repository.Namespace.Render("provider_inventory_models")
	if err != nil {
		return result, err
	}
	err = WithTransaction(ctx, repository.Pool, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, "SELECT s.inventory_snapshot_id FROM "+snapshotsTable+" s WHERE s.expires_at < $1 AND EXISTS (SELECT 1 FROM "+snapshotsTable+" newer WHERE newer.config_digest = s.config_digest AND newer.provider = s.provider AND newer.endpoint_id = s.endpoint_id AND newer.endpoint_account_hmac = s.endpoint_account_hmac AND (newer.observed_at > s.observed_at OR (newer.observed_at = s.observed_at AND newer.inventory_snapshot_id > s.inventory_snapshot_id))) ORDER BY s.expires_at, s.inventory_snapshot_id LIMIT $2 FOR UPDATE OF s SKIP LOCKED", expiresBefore, limit)
		if err != nil {
			return redactPostgresError(fmt.Errorf("claim expired provider inventory snapshots: %w", err))
		}
		defer rows.Close()
		var ids []uuid.UUID
		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				return fmt.Errorf("scan expired provider inventory snapshot: %w", err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return redactPostgresError(fmt.Errorf("iterate expired provider inventory snapshots: %w", err))
		}
		rows.Close()
		result.Examined = len(ids)
		if len(ids) == 0 {
			return nil
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+modelsTable+" WHERE inventory_snapshot_id = ANY($1::uuid[])", ids); err != nil {
			return redactPostgresError(fmt.Errorf("delete expired provider inventory models: %w", err))
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+snapshotsTable+" WHERE inventory_snapshot_id = ANY($1::uuid[])", ids); err != nil {
			return redactPostgresError(fmt.Errorf("delete expired provider inventory snapshots: %w", err))
		}
		result.Deleted = len(ids)
		return nil
	})
	if err != nil {
		return maintenance.RetentionResult{}, err
	}
	return result, nil
}

// PruneExpiredQueryExecutions removes a bounded batch of completed control
// query audit rows. Query executions are immutable and have no foreign-key
// dependants, so the maintenance role can delete them in place after locking
// the indexed expiry candidates. The short transaction keeps the SKIP LOCKED
// claim and delete atomic while allowing other maintenance workers to make
// progress on the remainder of the ledger.
func (repository MaintenanceRepository) PruneExpiredQueryExecutions(ctx context.Context, now, expiresBefore time.Time, limit int) (result maintenance.RetentionResult, returnErr error) {
	started := time.Now()
	defer func() { repository.observeMaintenance(ctx, "query_execution", started, result, returnErr) }()
	if err := repository.validate(); err != nil {
		return result, err
	}
	if err := validateBatch(now, limit); err != nil {
		return result, err
	}
	if expiresBefore.IsZero() || expiresBefore.After(now) {
		return result, errors.New("query execution expiry cutoff must not be after maintenance time")
	}
	executionsTable, err := repository.Namespace.Render("query_executions")
	if err != nil {
		return result, err
	}
	err = WithTransaction(ctx, repository.Pool, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, "SELECT query_execution_id FROM "+executionsTable+" WHERE retention_expires_at < $1 ORDER BY retention_expires_at, query_execution_id LIMIT $2 FOR UPDATE SKIP LOCKED", expiresBefore, limit)
		if err != nil {
			return redactPostgresError(fmt.Errorf("claim expired query executions: %w", err))
		}
		defer rows.Close()
		var ids []uuid.UUID
		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				return fmt.Errorf("scan expired query execution: %w", err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return redactPostgresError(fmt.Errorf("iterate expired query executions: %w", err))
		}
		rows.Close()
		result.Examined = len(ids)
		if len(ids) == 0 {
			return nil
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+executionsTable+" WHERE query_execution_id = ANY($1::uuid[])", ids); err != nil {
			return redactPostgresError(fmt.Errorf("delete expired query executions: %w", err))
		}
		result.Deleted = len(ids)
		return nil
	})
	if err != nil {
		return maintenance.RetentionResult{}, err
	}
	return result, nil
}

// PruneExpiredBudgetBuckets removes a bounded set of empty historical budget
// buckets. maxWindow is the largest configured budget window horizon; a cutoff
// newer than now-maxWindow is rejected so callers cannot accidentally delete a
// bucket still needed by a window. The method intentionally does not infer that
// horizon from Redis or load the active budget working set. A bucket is deleted
// only after its exact reserved/accounted projection is zero and no operation
// reservation still references it. Journal history remains intact for a future
// cold rebuild, and an open/finalized reservation remains a retention fence
// until operation retention has dealt with that row explicitly.
func (repository MaintenanceRepository) PruneExpiredBudgetBuckets(ctx context.Context, now, bucketsBefore time.Time, maxWindow time.Duration, limit int) (result maintenance.RetentionResult, returnErr error) {
	started := time.Now()
	defer func() { repository.observeMaintenance(ctx, "budget", started, result, returnErr) }()
	if err := repository.validate(); err != nil {
		return result, err
	}
	if err := validateBatch(now, limit); err != nil {
		return result, err
	}
	if maxWindow <= 0 {
		return result, errors.New("budget maximum window must be positive")
	}
	if bucketsBefore.IsZero() || bucketsBefore.After(now) {
		return result, errors.New("budget bucket cutoff must not be after maintenance time")
	}
	if bucketsBefore.After(now.Add(-maxWindow)) {
		return result, errors.New("budget bucket cutoff is newer than the maximum window horizon")
	}
	bucketsTable, err := repository.Namespace.Render("budget_buckets")
	if err != nil {
		return result, err
	}
	reservationsTable, err := repository.Namespace.Render("operation_budget_reservations")
	if err != nil {
		return result, err
	}
	err = WithTransaction(ctx, repository.Pool, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, "WITH candidates AS ("+
			" SELECT b.window_id, b.bucket_start"+
			" FROM "+bucketsTable+" b"+
			" WHERE b.bucket_start < $1"+
			" AND b.reserved_cost_usd = 0"+
			" AND b.accounted_cost_usd = 0"+
			" AND NOT EXISTS (SELECT 1 FROM "+reservationsTable+" r"+
			" WHERE r.window_id = b.window_id AND r.bucket_start = b.bucket_start)"+
			" ORDER BY b.bucket_start, b.window_id LIMIT $2"+
			" FOR UPDATE OF b SKIP LOCKED"+
			"), deleted AS ("+
			" DELETE FROM "+bucketsTable+" b USING candidates c"+
			" WHERE b.window_id = c.window_id AND b.bucket_start = c.bucket_start"+
			" RETURNING b.window_id, b.bucket_start"+
			") SELECT window_id, bucket_start FROM deleted", bucketsBefore, limit)
		if err != nil {
			return redactPostgresError(fmt.Errorf("claim expired budget buckets: %w", err))
		}
		defer rows.Close()
		for rows.Next() {
			var windowID uuid.UUID
			var bucketStart time.Time
			if err := rows.Scan(&windowID, &bucketStart); err != nil {
				return fmt.Errorf("scan expired budget bucket: %w", err)
			}
			result.Examined++
			result.Deleted++
		}
		if err := rows.Err(); err != nil {
			return redactPostgresError(fmt.Errorf("iterate expired budget buckets: %w", err))
		}
		return nil
	})
	if err != nil {
		return maintenance.RetentionResult{}, err
	}
	return result, nil
}

// PruneExpiredCheckpoints removes a bounded set of expired checkpoints after
// proving that no retained graph, operation, or cache row still refers to
// each candidate. Provider-state and affinity rows are deleted in the same
// transaction; blobs remain independently owned and are reclaimed by blob
// retention only after its own reference fence passes.
//
// The candidate rows are locked with FOR UPDATE SKIP LOCKED. PostgreSQL's
// foreign-key key-share locking makes a concurrent child/reference insert wait
// for this lock, so the locked reference predicates remain authoritative for
// the delete transaction.
func (repository MaintenanceRepository) PruneExpiredCheckpoints(ctx context.Context, now, expiresBefore time.Time, limit int) (result maintenance.RetentionResult, returnErr error) {
	started := time.Now()
	defer func() { repository.observeMaintenance(ctx, "checkpoint", started, result, returnErr) }()
	if err := repository.validate(); err != nil {
		return result, err
	}
	if err := validateBatch(now, limit); err != nil {
		return result, err
	}
	if expiresBefore.IsZero() || expiresBefore.After(now) {
		return result, errors.New("checkpoint expiry cutoff must not be after maintenance time")
	}
	checkpointsTable, err := repository.Namespace.Render("conversation_checkpoints")
	if err != nil {
		return result, err
	}
	providerStateTable, err := repository.Namespace.Render("checkpoint_provider_state")
	if err != nil {
		return result, err
	}
	providerAffinitiesTable, err := repository.Namespace.Render("checkpoint_provider_affinities")
	if err != nil {
		return result, err
	}
	operationsTable, err := repository.Namespace.Render("operations")
	if err != nil {
		return result, err
	}
	cacheEntriesTable, err := repository.Namespace.Render("response_cache_entries")
	if err != nil {
		return result, err
	}
	err = WithTransaction(ctx, repository.Pool, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, "SELECT c.checkpoint_id FROM "+checkpointsTable+
			" c WHERE c.expires_at < $1"+
			" AND NOT EXISTS (SELECT 1 FROM "+operationsTable+
			" origin WHERE origin.operation_id = c.origin_operation_id AND origin.state NOT IN ('completed', 'definite_failed', 'canceled'))"+
			" AND NOT EXISTS (SELECT 1 FROM "+checkpointsTable+
			" child WHERE child.parent_checkpoint_id = c.checkpoint_id)"+
			" AND NOT EXISTS (SELECT 1 FROM "+checkpointsTable+
			" compacted WHERE compacted.compacted_through_checkpoint_id = c.checkpoint_id)"+
			" AND NOT EXISTS (SELECT 1 FROM "+operationsTable+
			" o WHERE o.parent_checkpoint_id = c.checkpoint_id)"+
			" AND NOT EXISTS (SELECT 1 FROM "+cacheEntriesTable+
			" e WHERE e.origin_checkpoint_id = c.checkpoint_id)"+
			" ORDER BY c.expires_at, c.checkpoint_id LIMIT $2 FOR UPDATE OF c SKIP LOCKED", expiresBefore, limit)
		if err != nil {
			return redactPostgresError(fmt.Errorf("claim expired conversation checkpoints: %w", err))
		}
		var ids []uuid.UUID
		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return fmt.Errorf("scan expired conversation checkpoint: %w", err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return redactPostgresError(fmt.Errorf("iterate expired conversation checkpoints: %w", err))
		}
		rows.Close()
		result.Examined = len(ids)
		if len(ids) == 0 {
			return nil
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+providerStateTable+" WHERE checkpoint_id = ANY($1::uuid[])", ids); err != nil {
			return redactPostgresError(fmt.Errorf("delete expired checkpoint provider state: %w", err))
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+providerAffinitiesTable+" WHERE checkpoint_id = ANY($1::uuid[])", ids); err != nil {
			return redactPostgresError(fmt.Errorf("delete expired checkpoint provider affinities: %w", err))
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+checkpointsTable+" WHERE checkpoint_id = ANY($1::uuid[])", ids); err != nil {
			return redactPostgresError(fmt.Errorf("delete expired conversation checkpoints: %w", err))
		}
		result.Deleted = len(ids)
		return nil
	})
	if err != nil {
		return maintenance.RetentionResult{}, err
	}
	return result, nil
}

// PublishOutbox inserts idempotent safe intent. Duplicate event_kind/dedupe
// pairs are successful no-ops, so a retry cannot enqueue duplicate deletion.
func (repository MaintenanceRepository) PublishOutbox(ctx context.Context, event maintenance.Event) error {
	if err := repository.validate(); err != nil {
		return err
	}
	canonical, err := maintenance.NormalizeSafePayload(event.SafePayload)
	if err != nil {
		return err
	}
	event.SafePayload = canonical
	if err := event.Validate(); err != nil {
		return err
	}
	eventID, err := uuid.Parse(event.ID)
	if err != nil {
		return fmt.Errorf("maintenance outbox ID: %w", err)
	}
	aggregateID, err := uuid.Parse(event.AggregateID)
	if err != nil {
		return fmt.Errorf("maintenance outbox aggregate ID: %w", err)
	}
	outboxTable, err := repository.Namespace.Render("maintenance_outbox")
	if err != nil {
		return err
	}
	createdAt := event.CreatedAt
	if createdAt.IsZero() {
		createdAt = event.AvailableAt
	}
	tag, err := repository.Pool.Exec(ctx, "INSERT INTO "+outboxTable+
		" (outbox_id, event_kind, aggregate_type, aggregate_id, dedupe_key, safe_payload, state, attempt_count, available_at, created_at)"+
		" VALUES ($1, $2, $3, $4, $5, $6, 'pending', 0, $7, $8)"+
		" ON CONFLICT (event_kind, dedupe_key) DO NOTHING", eventID, string(event.Kind), event.AggregateType, aggregateID, event.DedupeKey[:], event.SafePayload, event.AvailableAt, createdAt)
	if err != nil {
		return redactPostgresError(fmt.Errorf("publish maintenance outbox: %w", err))
	}
	if tag.RowsAffected() == 0 {
		var same bool
		if err := repository.Pool.QueryRow(ctx, "SELECT aggregate_type = $2 AND aggregate_id = $3 AND safe_payload = $4::jsonb FROM "+outboxTable+" WHERE event_kind = $1 AND dedupe_key = $5", string(event.Kind), event.AggregateType, aggregateID, event.SafePayload, event.DedupeKey[:]).Scan(&same); err != nil {
			return redactPostgresError(fmt.Errorf("inspect maintenance outbox dedupe: %w", err))
		}
		if !same {
			return ErrMaintenanceOutboxConflict
		}
	}
	return nil
}

// ClaimOutbox uses a short transaction and FOR UPDATE SKIP LOCKED. Expired
// processing leases are eligible for recovery; live leases are untouched.
func (repository MaintenanceRepository) ClaimOutbox(ctx context.Context, options maintenance.ClaimOptions) ([]maintenance.Event, error) {
	if err := repository.validate(); err != nil {
		return nil, err
	}
	if err := options.Validate(); err != nil {
		return nil, err
	}
	outboxTable, err := repository.Namespace.Render("maintenance_outbox")
	if err != nil {
		return nil, err
	}
	newLeaseToken := repository.NewLeaseToken
	if newLeaseToken == nil {
		newLeaseToken = maintenance.NewLeaseToken
	}
	var events []maintenance.Event
	err = WithTransaction(ctx, repository.Pool, func(ctx context.Context, tx pgx.Tx) error {
		// Lock only a bounded candidate set. Tokens are generated and written one
		// row at a time so every claim gets a distinct fence; a stale claimant's
		// token can never authorize a successor's completion.
		rows, err := tx.Query(ctx, "SELECT outbox_id FROM "+outboxTable+
			" WHERE ((state IN ('pending', 'failed') AND available_at <= $1)"+
			" OR (state = 'processing' AND lease_expires_at <= $1))"+
			" ORDER BY available_at, outbox_id LIMIT $2 FOR UPDATE SKIP LOCKED", options.Now, options.Limit)
		if err != nil {
			return redactPostgresError(fmt.Errorf("claim maintenance outbox: %w", err))
		}
		var candidateIDs []uuid.UUID
		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return fmt.Errorf("scan maintenance outbox candidate: %w", err)
			}
			candidateIDs = append(candidateIDs, id)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return redactPostgresError(fmt.Errorf("iterate maintenance outbox candidates: %w", err))
		}
		rows.Close()
		for _, candidateID := range candidateIDs {
			token, err := newLeaseToken()
			if err != nil {
				return err
			}
			leaseUntil := options.Now.Add(options.Lease)
			row := tx.QueryRow(ctx, "UPDATE "+outboxTable+
				" SET state = 'processing', attempt_count = attempt_count + 1, lease_expires_at = $2, lease_token = $3"+
				" WHERE outbox_id = $1 AND ((state IN ('pending', 'failed') AND available_at <= $4)"+
				" OR (state = 'processing' AND lease_expires_at <= $4))"+
				" RETURNING outbox_id, event_kind, aggregate_type, aggregate_id, dedupe_key, safe_payload, state,"+
				" attempt_count, available_at, lease_expires_at, created_at, completed_at, lease_token", candidateID, leaseUntil, uuid.UUID(token), options.Now)
			var id, aggregateID uuid.UUID
			var kind, aggregateType, state string
			var dedupe, payload []byte
			var attempt int
			var available, created time.Time
			var lease, completed *time.Time
			var storedToken uuid.UUID
			if err := row.Scan(&id, &kind, &aggregateType, &aggregateID, &dedupe, &payload, &state, &attempt, &available, &lease, &created, &completed, &storedToken); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					continue
				}
				return redactPostgresError(fmt.Errorf("update claimed maintenance outbox: %w", err))
			}
			if len(dedupe) != 32 {
				return ErrMaintenanceInvalidPayload
			}
			var key [32]byte
			copy(key[:], dedupe)
			var leaseKey maintenance.LeaseToken
			copy(leaseKey[:], storedToken[:])
			event := maintenance.Event{ID: id.String(), Kind: maintenance.EventKind(kind), AggregateType: aggregateType, AggregateID: aggregateID.String(), DedupeKey: key, SafePayload: append([]byte(nil), payload...), State: maintenance.EventState(state), AttemptCount: attempt, AvailableAt: available, CreatedAt: created, LeaseToken: leaseKey}
			if lease != nil {
				event.LeaseExpiresAt = *lease
			}
			if completed != nil {
				event.CompletedAt = *completed
			}
			if err := event.Validate(); err != nil {
				return fmt.Errorf("validate claimed maintenance outbox: %w", err)
			}
			events = append(events, event)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}

func (repository MaintenanceRepository) CompleteOutbox(ctx context.Context, id string, token maintenance.LeaseToken, completedAt time.Time) error {
	return repository.updateOutboxState(ctx, id, token, completedAt, completedAt, "completed", true)
}

func (repository MaintenanceRepository) RetryOutbox(ctx context.Context, id string, token maintenance.LeaseToken, retriedAt, availableAt time.Time) error {
	return repository.updateOutboxState(ctx, id, token, retriedAt, availableAt, "failed", false)
}

func outboxStateUpdateSQL(outboxTable string, completed bool) string {
	if completed {
		return "UPDATE " + outboxTable + " SET state = 'completed', lease_expires_at = NULL, completed_at = $3 WHERE outbox_id = $1 AND lease_token = $2 AND state = 'processing' AND lease_expires_at > $3"
	}
	return "UPDATE " + outboxTable + " SET state = 'failed', lease_expires_at = NULL, available_at = $3, completed_at = NULL WHERE outbox_id = $1 AND lease_token = $2 AND state = 'processing' AND lease_expires_at > $4"
}

func (repository MaintenanceRepository) updateOutboxState(ctx context.Context, id string, token maintenance.LeaseToken, at, availableAt time.Time, state string, completed bool) error {
	if err := repository.validate(); err != nil {
		return err
	}
	if id == "" || token.IsZero() || at.IsZero() {
		return errors.New("maintenance outbox state identity and time are required")
	}
	if !completed && (availableAt.IsZero() || !availableAt.After(at)) {
		return errors.New("maintenance outbox retry must be scheduled after retry time")
	}
	outboxID, err := uuid.Parse(id)
	if err != nil {
		return fmt.Errorf("maintenance outbox ID: %w", err)
	}
	outboxTable, err := repository.Namespace.Render("maintenance_outbox")
	if err != nil {
		return err
	}
	var query string
	var args []any
	if completed {
		// Compare the lease with the caller's bounded maintenance clock.  The
		// in-memory contract and ClaimOutbox both use explicit timestamps; a
		// database wall-clock check here would let a caller's deterministic
		// clock advance past the lease without fencing the stale worker.
		query = outboxStateUpdateSQL(outboxTable, true)
		args = []any{outboxID, uuid.UUID(token), at}
	} else {
		query = outboxStateUpdateSQL(outboxTable, false)
		args = []any{outboxID, uuid.UUID(token), availableAt, at}
	}
	tag, err := repository.Pool.Exec(ctx, query, args...)
	if err != nil {
		return redactPostgresError(fmt.Errorf("update maintenance outbox: %w", err))
	}
	if tag.RowsAffected() != 1 {
		// Retrying a request after its own transaction response was lost is a
		// successful no-op when the same fence already reached the requested
		// terminal state. Any other state/token is a lost-lease error.
		var currentState string
		var currentToken *uuid.UUID
		if err := repository.Pool.QueryRow(ctx, "SELECT state, lease_token FROM "+outboxTable+" WHERE outbox_id = $1", outboxID).Scan(&currentState, &currentToken); err == nil && currentToken != nil {
			var current maintenance.LeaseToken
			copy(current[:], currentToken[:])
			if current == token && ((completed && currentState == "completed") || (!completed && currentState == "failed")) {
				return nil
			}
		}
		return ErrMaintenanceOutboxNotClaimed
	}
	return nil
}
