package postgres

import (
	"context"
	"strings"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/maintenance"
)

// MaintenanceObserver receives bounded maintenance progress without coupling
// the PostgreSQL adapter to one metrics implementation. Implementations must
// keep resource, outcome, and latency labels bounded; the repository only
// supplies fixed vocabulary values.
type MaintenanceObserver interface {
	RecordMaintenance(resource, outcome string, rows int, duration time.Duration)
	RecordMaintenanceFailure(resource string)
	RecordPostgresPool(total, acquired, idle, max int32)
	RecordPostgresLatency(kind string, duration time.Duration)
	RecordPostgresTableTuples(resource string, live, dead int64)
}

func (repository MaintenanceRepository) observeMaintenance(ctx context.Context, resource string, started time.Time, result maintenance.RetentionResult, err error) {
	if repository.Observer == nil {
		return
	}
	duration := time.Since(started)
	repository.Observer.RecordMaintenance(resource, "eligible", result.Examined, duration)
	repository.Observer.RecordMaintenance(resource, "tombstoned", result.Tombstoned, duration)
	repository.Observer.RecordMaintenance(resource, "deleted", result.Deleted, duration)
	repository.Observer.RecordMaintenance(resource, "skipped", result.Skipped, duration)
	if err != nil {
		repository.Observer.RecordMaintenanceFailure(resource)
	}
	repository.observePostgresBoundary(ctx, started)
}

func (repository MaintenanceRepository) observeBlobMaintenance(ctx context.Context, resource string, started time.Time, result BlobGCResult, err error) {
	if repository.Observer == nil {
		return
	}
	duration := time.Since(started)
	repository.Observer.RecordMaintenance(resource, "eligible", result.Examined, duration)
	// Eligibility marks the blob for a later, separately fenced deletion pass;
	// it is not evidence that an object was physically deleted.
	repository.Observer.RecordMaintenance(resource, "tombstoned", result.Eligible, duration)
	repository.Observer.RecordMaintenance(resource, "skipped", result.Skipped, duration)
	if err != nil {
		repository.Observer.RecordMaintenanceFailure(resource)
	}
	repository.observePostgresBoundary(ctx, started)
}

func (repository MaintenanceRepository) observePostgresBoundary(ctx context.Context, started time.Time) {
	if repository.Observer == nil {
		return
	}
	repository.Observer.RecordPostgresLatency("maintenance", time.Since(started))
	if repository.Pool == nil {
		return
	}
	stat := repository.Pool.Stat()
	repository.Observer.RecordPostgresPool(stat.TotalConns(), stat.AcquiredConns(), stat.IdleConns(), stat.MaxConns())
	repository.observePostgresTableTuples(ctx)
}

func (repository MaintenanceRepository) observePostgresTableTuples(ctx context.Context) {
	if repository.Observer == nil || repository.Pool == nil || ctx == nil || repository.Namespace.Validate() != nil {
		return
	}
	rows, err := repository.Pool.Query(ctx, `SELECT relname, n_live_tup, n_dead_tup
FROM pg_stat_user_tables
WHERE schemaname = $1 AND relname LIKE $2`, repository.Namespace.Schema, repository.Namespace.TablePrefix+"%")
	if err != nil {
		return
	}
	defer rows.Close()
	resources := map[string]struct{}{
		"blobs": {}, "operations": {}, "response_cache_entries": {},
		"provider_route_status": {}, "provider_inventory_snapshots": {},
		"query_executions": {}, "conversation_checkpoints": {},
		"budget_buckets": {}, "maintenance_outbox": {},
	}
	for rows.Next() {
		var relation string
		var live, dead int64
		if err := rows.Scan(&relation, &live, &dead); err != nil {
			return
		}
		logical := strings.TrimPrefix(relation, repository.Namespace.TablePrefix)
		if _, ok := resources[logical]; !ok {
			continue
		}
		// Keep the metric vocabulary aligned with the maintenance resource
		// labels; physical relation names never leave this adapter.
		switch logical {
		case "response_cache_entries":
			logical = "cache"
		case "provider_route_status":
			logical = "status"
		case "provider_inventory_snapshots":
			logical = "inventory"
		case "query_executions":
			logical = "query_execution"
		case "conversation_checkpoints":
			logical = "checkpoint"
		case "budget_buckets":
			logical = "budget"
		case "maintenance_outbox":
			logical = "outbox"
		case "blobs":
			logical = "blob"
		case "operations":
			logical = "operation"
		}
		repository.Observer.RecordPostgresTableTuples(logical, live, dead)
	}
}
