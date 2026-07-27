package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// UnknownCostCursor is the stable, opaque continuation position for one
// scope's reconciliation queue. Both fields are required together so a caller
// cannot accidentally skip rows with the same completion timestamp.
type UnknownCostCursor struct {
	CompletedAt time.Time
	OperationID uuid.UUID
}

// UnknownCostListOptions bounds one read-only reconciliation queue page.
// ScopeID has already crossed the authenticated maintenance boundary; this
// repository deliberately does not accept tenant or project cleartext.
type UnknownCostListOptions struct {
	ScopeID uuid.UUID
	After   *UnknownCostCursor
	Limit   int
}

// UnknownCostCandidate contains only the durable identity and safe reason
// needed to choose a later authoritative billing lookup. Provider IDs, request
// data, results, and credentials remain encrypted or outside this queue.
type UnknownCostCandidate struct {
	OperationID       uuid.UUID
	CompletedAt       time.Time
	UnknownReasonCode string
}

// UnknownCostRepository is a read-only, bounded reconciliation queue over
// completed operations whose actual cost is still unknown. It deliberately
// does not accept an exact amount or mutate budget state: authoritative billing
// evidence and the compound operation/journal transaction are separate work.
type UnknownCostRepository struct {
	Pool      *pgxpool.Pool
	Namespace Namespace
}

func (repository UnknownCostRepository) validate() error {
	if repository.Pool == nil {
		return errors.New("unknown-cost PostgreSQL pool is nil")
	}
	return repository.Namespace.Validate()
}

func (options *UnknownCostListOptions) normalize() error {
	if options == nil {
		return errors.New("unknown-cost list options are nil")
	}
	if options.ScopeID == uuid.Nil {
		return errors.New("unknown-cost scope id is required")
	}
	if options.Limit <= 0 || options.Limit > maxMaintenanceBatch {
		return fmt.Errorf("unknown-cost list limit must be between 1 and %d", maxMaintenanceBatch)
	}
	if options.After == nil {
		return nil
	}
	if options.After.CompletedAt.IsZero() || options.After.OperationID == uuid.Nil {
		return errors.New("unknown-cost cursor requires completion time and operation id")
	}
	options.After.CompletedAt = options.After.CompletedAt.UTC()
	return nil
}

// List returns one stable page ordered newest-first. The tuple cursor is a
// strict "after" position in that order, so retries and concurrent inserts
// cannot duplicate a row already acknowledged by a caller. It is intentionally
// read-only and performs no provider, Redis, or network work.
func (repository UnknownCostRepository) List(ctx context.Context, options UnknownCostListOptions) ([]UnknownCostCandidate, error) {
	if err := repository.validate(); err != nil {
		return nil, err
	}
	if err := options.normalize(); err != nil {
		return nil, err
	}
	operations, err := repository.Namespace.Render("operations")
	if err != nil {
		return nil, err
	}
	var completedAt any
	var operationID any
	if options.After != nil {
		completedAt = options.After.CompletedAt
		operationID = options.After.OperationID
	}
	rows, err := repository.Pool.Query(ctx, unknownCostListQuery(operations), options.ScopeID, completedAt, operationID, options.Limit)
	if err != nil {
		return nil, redactPostgresError(fmt.Errorf("list unknown-cost operations: %w", err))
	}
	defer rows.Close()
	result := make([]UnknownCostCandidate, 0, options.Limit)
	for rows.Next() {
		var candidate UnknownCostCandidate
		if err := rows.Scan(&candidate.OperationID, &candidate.CompletedAt, &candidate.UnknownReasonCode); err != nil {
			return nil, redactPostgresError(fmt.Errorf("scan unknown-cost operation: %w", err))
		}
		candidate.CompletedAt = candidate.CompletedAt.UTC()
		result = append(result, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, redactPostgresError(fmt.Errorf("iterate unknown-cost operations: %w", err))
	}
	return result, nil
}

func unknownCostListQuery(operations string) string {
	return "SELECT operation_id, completed_at, cost_unknown_reason_code FROM " + operations +
		" WHERE scope_id=$1 AND state='completed' AND cost_status='unknown'" +
		" AND ($2::timestamptz IS NULL OR (completed_at, operation_id) < ($2::timestamptz, $3::uuid))" +
		" ORDER BY completed_at DESC, operation_id DESC LIMIT $4"
}
