package postgres

// This file contains the bounded PostgreSQL read side for the spend-summary
// query. It deliberately reads only completed operation rows and retained
// query-execution audit rows. The control layer remains responsible for
// authorization, request validation, and converting this typed result to the
// signed query response.

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mfow/llm-temporal-worker/golang/control"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

// SpendSummaryListOptions retains the legacy adapter name for the neutral contract.
type SpendSummaryListOptions = control.SpendSummaryListOptions

var _ control.SpendSummaryReader = SpendSummaryRepository{}

// SpendSummaryRepository reads exact and unknown costs from both durable
// ledgers. A nil ActualCostUSD is never treated as zero: unknown rows are
// counted separately and make their bucket partial.
type SpendSummaryRepository struct {
	Pool      *pgxpool.Pool
	Namespace Namespace
}

func (repository SpendSummaryRepository) validate() error {
	if repository.Pool == nil {
		return errors.New("spend summary repository pool is nil")
	}
	return repository.Namespace.Validate()
}

func normalizeSpendSummaryOptions(options *SpendSummaryListOptions) error {
	if options == nil {
		return errors.New("spend summary options are nil")
	}
	if options.ScopeID == uuid.Nil {
		return errors.New("spend summary scope id is required")
	}
	if options.StartTime.IsZero() || options.EndTime.IsZero() {
		return errors.New("spend summary time bounds are required")
	}
	options.StartTime = options.StartTime.UTC()
	options.EndTime = options.EndTime.UTC()
	if !options.EndTime.After(options.StartTime) {
		return errors.New("spend summary end time must be after start time")
	}
	seenDimensions := make(map[control.SpendDimension]struct{}, len(options.GroupBy))
	for _, dimension := range options.GroupBy {
		if _, exists := seenDimensions[dimension]; exists {
			return fmt.Errorf("spend summary group dimension %q is repeated", dimension)
		}
		seenDimensions[dimension] = struct{}{}
		switch dimension {
		case control.SpendByOperation, control.SpendByProvider, control.SpendByModel:
		default:
			return fmt.Errorf("spend summary group dimension %q is invalid", dimension)
		}
	}
	seenKinds := make(map[control.OperationKind]struct{}, len(options.OperationKinds))
	for _, kind := range options.OperationKinds {
		if _, exists := seenKinds[kind]; exists {
			return fmt.Errorf("spend summary operation kind %q is repeated", kind)
		}
		seenKinds[kind] = struct{}{}
		switch kind {
		case control.OperationGenerate, control.OperationCompact, control.OperationQuery:
		default:
			return fmt.Errorf("spend summary operation kind %q is invalid", kind)
		}
	}
	return nil
}

// ListSpendSummary executes one aggregate query. The operation and query
// ledgers are UNION ALL'd so every completed charge is counted exactly once;
// query rows have no provider/model columns and therefore form a NULL group
// when those dimensions are requested.
func (repository SpendSummaryRepository) ListSpendSummary(ctx context.Context, options SpendSummaryListOptions) (control.SpendSummaryResult, error) {
	var result control.SpendSummaryResult
	if err := repository.validate(); err != nil {
		return result, err
	}
	if err := normalizeSpendSummaryOptions(&options); err != nil {
		return result, err
	}
	operations, err := repository.Namespace.Render("operations")
	if err != nil {
		return result, err
	}
	executions, err := repository.Namespace.Render("query_executions")
	if err != nil {
		return result, err
	}
	attempts, err := repository.Namespace.Render("operation_attempts")
	if err != nil {
		return result, err
	}
	query := spendSummaryQuery(operations, attempts, executions, options.GroupBy)
	rows, err := repository.Pool.Query(ctx, query, options.ScopeID, options.StartTime, options.EndTime, nullableOperationKinds(options.OperationKinds))
	if err != nil {
		return result, redactPostgresError(fmt.Errorf("list spend summary: %w", err))
	}
	defer rows.Close()
	result.StartTime, result.EndTime = options.StartTime, options.EndTime
	for rows.Next() {
		var operationKind, provider, model *string
		var knownCost *string
		var exactCount, unknownCount int64
		if err := rows.Scan(&operationKind, &provider, &model, &knownCost, &exactCount, &unknownCount); err != nil {
			return control.SpendSummaryResult{}, redactPostgresError(fmt.Errorf("scan spend summary: %w", err))
		}
		// An ungrouped aggregate always represents the requested interval,
		// including an empty interval. Preserve that global zero bucket so
		// callers can distinguish "no spend" from a missing result. Grouped
		// aggregates have no rows when there are no matching ledger entries.
		if exactCount == 0 && unknownCount == 0 && len(options.GroupBy) > 0 {
			continue
		}
		// SUM over an empty exact-cost set is NULL. Keep that distinction at
		// the SQL boundary instead of coalescing it to zero: an unknown-only
		// bucket must remain visibly unknown, while a global empty interval can
		// still be represented as an exact zero in the typed result.
		knownCostText := "0"
		if knownCost != nil {
			knownCostText = *knownCost
		} else if exactCount != 0 {
			return control.SpendSummaryResult{}, errors.New("PostgreSQL spend summary exact count has no aggregate cost")
		}
		usd, err := pricing.ParseUSD(knownCostText)
		if err != nil {
			return control.SpendSummaryResult{}, fmt.Errorf("PostgreSQL spend summary amount is invalid: %w", err)
		}
		bucket := control.SpendBucket{KnownActualCostUSD: control.DecimalUSD(usd.String()), ExactOperationCount: exactCount, UnknownOperationCount: unknownCount, Completeness: "complete"}
		if unknownCount > 0 {
			bucket.Completeness = "partial"
		}
		if operationKind != nil || provider != nil || model != nil {
			group := &control.SpendGroup{}
			if operationKind != nil {
				value := control.OperationKind(*operationKind)
				group.OperationKind = &value
			}
			if provider != nil {
				value := control.ProviderID(*provider)
				group.Provider = &value
			}
			if model != nil {
				value := control.ProviderModelID(*model)
				group.Model = &value
			}
			bucket.Group = group
		}
		result.Buckets = append(result.Buckets, bucket)
	}
	if err := rows.Err(); err != nil {
		return control.SpendSummaryResult{}, redactPostgresError(fmt.Errorf("read spend summary: %w", err))
	}
	return result, nil
}

func nullableOperationKinds(kinds []control.OperationKind) []string {
	if len(kinds) == 0 {
		return nil
	}
	values := make([]string, len(kinds))
	for index, kind := range kinds {
		values[index] = string(kind)
	}
	return values
}

func spendSummaryQuery(operations, attempts, executions string, groupBy []control.SpendDimension) string {
	selectColumns := []string{"NULL::text", "NULL::text", "NULL::text"}
	groupColumns := make([]string, 0, len(groupBy))
	orderColumns := make([]string, 0, len(groupBy))
	for _, dimension := range groupBy {
		index := 0
		switch dimension {
		case control.SpendByOperation:
			index = 0
		case control.SpendByProvider:
			index = 1
		case control.SpendByModel:
			index = 2
		}
		column := []string{"operation_kind", "provider", "model"}[index]
		selectColumns[index] = column
		groupColumns = append(groupColumns, column)
		orderColumns = append(orderColumns, column+" ASC NULLS FIRST")
	}
	groupSQL := ""
	if len(groupColumns) > 0 {
		groupSQL = " GROUP BY " + strings.Join(groupColumns, ", ")
	}
	orderSQL := ""
	if len(orderColumns) > 0 {
		orderSQL = " ORDER BY " + strings.Join(orderColumns, ", ")
	}
	return "WITH cost_rows AS (" +
		"SELECT o.operation_kind, NULLIF(final_attempt.provider, 'unknown') AS provider, NULLIF(final_attempt.resolved_model, 'unknown') AS model, o.actual_cost_usd, o.cost_status, o.completed_at FROM " + operations + " o LEFT JOIN LATERAL (SELECT provider, resolved_model FROM " + attempts + " WHERE operation_id = o.operation_id ORDER BY attempt_number DESC LIMIT 1) final_attempt ON TRUE WHERE o.scope_id = $1 AND o.state = 'completed' AND o.completed_at >= $2 AND o.completed_at < $3 " +
		"UNION ALL SELECT 'query'::text, NULL::text, NULL::text, actual_cost_usd, cost_status, completed_at FROM " + executions + " WHERE scope_id = $1 AND completed_at >= $2 AND completed_at < $3" +
		") SELECT " + strings.Join(selectColumns, ", ") + ", SUM(actual_cost_usd) FILTER (WHERE cost_status = 'exact')::text, COUNT(*) FILTER (WHERE cost_status = 'exact'), COUNT(*) FILTER (WHERE cost_status = 'unknown') FROM cost_rows WHERE ($4::text[] IS NULL OR operation_kind = ANY($4::text[]))" + groupSQL + orderSQL
}
