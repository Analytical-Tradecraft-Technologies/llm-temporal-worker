package postgres

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/maintenance"
)

// RetentionBatchOptions is the explicit input for one bounded maintenance
// invocation. All timestamps must be UTC so a retry or an operator replay
// evaluates the same cutoff, regardless of the host timezone.
type RetentionBatchOptions struct {
	Now                          time.Time
	Limit                        int
	CacheUnusedBefore            time.Time
	ProviderStatusExpiresBefore  time.Time
	InventoryExpiresBefore       time.Time
	QueryExecutionsExpiresBefore time.Time
	OperationsExpiresBefore      time.Time
	BudgetBucketsBefore          time.Time
	CheckpointsExpiresBefore     time.Time
	MaxBudgetWindow              time.Duration
}

func (options RetentionBatchOptions) Validate() error {
	if options.Now.IsZero() {
		return errors.New("retention batch time is required")
	}
	if options.Now.Location() != time.UTC {
		return errors.New("retention batch time must be UTC")
	}
	if options.Limit <= 0 || options.Limit > maxMaintenanceBatch {
		return fmt.Errorf("retention batch limit must be between 1 and %d", maxMaintenanceBatch)
	}
	cutoffs := []struct {
		name   string
		cutoff time.Time
	}{
		{name: "cache", cutoff: options.CacheUnusedBefore},
		{name: "provider status", cutoff: options.ProviderStatusExpiresBefore},
		{name: "inventory", cutoff: options.InventoryExpiresBefore},
		{name: "query executions", cutoff: options.QueryExecutionsExpiresBefore},
		{name: "operations", cutoff: options.OperationsExpiresBefore},
		{name: "budget buckets", cutoff: options.BudgetBucketsBefore},
		{name: "checkpoints", cutoff: options.CheckpointsExpiresBefore},
	}
	for _, entry := range cutoffs {
		name, cutoff := entry.name, entry.cutoff
		if cutoff.IsZero() {
			return fmt.Errorf("retention %s cutoff is required", name)
		}
		if cutoff.Location() != time.UTC {
			return fmt.Errorf("retention %s cutoff must be UTC", name)
		}
		if cutoff.After(options.Now) {
			return fmt.Errorf("retention %s cutoff must not be after maintenance time", name)
		}
	}
	if options.MaxBudgetWindow <= 0 {
		return errors.New("retention maximum budget window must be positive")
	}
	if options.BudgetBucketsBefore.After(options.Now.Add(-options.MaxBudgetWindow)) {
		return errors.New("retention budget bucket cutoff is newer than the maximum window horizon")
	}
	return nil
}

// RetentionBatchPass records one bounded pass. A failed pass is retained in
// the result before RunRetentionBatch returns, so callers can safely emit
// progress metrics without parsing an error string.
type RetentionBatchPass struct {
	Name   string
	Result maintenance.RetentionResult
	Err    error
}

// RetentionBatchResult contains only the passes reached by the invocation.
// The runner stops at the first failed pass or context cancellation; later
// resources are never silently skipped as if they had succeeded.
type RetentionBatchResult struct {
	Passes []RetentionBatchPass
}

// RetentionBatchStore is the bounded PostgreSQL maintenance surface consumed
// by the orchestration helper. Keeping this interface narrow makes ordering,
// fail-stop, and cancellation tests independent of a live database.
type RetentionBatchStore interface {
	PruneExpiredCache(context.Context, time.Time, time.Time, int) (maintenance.RetentionResult, error)
	PruneExpiredProviderStatus(context.Context, time.Time, time.Time, int) (maintenance.RetentionResult, error)
	PruneExpiredInventory(context.Context, time.Time, time.Time, int) (maintenance.RetentionResult, error)
	PruneExpiredQueryExecutions(context.Context, time.Time, time.Time, int) (maintenance.RetentionResult, error)
	PruneExpiredOperations(context.Context, time.Time, time.Time, int) (maintenance.RetentionResult, error)
	PruneExpiredBudgetBuckets(context.Context, time.Time, time.Time, time.Duration, int) (maintenance.RetentionResult, error)
	PruneExpiredCheckpoints(context.Context, time.Time, time.Time, int) (maintenance.RetentionResult, error)
}

// RunRetentionBatch executes all bounded retention passes with one explicit
// UTC cutoff snapshot. MaintenanceRepository already owns the dedicated
// PostgreSQL maintenance-role observer, so each pass emits its normal bounded
// progress and failure metrics while this method supplies fail-stop control.
func (repository MaintenanceRepository) RunRetentionBatch(ctx context.Context, options RetentionBatchOptions) (RetentionBatchResult, error) {
	return runRetentionBatch(ctx, repository, options)
}

func runRetentionBatch(ctx context.Context, store RetentionBatchStore, options RetentionBatchOptions) (RetentionBatchResult, error) {
	var result RetentionBatchResult
	if ctx == nil {
		return result, errors.New("retention batch context is nil")
	}
	if isNilRetentionBatchStore(store) {
		return result, errors.New("retention batch store is nil")
	}
	if err := options.Validate(); err != nil {
		return result, err
	}
	passes := []struct {
		name string
		run  func(context.Context) (maintenance.RetentionResult, error)
	}{
		{"cache", func(ctx context.Context) (maintenance.RetentionResult, error) {
			return store.PruneExpiredCache(ctx, options.Now, options.CacheUnusedBefore, options.Limit)
		}},
		{"provider_status", func(ctx context.Context) (maintenance.RetentionResult, error) {
			return store.PruneExpiredProviderStatus(ctx, options.Now, options.ProviderStatusExpiresBefore, options.Limit)
		}},
		{"inventory", func(ctx context.Context) (maintenance.RetentionResult, error) {
			return store.PruneExpiredInventory(ctx, options.Now, options.InventoryExpiresBefore, options.Limit)
		}},
		{"query_executions", func(ctx context.Context) (maintenance.RetentionResult, error) {
			return store.PruneExpiredQueryExecutions(ctx, options.Now, options.QueryExecutionsExpiresBefore, options.Limit)
		}},
		{"operations", func(ctx context.Context) (maintenance.RetentionResult, error) {
			return store.PruneExpiredOperations(ctx, options.Now, options.OperationsExpiresBefore, options.Limit)
		}},
		{"budget_buckets", func(ctx context.Context) (maintenance.RetentionResult, error) {
			return store.PruneExpiredBudgetBuckets(ctx, options.Now, options.BudgetBucketsBefore, options.MaxBudgetWindow, options.Limit)
		}},
		{"checkpoints", func(ctx context.Context) (maintenance.RetentionResult, error) {
			return store.PruneExpiredCheckpoints(ctx, options.Now, options.CheckpointsExpiresBefore, options.Limit)
		}},
	}
	for _, pass := range passes {
		if err := ctx.Err(); err != nil {
			result.Passes = append(result.Passes, RetentionBatchPass{Name: pass.name, Err: err})
			return result, fmt.Errorf("retention %s: %w", pass.name, err)
		}
		passResult, err := pass.run(ctx)
		result.Passes = append(result.Passes, RetentionBatchPass{Name: pass.name, Result: passResult, Err: err})
		if err != nil {
			return result, fmt.Errorf("retention %s: %w", pass.name, err)
		}
	}
	return result, nil
}

// Interfaces can contain a typed nil pointer. Treat that as absent before the
// first pass so a miswired maintenance process fails closed instead of
// panicking while invoking a method on a nil repository.
func isNilRetentionBatchStore(store RetentionBatchStore) bool {
	if store == nil {
		return true
	}
	value := reflect.ValueOf(store)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
