package postgres

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/maintenance"
)

func validRetentionBatchOptions() RetentionBatchOptions {
	now := time.Date(2026, 7, 29, 0, 0, 0, 0, time.UTC)
	return RetentionBatchOptions{
		Now:                          now,
		Limit:                        100,
		CacheUnusedBefore:            now.Add(-24 * time.Hour),
		ProviderStatusExpiresBefore:  now.Add(-48 * time.Hour),
		InventoryExpiresBefore:       now.Add(-48 * time.Hour),
		QueryExecutionsExpiresBefore: now.Add(-48 * time.Hour),
		OperationsExpiresBefore:      now.Add(-72 * time.Hour),
		BudgetBucketsBefore:          now.Add(-8 * 24 * time.Hour),
		CheckpointsExpiresBefore:     now.Add(-72 * time.Hour),
		MaxBudgetWindow:              7 * 24 * time.Hour,
	}
}

func TestRunRetentionBatchOrdersAllPasses(t *testing.T) {
	store := &retentionBatchFake{}
	result, err := runRetentionBatch(context.Background(), store, validRetentionBatchOptions())
	if err != nil {
		t.Fatalf("run retention batch: %v", err)
	}
	want := []string{"cache", "provider_status", "inventory", "query_executions", "operations", "budget_buckets", "checkpoints"}
	if !reflect.DeepEqual(store.calls, want) {
		t.Fatalf("pass order = %#v, want %#v", store.calls, want)
	}
	if len(result.Passes) != len(want) {
		t.Fatalf("pass count = %d, want %d", len(result.Passes), len(want))
	}
	for _, pass := range result.Passes {
		if pass.Err != nil {
			t.Fatalf("pass %s returned error: %v", pass.Name, pass.Err)
		}
		if pass.Result.Examined != 1 {
			t.Fatalf("pass %s result = %#v, want one examined row", pass.Name, pass.Result)
		}
	}
}

func TestRunRetentionBatchStopsAfterFirstFailure(t *testing.T) {
	store := &retentionBatchFake{fail: "inventory"}
	result, err := runRetentionBatch(context.Background(), store, validRetentionBatchOptions())
	if err == nil || !strings.Contains(err.Error(), "retention inventory") {
		t.Fatalf("batch error = %v, want inventory failure", err)
	}
	want := []string{"cache", "provider_status", "inventory"}
	if !reflect.DeepEqual(store.calls, want) {
		t.Fatalf("calls after failure = %#v, want %#v", store.calls, want)
	}
	if len(result.Passes) != len(want) || result.Passes[len(result.Passes)-1].Err == nil {
		t.Fatalf("failed pass was not retained: %#v", result.Passes)
	}
}

func TestRunRetentionBatchStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &retentionBatchFake{cancelAfter: "cache", cancel: cancel}
	result, err := runRetentionBatch(ctx, store, validRetentionBatchOptions())
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("batch error = %v, want context cancellation", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"cache"}) {
		t.Fatalf("calls after cancellation = %#v, want cache only", store.calls)
	}
	if len(result.Passes) != 2 || result.Passes[1].Name != "provider_status" || !errors.Is(result.Passes[1].Err, context.Canceled) {
		t.Fatalf("cancellation pass = %#v", result.Passes)
	}
}

func TestRunRetentionBatchRejectsTypedNilStore(t *testing.T) {
	var store *retentionBatchFake
	if _, err := runRetentionBatch(context.Background(), store, validRetentionBatchOptions()); err == nil {
		t.Fatal("typed nil store accepted")
	}
}

func TestRetentionBatchOptionsRejectUnsafeBounds(t *testing.T) {
	cases := []struct {
		name string
		edit func(*RetentionBatchOptions)
	}{
		{"non-UTC now", func(options *RetentionBatchOptions) {
			options.Now = options.Now.In(time.FixedZone("local", 3600))
		}},
		{"non-UTC cutoff", func(options *RetentionBatchOptions) {
			options.InventoryExpiresBefore = options.InventoryExpiresBefore.In(time.FixedZone("local", 3600))
		}},
		{"missing cutoff", func(options *RetentionBatchOptions) {
			options.CheckpointsExpiresBefore = time.Time{}
		}},
		{"limit too large", func(options *RetentionBatchOptions) {
			options.Limit = maxMaintenanceBatch + 1
		}},
		{"budget window missing", func(options *RetentionBatchOptions) {
			options.MaxBudgetWindow = 0
		}},
		{"budget cutoff too new", func(options *RetentionBatchOptions) {
			options.BudgetBucketsBefore = options.Now.Add(-time.Hour)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			options := validRetentionBatchOptions()
			tc.edit(&options)
			store := &retentionBatchFake{}
			if _, err := runRetentionBatch(context.Background(), store, options); err == nil {
				t.Fatal("unsafe retention options accepted")
			}
			if len(store.calls) != 0 {
				t.Fatalf("store called before option validation: %#v", store.calls)
			}
		})
	}
}

type retentionBatchFake struct {
	calls       []string
	fail        string
	cancelAfter string
	cancel      context.CancelFunc
}

func (store *retentionBatchFake) call(name string) (maintenance.RetentionResult, error) {
	store.calls = append(store.calls, name)
	if name == store.cancelAfter && store.cancel != nil {
		store.cancel()
	}
	if name == store.fail {
		return maintenance.RetentionResult{}, errors.New("injected maintenance failure")
	}
	return maintenance.RetentionResult{Examined: 1}, nil
}

func (store *retentionBatchFake) PruneExpiredCache(context.Context, time.Time, time.Time, int) (maintenance.RetentionResult, error) {
	return store.call("cache")
}
func (store *retentionBatchFake) PruneExpiredProviderStatus(context.Context, time.Time, time.Time, int) (maintenance.RetentionResult, error) {
	return store.call("provider_status")
}
func (store *retentionBatchFake) PruneExpiredInventory(context.Context, time.Time, time.Time, int) (maintenance.RetentionResult, error) {
	return store.call("inventory")
}
func (store *retentionBatchFake) PruneExpiredQueryExecutions(context.Context, time.Time, time.Time, int) (maintenance.RetentionResult, error) {
	return store.call("query_executions")
}
func (store *retentionBatchFake) PruneExpiredOperations(context.Context, time.Time, time.Time, int) (maintenance.RetentionResult, error) {
	return store.call("operations")
}
func (store *retentionBatchFake) PruneExpiredBudgetBuckets(context.Context, time.Time, time.Time, time.Duration, int) (maintenance.RetentionResult, error) {
	return store.call("budget_buckets")
}
func (store *retentionBatchFake) PruneExpiredCheckpoints(context.Context, time.Time, time.Time, int) (maintenance.RetentionResult, error) {
	return store.call("checkpoints")
}

var _ RetentionBatchStore = (*retentionBatchFake)(nil)
var _ RetentionBatchStore = MaintenanceRepository{}
