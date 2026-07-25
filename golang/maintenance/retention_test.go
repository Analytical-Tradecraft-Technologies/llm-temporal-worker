package maintenance

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestInMemoryRetentionProtectsActiveAndReferencedRows(t *testing.T) {
	now := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	store, err := NewInMemoryRetention([]RetentionRecord{
		{ID: "cache-old", Kind: ResourceCache, State: "ready", LastUsedAt: now.Add(-48 * time.Hour), ExpiresAt: now.Add(24 * time.Hour)},
		{ID: "cache-used", Kind: ResourceCache, State: "ready", LastUsedAt: now.Add(-2 * time.Hour), ExpiresAt: now.Add(-24 * time.Hour)},
		{ID: "cache-active", Kind: ResourceCache, State: "ready", LastUsedAt: now.Add(-48 * time.Hour), Active: true},
		{ID: "cache-child", Kind: ResourceCache, State: "ready", LastUsedAt: now.Add(-48 * time.Hour), HasRetainedDescendant: true},
		{ID: "cache-fill", Kind: ResourceCache, State: "ready", LastUsedAt: now.Add(-48 * time.Hour), HasActiveFill: true},
		{ID: "cache-use", Kind: ResourceCache, State: "ready", LastUsedAt: now.Add(-48 * time.Hour), HasActiveUse: true},
		{ID: "cache-blob", Kind: ResourceCache, State: "ready", LastUsedAt: now.Add(-48 * time.Hour), HasBlobReference: true},
		{ID: "operation-old", Kind: ResourceOperation, ExpiresAt: now.Add(-time.Hour)},
		{ID: "query-old", Kind: ResourceQueryExecution, ExpiresAt: now.Add(-time.Hour)},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Prune(context.Background(), RetentionPolicy{
		Now: now, Limit: 10, CacheUnusedBefore: now.Add(-24 * time.Hour), OperationExpiresBefore: now, QueryExecutionExpiresBefore: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Examined != 3 || result.Tombstoned != 1 || result.Deleted != 2 || result.Skipped != 0 {
		t.Fatalf("unexpected retention result: %+v", result)
	}
	records := store.Snapshot()
	if len(records) != 7 {
		t.Fatalf("expected protected rows plus tombstone, got %d", len(records))
	}
	for _, record := range records {
		if record.ID == "cache-old" && record.State != "tombstoned" {
			t.Fatalf("old cache was not tombstoned: %+v", record)
		}
		if record.ID == "cache-used" && record.State != "ready" {
			t.Fatalf("recently used cache was removed: %+v", record)
		}
		if record.ID == "cache-blob" && record.State != "ready" {
			t.Fatalf("cache with a retained blob reference was removed: %+v", record)
		}
	}
}

func TestInMemoryRetentionIsBoundedAndHonorsCancellation(t *testing.T) {
	now := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	store, err := NewInMemoryRetention([]RetentionRecord{
		{ID: "a", Kind: ResourceStatus, ExpiresAt: now.Add(-3 * time.Hour)},
		{ID: "b", Kind: ResourceStatus, ExpiresAt: now.Add(-2 * time.Hour)},
		{ID: "c", Kind: ResourceStatus, ExpiresAt: now.Add(-time.Hour)},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Prune(context.Background(), RetentionPolicy{Now: now, Limit: 2, StatusExpiresBefore: now})
	if err != nil {
		t.Fatal(err)
	}
	if result.Deleted != 2 || len(store.Snapshot()) != 1 {
		t.Fatalf("retention exceeded its batch limit: result=%+v rows=%d", result, len(store.Snapshot()))
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Prune(canceled, RetentionPolicy{Now: now, Limit: 1, StatusExpiresBefore: now}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func TestRetentionPolicyRejectsFutureCutoff(t *testing.T) {
	now := time.Now().UTC()
	if err := (RetentionPolicy{Now: now, Limit: 1, StatusExpiresBefore: now.Add(time.Second)}).Validate(); err == nil {
		t.Fatal("future retention cutoff accepted")
	}
}
