package cloudstate

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

func TestCloudCacheFinalizationRetriesPublicationSettlementAndFillReceipt(t *testing.T) {
	r, table, blobs, entry, _ := responseCacheFixture(t)
	ctx, lease := context.Background(), fillLease(entry)
	mustAcquireFill(t, r.ResponseFills(), lease, cache.FillOwned)
	mustStartFill(t, r.ResponseFills(), lease, lease.AcquiredAt)
	completion := cache.FillCompletion{Outcome: cache.FillPublished, EntryID: entry.ID, CompletedAt: entry.CompletedAt}
	newFinalizer := func() *durable.ResponseCache {
		t.Helper()
		opened := reopen(t, table, blobs)
		result, err := durable.NewResponseCache(opened.Responses(), opened.ResponseFills(), func() time.Time { return completion.CompletedAt })
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	settlements := 0
	settle := func(context.Context) error { settlements++; return nil }
	table.hook = func(_ string, item kv.KeyValueItem) (error, error) {
		if strings.Contains(item.PartitionKey, "/cache/success/") {
			return contracts.ErrUnavailable, nil
		}
		return nil, nil
	}
	if err := newFinalizer().CompleteAttempt(ctx, lease, completion, &entry, settle); !errors.Is(err, contracts.ErrUnavailable) || settlements != 0 {
		t.Fatalf("published before visibility: %v settlements=%d", err, settlements)
	}
	table.hook = nil
	settlementFailure := errors.New("Redis settlement temporarily unavailable")
	if err := newFinalizer().CompleteAttempt(ctx, lease, completion, &entry, func(context.Context) error {
		settlements++
		return settlementFailure
	}); !errors.Is(err, settlementFailure) {
		t.Fatal(err)
	}
	if hit := lookupResponse(t, r, entry.Key, entry.CompletedAt); hit == nil || hit.ID != entry.ID {
		t.Fatal("committed success not visible after settlement failure")
	}
	mustAcquireFill(t, r.ResponseFills(), lease, cache.FillRecoveryNeeded)
	table.hook = func(op string, item kv.KeyValueItem) (error, error) {
		if op == "replace" && strings.Contains(item.PartitionKey, "/cache/fill/") {
			return nil, contracts.ErrOutcomeUnknown
		}
		return nil, nil
	}
	if err := newFinalizer().CompleteAttempt(ctx, lease, completion, &entry, settle); !errors.Is(err, contracts.ErrOutcomeUnknown) {
		t.Fatal(err)
	}
	table.hook = nil
	if err := newFinalizer().CompleteAttempt(ctx, lease, completion, &entry, settle); err != nil {
		t.Fatalf("restart completion: %v", err)
	}
	mustAcquireFill(t, r.ResponseFills(), lease, cache.FillAttemptFinished)
	if settlements != 3 {
		t.Fatalf("settlement retries=%d", settlements)
	}
}

func TestCloudCacheFinalizationUnknownOutcomeDoesNotInvalidateSuccess(t *testing.T) {
	r, table, blobs, entry, _ := responseCacheFixture(t)
	ctx := context.Background()
	if err := r.Responses().Publish(ctx, entry); err != nil {
		t.Fatal(err)
	}
	next := fillLease(entry)
	next.Attempt = "unknown-attempt"
	next.OperationID = "unknown-operation"
	next.AcquiredAt = entry.CompletedAt.Add(time.Second)
	next.ExpiresAt = next.AcquiredAt.Add(time.Minute)
	mustAcquireFill(t, r.ResponseFills(), next, cache.FillOwned)
	mustStartFill(t, r.ResponseFills(), next, next.AcquiredAt)
	finalizer, err := durable.NewResponseCache(reopen(t, table, blobs).Responses(), reopen(t, table, blobs).ResponseFills(), func() time.Time { return next.ExpiresAt })
	if err != nil {
		t.Fatal(err)
	}
	settled := false
	completion := cache.FillCompletion{Outcome: cache.FillUnknown, CompletedAt: next.ExpiresAt}
	if err := finalizer.CompleteAttempt(ctx, next, completion, nil, func(context.Context) error {
		settled = true // Charge the original unknown attempt; no refund.
		return nil
	}); err != nil || !settled {
		t.Fatalf("unknown outcome: %v settled=%t", err, settled)
	}
	if hit := lookupResponse(t, r, entry.Key, next.ExpiresAt); hit == nil || hit.ID != entry.ID {
		t.Fatal("unknown outcome displaced older success")
	}
	mustAcquireFill(t, r.ResponseFills(), next, cache.FillAttemptFinished)
	if err := finalizer.CompleteAttempt(ctx, next, completion, &entry, func(context.Context) error { return nil }); err == nil {
		t.Fatal("accepted cache entry for unknown outcome")
	}
}

func TestCloudCacheFinalizationUseRequiresCommittedConsumer(t *testing.T) {
	r, table, blobs, entry, origin := responseCacheFixture(t)
	ctx := context.Background()
	if err := r.Responses().Publish(ctx, entry); err != nil {
		t.Fatal(err)
	}
	child := childCheckpoint(origin, "consumer")
	child.Kind, child.OriginCacheEntryID = state.CheckpointCacheReplay, &entry.ID
	use := cache.ResponseUse{ScopeID: origin.ScopeID, OperationID: child.OriginOperationID, EntryID: entry.ID, CheckpointID: child.ID, CompletedAt: entry.CompletedAt.Add(time.Hour)}
	finalizer, err := durable.NewResponseCache(r.Responses(), r.ResponseFills(), func() time.Time { return use.CompletedAt })
	if err != nil {
		t.Fatal(err)
	}
	if err := finalizer.RecordUse(ctx, entry, use); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("uncommitted consumer counted: %v", err)
	}
	if err := publishCheckpoint(ctx, r.Checkpoints(), child); err != nil {
		t.Fatal(err)
	}
	table.hook = func(_ string, item kv.KeyValueItem) (error, error) {
		if strings.Contains(item.PartitionKey, "/cache/use/") {
			return nil, contracts.ErrOutcomeUnknown
		}
		return nil, nil
	}
	if err := finalizer.RecordUse(ctx, entry, use); !errors.Is(err, contracts.ErrOutcomeUnknown) {
		t.Fatal(err)
	}
	table.hook = nil
	finalizer, err = durable.NewResponseCache(reopen(t, table, blobs).Responses(), reopen(t, table, blobs).ResponseFills(), func() time.Time { return use.CompletedAt })
	if err != nil {
		t.Fatal(err)
	}
	if err := finalizer.RecordUse(ctx, entry, use); err != nil {
		t.Fatalf("restart use: %v", err)
	}
	got, err := r.Responses().ReadUse(ctx, use.ScopeID, use.OperationID)
	if err != nil || got != use {
		t.Fatalf("use receipt=%+v error=%v", got, err)
	}
	bad := use
	bad.OperationID = origin.OriginOperationID
	if err := finalizer.RecordUse(ctx, entry, bad); err == nil {
		t.Fatal("origin operation used its own cache entry")
	}
}
