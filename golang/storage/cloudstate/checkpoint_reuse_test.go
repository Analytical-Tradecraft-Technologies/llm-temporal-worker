package cloudstate

import (
	"context"
	"reflect"
	"strings"
	"testing"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
)

// A committed checkpoint never changes, so one invocation reads it once
// (#1782). Only a found, validated read enters reuse; a miss, another
// invocation and another repository read storage again.
func TestCheckpointReuseWithinInvocation(t *testing.T) {
	r, table, blobs, cp := checkpointFixture(t)
	ctx := WithRecordReuse(context.Background())
	store := r.Checkpoints()
	testassert.ErrorIs(t, testassert.Err(store.Get(ctx, cp.ScopeID, cp.ID)), contracts.ErrNotFound, "unpublished checkpoint")
	mustPublishCheckpoint(t, store, cp)
	want, err := store.Get(context.Background(), cp.ScopeID, cp.ID)
	testassert.NoError(t, err)
	opens := 0
	blobs.open = func(blob.BlobKey) error { opens++; return nil }
	first, err := store.Get(ctx, cp.ScopeID, cp.ID)
	if err != nil || opens != 1 {
		t.Fatalf("a miss was reused, or the first read did not open its blob: opens=%d err=%v", opens, err)
	}
	first.OriginOperationID = "changed"
	// Remove every pointer row and blob: this invocation keeps its validated
	// read, and nothing else may see it.
	table.mu.Lock()
	for key := range table.rows {
		if strings.Contains(key.PartitionKey, "/checkpoint/") {
			delete(table.rows, key)
		}
	}
	table.mu.Unlock()
	blobs.mu.Lock()
	clear(blobs.values)
	blobs.mu.Unlock()
	second, err := store.Get(ctx, cp.ScopeID, cp.ID)
	if err != nil || !reflect.DeepEqual(second, want) || opens != 1 {
		t.Fatalf("reuse lost isolation or read again: opens=%d err=%v", opens, err)
	}
	testassert.ErrorIs(t, testassert.Err(store.Get(ctx, "another-scope", cp.ID)), contracts.ErrNotFound, "scope bypass")
	for name, read := range map[string]func() error{
		"new invocation":     func() error { _, err := store.Get(WithRecordReuse(ctx), cp.ScopeID, cp.ID); return err },
		"plain context":      func() error { _, err := store.Get(context.Background(), cp.ScopeID, cp.ID); return err },
		"another repository": func() error { _, err := reopen(t, table, blobs).Checkpoints().Get(ctx, cp.ScopeID, cp.ID); return err },
	} {
		t.Run(name, func(t *testing.T) {
			testassert.ErrorIs(t, read(), contracts.ErrNotFound, "removed checkpoint was visible")
		})
	}
}

// Payload keys are keyed digests of their plaintext, so a payload read once
// is reused within the invocation. A payload this invocation wrote is read
// from storage the first time, as proof it is readable.
func TestPayloadReuseOnlyAfterRead(t *testing.T) {
	r, table, blobs, cp := checkpointFixture(t)
	ctx := WithRecordReuse(context.Background())
	store := r.Checkpoints()
	opens := 0
	blobs.open = func(blob.BlobKey) error { opens++; return nil }
	written, err := store.Write(ctx, cp.ScopeID, []byte(`{"fresh":true}`), "application/json")
	testassert.NoError(t, err)
	for range 2 {
		if data, err := store.Read(ctx, cp.ScopeID, written); err != nil || string(data) != `{"fresh":true}` {
			t.Fatalf("%s %v", data, err)
		}
	}
	if opens != 1 {
		t.Fatalf("written payload opened %d times, want once", opens)
	}
	data, err := store.Read(ctx, cp.ScopeID, written)
	testassert.NoError(t, err)
	data[0] = '!'
	if again, err := store.Read(ctx, cp.ScopeID, written); err != nil || again[0] != '{' {
		t.Fatalf("reused payload aliases a caller's copy: %s %v", again, err)
	}
	blobs.mu.Lock()
	clear(blobs.values)
	blobs.mu.Unlock()
	testassert.NoError(t, testassert.Err(store.Read(ctx, cp.ScopeID, written)), "validated payload was not reused")
	testassert.ErrorIs(t, testassert.Err(store.Read(WithRecordReuse(ctx), cp.ScopeID, written)), ErrCorrupt, "lost payload hidden from a new invocation")
	testassert.ErrorIs(t, testassert.Err(reopen(t, table, blobs).Checkpoints().Read(ctx, cp.ScopeID, written)), ErrCorrupt, "lost payload hidden from another repository")
}
