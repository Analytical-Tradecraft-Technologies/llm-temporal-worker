package cloudstate

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
)

// A committed checkpoint never changes, so one invocation reads it once
// (#1782). Only a found, validated read enters reuse; a miss, another
// invocation and another repository read storage again.
func TestCheckpointReuseWithinInvocation(t *testing.T) {
	r, table, blobs, cp := checkpointFixture(t)
	ctx := WithRecordReuse(context.Background())
	store := r.Checkpoints()
	if _, err := store.Get(ctx, cp.ScopeID, cp.ID); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("unpublished checkpoint: %v", err)
	}
	if err := publishCheckpoint(context.Background(), store, cp); err != nil {
		t.Fatal(err)
	}
	want, err := store.Get(context.Background(), cp.ScopeID, cp.ID)
	if err != nil {
		t.Fatal(err)
	}
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
	if _, err := store.Get(ctx, "another-scope", cp.ID); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("scope bypass: %v", err)
	}
	for name, read := range map[string]func() error{
		"new invocation":     func() error { _, err := store.Get(WithRecordReuse(ctx), cp.ScopeID, cp.ID); return err },
		"plain context":      func() error { _, err := store.Get(context.Background(), cp.ScopeID, cp.ID); return err },
		"another repository": func() error { _, err := reopen(t, table, blobs).Checkpoints().Get(ctx, cp.ScopeID, cp.ID); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := read(); !errors.Is(err, contracts.ErrNotFound) {
				t.Fatalf("removed checkpoint was visible: %v", err)
			}
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
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if data, err := store.Read(ctx, cp.ScopeID, written); err != nil || string(data) != `{"fresh":true}` {
			t.Fatalf("%s %v", data, err)
		}
	}
	if opens != 1 {
		t.Fatalf("written payload opened %d times, want once", opens)
	}
	data, err := store.Read(ctx, cp.ScopeID, written)
	if err != nil {
		t.Fatal(err)
	}
	data[0] = '!'
	if again, err := store.Read(ctx, cp.ScopeID, written); err != nil || again[0] != '{' {
		t.Fatalf("reused payload aliases a caller's copy: %s %v", again, err)
	}
	blobs.mu.Lock()
	clear(blobs.values)
	blobs.mu.Unlock()
	if _, err := store.Read(ctx, cp.ScopeID, written); err != nil {
		t.Fatalf("validated payload was not reused: %v", err)
	}
	if _, err := store.Read(WithRecordReuse(ctx), cp.ScopeID, written); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("lost payload hidden from a new invocation: %v", err)
	}
	if _, err := reopen(t, table, blobs).Checkpoints().Read(ctx, cp.ScopeID, written); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("lost payload hidden from another repository: %v", err)
	}
}
