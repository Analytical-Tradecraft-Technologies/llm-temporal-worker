package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

func handoffFixture(t *testing.T) (*Repository, *memoryTable, *memoryBlobs, Record, state.DurableCheckpoint, FinalizationHandoff) {
	t.Helper()
	r, table, blobs, checkpoint := checkpointFixture(t)
	op := Operation{Scope: Scope{Tenant: "private-tenant", Project: "private-project"}, Kind: "generate", Key: string(checkpoint.OriginOperationID), Manifest: json.RawMessage(`{"version":1,"operation_key":"private-operation"}`), Now: checkpoint.CreatedAt}
	record, err := r.BeginOperation(context.Background(), op)
	if err != nil {
		t.Fatal(err)
	}
	handoff := FinalizationHandoff{Mode: "provider", CheckpointScope: checkpoint.ScopeID, CheckpointID: checkpoint.ID, Payload: json.RawMessage(`{"version":1,"response":"private answer","budget_generation":"generation-1"}`)}
	return r, table, blobs, record, checkpoint, handoff
}

func TestFinalizationHandoffRequiresCommittedMatchingCheckpoint(t *testing.T) {
	r, _, _, record, checkpoint, handoff := handoffFixture(t)
	ctx := context.Background()
	if err := r.SaveFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID, handoff, checkpoint.CreatedAt); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("uncommitted checkpoint: %v", err)
	}
	if err := publishCheckpoint(ctx, r.Checkpoints(), checkpoint); err != nil {
		t.Fatal(err)
	}
	wrong := handoff
	wrong.CheckpointScope = "another-scope"
	if err := r.SaveFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID, wrong, checkpoint.CreatedAt); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("cross-scope checkpoint: %v", err)
	}
	other := childCheckpoint(checkpoint, "other-operation")
	if err := publishCheckpoint(ctx, r.Checkpoints(), other); err != nil {
		t.Fatal(err)
	}
	wrong = handoff
	wrong.CheckpointID = other.ID
	if err := r.SaveFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID, wrong, checkpoint.CreatedAt); !errors.Is(err, contracts.ErrConflict) {
		t.Fatalf("other operation checkpoint: %v", err)
	}
	if _, err := r.LoadFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("missing handoff: %v", err)
	}
}

func TestFinalizationHandoffRestartReplayAndImmutableValues(t *testing.T) {
	r, table, blobs, record, checkpoint, handoff := handoffFixture(t)
	ctx := context.Background()
	if err := publishCheckpoint(ctx, r.Checkpoints(), checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := r.SaveFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID, handoff, checkpoint.CreatedAt); err != nil {
		t.Fatal(err)
	}
	handoff, _ = normalizeHandoff(handoff)
	got, err := reopen(t, table, blobs).LoadFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID)
	if err != nil || !equalHandoff(got, handoff) {
		t.Fatalf("restart: %+v %v", got, err)
	}
	before, err := r.Read(ctx, record.Request.Scope, record.Request.ID)
	if err != nil || before.Revision != record.Revision+1 {
		t.Fatalf("handoff revision: %v %v", before.Revision, err)
	}
	if err := reopen(t, table, blobs).SaveFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID, handoff, checkpoint.CreatedAt.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	after, _ := r.Read(ctx, record.Request.Scope, record.Request.ID)
	if after.Revision != before.Revision {
		t.Fatal("identical retry appended another event")
	}
	changed := handoff
	changed.Payload = json.RawMessage(`{"version":1,"response":"other"}`)
	if err := r.SaveFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID, changed, checkpoint.CreatedAt); !errors.Is(err, contracts.ErrConflict) {
		t.Fatalf("different handoff replaced committed value: %v", err)
	}
	wrongScope := record.Request.Scope
	wrongScope.Tenant = "someone-else"
	if _, err := r.LoadFinalizationHandoff(ctx, wrongScope, record.Request.ID); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("cross-tenant load: %v", err)
	}
	// Progress is an in-process value, but the plaintext must not reach KV.
	for _, row := range table.rows {
		for _, field := range row.Item.Fields {
			if data, ok := field.(kv.KeyValueBytes); ok && strings.Contains(string(data), "private answer") {
				t.Fatal("handoff leaked in KV")
			}
		}
	}
	for _, data := range blobs.values {
		if strings.Contains(string(data), "private answer") {
			t.Fatal("handoff leaked in blob storage")
		}
	}
}

func TestFinalizationHandoffRecoversLostAcknowledgements(t *testing.T) {
	for _, stage := range []string{"event", "index"} {
		t.Run(stage, func(t *testing.T) {
			r, table, blobs, record, checkpoint, handoff := handoffFixture(t)
			ctx := context.Background()
			if err := publishCheckpoint(ctx, r.Checkpoints(), checkpoint); err != nil {
				t.Fatal(err)
			}
			handoff, _ = normalizeHandoff(handoff)
			fired := false
			fault := &contracts.StorageError{Kind: contracts.ErrOutcomeUnknown}
			table.hook = func(action string, item kv.KeyValueItem) (error, error) {
				if fired {
					return nil, nil
				}
				if stage == "event" && action == "create" && strings.Contains(item.PartitionKey, "/request/") || stage == "index" && action == "replace" && strings.Contains(item.PartitionKey, "/pending/") {
					fired = true
					return nil, fault
				}
				return nil, nil
			}
			if err := r.SaveFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID, handoff, checkpoint.CreatedAt); err == nil || !fired {
				t.Fatalf("lost %s acknowledgement: %v", stage, err)
			}
			table.hook = nil
			if err := reopen(t, table, blobs).SaveFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID, handoff, checkpoint.CreatedAt); err != nil {
				t.Fatalf("retry after %s: %v", stage, err)
			}
			got, err := reopen(t, table, blobs).LoadFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID)
			if err != nil || !equalHandoff(got, handoff) {
				t.Fatalf("replay after %s: %v", stage, err)
			}
		})
	}
}

func TestFinalizationHandoffConcurrentSameAndDifferentWrites(t *testing.T) {
	r, table, blobs, record, checkpoint, handoff := handoffFixture(t)
	ctx := context.Background()
	if err := publishCheckpoint(ctx, r.Checkpoints(), checkpoint); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errorsFound := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errorsFound <- reopen(t, table, blobs).SaveFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID, handoff, checkpoint.CreatedAt)
		}()
	}
	wg.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatalf("concurrent same handoff: %v", err)
		}
	}
	got, err := r.Read(ctx, record.Request.Scope, record.Request.ID)
	if err != nil || got.Revision != record.Revision+1 {
		t.Fatalf("concurrent revision: %v %v", got.Revision, err)
	}
}

func TestFinalizationHandoffValidation(t *testing.T) {
	r, _, _, record, checkpoint, handoff := handoffFixture(t)
	ctx := context.Background()
	if err := publishCheckpoint(ctx, r.Checkpoints(), checkpoint); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*FinalizationHandoff){
		func(h *FinalizationHandoff) { h.Mode = "" },
		func(h *FinalizationHandoff) { h.CheckpointID = "" },
		func(h *FinalizationHandoff) { h.Payload = json.RawMessage(`[]`) },
		func(h *FinalizationHandoff) { h.Payload = json.RawMessage(`{"version":2}`) },
	} {
		invalid := handoff
		mutate(&invalid)
		if err := r.SaveFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID, invalid, checkpoint.CreatedAt); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid handoff accepted: %v", err)
		}
	}
	if err := r.SaveFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID, handoff, checkpoint.CreatedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CompleteOperation(ctx, record.Request.Scope, record.Request.ID, json.RawMessage(`{"version":1,"response":{}}`), checkpoint.CreatedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := r.LoadFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID); !errors.Is(err, contracts.ErrConflict) {
		t.Fatalf("terminal handoff still actionable: %v", err)
	}
}

func TestFinalizationHandoffCacheAndCompactKinds(t *testing.T) {
	for _, variant := range []struct {
		kind, mode     string
		checkpointKind state.CheckpointKind
	}{
		{kind: "generate", mode: "cache", checkpointKind: state.CheckpointCacheReplay},
		{kind: "compact", mode: "provider", checkpointKind: state.CheckpointCompaction},
		{kind: "compact", mode: "cache", checkpointKind: state.CheckpointCacheReplay},
	} {
		t.Run(variant.kind+"-"+variant.mode, func(t *testing.T) {
			r, _, _, checkpoint := checkpointFixture(t)
			checkpoint.Kind = variant.checkpointKind
			if variant.mode == "cache" {
				entry := state.CacheEntryID("origin-cache-entry")
				checkpoint.OriginCacheEntryID = &entry
			}
			op := Operation{Scope: Scope{Tenant: "private-tenant", Project: "private-project"}, Kind: variant.kind, Key: string(checkpoint.OriginOperationID), Manifest: json.RawMessage(`{"operation_key":"private-operation"}`), Now: checkpoint.CreatedAt}
			record, err := r.BeginOperation(context.Background(), op)
			if err != nil {
				t.Fatal(err)
			}
			if err := publishCheckpoint(context.Background(), r.Checkpoints(), checkpoint); err != nil {
				t.Fatal(err)
			}
			handoff := FinalizationHandoff{Mode: variant.mode, CheckpointScope: checkpoint.ScopeID, CheckpointID: checkpoint.ID, Payload: json.RawMessage(`{"version":1}`)}
			if err := r.SaveFinalizationHandoff(context.Background(), op.Scope, record.Request.ID, handoff, checkpoint.CreatedAt); err != nil {
				t.Fatal(err)
			}
			got, err := r.LoadFinalizationHandoff(context.Background(), op.Scope, record.Request.ID)
			if err != nil || got.Mode != variant.mode {
				t.Fatalf("load: %+v %v", got, err)
			}
		})
	}
}

func TestFinalizationHandoffRejectsManifestOperationMismatch(t *testing.T) {
	r, _, _, checkpoint := checkpointFixture(t)
	ctx := context.Background()
	op := Operation{Scope: Scope{Tenant: "private-tenant", Project: "private-project"}, Kind: "generate", Key: "different-key", Manifest: json.RawMessage(`{"operation_key":"private-operation"}`), Now: checkpoint.CreatedAt}
	record, err := r.BeginOperation(ctx, op)
	if err != nil {
		t.Fatal(err)
	}
	if err := publishCheckpoint(ctx, r.Checkpoints(), checkpoint); err != nil {
		t.Fatal(err)
	}
	handoff := FinalizationHandoff{Mode: "provider", CheckpointScope: checkpoint.ScopeID, CheckpointID: checkpoint.ID, Payload: json.RawMessage(`{"version":1}`)}
	if err := r.SaveFinalizationHandoff(ctx, op.Scope, record.Request.ID, handoff, checkpoint.CreatedAt); !errors.Is(err, contracts.ErrConflict) {
		t.Fatalf("manifest key not bound to request ID: %v", err)
	}
}
