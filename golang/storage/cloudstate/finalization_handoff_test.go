package cloudstate

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

func handoffFixture(t *testing.T) (*Repository, *memoryTable, *memoryBlobs, Record, state.DurableCheckpoint, FinalizationHandoff) {
	t.Helper()
	r, table, blobs, checkpoint := checkpointFixture(t)
	op := Operation{Scope: Scope{Tenant: "private-tenant", Project: "private-project"}, Kind: "generate", Key: string(checkpoint.OriginOperationID), Manifest: json.RawMessage(`{"version":1,"operation_key":"private-operation"}`), Now: checkpoint.CreatedAt}
	record := mustBeginOperation(t, r, op)
	handoff := FinalizationHandoff{OperationID: checkpoint.OriginOperationID, Mode: "provider", CheckpointScope: checkpoint.ScopeID, CheckpointID: checkpoint.ID, Payload: json.RawMessage(`{"version":1,"response":"private answer","budget_generation":"generation-1"}`)}
	return r, table, blobs, record, checkpoint, handoff
}

func TestFinalizationHandoffRequiresCommittedMatchingCheckpoint(t *testing.T) {
	r, _, _, record, checkpoint, handoff := handoffFixture(t)
	ctx := context.Background()
	requireErrorIs(t, r.SaveFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID, handoff, checkpoint.CreatedAt), contracts.ErrNotFound, "uncommitted checkpoint")
	mustPublishCheckpoint(t, r.Checkpoints(), checkpoint)
	wrong := handoff
	wrong.CheckpointScope = "another-scope"
	requireErrorIs(t, r.SaveFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID, wrong, checkpoint.CreatedAt), contracts.ErrNotFound, "cross-scope checkpoint")
	other := childCheckpoint(checkpoint, "other-operation")
	mustPublishCheckpoint(t, r.Checkpoints(), other)
	wrong = handoff
	wrong.CheckpointID = other.ID
	requireErrorIs(t, r.SaveFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID, wrong, checkpoint.CreatedAt), contracts.ErrConflict, "other operation checkpoint")
	requireErrorIs(t, errOf(r.LoadFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID)), contracts.ErrNotFound, "missing handoff")
}

func TestFinalizationHandoffRestartReplayAndImmutableValues(t *testing.T) {
	r, table, blobs, record, checkpoint, handoff := handoffFixture(t)
	ctx := context.Background()
	mustPublishCheckpoint(t, r.Checkpoints(), checkpoint)
	mustSaveHandoff(t, r, record.Request.Scope, record.Request.ID, handoff, checkpoint.CreatedAt)
	handoff, _ = normalizeHandoff(handoff)
	got, err := reopen(t, table, blobs).LoadFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID)
	if err != nil || !equalHandoff(got, handoff) {
		t.Fatalf("restart: %+v %v", got, err)
	}
	before, err := r.Read(ctx, record.Request.Scope, record.Request.ID)
	if err != nil || before.Revision != record.Revision+1 {
		t.Fatalf("handoff revision: %v %v", before.Revision, err)
	}
	mustSaveHandoff(t, reopen(t, table, blobs), record.Request.Scope, record.Request.ID, handoff, checkpoint.CreatedAt.Add(time.Hour))
	after, _ := r.Read(ctx, record.Request.Scope, record.Request.ID)
	if after.Revision != before.Revision {
		t.Fatal("identical retry appended another event")
	}
	changed := handoff
	changed.Payload = json.RawMessage(`{"version":1,"response":"other"}`)
	requireErrorIs(t, r.SaveFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID, changed, checkpoint.CreatedAt), contracts.ErrConflict, "different handoff replaced committed value")
	wrongScope := record.Request.Scope
	wrongScope.Tenant = "someone-else"
	requireErrorIs(t, errOf(r.LoadFinalizationHandoff(ctx, wrongScope, record.Request.ID)), contracts.ErrNotFound, "cross-tenant load")
	// Progress is an in-process value, but the plaintext must not reach KV.
	assertNotStoredInPlaintext(t, table, blobs, "private answer", "handoff")
}

func TestFinalizationHandoffRecoversLostAcknowledgements(t *testing.T) {
	for _, stage := range []string{"event", "index"} {
		t.Run(stage, func(t *testing.T) {
			r, table, blobs, record, checkpoint, handoff := handoffFixture(t)
			ctx := context.Background()
			mustPublishCheckpoint(t, r.Checkpoints(), checkpoint)
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
	mustPublishCheckpoint(t, r.Checkpoints(), checkpoint)
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
	mustPublishCheckpoint(t, r.Checkpoints(), checkpoint)
	for _, mutate := range []func(*FinalizationHandoff){
		func(h *FinalizationHandoff) { h.Mode = "" },
		func(h *FinalizationHandoff) { h.CheckpointID = "" },
		func(h *FinalizationHandoff) { h.Payload = json.RawMessage(`[]`) },
		func(h *FinalizationHandoff) { h.Payload = json.RawMessage(`{"version":2}`) },
	} {
		invalid := handoff
		mutate(&invalid)
		requireErrorIs(t, r.SaveFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID, invalid, checkpoint.CreatedAt), ErrInvalid, "invalid handoff accepted")
	}
	mustSaveHandoff(t, r, record.Request.Scope, record.Request.ID, handoff, checkpoint.CreatedAt)
	mustCompleteOperation(t, r, record.Request.Scope, record.Request.ID, json.RawMessage(`{"version":1,"response":{}}`), checkpoint.CreatedAt)
	requireErrorIs(t, errOf(r.LoadFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID)), contracts.ErrConflict, "terminal handoff still actionable")
}

func TestFinalizationHandoffCacheAndCompactKinds(t *testing.T) {
	for _, variant := range []struct {
		kind, mode     string
		checkpointKind state.CheckpointKind
	}{
		{kind: "generate", mode: "cache", checkpointKind: state.CheckpointCacheReplay},
		{kind: "compact", mode: "provider", checkpointKind: state.CheckpointCompaction},
		{kind: "compact", mode: "no_work", checkpointKind: state.CheckpointCompaction},
		{kind: "compact", mode: "cache", checkpointKind: state.CheckpointCompaction},
	} {
		t.Run(variant.kind+"-"+variant.mode, func(t *testing.T) {
			r, _, _, checkpoint := checkpointFixture(t)
			checkpoint.Kind = variant.checkpointKind
			if variant.mode == "cache" {
				entry := state.CacheEntryID("origin-cache-entry")
				checkpoint.OriginCacheEntryID = &entry
			}
			op := Operation{Scope: Scope{Tenant: "private-tenant", Project: "private-project"}, Kind: variant.kind, Key: string(checkpoint.OriginOperationID), Manifest: json.RawMessage(`{"operation_key":"private-operation"}`), Now: checkpoint.CreatedAt}
			record := mustBeginOperation(t, r, op)
			mustPublishCheckpoint(t, r.Checkpoints(), checkpoint)
			handoff := FinalizationHandoff{OperationID: checkpoint.OriginOperationID, Mode: variant.mode, CheckpointScope: checkpoint.ScopeID, CheckpointID: checkpoint.ID, Payload: json.RawMessage(`{"version":1}`)}
			mustSaveHandoff(t, r, op.Scope, record.Request.ID, handoff, checkpoint.CreatedAt)
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
	record := mustBeginOperation(t, r, op)
	mustPublishCheckpoint(t, r.Checkpoints(), checkpoint)
	handoff := FinalizationHandoff{OperationID: checkpoint.OriginOperationID, Mode: "provider", CheckpointScope: checkpoint.ScopeID, CheckpointID: checkpoint.ID, Payload: json.RawMessage(`{"version":1}`)}
	requireErrorIs(t, r.SaveFinalizationHandoff(ctx, op.Scope, record.Request.ID, handoff, checkpoint.CreatedAt), contracts.ErrConflict, "manifest key not bound to request ID")
}

func TestFinalizationHandoffSeparatesCallerKeyFromInternalOperationID(t *testing.T) {
	r, _, _, checkpoint := checkpointFixture(t)
	op := Operation{Scope: Scope{Tenant: "tenant", Project: "project"}, Kind: "generate", Key: "caller-idempotency-key", Manifest: json.RawMessage(`{"operation_key":"caller-idempotency-key"}`), Now: checkpoint.CreatedAt}
	record := mustBeginOperation(t, r, op)
	mustPublishCheckpoint(t, r.Checkpoints(), checkpoint)
	handoff := FinalizationHandoff{OperationID: checkpoint.OriginOperationID, Mode: "provider", CheckpointScope: checkpoint.ScopeID, CheckpointID: checkpoint.ID, Payload: json.RawMessage(`{"version":1}`)}
	mustSaveHandoff(t, r, op.Scope, record.Request.ID, handoff, checkpoint.CreatedAt)
	handoff.OperationID = state.OperationID(op.Key)
	requireErrorIs(t, r.SaveFinalizationHandoff(context.Background(), op.Scope, record.Request.ID, handoff, checkpoint.CreatedAt), contracts.ErrConflict, "caller key accepted as internal ID")
}

// assertNotStoredInPlaintext fails with "<what> leaked in KV" or "<what>
// leaked in blob storage" when any KV byte field or blob contains secret.
func assertNotStoredInPlaintext(t *testing.T, table *memoryTable, blobs *memoryBlobs, secret, what string) {
	t.Helper()
	for _, row := range table.rows {
		for _, field := range row.Item.Fields {
			if data, ok := field.(kv.KeyValueBytes); ok && strings.Contains(string(data), secret) {
				t.Fatal(what + " leaked in KV")
			}
		}
	}
	for _, data := range blobs.values {
		if strings.Contains(string(data), secret) {
			t.Fatal(what + " leaked in blob storage")
		}
	}
}
