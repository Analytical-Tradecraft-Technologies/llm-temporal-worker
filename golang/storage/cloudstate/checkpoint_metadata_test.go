package cloudstate

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

func TestCloudCheckpointCompactionAndOptionalMetadataRoundTrip(t *testing.T) {
	r, table, blobs, root := checkpointFixture(t)
	ctx, store := context.Background(), r.Checkpoints()
	if err := publishCheckpoint(ctx, store, root); err != nil {
		t.Fatal(err)
	}
	compacted := childCheckpoint(root, "compaction-op")
	compacted.Kind = state.CheckpointCompaction
	policy, prompt := "policy-v1", "prompt-v1"
	compacted.CompactionPolicyVersion, compacted.CompactionPromptVersion = &policy, &prompt
	compacted.CompactedThroughID = &root.ID
	expires := root.ExpiresAt.Add(time.Hour)
	providerBytes := []byte("opaque private provider context")
	ref, err := store.Write(ctx, root.ScopeID, providerBytes, "application/octet-stream")
	if err != nil {
		t.Fatal(err)
	}
	compacted.ProviderState = []state.CheckpointProviderState{{Provider: "example", EndpointID: "endpoint", EndpointAccountHMAC: [32]byte{7}, Region: "region", EndpointFamily: "family", ModelLineage: "model", StateKind: "context", StateBlob: ref, StateDigest: sha256.Sum256(providerBytes), Required: true, ImmutableForkSafe: true, CreatedAt: root.CreatedAt, ExpiresAt: &expires}}
	compacted.Affinities = state.ProviderCacheAffinitySet{{Provider: "example", RouteID: "route", EndpointID: "endpoint", EndpointAccountHMAC: [32]byte{7}, Region: "region", EndpointFamily: "family", ModelLineage: "model", RouteModelRevision: "model-v1", CacheEpoch: "epoch", LastSuccessAt: root.CreatedAt, ExpiresAt: &expires}}
	if err := publishCheckpoint(ctx, store, compacted); err != nil {
		t.Fatal(err)
	}
	got, err := reopen(t, table, blobs).Checkpoints().Get(ctx, root.ScopeID, compacted.ID)
	if err != nil {
		t.Fatal(err)
	}
	wantDigest, _ := compacted.CanonicalDigest()
	gotDigest, _ := got.CanonicalDigest()
	if wantDigest != gotDigest {
		t.Fatal("lost optional metadata")
	}
	got.ProviderState[0].Provider = "caller mutation"
	got.Affinities[0].Provider = "caller mutation"
	again, err := store.Get(ctx, root.ScopeID, compacted.ID)
	if err != nil || again.ProviderState[0].Provider != "example" || again.Affinities[0].Provider != "example" {
		t.Fatal("mutable read result")
	}
	// A cache replay is a separate immutable checkpoint with its own operation.
	replay := childCheckpoint(compacted, "cache-replay-op")
	replay.Kind = state.CheckpointCacheReplay
	cacheID := state.CacheEntryID("origin-cache-entry")
	replay.OriginCacheEntryID = &cacheID
	if err := publishCheckpoint(ctx, store, replay); err != nil {
		t.Fatal(err)
	}
	if got, err := store.Get(ctx, root.ScopeID, replay.ID); err != nil || got.Kind != state.CheckpointCacheReplay || got.ID == compacted.ID {
		t.Fatalf("cache replay checkpoint: %v", err)
	}
}

func TestCloudCheckpointCorruptPointersFailClosed(t *testing.T) {
	for _, mode := range []string{"schema", "key", "field", "blob-binding"} {
		t.Run(mode, func(t *testing.T) {
			r, table, _, cp := checkpointFixture(t)
			ctx := context.Background()
			if err := publishCheckpoint(ctx, r.Checkpoints(), cp); err != nil {
				t.Fatal(err)
			}
			s := r.Checkpoints().(*checkpointStore)
			key := s.key("id", s.identity(cp.ScopeID, "id", string(cp.ID)))
			row := table.rows[key]
			var pointer checkpointPointer
			if err := json.Unmarshal(row.Item.Fields["checkpoint"].(kv.KeyValueBytes), &pointer); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "schema":
				pointer.Version++
			case "key":
				row.Item.SortKey = "wrong-key"
			case "field":
				row.Item.Fields["checkpoint"] = kv.String("wrong-type")
			case "blob-binding":
				pointer.Blob = r.namespace + "/payload/" + strings.Repeat("0", 64)
			}
			if mode != "field" {
				data, _ := json.Marshal(pointer)
				row.Item.Fields["checkpoint"] = kv.Bytes(data)
			}
			table.rows[key] = row
			if _, err := s.Get(ctx, cp.ScopeID, cp.ID); err == nil {
				t.Fatal("corrupt checkpoint returned")
			}
		})
	}
}
