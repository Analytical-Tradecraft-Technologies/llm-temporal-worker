package cloudstate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/google/uuid"
)

func checkpointFixture(t *testing.T) (*Repository, *memoryTable, *memoryBlobs, state.DurableCheckpoint) {
	t.Helper()
	r, table, blobs, request := fixture(t)
	s := r.Checkpoints()
	codec := state.CheckpointBlobCodec{}
	delta, err := codec.EncodeDelta([]llm.Item{llm.HumanText("private prompt")})
	if err != nil {
		t.Fatal(err)
	}
	response, err := codec.EncodeResponse([]llm.Item{llm.ModelText("private answer")})
	if err != nil {
		t.Fatal(err)
	}
	patch, err := codec.EncodeSettingsPatch(state.SettingsPatch{Model: state.SetPatch("test-model"), ServiceClass: state.SetPatch(llm.ServiceClassStandard), Portability: state.SetPatch(llm.PortabilityStrict)})
	if err != nil {
		t.Fatal(err)
	}
	refs := make([]state.CheckpointBlobReference, 3)
	for i, data := range [][]byte{delta, response, patch} {
		refs[i], err = s.Write(context.Background(), "private-scope", data, "application/json")
		if err != nil {
			t.Fatal(err)
		}
	}
	cp := state.DurableCheckpoint{ID: state.CheckpointID(uuid.NewString()), ScopeID: "private-scope", PublicIDHMAC: sha256.Sum256([]byte("handle")), HandleKeyID: "key-1", Kind: state.CheckpointGeneration,
		OriginOperationID: "private-operation", DeltaBlob: refs[0], ResponseBlob: refs[1], SettingsPatchBlob: refs[2], CanonicalLineageDigest: [32]byte{1}, MaterializedSettingsDigest: [32]byte{2}, ToolFrontierDigest: [32]byte{3}, SchemaVersion: 1, CompilerEpoch: "compiler-v1", CreatedAt: request.CreatedAt, ExpiresAt: request.CreatedAt.Add(time.Hour)}
	return r, table, blobs, cp
}

func publishCheckpoint(ctx context.Context, store state.CheckpointRepository, checkpoint state.DurableCheckpoint) error {
	return state.WithCheckpointUnitOfWork(ctx, store, func(ctx context.Context, unit state.CheckpointUnitOfWork) error {
		return unit.PutCheckpoint(ctx, state.CheckpointWrite{Checkpoint: checkpoint})
	})
}

func childCheckpoint(parent state.DurableCheckpoint, name string) state.DurableCheckpoint {
	child := parent
	child.ID = state.CheckpointID(uuid.NewString())
	child.ParentID, child.Depth = &parent.ID, parent.Depth+1
	child.OriginOperationID = state.OperationID(name)
	child.PublicIDHMAC = sha256.Sum256([]byte(name))
	return child
}

func TestCloudCheckpointRestartAndHandleMaterialization(t *testing.T) {
	r, table, blobs, root := checkpointFixture(t)
	ctx := context.Background()
	mustPublishCheckpoint(t, r.Checkpoints(), root)
	child := childCheckpoint(root, "child-op")
	mustPublishCheckpoint(t, r.Checkpoints(), child)
	store := reopen(t, table, blobs).Checkpoints()
	keyring, err := state.NewKeyring([]state.Key{{ID: "key-1", Secret: bytes.Repeat([]byte{5}, 32)}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := keyring.IssueCheckpointHandle(root.ScopeID, child.ID)
	if err != nil {
		t.Fatal(err)
	}
	m := &state.DurableCheckpointMaterializer{Repository: store, Blobs: store, HandleVerifier: keyring, Now: func() time.Time { return root.CreatedAt }}
	got, err := m.MaterializeHandle(ctx, root.ScopeID, handle, state.MaterializeLimits{})
	if err != nil || len(got.Items) != 4 || got.Depth != 1 || got.Settings.Model != "test-model" || got.Handle != state.Handle(handle) {
		t.Fatalf("materialize: %+v, %v", got, err)
	}
	if _, err := m.MaterializeHandle(ctx, "another-scope", handle, state.MaterializeLimits{}); err == nil {
		t.Fatal("cross-scope handle accepted")
	}
	requireErrorIs(t, errOf(store.Get(ctx, "another-scope", child.ID)), contracts.ErrNotFound, "scope")
	m.Now = func() time.Time { return root.ExpiresAt }
	requireErrorIs(t, errOf(m.MaterializeHandle(ctx, root.ScopeID, handle, state.MaterializeLimits{})), state.ErrExpired, "expired replay")
	// Retention is independent from eligibility: history remains readable.
	mustNoError(t, errOf(store.Get(ctx, root.ScopeID, root.ID)))
}

func TestCloudCheckpointPublicationIsImmutableAndPrivate(t *testing.T) {
	r, table, blobs, cp := checkpointFixture(t)
	ctx, store := context.Background(), r.Checkpoints()
	mustPublishCheckpoint(t, store, cp)
	rowCount, blobCount := len(table.rows), len(blobs.values)
	var writes int
	table.trace = func(string) { writes++ }
	blobs.trace = func(string) { writes++ }
	if err := publishCheckpoint(ctx, store, cp); err != nil || writes != 0 {
		t.Fatalf("replay writes=%d: %v", writes, err)
	}
	conflict := cp
	conflict.CompilerEpoch = "different"
	requireErrorIs(t, publishCheckpoint(ctx, store, conflict), contracts.ErrConflict, "overwrite")
	if rowCount != len(table.rows) || blobCount != len(blobs.values) {
		t.Fatal("duplicate publication")
	}
	for _, row := range table.rows {
		data, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		fields, err := row.Item.Fields.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		for _, private := range []string{cp.ScopeID, string(cp.OriginOperationID), string(cp.ID), "private prompt", "private answer"} {
			if bytes.Contains(data, []byte(private)) || bytes.Contains(fields, []byte(private)) {
				t.Fatal("plaintext checkpoint metadata in table")
			}
		}
	}
	for _, data := range blobs.values {
		if bytes.Contains(data, []byte("private")) {
			t.Fatal("plaintext blob")
		}
	}
	page, err := r.ListPending(ctx, 0, 10, "")
	if err != nil || len(page.Requests) != 0 {
		t.Fatalf("checkpoint entered request recovery: %v", err)
	}
}

func TestCloudCheckpointConditionalPublicationRaces(t *testing.T) {
	for _, mode := range []string{"identical", "same-id", "same-operation", "same-handle"} {
		t.Run(mode, func(t *testing.T) {
			r, table, blobs, cp := checkpointFixture(t)
			const count = 12
			candidates := make([]state.DurableCheckpoint, count)
			results := make([]error, count)
			var wg sync.WaitGroup
			for i := range count {
				candidate := cp
				if mode != "identical" {
					candidate.CompilerEpoch = fmt.Sprint(i)
				}
				if mode == "same-operation" || mode == "same-handle" {
					candidate.ID = state.CheckpointID(uuid.NewString())
				}
				if mode == "same-operation" {
					candidate.PublicIDHMAC = sha256.Sum256([]byte(fmt.Sprint(i)))
				}
				if mode == "same-handle" {
					candidate.OriginOperationID = state.OperationID(fmt.Sprint(i))
				}
				candidates[i] = candidate
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					results[i] = publishCheckpoint(context.Background(), r.Checkpoints(), candidates[i])
				}(i)
			}
			wg.Wait()
			wins := 0
			for i, err := range results {
				if err == nil {
					wins++
				} else if !errors.Is(err, contracts.ErrConflict) {
					t.Fatalf("unexpected write error: %v", err)
				}
				if err != nil && (mode == "same-operation" || mode == "same-handle") {
					requireErrorIs(t, errOf(reopen(t, table, blobs).Checkpoints().Get(context.Background(), cp.ScopeID, candidates[i].ID)), contracts.ErrNotFound, "losing reservation is visible")
				}
			}
			want := 1
			if mode == "identical" {
				want = count
			}
			if wins != want {
				t.Fatalf("wins %d want %d", wins, want)
			}
		})
	}
}

func TestCloudCheckpointLostAcknowledgements(t *testing.T) {
	for _, point := range []string{"blob", "id", "handle", "operation"} {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/after=%t", point, after), func(t *testing.T) {
				r, table, blobs, cp := checkpointFixture(t)
				fired := false
				failure := func() (error, error) {
					if fired {
						return nil, nil
					}
					fired = true
					if after {
						return nil, contracts.ErrOutcomeUnknown
					}
					return contracts.ErrOutcomeUnknown, nil
				}
				if point == "blob" {
					blobs.hook = func(blob.BlobKey) (error, error) { return failure() }
				} else {
					table.hook = func(_ string, item kv.KeyValueItem) (error, error) {
						if strings.Contains(item.PartitionKey, "/checkpoint/"+point+"/") {
							return failure()
						}
						return nil, nil
					}
				}
				ctx := context.Background()
				requireErrorIs(t, publishCheckpoint(ctx, r.Checkpoints(), cp), contracts.ErrOutcomeUnknown, "unknown outcome lost")
				if !fired {
					t.Fatal("fault not injected")
				}
				_, err := r.Checkpoints().Get(ctx, cp.ScopeID, cp.ID)
				if point == "operation" && after {
					if err != nil {
						t.Fatalf("committed despite lost ack: %v", err)
					}
				} else if !errors.Is(err, contracts.ErrNotFound) {
					t.Fatalf("partial publication visible: %v", err)
				}
				if err := publishCheckpoint(ctx, reopen(t, table, blobs).Checkpoints(), cp); err != nil {
					t.Fatalf("retry: %v", err)
				}
				mustNoError(t, errOf(r.Checkpoints().Get(ctx, cp.ScopeID, cp.ID)))
			})
		}
	}
}

func TestCloudCheckpointStagingAndRollback(t *testing.T) {
	r, table, _, cp := checkpointFixture(t)
	ctx, store := context.Background(), r.Checkpoints()
	u, err := store.BeginCheckpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mustNoError(t, u.PutCheckpoint(ctx, state.CheckpointWrite{Checkpoint: cp}))
	if len(table.rows) != 0 {
		t.Fatal("PutCheckpoint published before Commit")
	}
	second := cp
	second.ID = "another"
	if err := u.PutCheckpoint(ctx, state.CheckpointWrite{Checkpoint: second}); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("accepted multiple rows")
	}
	mustNoError(t, u.Rollback(ctx))
	if err := u.Commit(ctx); err == nil || len(table.rows) != 0 {
		t.Fatal("rollback published")
	}
	u, _ = store.BeginCheckpoint(ctx)
	parent := state.CheckpointID("original-parent")
	staged := cp
	staged.ParentID, staged.Depth = &parent, 1
	mustNoError(t, u.PutCheckpoint(ctx, state.CheckpointWrite{Checkpoint: staged}))
	parent = "mutated-parent"
	if u.(*checkpointUnit).value.ParentID == staged.ParentID || *u.(*checkpointUnit).value.ParentID != "original-parent" {
		t.Fatal("caller mutated staged pointers")
	}
	if err := u.Commit(ctx); !errors.Is(err, contracts.ErrNotFound) || len(table.rows) != 0 {
		t.Fatalf("missing parent: %v", err)
	}
	mustPublishCheckpoint(t, store, cp)
	got, err := store.Get(ctx, cp.ScopeID, cp.ID)
	if err != nil {
		t.Fatal(err)
	}
	got.CompilerEpoch = "mutated"
	again, _ := store.Get(ctx, cp.ScopeID, cp.ID)
	if again.CompilerEpoch == got.CompilerEpoch {
		t.Fatal("read mutated stored value")
	}
}

func TestCloudCheckpointReferencesAndLimits(t *testing.T) {
	r, _, _, root := checkpointFixture(t)
	ctx, store := context.Background(), r.Checkpoints()
	mustPublishCheckpoint(t, store, root)
	for _, test := range []struct {
		name string
		edit func(*state.DurableCheckpoint)
	}{
		{"depth", func(c *state.DurableCheckpoint) { c.Depth = 5 }},
		{"cross-scope-parent", func(c *state.DurableCheckpoint) { c.ScopeID = "another" }},
		{"missing-compacted", func(c *state.DurableCheckpoint) {
			missing := state.CheckpointID("missing")
			c.CompactedThroughID = &missing
		}},
		{"forged-blob", func(c *state.DurableCheckpoint) { c.ResponseBlob.ByteLength++ }},
		{"wrong-schema", func(c *state.DurableCheckpoint) { c.SchemaVersion = 0 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cp := childCheckpoint(root, test.name)
			test.edit(&cp)
			if err := publishCheckpoint(ctx, store, cp); err == nil {
				t.Fatal("invalid checkpoint published")
			}
		})
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	requireErrorIs(t, errOf(store.BeginCheckpoint(cancelled)), context.Canceled, "")
	requireErrorIs(t, errOf(store.Get(cancelled, root.ScopeID, root.ID)), context.Canceled, "")
	if _, err := store.BeginCheckpoint(nil); err == nil {
		t.Fatal("nil context accepted")
	}
	if _, err := store.Write(ctx, root.ScopeID, make([]byte, maxPayloadBytes+1), "application/json"); !errors.Is(err, ErrInvalid) {
		t.Fatal("oversized blob accepted")
	}
}

type checkpointBlobSpy struct {
	blob.BlobStore
	opens int
}

func (s *checkpointBlobSpy) Open(ctx context.Context, key blob.BlobKey) (blob.BlobReadResult, error) {
	s.opens++
	return s.BlobStore.Open(ctx, key)
}

func TestCloudCheckpointBlobsAreScopedAndAuthenticated(t *testing.T) {
	r, table, blobs, cp := checkpointFixture(t)
	spy := &checkpointBlobSpy{BlobStore: blobs}
	store := reopen(t, table, spy).Checkpoints()
	ctx := context.Background()
	data, err := store.Read(ctx, cp.ScopeID, cp.DeltaBlob)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.Write(ctx, cp.ScopeID, data, cp.DeltaBlob.MediaType)
	if err != nil || ref != cp.DeltaBlob {
		t.Fatalf("blob retry: %v", err)
	}
	spy.opens = 0
	for _, edit := range []func(*state.CheckpointBlobReference){
		func(r *state.CheckpointBlobReference) { r.ByteLength++ },
		func(r *state.CheckpointBlobReference) { r.MediaType = "application/octet-stream" },
		func(r *state.CheckpointBlobReference) { r.Digest[0] ^= 1 },
	} {
		ref := cp.DeltaBlob
		edit(&ref)
		requireErrorIs(t, errOf(store.Read(ctx, cp.ScopeID, ref)), contracts.ErrNotFound, "forged metadata")
	}
	requireErrorIs(t, errOf(store.Read(ctx, "another-scope", cp.DeltaBlob)), contracts.ErrNotFound, "")
	if spy.opens != 0 {
		t.Fatal("unauthorized reference opened a blob")
	}
	mustPublishCheckpoint(t, r.Checkpoints(), cp)
	// Physical tampering is detected, including correctly shaped pointers to
	// another encrypted object. No decoded partial result is returned.
	for key, value := range blobs.values {
		value[len(value)-1] ^= 1
		blobs.values[key] = value
	}
	requireErrorIs(t, errOf(store.Read(ctx, cp.ScopeID, cp.DeltaBlob)), ErrCorrupt, "tampered blob")
	requireErrorIs(t, errOf(store.Get(ctx, cp.ScopeID, cp.ID)), ErrCorrupt, "tampered checkpoint")
}

func TestCloudCheckpointNilEmptyCollectionsAndTimeZonesReplay(t *testing.T) {
	r, table, blobs, cp := checkpointFixture(t)
	ctx := context.Background()
	mustPublishCheckpoint(t, r.Checkpoints(), cp)
	cp.ProviderState = []state.CheckpointProviderState{}
	cp.Affinities = state.ProviderCacheAffinitySet{}
	cp.CreatedAt = cp.CreatedAt.In(time.FixedZone("test", 3600))
	cp.ExpiresAt = cp.ExpiresAt.In(time.FixedZone("test", 3600))
	mustPublishCheckpoint(t, reopen(t, table, blobs).Checkpoints(), cp)
	got, err := r.Checkpoints().Get(ctx, cp.ScopeID, cp.ID)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := cp.CanonicalDigest()
	actual, _ := got.CanonicalDigest()
	if !reflect.DeepEqual(want, actual) {
		t.Fatal("retry changed digest")
	}
}
