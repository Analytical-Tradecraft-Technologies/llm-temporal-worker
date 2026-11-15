package cloudstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

func preparationFixture(t *testing.T) (*Repository, *memoryTable, *memoryBlobs, Record, RequestPreparation) {
	t.Helper()
	r, table, blobs, op := operationFixture(t)
	record := mustBeginOperation(t, r, op)
	decimal, _ := llm.NewDecimalV1("0.123456789012345678")
	settings := state.RootModelState("private-model")
	settings.TemperatureDecimal = &decimal
	settings.Extensions = map[string]json.RawMessage{"vendor": json.RawMessage(`{"integer":9007199254740993}`)}
	snapshot, err := (state.CheckpointBlobCodec{}).EncodeSnapshot(*state.NewCheckpointSnapshot(state.MaterializedState{
		Items:    []llm.Item{llm.HumanText("private parent transcript")},
		Settings: settings, Lineage: []state.Handle{"00000000-0000-4000-8000-000000000001"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	return r, table, blobs, record, RequestPreparation{Version: 1, ConfigDigest: [32]byte{1}, CheckpointScope: "private-checkpoint-scope", ParentSnapshot: snapshot, PreparedAt: op.Now.Add(time.Second)}
}

func TestRequestPreparationRestartAndEncryption(t *testing.T) {
	r, table, blobs, record, preparation := preparationFixture(t)
	ctx := context.Background()
	requireErrorIs(t, errOf(r.LoadRequestPreparation(ctx, record.Request.Scope, record.Request.ID)), ErrRequestPreparationMissing, "initial read")
	mustSavePreparation(t, r, record.Request.Scope, record.Request.ID, preparation)
	reopened := reopen(t, table, blobs)
	got, err := reopened.LoadRequestPreparation(ctx, record.Request.Scope, record.Request.ID)
	if err != nil || !equalExecutionJSON(got, preparation) {
		t.Fatalf("restart: %v", err)
	}
	decoded, err := (state.CheckpointBlobCodec{}).DecodeSnapshot(got.ParentSnapshot)
	if err != nil || decoded.Settings.TemperatureDecimal.String() != "0.123456789012345678" || !bytes.Contains(decoded.Settings.Extensions["vendor"], []byte("9007199254740993")) {
		t.Fatalf("lost exact values: %v", err)
	}
	got.ParentSnapshot[0] = '!'
	if _, err := reopened.LoadRequestPreparation(ctx, record.Request.Scope, record.Request.ID); err != nil {
		t.Fatal("returned data aliased storage", err)
	}
	requireErrorIs(t, errOf(reopened.LoadRequestPreparation(ctx, Scope{Tenant: record.Request.Scope.Tenant, Project: "other"}, record.Request.ID)), contracts.ErrNotFound, "cross-scope")
	var stored [][]byte
	for key, row := range table.rows {
		data, _ := row.Item.Fields.MarshalBinary()
		stored = append(stored, []byte(key.PartitionKey+key.SortKey), data)
	}
	for key, data := range blobs.values {
		stored = append(stored, []byte(key), data)
	}
	for _, data := range stored {
		for _, secret := range []string{"private parent transcript", "private-checkpoint-scope", "private-model", "9007199254740993"} {
			if bytes.Contains(data, []byte(secret)) {
				t.Fatalf("plaintext %s leaked", secret)
			}
		}
	}
	forbidWrites(t, table, blobs, "healthy replay")
	mustSavePreparation(t, reopened, record.Request.Scope, record.Request.ID, preparation)
}

func TestRequestPreparationConcurrentProposalsHaveOneWinner(t *testing.T) {
	r, table, blobs, record, preparation := preparationFixture(t)
	ctx := context.Background()
	var group sync.WaitGroup
	winners := make(chan [32]byte, 20)
	for i := 0; i < 20; i++ {
		candidate := preparation
		candidate.ConfigDigest[0] = byte(i%2 + 1)
		client := reopen(t, table, blobs)
		group.Add(1)
		go func() {
			defer group.Done()
			err := client.SaveRequestPreparation(ctx, record.Request.Scope, record.Request.ID, candidate)
			if err == nil {
				winners <- candidate.ConfigDigest
			} else if !errors.Is(err, contracts.ErrConflict) {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	close(winners)
	got, err := r.LoadRequestPreparation(ctx, record.Request.Scope, record.Request.ID)
	if err != nil || len(winners) == 0 {
		t.Fatalf("no winner: %v", err)
	}
	for winner := range winners {
		if winner != got.ConfigDigest {
			t.Fatal("different preparations both won")
		}
	}
	current, _ := r.Read(ctx, record.Request.Scope, record.Request.ID)
	if current.Revision != record.Revision+1 {
		t.Fatal("duplicate preparation events")
	}
}

func TestRequestPreparationCannotBeAddedAfterAdmission(t *testing.T) {
	for _, key := range []string{"budget_plan", "provider_execution", "checkpoint_finalization", "finalization_handoff"} {
		t.Run(key, func(t *testing.T) {
			r, _, _, record, preparation := preparationFixture(t)
			data, _ := json.Marshal(map[string]any{"version": 1, key: map[string]any{}})
			mustTryUpdate(t, r, record.Request.Scope, record.Request.ID, Update{ExpectedRevision: record.Revision, Token: "started", Status: StatusRunning, Progress: data, UpdatedAt: record.UpdatedAt})
			requireErrorIs(t, r.SaveRequestPreparation(context.Background(), record.Request.Scope, record.Request.ID, preparation), contracts.ErrConflict, "late save")
			requireErrorIs(t, errOf(r.LoadRequestPreparation(context.Background(), record.Request.Scope, record.Request.ID)), ErrCorrupt, "missing started preparation treated as cache miss")
		})
	}
}

func TestRequestPreparationPreservesOtherProgressAndRejectsReplacement(t *testing.T) {
	r, _, _, record, p := preparationFixture(t)
	ctx := context.Background()
	mustTryUpdate(t, r, record.Request.Scope, record.Request.ID, Update{ExpectedRevision: record.Revision, Token: "other", Status: StatusRunning, Progress: json.RawMessage(`{"version":1,"other":{"large":9007199254740993}}`), UpdatedAt: record.UpdatedAt})
	mustSavePreparation(t, r, record.Request.Scope, record.Request.ID, p)
	current, _ := r.Read(ctx, record.Request.Scope, record.Request.ID)
	if !strings.Contains(string(current.Progress), `"large":9007199254740993`) {
		t.Fatal("lost unrelated exact progress")
	}
	for _, mutate := range []func(*RequestPreparation){func(p *RequestPreparation) { p.ConfigDigest[0]++ }, func(p *RequestPreparation) { p.CheckpointScope += "-other" }, func(p *RequestPreparation) { p.PreparedAt = p.PreparedAt.Add(time.Second) }, func(p *RequestPreparation) { p.ParentSnapshot = nil }} {
		changed := p
		mutate(&changed)
		requireErrorIs(t, r.SaveRequestPreparation(ctx, record.Request.Scope, record.Request.ID, changed), contracts.ErrConflict, "replaced saved preparation")
	}
	for _, mutate := range []func(*RequestPreparation){func(p *RequestPreparation) { p.Version = 2 }, func(p *RequestPreparation) { p.ConfigDigest = [32]byte{} }, func(p *RequestPreparation) { p.CheckpointScope = "" }, func(p *RequestPreparation) { p.ParentSnapshot = json.RawMessage(`{}`) }, func(p *RequestPreparation) { p.PreparedAt = time.Time{} }} {
		invalid := p
		mutate(&invalid)
		requireErrorIs(t, r.SaveRequestPreparation(ctx, record.Request.Scope, record.Request.ID, invalid), ErrInvalid, "accepted invalid preparation")
	}
}

func TestRequestPreparationUncertainWritesRepairWithoutChangingInput(t *testing.T) {
	for _, stage := range uncertainWriteStages {
		t.Run(stage, func(t *testing.T) {
			r, table, blobs, record, plan := preparationFixture(t)
			fired := injectUncertainWrite(stage, table, blobs)
			ctx := context.Background()
			if err := r.SaveRequestPreparation(ctx, record.Request.Scope, record.Request.ID, plan); err == nil || !*fired {
				t.Fatalf("fault not observed: %v", err)
			}
			table.hook, blobs.hook = nil, nil
			mustSavePreparation(t, reopen(t, table, blobs), record.Request.Scope, record.Request.ID, plan)
			stored, err := reopen(t, table, blobs).LoadRequestPreparation(ctx, record.Request.Scope, record.Request.ID)
			if err != nil || !reflect.DeepEqual(stored.ConfigDigest, plan.ConfigDigest) {
				t.Fatalf("recovery changed preparation: %v", err)
			}
			current, _ := r.Read(ctx, record.Request.Scope, record.Request.ID)
			if current.Revision != record.Revision+1 {
				t.Fatal("uncertain retry appended another event")
			}
		})
	}
}

// storedPreparationOf returns the durable preparation of a request record.
func storedPreparationOf(t *testing.T, record Record) storedRequestPreparation {
	t.Helper()
	var progress map[string]json.RawMessage
	var stored storedRequestPreparation
	if json.Unmarshal(record.Progress, &progress) != nil || json.Unmarshal(progress["request_preparation"], &stored) != nil {
		t.Fatal("undecodable preparation")
	}
	return stored
}

// rewritePreparation replaces the stored preparation with raw, bypassing
// SaveRequestPreparation, as a legacy writer or a corruption would.
func rewritePreparation(t *testing.T, r *Repository, record Record, raw any) Record {
	t.Helper()
	var progress map[string]json.RawMessage
	if json.Unmarshal(record.Progress, &progress) != nil || progress == nil {
		t.Fatal("undecodable progress")
	}
	progress["request_preparation"], _ = json.Marshal(raw)
	data, _ := json.Marshal(progress)
	updated := mustTryUpdate(t, r, record.Request.Scope, record.Request.ID, Update{ExpectedRevision: record.Revision, Token: fmt.Sprintf("rewrite-%d", record.Revision), Status: record.Status, Progress: data, UpdatedAt: record.UpdatedAt})
	return updated
}

// TestRequestPreparationStoresParentOnceByReference covers #1112: the record
// keeps only a digest reference, so later progress rewrites leave the
// encrypted parent blob untouched, and loading resolves it exactly.
func TestRequestPreparationStoresParentOnceByReference(t *testing.T) {
	r, table, blobs, record, preparation := preparationFixture(t)
	r.parentSnapshotBlob = true
	ctx := context.Background()
	mustSavePreparation(t, r, record.Request.Scope, record.Request.ID, preparation)
	current := mustRead(t, r, record.Request.Scope, record.Request.ID)
	stored := storedPreparationOf(t, current)
	ref := stored.ParentSnapshotRef
	if ref == nil || len(stored.ParentSnapshot) != 0 || bytes.Contains(current.Progress, []byte(`"parent_snapshot"`)) {
		t.Fatalf("parent stored inline: %s", current.Progress)
	}
	if want := r.newParentSnapshotRef(record.Request.Scope, preparation.ParentSnapshot); *ref != want {
		t.Fatalf("reference %+v, want %+v", *ref, want)
	}
	sealed, ok := blobs.values[blob.BlobKey(ref.Blob)]
	if !ok || bytes.Contains(sealed, []byte("private parent transcript")) {
		t.Fatal("parent blob missing or not encrypted")
	}
	got, parent, err := reopen(t, table, blobs).LoadRequestPreparationParent(ctx, record.Request.Scope, record.Request.ID)
	if err != nil || !bytes.Equal(got.ParentSnapshot, preparation.ParentSnapshot) || !equalExecutionJSON(got, preparation) || parent == nil {
		t.Fatalf("round trip: %v", err)
	}
	if decoded, err := got.ValidateParent(); err != nil || !reflect.DeepEqual(*decoded, *parent) {
		t.Fatalf("returned parent differs from its snapshot: %v", err)
	}
	// A later progress write stores only the small record again.
	before := len(blobs.values)
	var progress map[string]json.RawMessage
	_ = json.Unmarshal(current.Progress, &progress)
	progress["other"] = json.RawMessage(`{"stage":1}`)
	data, _ := json.Marshal(progress)
	mustTryUpdate(t, r, current.Request.Scope, current.Request.ID, Update{ExpectedRevision: current.Revision, Token: "other", Status: StatusRunning, Progress: data, UpdatedAt: current.UpdatedAt})
	if len(blobs.values) != before+1 {
		t.Fatalf("progress write stored %d blobs", len(blobs.values)-before)
	}
	mustNoError(t, errOf(r.LoadRequestPreparation(ctx, record.Request.Scope, record.Request.ID)))
}

// TestRequestPreparationLegacyInlineStillLoads keeps preparations saved before
// #1112, with the snapshot inline, loadable and replayable unchanged.
func TestRequestPreparationLegacyInlineStillLoads(t *testing.T) {
	r, table, blobs, record, preparation := preparationFixture(t)
	ctx := context.Background()
	rewritePreparation(t, r, record, preparation)
	reopened := reopen(t, table, blobs)
	got, parent, err := reopened.LoadRequestPreparationParent(ctx, record.Request.Scope, record.Request.ID)
	if err != nil || !bytes.Equal(got.ParentSnapshot, preparation.ParentSnapshot) || !equalExecutionJSON(got, preparation) || parent == nil {
		t.Fatalf("legacy load: %v", err)
	}
	// An identical retry recognises the legacy form and writes nothing.
	forbidWrites(t, table, blobs, "legacy replay")
	mustSavePreparation(t, reopened, record.Request.Scope, record.Request.ID, preparation)
	table.hook, blobs.hook = nil, nil
	changed := preparation
	changed.ConfigDigest[0]++
	requireErrorIs(t, reopened.SaveRequestPreparation(ctx, record.Request.Scope, record.Request.ID, changed), contracts.ErrConflict, "replaced legacy preparation")
	// A paid attempt copies the legacy preparation and loads it.
	attempt := mustBeginRequestAttempt(t, reopened, record.Request.Scope, record.Request.ID, "", record.UpdatedAt)
	if child, err := reopened.LoadRequestPreparation(ctx, record.Request.Scope, attempt.ID); err != nil || !bytes.Equal(child.ParentSnapshot, preparation.ParentSnapshot) {
		t.Fatalf("legacy attempt preparation: %v", err)
	}
}

// TestRequestPreparationAttemptSharesParentReference proves an attempt child
// references the root's parent blob instead of copying the transcript.
func TestRequestPreparationAttemptSharesParentReference(t *testing.T) {
	r, _, _, record, preparation := preparationFixture(t)
	r.parentSnapshotBlob = true
	ctx := context.Background()
	mustSavePreparation(t, r, record.Request.Scope, record.Request.ID, preparation)
	attempt := mustBeginRequestAttempt(t, r, record.Request.Scope, record.Request.ID, "", record.UpdatedAt)
	child := mustRead(t, r, record.Request.Scope, attempt.ID)
	stored := storedPreparationOf(t, child)
	if stored.ParentSnapshotRef == nil || *stored.ParentSnapshotRef != r.newParentSnapshotRef(record.Request.Scope, preparation.ParentSnapshot) || len(stored.ParentSnapshot) != 0 {
		t.Fatal("attempt does not share the parent reference")
	}
	got, err := r.LoadRequestPreparation(ctx, record.Request.Scope, attempt.ID)
	if err != nil || !bytes.Equal(got.ParentSnapshot, preparation.ParentSnapshot) {
		t.Fatalf("attempt preparation: %v", err)
	}
}

// TestRequestPreparationRejectsTamperedOrMissingParent fails closed, as
// ErrCorrupt and never as a missing preparation, on any parent blob or
// reference that does not verify.
func TestRequestPreparationRejectsTamperedOrMissingParent(t *testing.T) {
	cases := map[string]parentCorruption{
		"missing blob":         deleteParentBlob,
		"tampered blob":        tamperParentBlob,
		"wrong digest":         zeroParentDigest,
		"wrong length":         extendParentLength,
		"other scope":          moveParentToForeignScope,
		"inline and reference": inlineAndReferenceParent,
		"other blob": func(t *testing.T, r *Repository, _ *memoryBlobs, record Record, _ RequestPreparation, stored storedRequestPreparation) {
			other, err := r.writeBlob(context.Background(), r.parentSnapshotStream(record.Request.Scope), []byte(`{"other":true}`))
			if err != nil {
				t.Fatal(err)
			}
			stored.ParentSnapshotRef.Blob = other
			rewritePreparation(t, r, record, stored)
		},
		"malformed key": func(t *testing.T, r *Repository, _ *memoryBlobs, record Record, _ RequestPreparation, stored storedRequestPreparation) {
			stored.ParentSnapshotRef.Blob += "0"
			rewritePreparation(t, r, record, stored)
		},
		"provenance without parent": func(t *testing.T, r *Repository, _ *memoryBlobs, record Record, _ RequestPreparation, stored storedRequestPreparation) {
			stored.ParentSnapshotRef = nil
			stored.ParentProvenance = []state.ProviderStateProvenance{{}}
			rewritePreparation(t, r, record, stored)
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			r, record, _ := corruptedPreparation(t, corrupt)
			requireErrorIsNot(t, errOf(r.LoadRequestPreparation(context.Background(), record.Request.Scope, record.Request.ID)), ErrCorrupt, contracts.ErrNotFound, "accepted a corrupt parent")
		})
	}
}

// parentCorruption damages a saved preparation's parent snapshot or its
// stored reference.
type parentCorruption func(*testing.T, *Repository, *memoryBlobs, Record, RequestPreparation, storedRequestPreparation)

// corruptedPreparation saves the fixture preparation with its parent in a
// blob, applies corrupt to the stored state and returns a reopened
// repository over the result.
func corruptedPreparation(t *testing.T, corrupt parentCorruption) (*Repository, Record, RequestPreparation) {
	t.Helper()
	r, table, blobs, record, preparation := preparationFixture(t)
	r.parentSnapshotBlob = true
	mustSavePreparation(t, r, record.Request.Scope, record.Request.ID, preparation)
	current := mustRead(t, r, record.Request.Scope, record.Request.ID)
	corrupt(t, r, blobs, current, preparation, storedPreparationOf(t, current))
	return reopen(t, table, blobs), record, preparation
}

func deleteParentBlob(_ *testing.T, _ *Repository, blobs *memoryBlobs, _ Record, _ RequestPreparation, stored storedRequestPreparation) {
	delete(blobs.values, blob.BlobKey(stored.ParentSnapshotRef.Blob))
}

func tamperParentBlob(_ *testing.T, _ *Repository, blobs *memoryBlobs, _ Record, _ RequestPreparation, stored storedRequestPreparation) {
	blobs.values[blob.BlobKey(stored.ParentSnapshotRef.Blob)][20] ^= 1
}

func zeroParentDigest(t *testing.T, r *Repository, _ *memoryBlobs, record Record, _ RequestPreparation, stored storedRequestPreparation) {
	stored.ParentSnapshotRef.Digest = strings.Repeat("0", 64)
	rewritePreparation(t, r, record, stored)
}

func extendParentLength(t *testing.T, r *Repository, _ *memoryBlobs, record Record, _ RequestPreparation, stored storedRequestPreparation) {
	stored.ParentSnapshotRef.ByteLength++
	rewritePreparation(t, r, record, stored)
}

// moveParentToForeignScope points the record at an identical parent blob
// written under another tenant and project.
func moveParentToForeignScope(t *testing.T, r *Repository, _ *memoryBlobs, record Record, p RequestPreparation, stored storedRequestPreparation) {
	foreignScope := Scope{Tenant: "other-tenant", Project: "other-project"}
	foreign := r.newParentSnapshotRef(foreignScope, p.ParentSnapshot)
	mustNoError(t, errOf(r.writeBlob(context.Background(), r.parentSnapshotStream(foreignScope), p.ParentSnapshot)))
	stored.ParentSnapshotRef = &foreign
	rewritePreparation(t, r, record, stored)
}

func inlineAndReferenceParent(t *testing.T, r *Repository, _ *memoryBlobs, record Record, p RequestPreparation, stored storedRequestPreparation) {
	stored.ParentSnapshot = p.ParentSnapshot
	rewritePreparation(t, r, record, stored)
}

// TestRequestPreparationStorageSettingWritesAndReads: the default writes the
// parent inline, blob writes a reference, and either repository reads both.
func TestRequestPreparationStorageSettingWritesAndReads(t *testing.T) {
	for _, writerBlob := range []bool{false, true} {
		t.Run(fmt.Sprintf("writer-blob=%t", writerBlob), func(t *testing.T) {
			r, table, blobs, record, preparation := preparationFixture(t)
			if r.parentSnapshotBlob {
				t.Fatal("parent-snapshot blob storage is on by default")
			}
			r.parentSnapshotBlob = writerBlob
			ctx := context.Background()
			mustSavePreparation(t, r, record.Request.Scope, record.Request.ID, preparation)
			current := mustRead(t, r, record.Request.Scope, record.Request.ID)
			stored := storedPreparationOf(t, current)
			if (stored.ParentSnapshotRef != nil) != writerBlob || (len(stored.ParentSnapshot) != 0) == writerBlob {
				t.Fatalf("stored form: reference=%t inline=%t", stored.ParentSnapshotRef != nil, len(stored.ParentSnapshot) != 0)
			}
			for _, readerBlob := range []bool{false, true} {
				reader := reopen(t, table, blobs)
				reader.parentSnapshotBlob = readerBlob
				got, err := reader.LoadRequestPreparation(ctx, record.Request.Scope, record.Request.ID)
				if err != nil || !bytes.Equal(got.ParentSnapshot, preparation.ParentSnapshot) || !equalExecutionJSON(got, preparation) {
					t.Fatalf("reader blob=%t: %v", readerBlob, err)
				}
				// An identical save under the other setting accepts the winner
				// and writes nothing.
				forbidWrites(t, table, blobs, "identical save")
				if err := reader.SaveRequestPreparation(ctx, record.Request.Scope, record.Request.ID, preparation); err != nil {
					t.Fatalf("identical save, blob=%t: %v", readerBlob, err)
				}
				table.hook, blobs.hook = nil, nil
			}
		})
	}
}

// TestRequestPreparationIdenticalInitializersConverge: concurrent workers
// saving the same preparation, some with each storage setting, all succeed
// with one event.
func TestRequestPreparationIdenticalInitializersConverge(t *testing.T) {
	r, table, blobs, record, preparation := preparationFixture(t)
	ctx := context.Background()
	before := len(blobs.values)
	var group sync.WaitGroup
	for i := 0; i < 20; i++ {
		client := reopen(t, table, blobs)
		client.parentSnapshotBlob = i%2 == 0
		group.Add(1)
		go func() {
			defer group.Done()
			if err := client.SaveRequestPreparation(ctx, record.Request.Scope, record.Request.ID, preparation); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	current, err := r.Read(ctx, record.Request.Scope, record.Request.ID)
	if err != nil || current.Revision != record.Revision+1 {
		t.Fatalf("revision %d: %v", current.Revision, err)
	}
	// A losing writer may leave its unpublished record blob, as any competing
	// writer can, but no more than one parent blob exists: it is content
	// addressed. Writers of each form leave at most one record blob each.
	if n := len(blobs.values) - before; n < 1 || n > 3 {
		t.Fatalf("stored %d blobs", n)
	}
	if got, err := r.LoadRequestPreparation(ctx, record.Request.Scope, record.Request.ID); err != nil || !equalExecutionJSON(got, preparation) {
		t.Fatalf("converged preparation: %v", err)
	}
}

// A preparation keeps when its worker's configuration began serving, so
// workers can order configurations during a rollout (#1320). It is optional.
func TestRequestPreparationConfigActivation(t *testing.T) {
	r, _, _, record, preparation := preparationFixture(t)
	ctx := context.Background()
	invalid := preparation
	invalid.ConfigActivatedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	requireErrorIs(t, r.SaveRequestPreparation(ctx, record.Request.Scope, record.Request.ID, invalid), ErrInvalid, "invalid activation")
	preparation.ConfigActivatedAt = preparation.PreparedAt.Add(-time.Hour).In(time.FixedZone("x", 3600))
	mustSavePreparation(t, r, record.Request.Scope, record.Request.ID, preparation)
	got, err := r.LoadRequestPreparation(ctx, record.Request.Scope, record.Request.ID)
	if err != nil || !got.ConfigActivatedAt.Equal(preparation.ConfigActivatedAt) || got.ConfigActivatedAt.Location() != time.UTC {
		t.Fatalf("activation = %v, %v", got.ConfigActivatedAt, err)
	}
	encoded, err := json.Marshal(RequestPreparation{Version: 1, PreparedAt: preparation.PreparedAt})
	if err != nil || bytes.Contains(encoded, []byte("config_activated_at")) {
		t.Fatalf("unset activation encoded: %s", encoded)
	}
}
