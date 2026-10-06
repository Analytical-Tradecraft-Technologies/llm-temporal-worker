package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// snapshotLineageFixture holds the same durable lineage twice: plain has no
// snapshot on any row (data written before cadence snapshots existed) and
// snapshotted carries one wherever the test asks. Both share one blob store,
// so a snapshot-shortened walk can be compared with the full walk it replaces.
type snapshotLineageFixture struct {
	t           *testing.T
	now         time.Time
	codec       CheckpointBlobCodec
	blobs       *countingBlobReader
	plain       *countingRepository
	snapshotted *countingRepository
}

type countingRepository struct {
	durableMaterializeRepository
	gets int
}

func (repository *countingRepository) Get(ctx context.Context, scope string, id CheckpointID) (DurableCheckpoint, error) {
	repository.gets++
	return repository.durableMaterializeRepository.Get(ctx, scope, id)
}

type countingBlobReader struct {
	durableMaterializeBlobReader
	reads int
}

func (reader *countingBlobReader) Read(ctx context.Context, scope string, reference CheckpointBlobReference) ([]byte, error) {
	reader.reads++
	return reader.durableMaterializeBlobReader.Read(ctx, scope, reference)
}

func newSnapshotLineageFixture(t *testing.T) *snapshotLineageFixture {
	t.Helper()
	newRepository := func() *countingRepository {
		return &countingRepository{durableMaterializeRepository: durableMaterializeRepository{scope: "scope-a", rows: make(map[CheckpointID]DurableCheckpoint)}}
	}
	return &snapshotLineageFixture{
		t: t, now: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC),
		blobs: &countingBlobReader{durableMaterializeBlobReader: durableMaterializeBlobReader{scope: "scope-a", values: make(map[BlobID][]byte)}},
		plain: newRepository(), snapshotted: newRepository(),
	}
}

func (fixture *snapshotLineageFixture) materializer(repository *countingRepository) *DurableCheckpointMaterializer {
	return &DurableCheckpointMaterializer{Repository: repository, Blobs: fixture.blobs, Codec: fixture.codec, Now: func() time.Time { return fixture.now }}
}

func (fixture *snapshotLineageFixture) putBlob(data []byte) CheckpointBlobReference {
	digest := sha256.Sum256(data)
	id := BlobID(fmt.Sprintf("blob-%d", len(fixture.blobs.values)))
	fixture.blobs.values[id] = append([]byte(nil), data...)
	return CheckpointBlobReference{ID: id, Digest: digest, ByteLength: int64(len(data)), MediaType: "application/json"}
}

// add publishes one row the way checkpoint publication does: the lineage
// digest is derived from the real handle list and an optional snapshot is the
// encoded full materialization of that row.
func (fixture *snapshotLineageFixture) add(id string, parent string, delta, output []llm.Item, patch SettingsPatch, snapshot bool) CheckpointID {
	t := fixture.t
	t.Helper()
	encode := func(data []byte, err error) CheckpointBlobReference {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return fixture.putBlob(data)
	}
	row := DurableCheckpoint{
		ID: CheckpointID(id), ScopeID: "scope-a", PublicIDHMAC: sha256.Sum256([]byte(id + "-public")), HandleKeyID: "key-1", Kind: CheckpointGeneration, OriginOperationID: OperationID("operation-" + id),
		DeltaBlob: encode(fixture.codec.EncodeDelta(delta)), ResponseBlob: encode(fixture.codec.EncodeResponse(output)), SettingsPatchBlob: encode(fixture.codec.EncodeSettingsPatch(patch)),
		MaterializedSettingsDigest: sha256.Sum256([]byte(id + "-settings")), ToolFrontierDigest: sha256.Sum256([]byte(id + "-frontier")), SchemaVersion: 1, CompilerEpoch: "checkpoint-v1",
		CreatedAt: fixture.now.Add(-time.Minute), ExpiresAt: fixture.now.Add(time.Hour),
	}
	lineage := []Handle{Handle(id)}
	if parent != "" {
		parentRow := fixture.plain.rows[CheckpointID(parent)]
		parentID := parentRow.ID
		row.ParentID, row.Depth = &parentID, parentRow.Depth+1
		full, err := fixture.materializer(fixture.plain).Materialize(context.Background(), "scope-a", parentID, MaterializeLimits{})
		if err != nil {
			t.Fatal(err)
		}
		lineage = append(append([]Handle(nil), full.Lineage...), Handle(id))
	}
	encodedLineage, err := json.Marshal(lineage)
	if err != nil {
		t.Fatal(err)
	}
	row.CanonicalLineageDigest = sha256.Sum256(encodedLineage)
	fixture.plain.rows[row.ID] = row
	if snapshot {
		full, err := fixture.materializer(fixture.plain).Materialize(context.Background(), "scope-a", row.ID, MaterializeLimits{})
		if err != nil {
			t.Fatal(err)
		}
		reference := encode(fixture.codec.EncodeSnapshot(*NewCheckpointSnapshot(full)))
		row.MaterializedSnapshotBlob = &reference
	}
	fixture.snapshotted.rows[row.ID] = row
	return row.ID
}

func snapshotLineageMessage(actor llm.Actor, text string) llm.Item {
	return llm.Message{Actor: actor, Content: []llm.Part{llm.TextPart{Text: text}}}
}

// chain appends turns parent+1..through to one branch. Every row whose depth
// is a positive multiple of interval is snapshotted; interval 0 writes none.
// The final turn leaves a tool call outstanding when pending is set.
func (fixture *snapshotLineageFixture) chain(branch, parent string, from, through, interval int, pending bool) string {
	for depth := from; depth <= through; depth++ {
		id := fmt.Sprintf("%s-%02d", branch, depth)
		patch := SettingsPatch{}
		switch depth {
		case 0:
			patch = SettingsPatch{Model: SetPatch("gpt-test"), ServiceClass: SetPatch(llm.ServiceClassStandard), Portability: SetPatch(llm.PortabilityStrict)}
		case 3:
			temperature := llm.DecimalV1("0.25")
			patch = SettingsPatch{Model: SetPatch("gpt-next"), TemperatureDecimal: SetPatch(temperature), Instructions: SetPatch([]llm.Instruction{{Text: "be brief"}})}
		case 11:
			patch = SettingsPatch{Instructions: Patch[[]llm.Instruction]{Clear: true}}
		}
		output := []llm.Item{snapshotLineageMessage(llm.ActorModel, fmt.Sprintf("%s answer %d", branch, depth))}
		if pending && depth == through {
			output = append(output, llm.ToolCall{ID: "call-" + id, Name: "lookup", Arguments: json.RawMessage(`{"q":"x"}`)})
		}
		parent = string(fixture.add(id, parent, []llm.Item{snapshotLineageMessage(llm.ActorHuman, fmt.Sprintf("%s turn %d", branch, depth))}, output, patch, interval > 0 && depth > 0 && depth%interval == 0))
	}
	return parent
}

// read materializes one checkpoint and reports the row and blob reads it cost.
func (fixture *snapshotLineageFixture) read(repository *countingRepository, id string, limits MaterializeLimits) (MaterializedState, int, int, error) {
	repository.gets, fixture.blobs.reads = 0, 0
	result, err := fixture.materializer(repository).Materialize(context.Background(), "scope-a", CheckpointID(id), limits)
	return result, repository.gets, fixture.blobs.reads, err
}

// requireSameMaterialization compares the complete materialized view: items,
// settings, pending tool frontier, depth and lineage must be byte-identical.
func (fixture *snapshotLineageFixture) requireSameMaterialization(id string) MaterializedState {
	t := fixture.t
	t.Helper()
	want, _, _, err := fixture.read(fixture.plain, id, MaterializeLimits{})
	if err != nil {
		t.Fatalf("full walk of %s: %v", id, err)
	}
	got, _, _, err := fixture.read(fixture.snapshotted, id, MaterializeLimits{})
	if err != nil {
		t.Fatalf("snapshot walk of %s: %v", id, err)
	}
	wantBytes, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	gotBytes, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotBytes, wantBytes) {
		t.Fatalf("snapshot walk of %s differs from the full walk:\n got %s\nwant %s", id, gotBytes, wantBytes)
	}
	return got
}

func TestDurableCheckpointMaterializerStopsAtNewestSnapshot(t *testing.T) {
	const depth, interval = 40, 8
	fixture := newSnapshotLineageFixture(t)
	fixture.chain("main", "", 0, depth, interval, true)

	// A lineage without snapshots still needs every row and three blobs each.
	if _, gets, reads, err := fixture.read(fixture.plain, "main-40", MaterializeLimits{}); err != nil || gets != depth+1 || reads != 3*(depth+1) {
		t.Fatalf("full walk gets=%d reads=%d err=%v", gets, reads, err)
	}
	for leaf := 0; leaf <= depth; leaf++ {
		id := fmt.Sprintf("main-%02d", leaf)
		got := fixture.requireSameMaterialization(id)
		if got.Depth != int32(leaf) || len(got.Lineage) != leaf+1 || len(got.Items) < 2*(leaf+1) {
			t.Fatalf("%s depth=%d lineage=%d items=%d", id, got.Depth, len(got.Lineage), len(got.Items))
		}
		// The walk reads the rows newer than the newest snapshot (three blobs
		// each) plus the snapshot row and its single blob, whatever the depth.
		// The first interval has no snapshot yet and reads up to the root.
		_, gets, reads, err := fixture.read(fixture.snapshotted, id, MaterializeLimits{})
		if err != nil || gets > interval || reads > 3*interval {
			t.Fatalf("%s gets=%d reads=%d err=%v; want at most %d rows and %d blobs", id, gets, reads, err, interval, 3*interval)
		}
	}
	leaf := fixture.requireSameMaterialization("main-40")
	if len(leaf.PendingToolCalls) != 1 || leaf.PendingToolCalls[0] != "call-main-40" || leaf.Settings.Model != "gpt-next" || leaf.Settings.TemperatureDecimal == nil || leaf.Settings.Instructions != nil {
		t.Fatalf("leaf frontier/settings = %#v / %#v", leaf.PendingToolCalls, leaf.Settings)
	}
	if _, gets, reads, _ := fixture.read(fixture.snapshotted, "main-40", MaterializeLimits{}); gets != 1 || reads != 1 {
		t.Fatalf("snapshot leaf gets=%d reads=%d; want one row and one blob", gets, reads)
	}
	// Ancestors older than the snapshot are not needed once it is verified.
	for old := 0; old < 32; old++ {
		delete(fixture.snapshotted.rows, CheckpointID(fmt.Sprintf("main-%02d", old)))
	}
	if got, _, _, err := fixture.read(fixture.snapshotted, "main-39", MaterializeLimits{}); err != nil || got.Depth != 39 || len(got.Lineage) != 40 {
		t.Fatalf("materialize above a retained snapshot: depth=%d lineage=%d err=%v", got.Depth, len(got.Lineage), err)
	}
}

func TestDurableCheckpointMaterializerForksAroundSnapshots(t *testing.T) {
	fixture := newSnapshotLineageFixture(t)
	fixture.chain("main", "", 0, 20, 8, false)
	// One fork leaves before the first snapshot and crosses its own cadence
	// boundary; the others are siblings on, and a branch after, a snapshot row.
	fixture.chain("early", "main-05", 6, 18, 8, false)
	fixture.chain("sibling", "main-08", 9, 9, 8, false)
	fixture.chain("late", "main-10", 11, 17, 8, true)
	for id, want := range map[string]struct {
		depth        int32
		first, last  string
		absent       Handle
		gets, blobs  int
		pendingCalls int
	}{
		"main-09":    {9, "main turn 0", "main answer 9", "sibling-09", 2, 4, 0},
		"sibling-09": {9, "main turn 0", "sibling answer 9", "main-09", 2, 4, 0},
		"early-07":   {7, "main turn 0", "early answer 7", "main-06", 8, 24, 0},
		"early-08":   {8, "main turn 0", "early answer 8", "main-08", 1, 1, 0},
		"early-18":   {18, "main turn 0", "early answer 18", "main-16", 3, 7, 0},
		"late-11":    {11, "main turn 0", "late answer 11", "main-11", 4, 10, 0},
		"late-16":    {16, "main turn 0", "late answer 16", "main-16", 1, 1, 0},
		"late-17":    {17, "main turn 0", "late answer 17", "main-16", 2, 4, 1},
	} {
		got := fixture.requireSameMaterialization(id)
		if got.Depth != want.depth || len(got.Lineage) != int(want.depth)+1 || got.Lineage[len(got.Lineage)-1] != Handle(id) || len(got.PendingToolCalls) != want.pendingCalls {
			t.Fatalf("%s depth=%d lineage=%v pending=%v", id, got.Depth, got.Lineage, got.PendingToolCalls)
		}
		for _, handle := range got.Lineage {
			if handle == want.absent {
				t.Fatalf("%s lineage crossed into another branch: %v", id, got.Lineage)
			}
		}
		if text := got.Items[0].(llm.Message).Content[0].(llm.TextPart).Text; text != want.first {
			t.Fatalf("%s first item = %q", id, text)
		}
		last := got.Items[len(got.Items)-1]
		if want.pendingCalls > 0 {
			last = got.Items[len(got.Items)-2]
		}
		if text := last.(llm.Message).Content[0].(llm.TextPart).Text; text != want.last {
			t.Fatalf("%s last message = %q", id, text)
		}
		if _, gets, reads, err := fixture.read(fixture.snapshotted, id, MaterializeLimits{}); err != nil || gets != want.gets || reads != want.blobs {
			t.Fatalf("%s gets=%d reads=%d err=%v; want %d/%d", id, gets, reads, err, want.gets, want.blobs)
		}
	}
}

func TestDurableCheckpointMaterializerRejectsUnusableSnapshotRows(t *testing.T) {
	type mutation func(fixture *snapshotLineageFixture, row *DurableCheckpoint)
	replaceSnapshot := func(change func(*CheckpointSnapshot)) mutation {
		return func(fixture *snapshotLineageFixture, row *DurableCheckpoint) {
			snapshot, err := fixture.codec.DecodeSnapshot(fixture.blobs.values[row.MaterializedSnapshotBlob.ID])
			if err != nil {
				fixture.t.Fatal(err)
			}
			change(&snapshot)
			data, err := fixture.codec.EncodeSnapshot(*NewCheckpointSnapshot(MaterializedState{Items: snapshot.Items, Settings: snapshot.Settings, Depth: snapshot.Depth, Lineage: snapshot.Lineage}))
			if err != nil {
				fixture.t.Fatal(err)
			}
			reference := fixture.putBlob(data)
			row.MaterializedSnapshotBlob = &reference
		}
	}
	for name, test := range map[string]struct {
		mutate mutation
		want   error
		limits MaterializeLimits
		scope  string
	}{
		"wrong scope": {mutate: func(_ *snapshotLineageFixture, row *DurableCheckpoint) { row.ScopeID = "scope-b" }, want: ErrTenantMismatch},
		"other scope": {mutate: func(*snapshotLineageFixture, *DurableCheckpoint) {}, want: ErrTenantMismatch, scope: "scope-b"},
		"depth limit": {mutate: func(*snapshotLineageFixture, *DurableCheckpoint) {}, want: ErrLimitExceeded, limits: MaterializeLimits{MaxDepth: 8}},
		"row limit":   {mutate: func(*snapshotLineageFixture, *DurableCheckpoint) {}, want: ErrLimitExceeded, limits: MaterializeLimits{MaxRows: 9}},
		"item limit":  {mutate: func(*snapshotLineageFixture, *DurableCheckpoint) {}, want: ErrLimitExceeded, limits: MaterializeLimits{MaxItems: 10}},
		"media type": {mutate: func(_ *snapshotLineageFixture, row *DurableCheckpoint) {
			reference := *row.MaterializedSnapshotBlob
			reference.MediaType = "text/plain"
			row.MaterializedSnapshotBlob = &reference
		}},
		"blob digest": {mutate: func(_ *snapshotLineageFixture, row *DurableCheckpoint) {
			reference := *row.MaterializedSnapshotBlob
			reference.Digest[0] ^= 1
			row.MaterializedSnapshotBlob = &reference
		}},
		"missing blob": {mutate: func(fixture *snapshotLineageFixture, row *DurableCheckpoint) {
			delete(fixture.blobs.values, row.MaterializedSnapshotBlob.ID)
		}},
		// The snapshot stays internally consistent; only the row's parent link
		// contradicts the lineage the snapshot stands in for.
		"row parent": {mutate: func(_ *snapshotLineageFixture, row *DurableCheckpoint) {
			parent := CheckpointID("main-03")
			row.ParentID = &parent
		}},
		"row depth": {mutate: func(_ *snapshotLineageFixture, row *DurableCheckpoint) { row.Depth = 7 }},
		"snapshot depth": {mutate: replaceSnapshot(func(snapshot *CheckpointSnapshot) {
			snapshot.Depth--
		})},
		"snapshot lineage": {mutate: replaceSnapshot(func(snapshot *CheckpointSnapshot) {
			snapshot.Lineage[2] = "main-99"
		})},
		"snapshot of another row": {mutate: replaceSnapshot(func(snapshot *CheckpointSnapshot) {
			snapshot.Lineage[len(snapshot.Lineage)-1] = "main-99"
		})},
		"snapshot cycle": {mutate: func(fixture *snapshotLineageFixture, row *DurableCheckpoint) {
			replaceSnapshot(func(snapshot *CheckpointSnapshot) { snapshot.Lineage[2] = "main-10" })(fixture, row)
			snapshot, _ := fixture.codec.DecodeSnapshot(fixture.blobs.values[row.MaterializedSnapshotBlob.ID])
			lineage, _ := json.Marshal(snapshot.Lineage)
			row.CanonicalLineageDigest = sha256.Sum256(lineage)
		}},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newSnapshotLineageFixture(t)
			fixture.chain("main", "", 0, 10, 8, false)
			// The leaf is two rows newer than the snapshot row under test.
			row := fixture.snapshotted.rows["main-08"]
			test.mutate(fixture, &row)
			fixture.snapshotted.rows["main-08"] = row
			scope := "scope-a"
			if test.scope != "" {
				scope = test.scope
			}
			_, err := fixture.materializer(fixture.snapshotted).Materialize(context.Background(), scope, "main-10", test.limits)
			if err == nil || (test.want != nil && !errors.Is(err, test.want)) {
				t.Fatalf("materialize = %v, want %v", err, test.want)
			}
		})
	}
}

func TestDurableCheckpointMaterializerEnforcesOnlyRequestedRowExpiry(t *testing.T) {
	fixture := newSnapshotLineageFixture(t)
	fixture.chain("main", "", 0, 10, 8, false)
	// A retained ancestor past its own deadline does not block a live child.
	row := fixture.snapshotted.rows["main-08"]
	row.ExpiresAt = fixture.now
	fixture.snapshotted.rows["main-08"] = row
	if _, err := fixture.materializer(fixture.snapshotted).Materialize(context.Background(), "scope-a", "main-10", MaterializeLimits{}); err != nil {
		t.Fatalf("materialize with expired ancestor = %v", err)
	}
	// The requested row's own deadline still gates the read.
	leaf := fixture.snapshotted.rows["main-10"]
	leaf.ExpiresAt = fixture.now
	fixture.snapshotted.rows["main-10"] = leaf
	if _, err := fixture.materializer(fixture.snapshotted).Materialize(context.Background(), "scope-a", "main-10", MaterializeLimits{}); !errors.Is(err, ErrExpired) {
		t.Fatalf("materialize expired leaf = %v, want %v", err, ErrExpired)
	}
}
