package state

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/llm"
)

type durableMaterializeFixture struct {
	repository *durableMaterializeRepository
	reader     *durableMaterializeBlobReader
	codec      CheckpointBlobCodec
	now        time.Time
	rootID     CheckpointID
	childID    CheckpointID
}

func newDurableMaterializeFixture(t *testing.T) durableMaterializeFixture {
	t.Helper()
	now := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	codec := CheckpointBlobCodec{}
	reader := &durableMaterializeBlobReader{scope: "scope-a", values: make(map[BlobID][]byte)}
	repository := &durableMaterializeRepository{scope: "scope-a", rows: make(map[CheckpointID]DurableCheckpoint)}
	rootID := CheckpointID("root")
	childID := CheckpointID("child")
	rootDelta, err := codec.EncodeDelta([]llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "root"}}}})
	if err != nil {
		t.Fatal(err)
	}
	rootResponse, err := codec.EncodeResponse(nil)
	if err != nil {
		t.Fatal(err)
	}
	rootPatch, err := codec.EncodeSettingsPatch(SettingsPatch{Model: SetPatch("gpt-test"), ServiceClass: SetPatch(llm.ServiceClassStandard), Portability: SetPatch(llm.PortabilityStrict)})
	if err != nil {
		t.Fatal(err)
	}
	childDelta, err := codec.EncodeDelta([]llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "child"}}}})
	if err != nil {
		t.Fatal(err)
	}
	childResponse, err := codec.EncodeResponse([]llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "answer"}}}})
	if err != nil {
		t.Fatal(err)
	}
	childPatch, err := codec.EncodeSettingsPatch(SettingsPatch{})
	if err != nil {
		t.Fatal(err)
	}
	rootRefs := durableMaterializePutBlobs(reader, rootDelta, rootResponse, rootPatch)
	childRefs := durableMaterializePutBlobs(reader, childDelta, childResponse, childPatch)
	newCheckpoint := func(id CheckpointID, parent *CheckpointID, depth int32, operation string, refs [3]CheckpointBlobReference) DurableCheckpoint {
		return DurableCheckpoint{
			ID: id, ScopeID: "scope-a", PublicIDHMAC: sha256.Sum256([]byte(string(id) + "-public")), HandleKeyID: "key-1", ParentID: parent, Kind: CheckpointGeneration, Depth: depth, OriginOperationID: OperationID(operation),
			DeltaBlob: refs[0], ResponseBlob: refs[1], SettingsPatchBlob: refs[2], CanonicalLineageDigest: sha256.Sum256([]byte(string(id) + "-lineage")), MaterializedSettingsDigest: sha256.Sum256([]byte(string(id) + "-settings")), ToolFrontierDigest: sha256.Sum256([]byte(string(id) + "-frontier")), SchemaVersion: 1, CompilerEpoch: "checkpoint-v1", CreatedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour),
		}
	}
	root := newCheckpoint(rootID, nil, 0, "operation-root", rootRefs)
	child := newCheckpoint(childID, &rootID, 1, "operation-child", childRefs)
	repository.rows[rootID] = root
	repository.rows[childID] = child
	return durableMaterializeFixture{repository: repository, reader: reader, codec: codec, now: now, rootID: rootID, childID: childID}
}

func durableMaterializePutBlobs(reader *durableMaterializeBlobReader, values ...[]byte) [3]CheckpointBlobReference {
	refs := [3]CheckpointBlobReference{}
	for index, value := range values {
		id := BlobID(fmt.Sprintf("blob-%d-%d", len(reader.values), index))
		digest := sha256.Sum256(value)
		refs[index] = CheckpointBlobReference{ID: id, Digest: digest, ByteLength: int64(len(value)), MediaType: "application/json"}
		reader.values[id] = append([]byte(nil), value...)
	}
	return refs
}

func TestDurableCheckpointMaterializerReplaysScopedRows(t *testing.T) {
	fixture := newDurableMaterializeFixture(t)
	materializer := &DurableCheckpointMaterializer{Repository: fixture.repository, Blobs: fixture.reader, Codec: fixture.codec, Now: func() time.Time { return fixture.now }}
	got, err := materializer.Materialize(context.Background(), "scope-a", fixture.childID, MaterializeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Handle != Handle(fixture.childID) || got.Tenant != "scope-a" || got.Depth != 1 || len(got.Items) != 3 {
		t.Fatalf("materialized state = %#v", got)
	}
	if got.Items[0].(llm.Message).Content[0].(llm.TextPart).Text != "root" || got.Items[2].(llm.Message).Content[0].(llm.TextPart).Text != "answer" {
		t.Fatalf("materialized items = %#v", got.Items)
	}
	if got.Settings.Model != "gpt-test" || len(got.Lineage) != 2 || got.Lineage[0] != Handle(fixture.rootID) {
		t.Fatalf("materialized settings/lineage = %#v/%#v", got.Settings, got.Lineage)
	}
}

func TestDurableCheckpointMaterializerVerifiesOpaqueHandleAndScope(t *testing.T) {
	fixture := newDurableMaterializeFixture(t)
	keyring, err := NewKeyring([]Key{{ID: "key-1", Secret: []byte("01234567890123456789012345678901"), Primary: true}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	checkpointUUID := uuidLikeCheckpointID(fixture.childID)
	handle, err := keyring.IssueCheckpointHandle("scope-a", checkpointUUID)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := keyring.VerifyCheckpointHandle(context.Background(), "scope-a", handle)
	if err != nil || verified != checkpointUUID {
		t.Fatalf("verified checkpoint ID = %q, %v; want %q", verified, err, checkpointUUID)
	}
	if _, err := keyring.VerifyCheckpointHandle(context.Background(), "scope-b", handle); err == nil {
		t.Fatal("cross-scope opaque handle was accepted")
	}
	materializer := &DurableCheckpointMaterializer{Repository: fixture.repository, Blobs: fixture.reader, HandleVerifier: staticCheckpointHandleVerifier{id: fixture.childID}, Now: func() time.Time { return fixture.now }}
	if got, err := materializer.MaterializeHandle(context.Background(), "scope-a", "opaque-handle", MaterializeLimits{}); err != nil || got.Handle != Handle("opaque-handle") {
		t.Fatalf("materialize opaque handle = %#v, %v", got, err)
	}
}

func uuidLikeCheckpointID(id CheckpointID) CheckpointID {
	return CheckpointID(uuid.NewSHA1(uuid.NameSpaceURL, []byte(id)).String())
}

func TestDurableCheckpointMaterializerFailsClosedOnBlobFaultsAndCycles(t *testing.T) {
	fixture := newDurableMaterializeFixture(t)
	materializer := &DurableCheckpointMaterializer{Repository: fixture.repository, Blobs: fixture.reader, Now: func() time.Time { return fixture.now }}
	fixture.reader.fail = errors.New("blob backend unavailable")
	if _, err := materializer.Materialize(context.Background(), "scope-a", fixture.childID, MaterializeLimits{}); err == nil || errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("blob backend fault classification = %v, want retryable storage failure", err)
	}
	fixture.reader.fail = nil
	fixture.repository.rows[fixture.childID] = func() DurableCheckpoint {
		row := fixture.repository.rows[fixture.childID]
		parent := fixture.childID
		row.ParentID = &parent
		row.Depth = 1
		return row
	}()
	if _, err := materializer.Materialize(context.Background(), "scope-a", fixture.childID, MaterializeLimits{}); err == nil || !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("self-parent cycle classification = %v, want ErrInvalidCheckpoint", err)
	}
}

func TestDurableCheckpointMaterializerStopsAtVerifiedPeriodicSnapshot(t *testing.T) {
	now := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	codec := CheckpointBlobCodec{}
	reader := &durableMaterializeBlobReader{scope: "scope-a", values: make(map[BlobID][]byte)}
	repository := &durableMaterializeRepository{scope: "scope-a", rows: make(map[CheckpointID]DurableCheckpoint)}
	lineage := make([]Handle, 0, 256)
	items := make([]llm.Item, 0, 256)
	var parent *CheckpointID
	for depth := int32(0); depth < 256; depth++ {
		id := CheckpointID(fmt.Sprintf("checkpoint-%03d", depth))
		lineage = append(lineage, Handle(id))
		item := llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: string(id)}}}
		items = append(items, item)
		patch := SettingsPatch{}
		if depth == 0 {
			patch.Model = SetPatch("gpt-test")
		}
		bundle, err := codec.EncodeBundle(CheckpointBundle{Delta: []llm.Item{item}, SettingsPatch: patch})
		if err != nil {
			t.Fatal(err)
		}
		bundleRef := durableMaterializePutBlob(reader, BlobID("bundle-"+string(id)), bundle)
		lineageBytes, _ := json.Marshal(lineage)
		frontier, err := ValidateTranscript(items)
		if err != nil {
			t.Fatal(err)
		}
		frontierBytes, _ := json.Marshal(frontier)
		settingsDigest, err := codec.DigestMaterializedSettings(RootModelState("gpt-test"))
		if err != nil {
			t.Fatal(err)
		}
		row := DurableCheckpoint{
			ID: id, ScopeID: "scope-a", PublicIDHMAC: sha256.Sum256([]byte(string(id) + "-public")),
			HandleKeyID: "key-1", ParentID: parent, Kind: CheckpointGeneration, Depth: depth,
			OriginOperationID: OperationID("operation-" + string(id)),
			DeltaBlob:         bundleRef, ResponseBlob: bundleRef, SettingsPatchBlob: bundleRef,
			CanonicalLineageDigest:     sha256.Sum256(lineageBytes),
			MaterializedSettingsDigest: settingsDigest,
			ToolFrontierDigest:         sha256.Sum256(frontierBytes),
			SchemaVersion:              1, CompilerEpoch: "checkpoint-v1",
			CreatedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour),
		}
		if depth == 240 {
			snapshot := NewCheckpointSnapshot(MaterializedState{
				Items: append([]llm.Item(nil), items...), Settings: RootModelState("gpt-test"),
				Depth: depth, Lineage: append([]Handle(nil), lineage...),
			})
			encoded, err := codec.EncodeSnapshot(*snapshot)
			if err != nil {
				t.Fatal(err)
			}
			ref := durableMaterializePutBlob(reader, "snapshot-240", encoded)
			row.MaterializedSnapshotBlob = &ref
		}
		repository.rows[id] = row
		parentValue := id
		parent = &parentValue
	}
	reader.delay = time.Millisecond
	materializer := &DurableCheckpointMaterializer{
		Repository: repository, Blobs: reader, Codec: codec, Now: func() time.Time { return now },
	}
	got, err := materializer.Materialize(context.Background(), "scope-a", "checkpoint-255", MaterializeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Depth != 255 || len(got.Items) != 256 || len(got.Lineage) != 256 {
		t.Fatalf("materialized periodic snapshot = depth %d, items %d, lineage %d", got.Depth, len(got.Items), len(got.Lineage))
	}
	if repository.lineageCalls != 1 || repository.getCalls != 0 {
		t.Fatalf("repository reads = lineage %d, point %d; want one batched lineage read", repository.lineageCalls, repository.getCalls)
	}
	if reads := reader.reads.Load(); reads != 16 {
		t.Fatalf("blob reads = %d, want one snapshot plus 15 suffix bundles", reads)
	}
	if peak := reader.peak.Load(); peak < 2 {
		t.Fatalf("peak concurrent blob reads = %d, want bounded parallel fetch", peak)
	}
}

func TestDurableCheckpointMaterializerFallsBackFromMismatchedSnapshot(t *testing.T) {
	fixture := newDurableMaterializeFixture(t)
	snapshot := NewCheckpointSnapshot(MaterializedState{
		Items:    []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "wrong"}}}},
		Settings: RootModelState("gpt-test"), Depth: 1, Lineage: []Handle{"wrong-root", Handle(fixture.childID)},
	})
	encoded, err := fixture.codec.EncodeSnapshot(*snapshot)
	if err != nil {
		t.Fatal(err)
	}
	ref := durableMaterializePutBlob(fixture.reader, "mismatched-snapshot", encoded)
	row := fixture.repository.rows[fixture.childID]
	row.MaterializedSnapshotBlob = &ref
	fixture.repository.rows[fixture.childID] = row
	materializer := &DurableCheckpointMaterializer{
		Repository: fixture.repository, Blobs: fixture.reader, Codec: fixture.codec,
		Now: func() time.Time { return fixture.now },
	}
	got, err := materializer.Materialize(context.Background(), "scope-a", fixture.childID, MaterializeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Items) != 3 || got.Lineage[0] != Handle(fixture.rootID) {
		t.Fatalf("fallback materialized state = %#v", got)
	}
	if fixture.repository.getCalls != 1 {
		t.Fatalf("snapshot fallback point reads = %d, want parent fallback", fixture.repository.getCalls)
	}
}

func durableMaterializePutBlob(reader *durableMaterializeBlobReader, id BlobID, value []byte) CheckpointBlobReference {
	digest := sha256.Sum256(value)
	reader.values[id] = append([]byte(nil), value...)
	return CheckpointBlobReference{ID: id, Digest: digest, ByteLength: int64(len(value)), MediaType: "application/json"}
}

type durableMaterializeRepository struct {
	scope        string
	rows         map[CheckpointID]DurableCheckpoint
	getCalls     int
	lineageCalls int
}

func (repository *durableMaterializeRepository) Get(_ context.Context, scope string, id CheckpointID) (DurableCheckpoint, error) {
	repository.getCalls++
	if scope != repository.scope {
		return DurableCheckpoint{}, ErrTenantMismatch
	}
	row, ok := repository.rows[id]
	if !ok {
		return DurableCheckpoint{}, ErrNotFound
	}
	return row, nil
}
func (repository *durableMaterializeRepository) GetLineage(_ context.Context, scope string, id CheckpointID, maxRows int) ([]DurableCheckpoint, error) {
	repository.lineageCalls++
	if scope != repository.scope {
		return nil, ErrTenantMismatch
	}
	rows := make([]DurableCheckpoint, 0, min(maxRows, 16))
	current := id
	for current != "" && len(rows) < maxRows {
		row, ok := repository.rows[current]
		if !ok {
			return nil, ErrNotFound
		}
		rows = append(rows, row)
		if row.MaterializedSnapshotBlob != nil || row.ParentID == nil {
			break
		}
		current = *row.ParentID
	}
	return rows, nil
}

func (repository *durableMaterializeRepository) BeginCheckpoint(context.Context) (CheckpointUnitOfWork, error) {
	return nil, errors.New("not implemented in materializer test")
}

type durableMaterializeBlobReader struct {
	scope      string
	values     map[BlobID][]byte
	fail       error
	delay      time.Duration
	reads      atomic.Int64
	concurrent atomic.Int64
	peak       atomic.Int64
}

type staticCheckpointHandleVerifier struct{ id CheckpointID }

func (verifier staticCheckpointHandleVerifier) VerifyCheckpointHandle(_ context.Context, scope, _ string) (CheckpointID, error) {
	if scope != "scope-a" {
		return "", ErrTenantMismatch
	}
	return verifier.id, nil
}

func (reader *durableMaterializeBlobReader) Read(ctx context.Context, scope string, reference CheckpointBlobReference) ([]byte, error) {
	reader.reads.Add(1)
	current := reader.concurrent.Add(1)
	defer reader.concurrent.Add(-1)
	for {
		peak := reader.peak.Load()
		if current <= peak || reader.peak.CompareAndSwap(peak, current) {
			break
		}
	}
	if reader.delay > 0 {
		timer := time.NewTimer(reader.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if reader.fail != nil {
		return nil, reader.fail
	}
	if scope != reader.scope {
		return nil, ErrTenantMismatch
	}
	value, ok := reader.values[reference.ID]
	if !ok {
		return nil, ErrNotFound
	}
	if int64(len(value)) != reference.ByteLength || sha256.Sum256(value) != reference.Digest {
		return nil, errors.New("blob digest mismatch")
	}
	return append([]byte(nil), value...), nil
}
