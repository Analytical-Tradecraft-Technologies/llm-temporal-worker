package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

// countingCheckpointStore counts the checkpoint rows and checkpoint blobs the
// composed runtime reads through the real cloud checkpoint store.
type countingCheckpointStore struct {
	state.CheckpointStore
	gets, reads atomic.Int64
}

func (s *countingCheckpointStore) Get(ctx context.Context, scope string, id state.CheckpointID) (state.DurableCheckpoint, error) {
	s.gets.Add(1)
	return s.CheckpointStore.Get(ctx, scope, id)
}

func (s *countingCheckpointStore) Read(ctx context.Context, scope string, reference state.CheckpointBlobReference) ([]byte, error) {
	s.reads.Add(1)
	return s.CheckpointStore.Read(ctx, scope, reference)
}

// snapshotCadenceFixture drives whole conversations through the bounded cloud
// runtime and remembers the human turns each published checkpoint must replay.
type snapshotCadenceFixture struct {
	*boundedCloudFixture
	store *countingCheckpointStore
	turns map[llm.CheckpointHandle][]string
}

// snapshotCadenceWithoutSnapshots is a cadence no test lineage reaches, so the
// fixture publishes what Generate published before cadence snapshots existed.
const snapshotCadenceWithoutSnapshots = 1 << 30

func snapshotCadenceCloud(t *testing.T, interval int32) *snapshotCadenceFixture {
	t.Helper()
	f := &snapshotCadenceFixture{boundedCloudFixture: boundedCloud(t, false), turns: map[llm.CheckpointHandle][]string{}}
	f.store = &countingCheckpointStore{CheckpointStore: f.repository.Checkpoints()}
	f.cap.Checkpoints = CheckpointCapabilities{Repository: f.store, Blobs: f.store, BlobWriter: f.store, Materializer: &state.DurableCheckpointMaterializer{Repository: f.store, Blobs: f.store, HandleVerifier: f.options.Keyring, Now: f.cap.Clock}}
	f.options.Limits.SnapshotInterval = interval
	f.restart(t)
	return f
}

// turn runs one complete paid Generate (prepare, acquire, generate, complete)
// as a child of parent and returns the published handle.
func (f *snapshotCadenceFixture) turn(t *testing.T, parent *llm.CheckpointHandle, text string) llm.CheckpointHandle {
	t.Helper()
	f.now = f.now.Add(time.Second)
	f.request.OperationKey, f.request.Parent = text, parent
	f.request.Append = []llm.Item{preparationMessage(text)}
	var inherited []string
	if parent != nil {
		f.request.SettingsPatch = llm.SettingsPatchV1{}
		inherited = f.turns[*parent]
	}
	result := f.finish(t)
	if result.Generate == nil {
		t.Fatalf("turn %q published no checkpoint", text)
	}
	handle := result.Generate.Checkpoint.Handle
	f.turns[handle] = append(append([]string(nil), inherited...), text)
	return handle
}

// chain extends parent by count turns and returns every handle it published.
func (f *snapshotCadenceFixture) chain(t *testing.T, parent *llm.CheckpointHandle, branch string, count int) []llm.CheckpointHandle {
	t.Helper()
	handles := make([]llm.CheckpointHandle, 0, count)
	for index := 0; index < count; index++ {
		handle := f.turn(t, parent, fmt.Sprintf("%s-%d", branch, len(f.turns[snapshotCadenceParent(parent)])))
		handles, parent = append(handles, handle), &handle
	}
	return handles
}

func snapshotCadenceParent(value *llm.CheckpointHandle) llm.CheckpointHandle {
	if value == nil {
		return ""
	}
	return *value
}

// materialize replays one handle through the composed materializer and reports
// how many checkpoint rows and checkpoint blobs that single replay read.
func (f *snapshotCadenceFixture) materialize(t *testing.T, handle llm.CheckpointHandle) (state.MaterializedState, int64, int64) {
	t.Helper()
	f.store.gets.Store(0)
	f.store.reads.Store(0)
	got, err := f.cap.Checkpoints.Materializer.(state.CheckpointHandleMaterializer).MaterializeHandle(context.Background(), "trusted-scope", string(handle), f.options.Limits)
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	var humans []string
	for _, item := range got.Items {
		if message, ok := item.(llm.Message); ok && message.Actor == llm.ActorHuman {
			humans = append(humans, message.Content[0].(llm.TextPart).Text)
		}
	}
	want := f.turns[handle]
	if !reflect.DeepEqual(humans, want) || len(got.Items) != 2*len(want) || got.Depth != int32(len(want)-1) || len(got.Lineage) != len(want) || len(got.PendingToolCalls) != 0 {
		t.Fatalf("materialized depth=%d lineage=%d items=%d turns=%v; want turns %v", got.Depth, len(got.Lineage), len(got.Items), humans, want)
	}
	return got, f.store.gets.Load(), f.store.reads.Load()
}

func (f *snapshotCadenceFixture) row(t *testing.T, handle llm.CheckpointHandle) state.DurableCheckpoint {
	t.Helper()
	id, err := f.options.Keyring.VerifyCheckpointHandle(context.Background(), "trusted-scope", string(handle))
	if err != nil {
		t.Fatal(err)
	}
	row, err := f.store.CheckpointStore.Get(context.Background(), "trusted-scope", id)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

// semantic is everything a replay hands to validation, routing and provider
// compilation except the checkpoint identities, which differ between fixtures.
func snapshotCadenceSemantic(t *testing.T, value state.MaterializedState) []byte {
	t.Helper()
	data, err := json.Marshal(struct {
		Items    []llm.Item
		Settings state.ModelState
		Pending  []string
		Depth    int32
		Rows     int
	}{value.Items, value.Settings, value.PendingToolCalls, value.Depth, len(value.Lineage)})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestCloudRuntimeLineageWalkIsBoundedBySnapshotCadence(t *testing.T) {
	const depth, interval = 40, int(state.DefaultSnapshotInterval)
	f := snapshotCadenceCloud(t, 0)
	full := snapshotCadenceCloud(t, snapshotCadenceWithoutSnapshots)
	handles, fullHandles := f.chain(t, nil, "turn", depth+1), full.chain(t, nil, "turn", depth+1)

	for index, handle := range handles {
		row := f.row(t, handle)
		if want := index > 0 && index%interval == 0; row.Depth != int32(index) || (row.MaterializedSnapshotBlob != nil) != want {
			t.Fatalf("depth %d row depth=%d snapshot=%t; want snapshot=%t", index, row.Depth, row.MaterializedSnapshotBlob != nil, want)
		}
		if full.row(t, fullHandles[index]).MaterializedSnapshotBlob != nil {
			t.Fatalf("depth %d comparison lineage has a snapshot", index)
		}
		got, gets, reads := f.materialize(t, handle)
		want, fullGets, fullReads := full.materialize(t, fullHandles[index])
		// The comparison lineage is the full walk: every row, three blobs each.
		if fullGets != int64(index+1) || fullReads != int64(3*(index+1)) {
			t.Fatalf("depth %d full walk gets=%d reads=%d", index, fullGets, fullReads)
		}
		// With cadence snapshots the walk never depends on the depth.
		if gets > int64(interval) || reads > int64(3*interval) {
			t.Fatalf("depth %d walk read %d rows and %d blobs; want at most %d and %d", index, gets, reads, interval, 3*interval)
		}
		if a, b := snapshotCadenceSemantic(t, got), snapshotCadenceSemantic(t, want); !bytes.Equal(a, b) {
			t.Fatalf("depth %d snapshot walk differs from full walk:\n got %s\nwant %s", index, a, b)
		}
	}
	// A Generate on the cadence boundary wrote the snapshot; replaying it, and
	// the child published from it, reads from that snapshot alone.
	if _, gets, reads := f.materialize(t, handles[interval]); gets != 1 || reads != 1 {
		t.Fatalf("boundary replay read %d rows and %d blobs", gets, reads)
	}
	if _, gets, reads := f.materialize(t, handles[interval+1]); gets != 2 || reads != 4 {
		t.Fatalf("replay after the boundary read %d rows and %d blobs", gets, reads)
	}

	// The prepare step of the next turn is what every conversation turn pays.
	prepare := func(f *snapshotCadenceFixture, parent llm.CheckpointHandle) (int64, int64) {
		t.Helper()
		f.now = f.now.Add(time.Second)
		f.request.OperationKey, f.request.Parent, f.request.SettingsPatch = "next", &parent, llm.SettingsPatchV1{}
		f.request.Append = []llm.Item{preparationMessage("next")}
		f.store.gets.Store(0)
		f.store.reads.Store(0)
		v, err := f.runtime.PrepareExecutionV1(context.Background(), llm.PrepareExecutionV1{Generate: &f.request})
		boundedState(t, v, err, llm.ExecutionBudgetRequired)
		return f.store.gets.Load(), f.store.reads.Load()
	}
	// Depth 39 is the worst case: seven rows above the snapshot at depth 32.
	gets, reads := prepare(f, handles[depth-1])
	fullGets, fullReads := prepare(full, fullHandles[depth-1])
	if gets > int64(interval) || reads > int64(3*interval) || fullGets < int64(depth) || fullReads < int64(3*depth) {
		t.Fatalf("prepare at depth %d read %d rows and %d blobs (full walk %d and %d); want at most %d and %d", depth-1, gets, reads, fullGets, fullReads, interval, 3*interval)
	}
}

func TestCloudRuntimeForksAroundCadenceSnapshots(t *testing.T) {
	f := snapshotCadenceCloud(t, 0)
	main := f.chain(t, nil, "main", 11)
	// early leaves before the first snapshot and crosses the boundary itself;
	// sibling shares the snapshot row as its parent; late starts after it.
	early := f.chain(t, &main[5], "early", 4)
	sibling := f.chain(t, &main[8], "sibling", 1)
	late := f.chain(t, &main[10], "late", 6)
	for name, test := range map[string]struct {
		handle      llm.CheckpointHandle
		gets, reads int64
		snapshot    bool
	}{
		"main child of snapshot":    {main[9], 2, 4, false},
		"sibling child of snapshot": {sibling[0], 2, 4, false},
		"early before boundary":     {early[1], 8, 24, false},
		"early on boundary":         {early[2], 1, 1, true},
		"early after boundary":      {early[3], 2, 4, false},
		"late above main snapshot":  {late[0], 4, 10, false},
		"late on boundary":          {late[5], 1, 1, true},
	} {
		// materialize also proves each branch replays exactly its own turns.
		if _, gets, reads := f.materialize(t, test.handle); gets != test.gets || reads != test.reads {
			t.Fatalf("%s read %d rows and %d blobs; want %d and %d", name, gets, reads, test.gets, test.reads)
		}
		if got := f.row(t, test.handle).MaterializedSnapshotBlob != nil; got != test.snapshot {
			t.Fatalf("%s snapshot=%t", name, got)
		}
	}
	// A snapshot row is still authorized and expired like any other row.
	materializer := f.cap.Checkpoints.Materializer.(state.CheckpointHandleMaterializer)
	if _, err := materializer.MaterializeHandle(context.Background(), "another-scope", string(main[8]), f.options.Limits); err == nil {
		t.Fatal("snapshot row replayed in another scope")
	}
	f.now = f.now.Add(f.options.CheckpointTTL)
	for _, expired := range []llm.CheckpointHandle{main[8], main[9]} {
		if _, err := materializer.MaterializeHandle(context.Background(), "trusted-scope", string(expired), f.options.Limits); !errors.Is(err, state.ErrExpired) {
			t.Fatalf("expired snapshot lineage replayed: %v", err)
		}
	}
}

func TestCloudRuntimeAdoptsSnapshotsOnLineagePublishedWithoutThem(t *testing.T) {
	f := snapshotCadenceCloud(t, snapshotCadenceWithoutSnapshots)
	old := f.chain(t, nil, "old", 11)
	f.options.Limits.SnapshotInterval = 0
	f.restart(t)
	next := f.chain(t, &old[10], "new", 7)
	// Depths 11..15 still walk to the root; depth 16 is the first boundary.
	if _, gets, reads := f.materialize(t, next[4]); gets != 16 || reads != 48 {
		t.Fatalf("pre-snapshot lineage read %d rows and %d blobs", gets, reads)
	}
	if f.row(t, old[8]).MaterializedSnapshotBlob != nil || f.row(t, next[5]).MaterializedSnapshotBlob == nil {
		t.Fatal("unexpected snapshot placement on adopted lineage")
	}
	if _, gets, reads := f.materialize(t, next[6]); gets != 2 || reads != 4 {
		t.Fatalf("adopted lineage read %d rows and %d blobs", gets, reads)
	}
}
