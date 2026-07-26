package state

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

// TestCheckpointGraphReplaysLongLineageWithSnapshotsAndForks exercises the
// storage-neutral part of the v1 recovery contract at a production-shaped
// scale. Ten thousand bounded turns are published, a materialized snapshot is
// taken every 500 turns (the same immutable artifact used by compaction), and
// three children are then created from one immutable parent. A replacement
// graph replays the published rows to model a worker restart/restore. This is
// intentionally an offline proof: durable PostgreSQL/blob backup and Temporal
// crash-boundary tests remain separate integration gates.
func TestCheckpointGraphReplaysLongLineageWithSnapshotsAndForks(t *testing.T) {
	const (
		turns            = 10_000
		snapshotInterval = 500
		forkTurn         = 7_500
		forks            = 100
	)
	now := time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC)
	limits := MaterializeLimits{
		MaxDepth: turns + 16,
		MaxRows:  turns + 16,
		MaxItems: (turns+16)*2 + 8,
		MaxBytes: 64 << 20,
	}
	graph := NewCheckpointGraph(limits)
	graph.Now = func() time.Time { return now }
	published := make([]Checkpoint, 0, turns+4)

	root := syntheticLineageCheckpoint("turn-00000", "", 0, now, true)
	if err := graph.PutRoot(root); err != nil {
		t.Fatal(err)
	}
	published = append(published, root)
	head := Handle(root.Handle)
	current, err := graph.Materialize("tenant-v1", head)
	if err != nil {
		t.Fatalf("materialize root: %v", err)
	}
	var forkParent MaterializedState

	for turn := 1; turn <= turns; turn++ {
		handle := Handle(fmt.Sprintf("turn-%05d", turn))
		parentID := head
		checkpoint := syntheticLineageCheckpoint(string(handle), string(parentID), turn, now, false)
		delta, output := checkpoint.Delta, checkpoint.Output
		if turn%snapshotInterval == 0 {
			checkpoint.Snapshot = syntheticChildSnapshot(current, handle, delta, output)
			// The snapshot contains this turn, so retaining the delta/output
			// would make replay append the same items twice.
			checkpoint.Delta = nil
			checkpoint.Output = nil
		}
		if err := graph.PutChild(checkpoint); err != nil {
			t.Fatalf("publish turn %d: %v", turn, err)
		}
		published = append(published, checkpoint)
		head = handle
		current.Handle = handle
		current.Depth++
		current.Items = appendItems(current.Items, delta...)
		current.Items = appendItems(current.Items, output...)
		current.Lineage = append(append([]Handle(nil), current.Lineage...), handle)
		if turn == forkTurn {
			forkParent = current
		}
	}

	final, err := graph.Materialize("tenant-v1", head)
	if err != nil {
		t.Fatalf("materialize final lineage: %v", err)
	}
	if got, want := len(final.Items), turns+1; got != want {
		t.Fatalf("final item count = %d, want %d", got, want)
	}
	if final.Depth != turns || len(final.Lineage) != turns+1 {
		t.Fatalf("final depth/lineage = %d/%d, want %d/%d", final.Depth, len(final.Lineage), turns, turns+1)
	}

	// Fork concurrently from one immutable parent. Each child must retain the
	// complete parent transcript and add only its own bounded turn. One hundred
	// siblings exercises the production fan-out envelope without requiring a
	// provider, database, or Temporal server.
	forked := make([]Checkpoint, forks)
	var group sync.WaitGroup
	for index := range forked {
		index := index
		group.Add(1)
		go func() {
			defer group.Done()
			id := Handle(fmt.Sprintf("fork-%d", index))
			forked[index] = syntheticLineageCheckpoint(string(id), string(forkParent.Handle), forkTurn+index+1, now, false)
			forked[index].Snapshot = syntheticChildSnapshot(forkParent, id, forked[index].Delta, forked[index].Output)
			forked[index].Delta = nil
			forked[index].Output = nil
			if err := graph.PutChild(forked[index]); err != nil {
				t.Errorf("publish fork %d: %v", index, err)
			}
		}()
	}
	group.Wait()
	for index := range forked {
		branch, err := graph.Materialize("tenant-v1", forked[index].Handle)
		if err != nil {
			t.Fatalf("materialize fork %d: %v", index, err)
		}
		if got, want := len(branch.Items), len(forkParent.Items)+1; got != want {
			t.Fatalf("fork %d item count = %d, want %d", index, got, want)
		}
		if !reflect.DeepEqual(branch.Items[:len(forkParent.Items)], forkParent.Items) {
			t.Fatalf("fork %d changed immutable parent transcript", index)
		}
	}

	// Rebuild a replacement graph from the rows that were durably published.
	// This catches snapshot lineage/digest mistakes that an in-place replay can
	// hide and proves all three branches survive a restart independently.
	restored := NewCheckpointGraph(limits)
	restored.Now = func() time.Time { return now }
	for index, checkpoint := range published {
		var publishErr error
		if index == 0 {
			publishErr = restored.PutRoot(checkpoint)
		} else {
			publishErr = restored.PutChild(checkpoint)
		}
		if publishErr != nil {
			t.Fatalf("restore checkpoint %d: %v", index, publishErr)
		}
	}
	for index, checkpoint := range forked {
		if err := restored.PutChild(checkpoint); err != nil {
			t.Fatalf("restore fork %d: %v", index, err)
		}
		branch, err := restored.Materialize("tenant-v1", checkpoint.Handle)
		if err != nil {
			t.Fatalf("materialize restored fork %d: %v", index, err)
		}
		if got, want := len(branch.Items), len(forkParent.Items)+1; got != want {
			t.Fatalf("restored fork %d item count = %d, want %d", index, got, want)
		}
	}
	restoredFinal, err := restored.Materialize("tenant-v1", head)
	if err != nil {
		t.Fatalf("materialize restored final lineage: %v", err)
	}
	if !reflect.DeepEqual(final.Items, restoredFinal.Items) || !reflect.DeepEqual(final.Lineage, restoredFinal.Lineage) {
		t.Fatal("restored final lineage diverged from original")
	}
}

func syntheticLineageCheckpoint(handle, parent string, turn int, now time.Time, root bool) Checkpoint {
	checkpoint := Checkpoint{
		Handle:       Handle(handle),
		Tenant:       "tenant-v1",
		OperationKey: "operation-" + handle,
		Delta:        []llm.Item{syntheticLineageMessage(llm.ActorHuman, "input", turn)},
		ExpiresAt:    now.Add(24 * time.Hour),
	}
	if root {
		checkpoint.SettingsPatch = SettingsPatch{
			Model:        SetPatch("gpt-test"),
			ServiceClass: SetPatch(llm.ServiceClassStandard),
			Portability:  SetPatch(llm.PortabilityStrict),
		}
	} else {
		parentID := Handle(parent)
		checkpoint.Parent = &parentID
	}
	return checkpoint
}

func syntheticLineageMessage(actor llm.Actor, kind string, turn int) llm.Message {
	return llm.Message{Actor: actor, Content: []llm.Part{llm.TextPart{Text: fmt.Sprintf("%s-%05d", kind, turn)}}}
}

func syntheticChildSnapshot(parent MaterializedState, handle Handle, delta, output []llm.Item) *CheckpointSnapshot {
	items := appendItems(parent.Items, delta...)
	items = appendItems(items, output...)
	lineage := append(append([]Handle(nil), parent.Lineage...), handle)
	return NewCheckpointSnapshot(MaterializedState{
		Handle:   handle,
		Tenant:   parent.Tenant,
		Project:  parent.Project,
		Depth:    parent.Depth + 1,
		Items:    items,
		Settings: parent.Settings.Clone(),
		Lineage:  lineage,
	})
}
