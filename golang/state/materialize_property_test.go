package state

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func message(text string) llm.Item {
	return llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}}
}

func rootCheckpoint(handle, tenant, operation string) Checkpoint {
	return Checkpoint{
		Handle:       Handle(handle),
		Tenant:       tenant,
		Project:      "project",
		OperationKey: operation,
		SettingsPatch: SettingsPatch{
			Model:        SetPatch("gpt-test"),
			ServiceClass: SetPatch(llm.ServiceClassStandard),
		},
		Delta: []llm.Item{message("root")},
	}
}

func childCheckpoint(handle, parent, tenant, operation, text string) Checkpoint {
	parentHandle := Handle(parent)
	return Checkpoint{Handle: Handle(handle), Parent: &parentHandle, Tenant: tenant, OperationKey: operation, Delta: []llm.Item{message(text)}}
}

func canonicalItemsForTest(t *testing.T, items []llm.Item) []byte {
	t.Helper()
	raw, err := json.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := llm.CanonicalJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}

func TestCheckpointGraphUsesInjectedClockForExpiry(t *testing.T) {
	clock := time.Unix(0, 0).UTC()
	graph := NewCheckpointGraph(MaterializeLimits{})
	graph.Now = func() time.Time { return clock }
	checkpoint := rootCheckpoint("root", "tenant-a", "operation-root")
	checkpoint.ExpiresAt = clock.Add(time.Hour)
	if err := graph.PutRoot(checkpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.Materialize("tenant-a", "root"); err != nil {
		t.Fatalf("materialize with injected live clock: %v", err)
	}
}

func TestCheckpointGraphRootLinearAndSiblingMaterialization(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{})
	if err := graph.PutRoot(rootCheckpoint("root", "tenant-a", "op-root")); err != nil {
		t.Fatal(err)
	}
	if err := graph.PutChild(childCheckpoint("one", "root", "tenant-a", "op-one", "one")); err != nil {
		t.Fatal(err)
	}
	if err := graph.PutChild(childCheckpoint("two", "root", "tenant-a", "op-two", "two")); err != nil {
		t.Fatal(err)
	}
	first, err := graph.Materialize("tenant-a", "one")
	if err != nil {
		t.Fatal(err)
	}
	second, err := graph.Materialize("tenant-a", "two")
	if err != nil {
		t.Fatal(err)
	}
	if first.Depth != 1 || second.Depth != 1 || len(first.Items) != 2 || len(second.Items) != 2 {
		t.Fatalf("unexpected materialized branches: %#v %#v", first, second)
	}
	if first.Items[1].(llm.Message).Content[0].(llm.TextPart).Text != "one" || second.Items[1].(llm.Message).Content[0].(llm.TextPart).Text != "two" {
		t.Fatal("sibling branches shared a delta")
	}
	if _, err := graph.Materialize("tenant-b", "one"); err != ErrTenantMismatch {
		t.Fatalf("cross-tenant materialization error = %v", err)
	}
}

func TestCheckpointGraphScopesOperationKeysByTenantAndProject(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{})
	if err := graph.PutRoot(rootCheckpoint("tenant-a-root", "tenant-a", "same-operation")); err != nil {
		t.Fatal(err)
	}
	// Operation keys are stable retry identities within a caller scope, not
	// globally unique values. Independent tenants may safely reuse a key.
	otherTenant := rootCheckpoint("tenant-b-root", "tenant-b", "same-operation")
	if err := graph.PutRoot(otherTenant); err != nil {
		t.Fatalf("same operation key in another tenant was rejected: %v", err)
	}
	otherProject := rootCheckpoint("project-b-root", "tenant-a", "same-operation")
	otherProject.Project = "another-project"
	if err := graph.PutRoot(otherProject); err != nil {
		t.Fatalf("same operation key in another project was rejected: %v", err)
	}
	conflict := rootCheckpoint("conflict", "tenant-a", "same-operation")
	if err := graph.PutRoot(conflict); err != ErrConflict {
		t.Fatalf("same scoped operation key returned %v, want %v", err, ErrConflict)
	}
}

func TestCheckpointGraphThreeWayForksRemainIsolated(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{})
	root := rootCheckpoint("root", "tenant-a", "op-root")
	root.SettingsPatch.Tools = SetPatch([]llm.Tool{{
		Name:        "lookup",
		InputSchema: []byte(`{"type":"object"}`),
	}})
	if err := graph.PutRoot(root); err != nil {
		t.Fatal(err)
	}
	branches := []struct {
		handle string
		text   string
	}{
		{handle: "one", text: "one"},
		{handle: "two", text: "two"},
		{handle: "three", text: "three"},
	}
	for _, branch := range branches {
		if err := graph.PutChild(childCheckpoint(branch.handle, "root", "tenant-a", "op-"+branch.handle, branch.text)); err != nil {
			t.Fatal(err)
		}
	}

	materialized := make(map[string]MaterializedState, len(branches))
	for _, branch := range branches {
		state, err := graph.Materialize("tenant-a", Handle(branch.handle))
		if err != nil {
			t.Fatal(err)
		}
		if len(state.Items) != 2 || state.Items[0].(llm.Message).Content[0].(llm.TextPart).Text != "root" || state.Items[1].(llm.Message).Content[0].(llm.TextPart).Text != branch.text {
			t.Fatalf("branch %s materialized unexpected items: %#v", branch.handle, state.Items)
		}
		if len(state.Settings.Tools) != 1 || string(state.Settings.Tools[0].InputSchema) != `{"type":"object"}` {
			t.Fatalf("branch %s materialized unexpected settings: %#v", branch.handle, state.Settings)
		}
		materialized[branch.handle] = state
	}

	// Callers may mutate one materialized result without changing a sibling or
	// the graph's immutable lineage.
	mutated := materialized["one"]
	rootMessage := mutated.Items[0].(llm.Message)
	rootMessage.Content[0] = llm.TextPart{Text: "mutated"}
	mutated.Items[0] = rootMessage
	mutated.Settings.Model = "mutated-model"
	mutated.Settings.Tools[0].InputSchema[0] = 'x'
	materialized["one"] = mutated

	if got := materialized["two"].Items[0].(llm.Message).Content[0].(llm.TextPart).Text; got != "root" {
		t.Fatalf("mutating branch one changed branch two item to %q", got)
	}
	if got := materialized["two"].Settings.Model; got != "gpt-test" {
		t.Fatalf("mutating branch one changed branch two model to %q", got)
	}
	if got := string(materialized["two"].Settings.Tools[0].InputSchema); got != `{"type":"object"}` {
		t.Fatalf("mutating branch one changed branch two tool schema to %q", got)
	}

	for _, branch := range branches {
		state, err := graph.Materialize("tenant-a", Handle(branch.handle))
		if err != nil {
			t.Fatal(err)
		}
		if got := state.Items[0].(llm.Message).Content[0].(llm.TextPart).Text; got != "root" {
			t.Fatalf("mutating branch one changed graph root for %s to %q", branch.handle, got)
		}
		if got := state.Settings.Model; got != "gpt-test" {
			t.Fatalf("mutating branch one changed graph model for %s to %q", branch.handle, got)
		}
		if got := string(state.Settings.Tools[0].InputSchema); got != `{"type":"object"}` {
			t.Fatalf("mutating branch one changed graph tool schema for %s to %q", branch.handle, got)
		}
	}
}

func TestSettingsPatchOmittedSetAndClearRemainDistinct(t *testing.T) {
	base := RootModelState("gpt-test")
	base.Tools = []llm.Tool{{Name: "lookup", InputSchema: []byte(`{"type":"object"}`)}}
	high := llm.ReasoningEffortHigh
	patched, err := ApplySettingsPatch(base, SettingsPatch{ReasoningEffort: SetPatch(high)})
	if err != nil {
		t.Fatal(err)
	}
	if patched.ReasoningEffort != high || len(patched.Tools) != 1 {
		t.Fatalf("omitted leaves were not inherited: %#v", patched)
	}
	cleared, err := ApplySettingsPatch(patched, SettingsPatch{Tools: ClearPatch[[]llm.Tool]()})
	if err != nil {
		t.Fatal(err)
	}
	if cleared.Tools != nil || cleared.ReasoningEffort != high {
		t.Fatalf("clear reset unrelated leaves: %#v", cleared)
	}
	if _, err := ApplySettingsPatch(base, SettingsPatch{Model: Patch[string]{Set: ptr("x"), Clear: true}}); err == nil {
		t.Fatal("set and clear were accepted together")
	}
}

func TestMaterializationCarriesEveryAncestorAndSnapshotMatchesReplay(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{})
	if err := graph.PutRoot(rootCheckpoint("root", "tenant-a", "op-root")); err != nil {
		t.Fatal(err)
	}
	parent := childCheckpoint("one", "root", "tenant-a", "op-one", "one")
	if err := graph.PutChild(parent); err != nil {
		t.Fatal(err)
	}
	if err := graph.PutChild(childCheckpoint("two", "one", "tenant-a", "op-two", "two")); err != nil {
		t.Fatal(err)
	}
	full, err := graph.Materialize("tenant-a", "two")
	if err != nil {
		t.Fatal(err)
	}
	snapshot := NewCheckpointSnapshot(full)
	leaf := childCheckpoint("three", "two", "tenant-a", "op-three", "three")
	snapshot.Items = append(snapshot.Items, message("three"))
	snapshot.Depth = 3
	snapshot.Lineage = append(snapshot.Lineage, Handle("three"))
	snapshot.Digest = snapshot.digest()
	leaf.Delta = nil // the self-contained snapshot already includes this node
	leaf.Snapshot = snapshot
	if err := graph.PutChild(leaf); err != nil {
		t.Fatal(err)
	}
	withSnapshot, err := graph.Materialize("tenant-a", "three")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(full.Items, withSnapshot.Items[:len(full.Items)]) || withSnapshot.Depth != 3 {
		t.Fatalf("snapshot replay diverged: full=%#v snapshot=%#v", full, withSnapshot)
	}
	if got := withSnapshot.Items[len(withSnapshot.Items)-1].(llm.Message).Content[0].(llm.TextPart).Text; got != "three" {
		t.Fatalf("snapshot leaf output = %q", got)
	}
}

func TestSnapshotReplayEqualsFullReplay(t *testing.T) {
	withoutSnapshot := NewCheckpointGraph(MaterializeLimits{})
	if err := withoutSnapshot.PutRoot(rootCheckpoint("root", "tenant-a", "op-root")); err != nil {
		t.Fatal(err)
	}
	parent := childCheckpoint("one", "root", "tenant-a", "op-one", "one")
	if err := withoutSnapshot.PutChild(parent); err != nil {
		t.Fatal(err)
	}
	leaf := childCheckpoint("two", "one", "tenant-a", "op-two", "two")
	if err := withoutSnapshot.PutChild(leaf); err != nil {
		t.Fatal(err)
	}
	full, err := withoutSnapshot.Materialize("tenant-a", "two")
	if err != nil {
		t.Fatal(err)
	}

	withSnapshot := NewCheckpointGraph(MaterializeLimits{})
	if err := withSnapshot.PutRoot(rootCheckpoint("root", "tenant-a", "op-root")); err != nil {
		t.Fatal(err)
	}
	if err := withSnapshot.PutChild(parent); err != nil {
		t.Fatal(err)
	}
	snapshotLeaf := leaf
	snapshotLeaf.Snapshot = NewCheckpointSnapshot(full)
	snapshotLeaf.Delta = nil
	if err := withSnapshot.PutChild(snapshotLeaf); err != nil {
		t.Fatal(err)
	}
	optimized, err := withSnapshot.Materialize("tenant-a", "two")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(full.Items, optimized.Items) || !reflect.DeepEqual(full.Settings, optimized.Settings) || full.Depth != optimized.Depth {
		t.Fatalf("snapshot replay diverged: full=%#v optimized=%#v", full, optimized)
	}
	if got, want := canonicalItemsForTest(t, optimized.Items), canonicalItemsForTest(t, full.Items); !bytes.Equal(got, want) {
		t.Fatalf("snapshot transcript bytes diverged: got=%s want=%s", got, want)
	}
}

func TestMaterializationIsIndependentOfDeltaSegmentation(t *testing.T) {
	const eventCount = 64

	segmented := NewCheckpointGraph(MaterializeLimits{})
	root := rootCheckpoint("segmented-root", "tenant-a", "segmented-root-operation")
	root.Output = []llm.Item{message("root-output")}
	if err := segmented.PutRoot(root); err != nil {
		t.Fatal(err)
	}
	parent := root.Handle
	for index := 0; index < eventCount; index++ {
		handle := Handle(fmt.Sprintf("segmented-%03d", index))
		parentHandle := parent
		checkpoint := Checkpoint{
			Handle:       handle,
			Parent:       &parentHandle,
			Tenant:       "tenant-a",
			OperationKey: fmt.Sprintf("segmented-operation-%03d", index),
			Delta: []llm.Item{
				message(fmt.Sprintf("delta-%03d", index)),
				message(fmt.Sprintf("response-%03d", index)),
			},
		}
		if err := segmented.PutChild(checkpoint); err != nil {
			t.Fatal(err)
		}
		parent = handle
	}
	segmentedState, err := segmented.Materialize("tenant-a", parent)
	if err != nil {
		t.Fatal(err)
	}

	grouped := NewCheckpointGraph(MaterializeLimits{})
	groupedRoot := rootCheckpoint("grouped-root", "tenant-a", "grouped-root-operation")
	groupedRoot.Output = []llm.Item{message("root-output")}
	if err := grouped.PutRoot(groupedRoot); err != nil {
		t.Fatal(err)
	}
	allEvents := make([]llm.Item, 0, eventCount*2)
	for index := 0; index < eventCount; index++ {
		allEvents = append(allEvents,
			message(fmt.Sprintf("delta-%03d", index)),
			message(fmt.Sprintf("response-%03d", index)),
		)
	}
	groupedParent := groupedRoot.Handle
	groupedParentHandle := groupedParent
	groupedLeaf := Checkpoint{
		Handle:       "grouped-leaf",
		Parent:       &groupedParentHandle,
		Tenant:       "tenant-a",
		OperationKey: "grouped-leaf-operation",
		Delta:        allEvents,
	}
	if err := grouped.PutChild(groupedLeaf); err != nil {
		t.Fatal(err)
	}
	groupedState, err := grouped.Materialize("tenant-a", groupedLeaf.Handle)
	if err != nil {
		t.Fatal(err)
	}

	if got, want := canonicalItemsForTest(t, segmentedState.Items), canonicalItemsForTest(t, groupedState.Items); !bytes.Equal(got, want) {
		t.Fatalf("segmented replay changed transcript bytes: got=%s want=%s", got, want)
	}
	if !reflect.DeepEqual(segmentedState.Settings, groupedState.Settings) {
		t.Fatalf("segmented replay changed effective settings: segmented=%#v grouped=%#v", segmentedState.Settings, groupedState.Settings)
	}
	if len(segmentedState.Items) != 1+1+eventCount*2 {
		t.Fatalf("segmented item count = %d, want %d", len(segmentedState.Items), 2+eventCount*2)
	}
}

func TestMaterializationRejectsUnmatchedToolFrontierAndLimits(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{MaxItems: 2})
	root := rootCheckpoint("root", "tenant-a", "op-root")
	root.Delta = []llm.Item{llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{"q":"x"}`)}}
	if err := graph.PutRoot(root); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.Materialize("tenant-a", "root"); err != nil {
		t.Fatalf("root with pending tool call should be materializable: %v", err)
	}
	bad := childCheckpoint("bad", "root", "tenant-a", "op-bad", "message before result")
	if err := graph.PutChild(bad); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.Materialize("tenant-a", "bad"); err == nil {
		t.Fatal("child began inside an unmatched tool exchange")
	}
	good := childCheckpoint("good", "root", "tenant-a", "op-good", "")
	good.Delta = []llm.Item{llm.ToolResult{CallID: "call-1", Content: []llm.Part{llm.TextPart{Text: "ok"}}}}
	if err := graph.PutChild(good); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.Materialize("tenant-a", "good"); err != nil {
		t.Fatalf("matching tool result rejected: %v", err)
	}
}

func TestMaterializationAllowsParallelToolCalls(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{})
	root := rootCheckpoint("parallel", "tenant-a", "op-parallel")
	root.Delta = []llm.Item{
		llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{"q":"one"}`)},
		llm.ToolCall{ID: "call-2", Name: "lookup", Arguments: []byte(`{"q":"two"}`)},
	}
	if err := graph.PutRoot(root); err != nil {
		t.Fatal(err)
	}
	materialized, err := graph.Materialize("tenant-a", "parallel")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(materialized.PendingToolCalls, []string{"call-1", "call-2"}) {
		t.Fatalf("pending tool calls = %#v", materialized.PendingToolCalls)
	}
}

func TestMaterializationRejectsToolCallAfterParallelResultsBegin(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{})
	root := rootCheckpoint("interleaved", "tenant-a", "op-interleaved-root")
	root.Delta = []llm.Item{
		llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{"q":"one"}`)},
		llm.ToolCall{ID: "call-2", Name: "lookup", Arguments: []byte(`{"q":"two"}`)},
	}
	if err := graph.PutRoot(root); err != nil {
		t.Fatal(err)
	}
	parent := Handle("interleaved")
	child := childCheckpoint("bad-interleaved", parent.String(), "tenant-a", "op-interleaved-child", "")
	child.Delta = []llm.Item{
		llm.ToolResult{CallID: "call-1", Content: []llm.Part{llm.TextPart{Text: "one"}}},
		llm.ToolCall{ID: "call-3", Name: "lookup", Arguments: []byte(`{"q":"three"}`)},
	}
	if err := graph.PutChild(child); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.Materialize("tenant-a", child.Handle); err == nil {
		t.Fatal("materialization accepted a new tool call after parallel results began")
	}
}

func ptr(value string) *string { return &value }
