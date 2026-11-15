package state

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

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
		Delta: []llm.Item{llm.HumanText("root")},
	}
}

func childCheckpoint(handle, parent, tenant, operation, text string) Checkpoint {
	parentHandle := Handle(parent)
	return Checkpoint{Handle: Handle(handle), Parent: &parentHandle, Tenant: tenant, OperationKey: operation, Delta: []llm.Item{llm.HumanText(text)}}
}

func mustPutRoot(t testing.TB, graph *CheckpointGraph, checkpoint Checkpoint) {
	t.Helper()
	testassert.NoError(t, graph.PutRoot(checkpoint))
}

func mustPutChild(t testing.TB, graph *CheckpointGraph, checkpoint Checkpoint) {
	t.Helper()
	testassert.NoError(t, graph.PutChild(checkpoint))
}

func mustMaterialize(t testing.TB, graph *CheckpointGraph, tenant string, handle Handle) MaterializedState {
	t.Helper()
	state, err := graph.Materialize(tenant, handle)
	testassert.NoError(t, err)
	return state
}

// mustJSON marshals value, failing the test on an error.
func mustJSON(t testing.TB, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	testassert.NoError(t, err)
	return data
}

// lookupCall is a call of the "lookup" tool with raw JSON arguments.
func lookupCall(id, arguments string) llm.ToolCall {
	return llm.ToolCall{ID: id, Name: "lookup", Arguments: []byte(arguments)}
}

// assertMaterializeErr checks the error graph.Materialize returns for handle.
func assertMaterializeErr(t testing.TB, graph *CheckpointGraph, tenant string, handle Handle, want error, what string) {
	t.Helper()
	_, err := graph.Materialize(tenant, handle)
	testassert.ErrorIs(t, err, want, what)
}

func canonicalItemsForTest(t *testing.T, items []llm.Item) []byte {
	t.Helper()
	canonical, err := llm.CanonicalJSON(mustJSON(t, items))
	testassert.NoError(t, err)
	return canonical
}

func TestCheckpointGraphUsesInjectedClockForExpiry(t *testing.T) {
	clock := time.Unix(0, 0).UTC()
	graph := NewCheckpointGraph(MaterializeLimits{})
	graph.Now = func() time.Time { return clock }
	checkpoint := rootCheckpoint("root", "tenant-a", "operation-root")
	checkpoint.ExpiresAt = clock.Add(time.Hour)
	mustPutRoot(t, graph, checkpoint)
	assertMaterializeErr(t, graph, "tenant-a", "root", nil, "materialize with injected live clock")
}

func TestCheckpointGraphLiveChildOutlivesRetainedAncestor(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	clock := start
	graph := NewCheckpointGraph(MaterializeLimits{})
	graph.Now = func() time.Time { return clock }
	root := rootCheckpoint("root", "tenant-a", "operation-root")
	root.ExpiresAt = start.Add(time.Hour)
	mustPutRoot(t, graph, root)
	clock = start.Add(50 * time.Minute)
	child := childCheckpoint("child", "root", "tenant-a", "operation-child", "child")
	child.ExpiresAt = clock.Add(time.Hour)
	mustPutChild(t, graph, child)
	clock = start.Add(70 * time.Minute)
	assertMaterializeErr(t, graph, "tenant-a", "child", nil, "materialize live child of expired root")
	assertMaterializeErr(t, graph, "tenant-a", "root", ErrExpired, "materialize expired root")
	// A child cannot be added under a parent that has already expired, so
	// expired content is never resurrected through a new descendant.
	late := childCheckpoint("late", "root", "tenant-a", "operation-late", "late")
	late.ExpiresAt = clock.Add(time.Hour)
	testassert.ErrorIs(t, graph.PutChild(late), ErrExpired, "child of expired parent")
	// Republishing the existing child stays idempotent.
	testassert.NoError(t, graph.PutChild(child), "republish child")
	clock = start.Add(110 * time.Minute)
	assertMaterializeErr(t, graph, "tenant-a", "child", ErrExpired, "materialize expired child")
}

func TestCheckpointGraphRootLinearAndSiblingMaterialization(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{})
	mustPutRoot(t, graph, rootCheckpoint("root", "tenant-a", "op-root"))
	mustPutChild(t, graph, childCheckpoint("one", "root", "tenant-a", "op-one", "one"))
	mustPutChild(t, graph, childCheckpoint("two", "root", "tenant-a", "op-two", "two"))
	first := mustMaterialize(t, graph, "tenant-a", "one")
	second := mustMaterialize(t, graph, "tenant-a", "two")
	if first.Depth != 1 || second.Depth != 1 || len(first.Items) != 2 || len(second.Items) != 2 {
		t.Fatalf("unexpected materialized branches: %#v %#v", first, second)
	}
	if first.Items[1].(llm.Message).Content[0].(llm.TextPart).Text != "one" || second.Items[1].(llm.Message).Content[0].(llm.TextPart).Text != "two" {
		t.Fatal("sibling branches shared a delta")
	}
	assertMaterializeErr(t, graph, "tenant-b", "one", ErrTenantMismatch, "cross-tenant materialization error")
}

func TestCheckpointGraphScopesOperationKeysByTenantAndProject(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{})
	mustPutRoot(t, graph, rootCheckpoint("tenant-a-root", "tenant-a", "same-operation"))
	// Operation keys are stable retry identities within a caller scope, not
	// globally unique values. Independent tenants may safely reuse a key.
	otherTenant := rootCheckpoint("tenant-b-root", "tenant-b", "same-operation")
	testassert.NoError(t, graph.PutRoot(otherTenant), "same operation key in another tenant")
	otherProject := rootCheckpoint("project-b-root", "tenant-a", "same-operation")
	otherProject.Project = "another-project"
	testassert.NoError(t, graph.PutRoot(otherProject), "same operation key in another project")
	testassert.ErrorIs(t, graph.PutRoot(rootCheckpoint("conflict", "tenant-a", "same-operation")), ErrConflict, "same scoped operation key")
}

func TestCheckpointGraphThreeWayForksRemainIsolated(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{})
	root := rootCheckpoint("root", "tenant-a", "op-root")
	root.SettingsPatch.Tools = SetPatch([]llm.Tool{{
		Name:        "lookup",
		InputSchema: []byte(`{"type":"object"}`),
	}})
	mustPutRoot(t, graph, root)
	branches := []struct {
		handle string
		text   string
	}{
		{handle: "one", text: "one"},
		{handle: "two", text: "two"},
		{handle: "three", text: "three"},
	}
	for _, branch := range branches {
		mustPutChild(t, graph, childCheckpoint(branch.handle, "root", "tenant-a", "op-"+branch.handle, branch.text))
	}

	materialized := make(map[string]MaterializedState, len(branches))
	for _, branch := range branches {
		state := mustMaterialize(t, graph, "tenant-a", Handle(branch.handle))
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
		state := mustMaterialize(t, graph, "tenant-a", Handle(branch.handle))
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

func TestExtraHighReasoningSurvivesSettingsInheritance(t *testing.T) {
	base := RootModelState("openai/gpt-6-luna")
	patched, err := ApplySettingsPatch(base, SettingsPatch{ReasoningEffort: SetPatch(llm.ReasoningEffortExtraHigh)})
	testassert.NoError(t, err)
	inherited, err := ApplySettingsPatch(patched, SettingsPatch{})
	if err != nil || inherited.ReasoningEffort != llm.ReasoningEffortExtraHigh {
		t.Fatalf("extra high was not preserved: %s, %v", inherited.ReasoningEffort, err)
	}
	cleared, err := ApplySettingsPatch(patched, SettingsPatch{ReasoningEffort: ClearPatch[llm.ReasoningEffort]()})
	if err != nil || cleared.ReasoningEffort == llm.ReasoningEffortExtraHigh {
		t.Fatalf("extra high was not cleared: %s, %v", cleared.ReasoningEffort, err)
	}
}

func TestSettingsPatchOmittedSetAndClearRemainDistinct(t *testing.T) {
	base := RootModelState("gpt-test")
	base.Tools = []llm.Tool{{Name: "lookup", InputSchema: []byte(`{"type":"object"}`)}}
	high := llm.ReasoningEffortHigh
	patched, err := ApplySettingsPatch(base, SettingsPatch{ReasoningEffort: SetPatch(high)})
	testassert.NoError(t, err)
	if patched.ReasoningEffort != high || len(patched.Tools) != 1 {
		t.Fatalf("omitted leaves were not inherited: %#v", patched)
	}
	cleared, err := ApplySettingsPatch(patched, SettingsPatch{Tools: ClearPatch[[]llm.Tool]()})
	testassert.NoError(t, err)
	if cleared.Tools != nil || cleared.ReasoningEffort != high {
		t.Fatalf("clear reset unrelated leaves: %#v", cleared)
	}
	if _, err := ApplySettingsPatch(base, SettingsPatch{Model: Patch[string]{Set: ptr("x"), Clear: true}}); err == nil {
		t.Fatal("set and clear were accepted together")
	}
}

func TestMaterializationCarriesEveryAncestorAndSnapshotMatchesReplay(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{})
	mustPutRoot(t, graph, rootCheckpoint("root", "tenant-a", "op-root"))
	parent := childCheckpoint("one", "root", "tenant-a", "op-one", "one")
	mustPutChild(t, graph, parent)
	mustPutChild(t, graph, childCheckpoint("two", "one", "tenant-a", "op-two", "two"))
	full := mustMaterialize(t, graph, "tenant-a", "two")
	snapshot := NewCheckpointSnapshot(full)
	leaf := childCheckpoint("three", "two", "tenant-a", "op-three", "three")
	snapshot.Items = append(snapshot.Items, llm.HumanText("three"))
	snapshot.Depth = 3
	snapshot.Lineage = append(snapshot.Lineage, Handle("three"))
	snapshot.Digest = snapshot.digest()
	leaf.Delta = nil // the self-contained snapshot already includes this node
	leaf.Snapshot = snapshot
	mustPutChild(t, graph, leaf)
	withSnapshot := mustMaterialize(t, graph, "tenant-a", "three")
	if !reflect.DeepEqual(full.Items, withSnapshot.Items[:len(full.Items)]) || withSnapshot.Depth != 3 {
		t.Fatalf("snapshot replay diverged: full=%#v snapshot=%#v", full, withSnapshot)
	}
	if got := withSnapshot.Items[len(withSnapshot.Items)-1].(llm.Message).Content[0].(llm.TextPart).Text; got != "three" {
		t.Fatalf("snapshot leaf output = %q", got)
	}
}

func TestSnapshotReplayEqualsFullReplay(t *testing.T) {
	withoutSnapshot := NewCheckpointGraph(MaterializeLimits{})
	mustPutRoot(t, withoutSnapshot, rootCheckpoint("root", "tenant-a", "op-root"))
	parent := childCheckpoint("one", "root", "tenant-a", "op-one", "one")
	mustPutChild(t, withoutSnapshot, parent)
	leaf := childCheckpoint("two", "one", "tenant-a", "op-two", "two")
	mustPutChild(t, withoutSnapshot, leaf)
	full := mustMaterialize(t, withoutSnapshot, "tenant-a", "two")

	withSnapshot := NewCheckpointGraph(MaterializeLimits{})
	mustPutRoot(t, withSnapshot, rootCheckpoint("root", "tenant-a", "op-root"))
	mustPutChild(t, withSnapshot, parent)
	snapshotLeaf := leaf
	snapshotLeaf.Snapshot = NewCheckpointSnapshot(full)
	snapshotLeaf.Delta = nil
	mustPutChild(t, withSnapshot, snapshotLeaf)
	optimized := mustMaterialize(t, withSnapshot, "tenant-a", "two")
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
	root.Output = []llm.Item{llm.HumanText("root-output")}
	mustPutRoot(t, segmented, root)
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
				llm.HumanText(fmt.Sprintf("delta-%03d", index)),
				llm.HumanText(fmt.Sprintf("response-%03d", index)),
			},
		}
		mustPutChild(t, segmented, checkpoint)
		parent = handle
	}
	segmentedState := mustMaterialize(t, segmented, "tenant-a", parent)

	grouped := NewCheckpointGraph(MaterializeLimits{})
	groupedRoot := rootCheckpoint("grouped-root", "tenant-a", "grouped-root-operation")
	groupedRoot.Output = []llm.Item{llm.HumanText("root-output")}
	mustPutRoot(t, grouped, groupedRoot)
	allEvents := make([]llm.Item, 0, eventCount*2)
	for index := 0; index < eventCount; index++ {
		allEvents = append(allEvents,
			llm.HumanText(fmt.Sprintf("delta-%03d", index)),
			llm.HumanText(fmt.Sprintf("response-%03d", index)),
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
	mustPutChild(t, grouped, groupedLeaf)
	groupedState := mustMaterialize(t, grouped, "tenant-a", groupedLeaf.Handle)

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
	root.Delta = []llm.Item{lookupCall("call-1", `{"q":"x"}`)}
	mustPutRoot(t, graph, root)
	assertMaterializeErr(t, graph, "tenant-a", "root", nil, "materialize root with pending tool call")
	bad := childCheckpoint("bad", "root", "tenant-a", "op-bad", "message before result")
	mustPutChild(t, graph, bad)
	if _, err := graph.Materialize("tenant-a", "bad"); err == nil {
		t.Fatal("child began inside an unmatched tool exchange")
	}
	good := childCheckpoint("good", "root", "tenant-a", "op-good", "")
	good.Delta = []llm.Item{llm.ToolResult{CallID: "call-1", Content: []llm.Part{llm.TextPart{Text: "ok"}}}}
	mustPutChild(t, graph, good)
	assertMaterializeErr(t, graph, "tenant-a", "good", nil, "materialize matching tool result")
}

func TestMaterializationAllowsParallelToolCalls(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{})
	root := rootCheckpoint("parallel", "tenant-a", "op-parallel")
	root.Delta = []llm.Item{lookupCall("call-1", `{"q":"one"}`), lookupCall("call-2", `{"q":"two"}`)}
	mustPutRoot(t, graph, root)
	materialized := mustMaterialize(t, graph, "tenant-a", "parallel")
	if !reflect.DeepEqual(materialized.PendingToolCalls, []string{"call-1", "call-2"}) {
		t.Fatalf("pending tool calls = %#v", materialized.PendingToolCalls)
	}
}

func TestMaterializationRejectsToolCallAfterParallelResultsBegin(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{})
	root := rootCheckpoint("interleaved", "tenant-a", "op-interleaved-root")
	root.Delta = []llm.Item{lookupCall("call-1", `{"q":"one"}`), lookupCall("call-2", `{"q":"two"}`)}
	mustPutRoot(t, graph, root)
	parent := Handle("interleaved")
	child := childCheckpoint("bad-interleaved", parent.String(), "tenant-a", "op-interleaved-child", "")
	child.Delta = []llm.Item{
		llm.ToolResult{CallID: "call-1", Content: []llm.Part{llm.TextPart{Text: "one"}}},
		lookupCall("call-3", `{"q":"three"}`),
	}
	mustPutChild(t, graph, child)
	if _, err := graph.Materialize("tenant-a", child.Handle); err == nil {
		t.Fatal("materialization accepted a new tool call after parallel results began")
	}
}

func ptr(value string) *string { return &value }

func TestMaterializationAllowsTextWithinParallelToolResponse(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{})
	root := rootCheckpoint("mixed", "tenant-a", "mixed-root")
	root.Output = []llm.Item{
		lookupCall("a", "{}"),
		llm.ModelText("Checking another source"),
		lookupCall("b", "{}"),
	}
	mustPutRoot(t, graph, root)
	materialized := mustMaterialize(t, graph, "tenant-a", root.Handle)
	if !reflect.DeepEqual(materialized.PendingToolCalls, []string{"a", "b"}) {
		t.Fatalf("pending = %v", materialized.PendingToolCalls)
	}
	child := childCheckpoint("mixed-child", root.Handle.String(), "tenant-a", "mixed-child-op", "")
	child.Delta = []llm.Item{llm.ToolResult{CallID: "b"}, llm.ToolResult{CallID: "a"}}
	child.Output = []llm.Item{llm.ModelText("Done")}
	mustPutChild(t, graph, child)
	resolved := mustMaterialize(t, graph, "tenant-a", child.Handle)
	if len(resolved.PendingToolCalls) != 0 {
		t.Fatalf("pending = %v", resolved.PendingToolCalls)
	}
}

func TestTranscriptRejectsMessagesDuringUnresolvedToolExchange(t *testing.T) {
	calls := []llm.Item{lookupCall("a", "{}"), lookupCall("b", "{}")}
	for _, actor := range []llm.Actor{llm.ActorHuman, llm.ActorModel} {
		items := append([]llm.Item{}, calls...)
		if actor == llm.ActorModel {
			items = append(items, llm.ToolResult{CallID: "a"})
		}
		items = append(items, llm.Message{Actor: actor, Content: []llm.Part{llm.TextPart{Text: "new turn"}}})
		if _, err := ValidateTranscript(items); err == nil {
			t.Fatalf("accepted premature %s message", actor)
		}
	}
}
