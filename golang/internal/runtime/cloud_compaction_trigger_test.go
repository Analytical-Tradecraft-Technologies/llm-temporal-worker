package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// compactionTriggerFixture drives the real cloud composition with the bytes/4
// fallback estimator so compaction planning sees the sizes a caller would.
func compactionTriggerFixture(t *testing.T, policy string, instructions ...llm.Instruction) *boundedCloudFixture {
	t.Helper()
	f := boundedCloud(t, false)
	f.cap.BudgetEstimator.Tokenizer = nil
	if policy != "" {
		raw := json.RawMessage(policy)
		f.request.SettingsPatch.CompactionPolicy.Set = &raw
	}
	if len(instructions) != 0 {
		f.request.SettingsPatch.Instructions.Set = &instructions
	}
	f.restart(t)
	return f
}

// turn runs one Generate as a child of the previous checkpoint and returns the
// new checkpoint handle.
func (f *boundedCloudFixture) turn(t *testing.T, key, text string) llm.CheckpointHandle {
	t.Helper()
	f.request.OperationKey = key
	f.request.Append = []llm.Item{preparationMessage(text)}
	result := f.finish(t)
	handle := result.Generate.Checkpoint.Handle
	f.request.Parent = &handle
	f.request.SettingsPatch = llm.SettingsPatchV1{}
	f.now = f.now.Add(time.Second)
	return handle
}

// compact runs the automatic compaction the generation workflow would run
// before the Generate the fixture is about to plan.
func (f *boundedCloudFixture) compact(t *testing.T, key string) llm.CheckpointHandle {
	t.Helper()
	ctx := context.Background()
	request := llm.CompactRequestV1{OperationKey: key, Context: f.request.Context, Parent: *f.request.Parent, Cache: &llm.CachePolicyV1{}}
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Compact: &request})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: request.Context}
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionAcquired)
	f.restart(t)
	v, err = f.runtime.CompactStepV1(ctx, request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	f.restart(t)
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionCompleted)
	f.request.Parent = &v.Compact.Checkpoint.Handle
	f.now = f.now.Add(time.Second)
	return v.Compact.Checkpoint.Handle
}

func (f *boundedCloudFixture) plan(t *testing.T, key, text string) bool {
	t.Helper()
	f.request.OperationKey = key
	f.request.Append = []llm.Item{preparationMessage(text)}
	before := f.submits.Load()
	decision, err := f.runtime.PlanGenerationV1(context.Background(), f.request)
	if err != nil {
		t.Fatal(err)
	}
	if f.submits.Load() != before {
		t.Fatal("planning performed paid work")
	}
	return decision.CompactBeforeGenerate
}

// A system prompt that alone reaches the default trigger cannot be removed by
// compaction, so planning must not buy a summariser call on every turn.
func TestCloudGenerationPlanSkipsCompactionThatCannotReachTrigger(t *testing.T) {
	prompt := llm.Instruction{Kind: llm.InstructionKindText, Level: llm.InstructionLevelApplication, Text: strings.Repeat("rule ", 12<<10)}
	f := compactionTriggerFixture(t, "", prompt)
	for i, text := range []string{"first", "second", "third"} {
		f.turn(t, "turn-"+string(rune('1'+i)), text)
	}
	if f.plan(t, "turn-4", "fourth") {
		t.Fatal("compaction requested although the system prompt alone exceeds the trigger")
	}
	// The same decision after a compaction: the summary cannot shrink the
	// prompt either, so the next turn must not compact again.
	f.compact(t, "compaction-1")
	if f.plan(t, "turn-5", "fifth") {
		t.Fatal("compaction requested again right after compacting")
	}
	f.turn(t, "turn-5", "fifth")
	if f.plan(t, "turn-6", "sixth") {
		t.Fatal("compaction requested on every turn")
	}
}

// A recent window larger than target_tokens is shortened, oldest turn first,
// so the compacted checkpoint lands within the target instead of carrying the
// oversized turn forward and compacting again on the next Generate.
func TestCloudCompactionWindowHonoursTargetTokens(t *testing.T) {
	f := compactionTriggerFixture(t, `{"recent_turns":4,"trigger_tokens":12000,"target_tokens":8000}`)
	f.turn(t, "turn-1", "first")
	f.turn(t, "turn-2", strings.Repeat("image ", 9<<10))
	f.turn(t, "turn-3", "third")
	// Parent items: first, answer, image, answer, third, answer. The window
	// of four would start at the oversized turn; three turns fit the target.
	if !f.plan(t, "turn-4", "fourth") {
		t.Fatal("oversized history did not request compaction")
	}
	handle := f.compact(t, "compaction-1")
	replay, err := f.runtime.preparation.replay.Compact(context.Background(), llm.CompactRequestV1{OperationKey: "inspect", Context: f.request.Context, Parent: handle})
	if err != nil {
		t.Fatal(err)
	}
	if len(replay.State.Items) != 4 {
		t.Fatalf("compacted checkpoint kept %d items, want summary plus three turns", len(replay.State.Items))
	}
	for _, item := range replay.State.Items[1:] {
		if data, _ := json.Marshal(item); strings.Contains(string(data), "image image") {
			t.Fatal("oversized turn survived in the retained window")
		}
	}
	if f.plan(t, "after-compaction", "fourth") {
		t.Fatal("checkpoint within the target requested compaction again")
	}
}

// Old turns that compaction can remove still trigger it, and the compacted
// checkpoint stays below the trigger so the following turn does not compact
// again.
func TestCloudGenerationPlanCompactsRemovablePrefixOnce(t *testing.T) {
	f := compactionTriggerFixture(t, `{"recent_turns":1}`)
	big := strings.Repeat("history ", 3<<10)
	for i := 1; i <= 3; i++ {
		f.turn(t, "turn-"+string(rune('0'+i)), big)
	}
	if !f.plan(t, "turn-4", "fourth") {
		t.Fatal("removable history above the trigger did not request compaction")
	}
	f.compact(t, "compaction-1")
	if f.plan(t, "after-compaction", "fourth") {
		t.Fatal("compacted checkpoint requested compaction again")
	}
}
