package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// screenshotMessage is a human turn carrying one inline PNG of size bytes.
// The token estimate excludes inline media, so only checkpoint bytes see it.
func screenshotMessage(step, size int) llm.Item {
	return llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
		llm.TextPart{Text: fmt.Sprintf("screenshot %d", step)},
		llm.ImagePart{Bytes: bytes.Repeat([]byte{byte(step)}, size), MediaType: "image/png"},
	}}
}

// A screenshot loop keeps more recent images than the extendable bound
// holds. Compaction must summarize enough of them for the next turn to fit,
// so the loop continues and every paid summary is followed by a turn (#1346).
func TestCloudScreenshotLoopContinuesAcrossCompaction(t *testing.T) {
	t.Parallel()
	f := compactionTriggerFixture(t, `{"recent_turns":8}`)
	const image = 600 << 10 // 800 KiB once base64 encoded
	compactions := 0
	for step := 0; step < 7; step++ {
		f.request.OperationKey = fmt.Sprintf("plan-%d", step)
		f.request.Append = []llm.Item{screenshotMessage(step, image)}
		plan, err := f.runtime.PlanGenerationV1(context.Background(), f.request)
		if err != nil {
			t.Fatalf("step %d: plan = %v", step, err)
		}
		if plan.CompactBeforeGenerate {
			compactions++
			f.compact(t, fmt.Sprintf("compact-%d", step))
			f.request.OperationKey = fmt.Sprintf("replan-%d", step)
			replanned, err := f.runtime.PlanGenerationV1(context.Background(), f.request)
			if err != nil || replanned.CompactBeforeGenerate {
				t.Fatalf("step %d: plan on the compacted parent = %+v, %v; want no further compaction", step, replanned, err)
			}
		}
		f.request.OperationKey = fmt.Sprintf("turn-%d", step)
		result := f.finish(t)
		f.request.Parent = &result.Generate.Checkpoint.Handle
		f.request.SettingsPatch = llm.SettingsPatchV1{}
	}
	if compactions < 2 {
		t.Fatalf("the loop compacted %d times, want repeated compactions", compactions)
	}
}

// A single append that cannot fit even beside an empty compacted parent is
// refused at planning, before any summary is paid for.
func TestCloudGenerationPlanRefusesATurnCompactionCannotFit(t *testing.T) {
	t.Parallel()
	f := compactionTriggerFixture(t, `{"recent_turns":1}`)
	for step := 0; step < 3; step++ {
		f.turn(t, fmt.Sprintf("turn-%d", step), "small")
	}
	f.request.OperationKey = "oversized"
	// A retained turn larger than the extendable bound on its own.
	f.request.Append = []llm.Item{screenshotMessage(0, 900<<10), screenshotMessage(1, 900<<10), screenshotMessage(2, 900<<10)}
	before := f.submits.Load()
	_, err := f.runtime.PlanGenerationV1(context.Background(), f.request)
	var failure *provider.Error
	if !errors.As(err, &failure) || failure.Code != provider.CodeInvalidArgument || failure.Retry != provider.RetryNever || f.submits.Load() != before {
		t.Fatalf("plan = %v; want an unpaid invalid_argument", err)
	}
}

// A lineage at the item bound is compacted automatically instead of being
// planned as fitting and then refused by preparation (#1346).
func TestCloudGenerationPlanCompactsALineageAtTheItemBound(t *testing.T) {
	t.Parallel()
	f := compactionTriggerFixture(t, `{"recent_turns":2,"trigger_tokens":1000000,"target_tokens":999999}`)
	f.options.Limits.MaxItems = 12
	f.restart(t)
	for step := 0; step < 6; step++ {
		f.turn(t, fmt.Sprintf("turn-%d", step), "small")
	}
	if !f.plan(t, "at-bound", "small") {
		t.Fatal("planning did not request compaction for a lineage at the item bound")
	}
	f.compact(t, "rescue")
	if f.plan(t, "after-compaction", "small") {
		t.Fatal("planning requested compaction again on the compacted parent")
	}
	f.turn(t, "after-compaction", "small")
}
