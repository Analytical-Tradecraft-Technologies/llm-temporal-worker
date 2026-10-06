package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
)

// requireLineageLimitRejected asserts that Prepare refused the request before
// any budget or provider effect: no dispatch, no pending row, and a
// non-retryable invalid_argument that was never dispatched.
func (f *boundedCloudFixture) requireLineageLimitRejected(t *testing.T, input llm.PrepareExecutionV1) {
	t.Helper()
	submits := f.submits.Load()
	_, err := f.runtime.PrepareExecutionV1(context.Background(), input)
	var failure *provider.Error
	if !errors.As(err, &failure) || failure.Code != provider.CodeInvalidArgument || failure.Retry != provider.RetryNever || failure.Dispatch != provider.DispatchNotDispatched {
		t.Fatalf("prepare error = %v", err)
	}
	if f.submits.Load() != submits {
		t.Fatal("rejected request reached the provider")
	}
	for shard := range cloudstate.PendingShards {
		page, err := f.repository.ListPending(context.Background(), shard, 100, "")
		if err != nil || len(page.Requests) != 0 {
			t.Fatalf("rejected request left pending rows: %+v, err=%v", page.Requests, err)
		}
	}
}

// A parent at the configured depth is publishable, so it must be readable, but
// no child can ever be published on it. The real composition rejects such a
// request in Prepare instead of paying for a response it cannot publish.
func TestCloudPrepareRejectsGenerateAtMaxDepth(t *testing.T) {
	f := boundedCloud(t, false)
	f.options.Limits = state.MaterializeLimits{MaxDepth: 2}
	f.restart(t)
	root := f.finish(t)
	f.continueFrom(root, "child")
	child := f.finish(t)
	f.continueFrom(child, "grandchild")
	leaf := f.finish(t)
	if leaf.Generate.Checkpoint.Depth != 2 || f.submits.Load() != 3 {
		t.Fatalf("depth=%d submits=%d", leaf.Generate.Checkpoint.Depth, f.submits.Load())
	}
	f.continueFrom(leaf, "too-deep")
	f.requireLineageLimitRejected(t, llm.PrepareExecutionV1{Generate: &f.request})
}

func TestCloudPrepareRejectsCompactAtMaxDepth(t *testing.T) {
	f := boundedCloud(t, false)
	f.options.Limits = state.MaterializeLimits{MaxDepth: 1}
	f.restart(t)
	// recent_turns 0 gives the compaction a prefix to summarize, so on master
	// this scenario paid for the summary before publication refused it.
	policy := json.RawMessage(`{"recent_turns":0}`)
	f.request.SettingsPatch.CompactionPolicy.Set = &policy
	root := f.finish(t)
	f.continueFrom(root, "child")
	leaf := f.finish(t)
	if leaf.Generate.Checkpoint.Depth != 1 {
		t.Fatalf("depth=%d", leaf.Generate.Checkpoint.Depth)
	}
	f.now = f.now.Add(time.Minute)
	request := llm.CompactRequestV1{OperationKey: "compact", Context: f.request.Context, Parent: leaf.Generate.Checkpoint.Handle}
	f.requireLineageLimitRejected(t, llm.PrepareExecutionV1{Compact: &request})
}

// The row bound follows the configured depth: a deployment that raises
// continuation_depth above the default row count must not be capped silently.
func TestCloudPrepareRejectsGenerateAtMaxRows(t *testing.T) {
	f := boundedCloud(t, false)
	f.options.Limits = state.MaterializeLimits{MaxDepth: 8, MaxRows: 2}
	f.restart(t)
	root := f.finish(t)
	f.continueFrom(root, "child")
	leaf := f.finish(t)
	f.continueFrom(leaf, "too-many-rows")
	f.requireLineageLimitRejected(t, llm.PrepareExecutionV1{Generate: &f.request})
}

// A transcript with no room left for a single output item cannot publish, so
// it is refused before the provider is paid.
func TestCloudPrepareRejectsGenerateAtMaxItems(t *testing.T) {
	f := boundedCloud(t, false)
	f.options.Limits = state.MaterializeLimits{MaxItems: 4}
	f.restart(t)
	user := func(text string) llm.Item {
		return llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}}
	}
	f.request.Append = []llm.Item{user("a"), user("b"), user("c")}
	if parent := f.finish(t); len(parent.Generate.Output) != 1 {
		t.Fatalf("output = %+v", parent.Generate.Output)
	}
	f.request.OperationKey, f.request.Append = "full", []llm.Item{user("a"), user("b"), user("c"), user("d")}
	f.requireLineageLimitRejected(t, llm.PrepareExecutionV1{Generate: &f.request})
}
