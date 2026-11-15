package runtime

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// A response that carries citations publishes them in the checkpoint
// transcript. The next turn and a later compaction replay that transcript
// through the real request compiler, which must leave the annotations out
// instead of rejecting the only candidate as no_route.
func TestCloudExecutionRuntimeContinuesCheckpointWithReferenceOutput(t *testing.T) {
	f := boundedCloud(t, false)
	compiler := responsesAdapter(t, &http.Client{Transport: planningTransportFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected HTTP request")
	})})
	f.adapter.compile = func(input provider.CompileInput) (provider.Call, error) {
		// The fixture adapter declares no features, so compile against the
		// real adapter's own capability set.
		real := input
		var err error
		if real.Capability, err = compiler.Capabilities(context.Background(), input.Query); err != nil {
			return provider.Call{}, err
		}
		if _, err := compiler.Compile(context.Background(), real); err != nil {
			return provider.Call{}, err
		}
		return planningCall(input), nil
	}
	reference := llm.Reference{URI: "https://example.com/cited-source"}
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		cited := f.submits.Add(1) < 3 // the summary itself is plain text
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		v := executionResponse(call)
		v.Result.Response.Output = []llm.Item{llm.ModelText("answer")}
		if cited {
			v.Result.Response.Output = append(v.Result.Response.Output, reference)
		}
		return v.Result, nil
	}
	// Compaction later summarises the whole transcript, citation included.
	keepNoRecentTurns(&f.request)
	root := f.finish(t)
	if root.Generate == nil || len(root.Generate.Output) != 2 || root.Generate.Output[1].ItemKind() != llm.ItemKindReference {
		t.Fatalf("root response lost its citation: %+v", root.Generate)
	}

	f.now = f.now.Add(time.Minute)
	f.request = f.followUp(root.Generate.Checkpoint.Handle, "follow-up", "follow-up")
	child := f.finish(t)
	if child.Generate == nil || child.Generate.Checkpoint.Parent == nil || *child.Generate.Checkpoint.Parent != root.Generate.Checkpoint.Handle || f.submits.Load() != 2 {
		t.Fatalf("follow-up turn did not continue the checkpoint: %+v", child.Generate)
	}
	f.adapter.mu.Lock()
	replayed := f.adapter.inputs[len(f.adapter.inputs)-1].Request.Input
	f.adapter.mu.Unlock()
	// input, answer, citation, follow-up: the transcript keeps the citation.
	if len(replayed) != 4 || replayed[2].ItemKind() != llm.ItemKindReference {
		t.Fatalf("replayed transcript = %#v", replayed)
	}

	f.now = f.now.Add(time.Minute)
	ctx := context.Background()
	compact := f.compactRequest(child, nil)
	v := f.runCompact(t, ctx, compact, false)
	if v.Compact == nil || f.submits.Load() != 3 {
		t.Fatal("compaction did not summarise the cited transcript")
	}

	// A plain-text summary cannot hold a citation, so the compacted
	// transcript carries both citations verbatim after it.
	f.now = f.now.Add(time.Minute)
	f.request = f.followUp(v.Compact.Checkpoint.Handle, "after-compaction", "after compaction")
	if after := f.finish(t); after.Generate == nil || f.submits.Load() != 4 {
		t.Fatalf("turn after compaction did not continue: %+v", after.Generate)
	}
	f.adapter.mu.Lock()
	replayed = f.adapter.inputs[len(f.adapter.inputs)-1].Request.Input
	f.adapter.mu.Unlock()
	if len(replayed) != 4 || replayed[0].ItemKind() != llm.ItemKindMessage || replayed[1].ItemKind() != llm.ItemKindReference || replayed[2].ItemKind() != llm.ItemKindReference {
		t.Fatalf("compacted transcript lost its citations: %#v", replayed)
	}
}
