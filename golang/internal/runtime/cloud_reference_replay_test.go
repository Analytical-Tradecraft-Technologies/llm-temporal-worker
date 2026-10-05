package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/openairesponses"
)

// A response that carries citations publishes them in the checkpoint
// transcript. The next turn and a later compaction replay that transcript
// through the real request compiler, which must leave the annotations out
// instead of rejecting the only candidate as no_route.
func TestCloudExecutionRuntimeContinuesCheckpointWithReferenceOutput(t *testing.T) {
	f := boundedCloud(t, false)
	client, err := openairesponses.NewClient(openairesponses.ClientConfig{BaseURL: "https://api.openai.com/v1/", APIKey: "test-key", HTTPClient: &http.Client{Transport: planningTransportFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected HTTP request")
	})}})
	if err != nil {
		t.Fatal(err)
	}
	compiler, err := openairesponses.New(client, "endpoint", "profile/v1")
	if err != nil {
		t.Fatal(err)
	}
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
		v.Result.Response.Output = []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "answer"}}}}
		if cited {
			v.Result.Response.Output = append(v.Result.Response.Output, reference)
		}
		return v.Result, nil
	}
	// Compaction later summarises the whole transcript, citation included.
	policy := json.RawMessage(`{"recent_turns":0}`)
	f.request.SettingsPatch.CompactionPolicy.Set = &policy
	root := f.finish(t)
	if root.Generate == nil || len(root.Generate.Output) != 2 || root.Generate.Output[1].ItemKind() != llm.ItemKindReference {
		t.Fatalf("root response lost its citation: %+v", root.Generate)
	}

	f.now = f.now.Add(time.Minute)
	f.request = llm.GenerateRequestV1{OperationKey: "follow-up", Context: f.request.Context, Parent: &root.Generate.Checkpoint.Handle, Append: []llm.Item{preparationMessage("follow-up")}}
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
	compact := llm.CompactRequestV1{OperationKey: "compact", Context: f.request.Context, Parent: child.Generate.Checkpoint.Handle}
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Compact: &compact})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: compact.Context}
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionAcquired)
	v, err = f.runtime.CompactStepV1(ctx, compact)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	if boundedState(t, v, err, llm.ExecutionCompleted).Compact == nil || f.submits.Load() != 3 {
		t.Fatal("compaction did not summarise the cited transcript")
	}
}
