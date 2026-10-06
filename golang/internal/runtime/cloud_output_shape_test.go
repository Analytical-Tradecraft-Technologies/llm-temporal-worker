package runtime

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
)

func outputShapeCall(id string) llm.ToolCall {
	return llm.ToolCall{ID: id, Name: "lookup", Arguments: json.RawMessage(`{}`)}
}

func outputShapeResult(id string) llm.ToolResult {
	return llm.ToolResult{CallID: id, Content: []llm.Part{llm.TextPart{Text: "ok"}}}
}

// answerWith makes the provider return each output in turn, one per paid call.
func (f *boundedCloudFixture) answerWith(outputs ...[]llm.Item) {
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		index := int(f.submits.Add(1)) - 1
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		result := executionResponse(call).Result
		result.Response.Output = outputs[index]
		for _, item := range outputs[index] {
			if _, ok := item.(llm.ToolCall); ok {
				result.Response.Status = llm.ResponseStatusToolCalls
			}
		}
		return result, nil
	}
}

func (f *boundedCloudFixture) continueFrom(parent llm.ExecutionResultV1, key string, items ...llm.Item) {
	handle := parent.Generate.Checkpoint.Handle
	f.now = f.now.Add(time.Minute)
	f.request.OperationKey, f.request.Parent, f.request.Append = key, &handle, items
}

// OpenAI-compatible backends commonly number tool calls from zero in every
// response. The reused ID is replaced before the paid response is saved, so
// the turn publishes and continues with the ID the caller was given.
func TestCloudGenerateReusedLineageToolCallIDPublishes(t *testing.T) {
	f := boundedCloud(t, false)
	f.answerWith(
		[]llm.Item{outputShapeCall("call_0")},
		[]llm.Item{outputShapeCall("call_0"), outputShapeCall("call_1")},
		[]llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "done"}}}},
	)
	first := f.finish(t)
	f.continueFrom(first, "second", outputShapeResult("call_0"))
	second := f.finish(t)
	if len(second.Generate.Output) != 2 {
		t.Fatalf("output = %+v", second.Generate.Output)
	}
	reused, fresh := second.Generate.Output[0].(llm.ToolCall), second.Generate.Output[1].(llm.ToolCall)
	if reused.ID == "call_0" || reused.Name != "lookup" || fresh.ID != "call_1" || !regexp.MustCompile(`^[A-Za-z0-9_-]{1,40}$`).MatchString(reused.ID) {
		t.Fatalf("reused=%q fresh=%q", reused.ID, fresh.ID)
	}
	// A retry of the same operation replays the saved ID without paying again.
	if again := f.finish(t); again.Generate.Output[0].(llm.ToolCall).ID != reused.ID || f.submits.Load() != 2 {
		t.Fatalf("retry changed the tool-call ID or resubmitted: submits=%d", f.submits.Load())
	}
	f.continueFrom(second, "third", outputShapeResult(reused.ID), outputShapeResult("call_1"))
	if third := f.finish(t); third.Generate.Checkpoint.Depth != 2 || f.submits.Load() != 3 {
		t.Fatalf("depth=%d submits=%d", third.Generate.Checkpoint.Depth, f.submits.Load())
	}
}

func TestCloudGenerateModelTurnContentInsideToolBatchPublishes(t *testing.T) {
	thinking := llm.ProviderState{Provider: "provider", EndpointFamily: "family", MediaType: "application/json", Opaque: []byte(`{}`)}
	citation := llm.Reference{URI: "https://example.com/source"}
	for name, output := range map[string][]llm.Item{
		"call then state":        {outputShapeCall("a"), thinking},
		"state call state call":  {thinking, outputShapeCall("a"), thinking, outputShapeCall("b")},
		"call then reference":    {outputShapeCall("a"), citation},
		"call text state source": {outputShapeCall("a"), llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "checking"}}}, thinking, citation, outputShapeCall("b")},
	} {
		t.Run(name, func(t *testing.T) {
			f := boundedCloud(t, false)
			f.answerWith(output)
			result := f.finish(t)
			if len(result.Generate.Output) != len(output) || f.submits.Load() != 1 {
				t.Fatalf("output=%+v submits=%d", result.Generate.Output, f.submits.Load())
			}
		})
	}
}

// Output that can never extend the transcript ends the request as soon as the
// paid response is saved. The cost stays settled on the attempt, nothing is
// left running or pending, and no step of a retry buys another response.
func TestCloudGenerateUnusableOutputFailsTerminally(t *testing.T) {
	for name, output := range map[string][]llm.Item{
		"duplicate call ID in one response": {outputShapeCall("a"), outputShapeCall("a")},
		"unmatched tool result":             {outputShapeResult("missing")},
	} {
		t.Run(name, func(t *testing.T) {
			f := boundedCloud(t, false)
			f.request.Cache = &llm.CachePolicyV1{}
			f.answerWith(output)
			ctx := context.Background()
			v, err := f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionFailed)
			if v.FailureCode != "incomplete_response" || v.Retryable {
				t.Fatalf("failure = %+v", v)
			}
			scope := cloudstate.Scope{Tenant: "tenant", Project: "project"}
			attempt, err := f.repository.LoadRequestAttempt(ctx, scope, cloudstate.RequestID(v.RequestID))
			if err != nil {
				t.Fatal(err)
			}
			assertCloudStatus(t, f, attempt.ID, cloudstate.StatusFailed)
			assertCloudStatus(t, f, attempt.RootID, cloudstate.StatusFailed)
			saved, err := f.repository.LoadProviderExecution(ctx, scope, attempt.ID)
			if err != nil || saved.Execution.Stage != cloudstate.ExecutionSucceeded || !saved.Execution.Settled || saved.Execution.Response.Cost.ActualCostUSD == nil {
				t.Fatalf("paid attempt was not settled with its cost: %+v, err=%v", saved.Execution, err)
			}
			ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
			f.now = f.now.Add(time.Hour)
			f.restart(t)
			for _, run := range []func() (llm.ExecutionResultV1, error){
				func() (llm.ExecutionResultV1, error) {
					return f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
				},
				func() (llm.ExecutionResultV1, error) { return f.runtime.AcquireBudgetV1(ctx, ref) },
				func() (llm.ExecutionResultV1, error) { return f.runtime.GenerateStepV1(ctx, f.request) },
				func() (llm.ExecutionResultV1, error) { return f.runtime.PollExecutionV1(ctx, ref) },
				func() (llm.ExecutionResultV1, error) { return f.runtime.CompleteExecutionV1(ctx, ref) },
			} {
				again, err := run()
				boundedState(t, again, err, llm.ExecutionFailed)
				if again.FailureCode != "incomplete_response" || again.Retryable {
					t.Fatalf("failure changed: %+v", again)
				}
			}
			if f.submits.Load() != 1 {
				t.Fatalf("unusable output was paid for %d times", f.submits.Load())
			}
			// The unusable response is not cached: a new operation is a new attempt.
			f.answerWith(nil, []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "answer"}}}})
			f.request.OperationKey = "independent"
			if next := f.finish(t); next.Generate.Cache.Disposition == "hit" || f.submits.Load() != 2 {
				t.Fatalf("disposition=%q submits=%d", next.Generate.Cache.Disposition, f.submits.Load())
			}
		})
	}
}

// A resumable provider is polled after the submission activity. The poll
// reconstructs the call and must apply the same replacement and validation.
func TestCloudGenerateOutputShapeAfterPoll(t *testing.T) {
	f := boundedCloud(t, true)
	outputs := [][]llm.Item{{outputShapeCall("call_0")}, {outputShapeCall("call_0")}, {outputShapeCall("a"), outputShapeCall("a")}}
	f.adapter.poll = func(_ context.Context, call provider.Call, _ string, _ provider.Observer) (provider.ResumableResult, error) {
		f.polls.Add(1)
		v := executionResponse(call)
		v.ProviderOperationID = "job"
		v.Result.Response.Output, v.Result.Response.Status = outputs[f.submits.Load()-1], llm.ResponseStatusToolCalls
		return v, nil
	}
	first := f.finish(t)
	f.continueFrom(first, "second", outputShapeResult("call_0"))
	second := f.finish(t)
	id := second.Generate.Output[0].(llm.ToolCall).ID
	if id == "call_0" {
		t.Fatal("polled output kept a reused tool-call ID")
	}
	f.continueFrom(second, "third", outputShapeResult(id))
	ctx := context.Background()
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionPending)
	f.now = f.now.Add(2 * time.Second)
	v, err = f.runtime.PollExecutionV1(ctx, llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context})
	boundedState(t, v, err, llm.ExecutionFailed)
	if v.FailureCode != "incomplete_response" || f.submits.Load() != 3 {
		t.Fatalf("failure=%+v submits=%d", v, f.submits.Load())
	}
}

func TestUniqueToolCallIDs(t *testing.T) {
	transcript := []llm.Item{outputShapeCall("call_0"), outputShapeResult("call_0"), &llm.ToolCall{ID: "call_1", Name: "lookup", Arguments: json.RawMessage(`{}`)}, outputShapeResult("call_1")}
	output := []llm.Item{outputShapeCall("call_0"), outputShapeResult("call_0"), outputShapeCall("call_2"), &llm.ToolCall{ID: "call_1", Name: "lookup", Arguments: json.RawMessage(`{}`)}}
	got := uniqueToolCallIDs("operation", transcript, output)
	first, second := got[0].(llm.ToolCall).ID, got[3].(llm.ToolCall).ID
	if first == "call_0" || second == "call_1" || first == second || got[1].(llm.ToolResult).CallID != first || got[2].(llm.ToolCall).ID != "call_2" {
		t.Fatalf("got %+v", got)
	}
	if output[0].(llm.ToolCall).ID != "call_0" || output[3].(*llm.ToolCall).ID != "call_1" {
		t.Fatal("provider output was mutated")
	}
	if again := uniqueToolCallIDs("operation", transcript, output); again[0].(llm.ToolCall).ID != first || again[3].(llm.ToolCall).ID != second {
		t.Fatal("replacement is not deterministic")
	}
	if other := uniqueToolCallIDs("other", transcript, output); other[0].(llm.ToolCall).ID == first {
		t.Fatal("replacement ignores the attempt")
	}
	// A replacement never lands on an ID already present in the lineage or output.
	taken := append(append([]llm.Item(nil), transcript...), outputShapeCall(first), outputShapeResult(first))
	if moved := uniqueToolCallIDs("operation", taken, output)[0].(llm.ToolCall).ID; moved == first || moved == "call_0" {
		t.Fatalf("replacement collided: %q", moved)
	}
	// Calls that share an ID within one response still share one afterwards.
	twice := uniqueToolCallIDs("operation", transcript, []llm.Item{outputShapeCall("call_0"), outputShapeCall("call_0")})
	if twice[0].(llm.ToolCall).ID != twice[1].(llm.ToolCall).ID {
		t.Fatal("ambiguous duplicate calls were separated")
	}
	untouched := []llm.Item{outputShapeCall("new"), outputShapeResult("call_0")}
	if kept := uniqueToolCallIDs("operation", transcript, untouched); kept[0].(llm.ToolCall).ID != "new" || kept[1].(llm.ToolResult).CallID != "call_0" {
		t.Fatalf("unrelated output changed: %+v", kept)
	}
}
