package openairesponses

import (
	"encoding/json"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/openai/openai-go/v3/responses"
	"testing"
)

func TestHostedToolsSurviveSDKWire(t *testing.T) {
	for _, on := range []bool{false, true} {
		wire := captureWireBody(t, wireAuditProfiles()[0], llm.Request{OperationKey: "hosted", Model: "gpt-test", WebSearch: on, CodeExecution: on, Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "calculate"}}}}})
		if !on {
			if _, ok := wire["tools"]; ok {
				t.Fatal("off still sends tools")
			}
			continue
		}
		tools := wire["tools"].([]any)
		if len(tools) != 2 || tools[1].(map[string]any)["type"] != "web_search" || tools[0].(map[string]any)["type"] != "code_interpreter" {
			t.Fatalf("tools=%#v", tools)
		}
		if tools[0].(map[string]any)["container"].(map[string]any)["memory_limit"] != "1g" {
			t.Fatal("unbounded container")
		}
		if wire["max_tool_calls"] != float64(3) {
			t.Fatal("missing tool-call cap")
		}
	}
}
func TestHostedExecutionResultAndArtifact(t *testing.T) {
	var response responses.Response
	if err := json.Unmarshal([]byte(`{"id":"resp-code","object":"response","status":"completed","model":"gpt-test","output":[{"id":"ci-1","type":"code_interpreter_call","status":"completed","container_id":"cntr-1","code":"print(4)","outputs":[{"type":"logs","logs":"4"}]},{"id":"msg-1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"4","annotations":[{"type":"container_file_citation","container_id":"cntr-1","file_id":"file-1","filename":"result.csv","start_index":0,"end_index":1}]}]}],"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}`), &response); err != nil {
		t.Fatal(err)
	}
	got, err := liftResponse(provider.Call{OperationKey: "hosted", Model: "gpt-test", ServiceClass: llm.ServiceClassStandard, Metadata: provider.CallMetadata{CodeExecution: true}}, &response, "request")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != llm.ResponseStatusCompleted || got.Cost.Status != llm.CostStatusUnknown {
		t.Fatalf("result=%#v", got)
	}
	if len(got.Output) != 3 {
		t.Fatalf("output=%#v", got.Output)
	}
	if _, ok := got.Output[0].(llm.ProviderState); !ok {
		t.Fatal("execution became an application call")
	}
	if r, ok := got.Output[2].(llm.Reference); !ok || len(r.Metadata["artifact"]) == 0 {
		t.Fatal("artifact citation lost")
	}
}

func TestHostedCallsRetainEncryptedReasoning(t *testing.T) {
	for _, kind := range []string{"web_search_call", "code_interpreter_call"} {
		items := []map[string]any{{"type": "reasoning", "encrypted_content": "opaque"}, {"type": kind}}
		replay, present := replayableItems(items, true)
		if !present || len(replay) != 2 {
			t.Fatalf("%s replay lost encrypted reasoning: %#v", kind, replay)
		}
	}
}
