package anthropicmessages

import (
	"encoding/json"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/anthropics/anthropic-sdk-go"
	"testing"
)

func TestHostedToolsAndPausedContinuation(t *testing.T) {
	profile := mustProfile(t, testProfile())
	request := llm.Request{OperationKey: "hosted", Model: "claude-contract", WebSearch: true, WebFetch: true, CodeExecution: true, Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "fetch and calculate"}}}}}
	params, err := lowerRequest(request, profile, "")
	if err != nil {
		t.Fatal(err)
	}
	wire := marshalWire(t, params)
	tools := wire["tools"].([]any)
	if len(tools) != 3 {
		t.Fatalf("tools=%#v", tools)
	}
	if tools[0].(map[string]any)["type"] != "web_search_20250305" || tools[1].(map[string]any)["type"] != "web_fetch_20250910" || tools[2].(map[string]any)["type"] != "code_execution_20250825" {
		t.Fatalf("tools=%#v", tools)
	}
	var message anthropic.Message
	if err := json.Unmarshal([]byte(`{"id":"msg-1","type":"message","role":"assistant","model":"claude-contract","stop_reason":"pause_turn","container":{"id":"container-1","expires_at":"2026-10-07T00:00:00Z"},"content":[{"type":"server_tool_use","id":"srv-1","name":"bash_code_execution","input":{"command":"echo 4"}},{"type":"bash_code_execution_tool_result","tool_use_id":"srv-1","content":{"type":"bash_code_execution_result","stdout":"4","stderr":"","return_code":0,"content":[{"type":"bash_code_execution_output","file_id":"file-1"}]}}],"usage":{"input_tokens":3,"output_tokens":2,"service_tier":"standard"}}`), &message); err != nil {
		t.Fatal(err)
	}
	got, err := profile.liftResponse(provider.Call{OperationKey: "hosted", Model: request.Model, ServiceClass: llm.ServiceClassStandard, Metadata: provider.CallMetadata{CodeExecution: true}}, &message, "req")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != llm.ResponseStatusPaused || got.Cost.Status != llm.CostStatusUnknown || len(got.Output) != 3 {
		t.Fatalf("got=%#v", got)
	}
	request.Input = got.Output
	request.CodeExecution = false
	request.WebFetch = false
	request.WebSearch = false
	replay, err := lowerRequest(request, profile, "")
	if err != nil {
		t.Fatalf("cannot replay paused result: %v", err)
	}
	if marshalWire(t, replay)["container"] != "container-1" {
		t.Fatal("paused continuation lost container")
	}
	request.Input = nil
	request.Continuation = got.Continuation
	replay, err = lowerRequest(request, profile, "")
	if err != nil {
		t.Fatalf("cannot replay continuation state: %v", err)
	}
	if marshalWire(t, replay)["container"] != "container-1" {
		t.Fatal("legacy continuation lost container")
	}
}
