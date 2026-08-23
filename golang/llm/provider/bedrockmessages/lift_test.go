package bedrockmessages

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestLiftPreservesThinkingToolOrderUsageAndActualTier(t *testing.T) {
	var response anthropic.Message
	const thinkingRaw = `{"type":"thinking","thinking":"private","signature":"sig-1"}`
	const redactedRaw = `{"type":"redacted_thinking","data":"redacted-1"}`
	if err := json.Unmarshal([]byte(`{
      "id":"msg_1",
      "type":"message",
      "role":"assistant",
      "model":"claude-resolved",
      "content":[`+thinkingRaw+`,`+redactedRaw+`,
        {"type":"text","text":"hello"},
        {"type":"text","text":" world"},
        {"type":"tool_use","id":"toolu_1","name":"lookup","input":{"q":"sydney"}}
      ],
      "stop_reason":"tool_use",
      "stop_sequence":null,
      "usage":{
        "input_tokens":10,
        "output_tokens":7,
        "cache_creation_input_tokens":2,
        "cache_read_input_tokens":3,
        "output_tokens_details":{"thinking_tokens":4},
        "service_tier":"priority"
      }
    }`), &response); err != nil {
		t.Fatal(err)
	}
	profile := mustBedrockProfile(t, "")
	call := provider.Call{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract", OperationKey: "bedrock-lift", ServiceClass: llm.ServiceClassPriority}
	lifted, err := profile.liftResponse(call, &response, "req-1")
	if err != nil {
		t.Fatal(err)
	}
	if lifted.Status != llm.ResponseStatusToolCalls || lifted.Service.Actual == nil || *lifted.Service.Actual != llm.ServiceClassPriority {
		t.Fatalf("status/service = %#v %#v", lifted.Status, lifted.Service)
	}
	if lifted.Provider.ResponseID != "msg_1" || lifted.Provider.RequestID != "req-1" || lifted.Provider.FinishReason != "tool_use" {
		t.Fatalf("provider facts = %#v", lifted.Provider)
	}
	if lifted.Route.ResolvedModel != "claude-resolved" || lifted.Route.APIFamily != string(provider.FamilyBedrockMessages) {
		t.Fatalf("route = %#v", lifted.Route)
	}
	if lifted.Usage.InputTokens != 10 || lifted.Usage.OutputTokens != 7 || lifted.Usage.ReasoningTokens != 4 || lifted.Usage.CacheReadTokens != 3 || lifted.Usage.CacheWriteTokens != 2 {
		t.Fatalf("usage = %#v", lifted.Usage)
	}
	if len(lifted.Output) != 4 {
		t.Fatalf("output length = %d (%#v)", len(lifted.Output), lifted.Output)
	}
	if state, ok := lifted.Output[0].(llm.ProviderState); !ok || string(state.Opaque) != thinkingRaw {
		t.Fatalf("thinking state = %#v", lifted.Output[0])
	}
	if state, ok := lifted.Output[1].(llm.ProviderState); !ok || string(state.Opaque) != redactedRaw {
		t.Fatalf("redacted state = %#v", lifted.Output[1])
	}
	if message, ok := lifted.Output[2].(llm.Message); !ok || message.Actor != llm.ActorModel || len(message.Content) != 2 || message.Content[0].(llm.TextPart).Text != "hello" || message.Content[1].(llm.TextPart).Text != " world" {
		t.Fatalf("text output = %#v", lifted.Output[2])
	}
	if toolCall, ok := lifted.Output[3].(llm.ToolCall); !ok || toolCall.ID != "toolu_1" || toolCall.Name != "lookup" || string(toolCall.Arguments) != `{"q":"sydney"}` {
		t.Fatalf("tool output = %#v", lifted.Output[3])
	}
	if lifted.Continuation == nil || lifted.Continuation.Handle != "bedrock-messages:msg_1" || len(lifted.Continuation.ProviderStates) != 2 {
		t.Fatalf("continuation = %#v", lifted.Continuation)
	}
	if string(lifted.Usage.ProviderRaw["cache_creation_input_tokens"]) != "2" {
		t.Fatalf("usage raw = %#v", lifted.Usage.ProviderRaw)
	}
}

func TestLiftEmitsContinuationOnlyForReplayableProviderState(t *testing.T) {
	profile := mustBedrockProfile(t, "")
	call := provider.Call{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract", OperationKey: "bedrock-continuation", ServiceClass: llm.ServiceClassStandard}
	for _, test := range []struct {
		name             string
		response         string
		wantContinuation bool
	}{
		{
			name: "text only",
			response: `{
				"id":"msg_text", "type":"message", "role":"assistant", "model":"claude-contract",
				"content":[{"type":"text","text":"hello"}], "stop_reason":"end_turn",
				"usage":{"input_tokens":1,"output_tokens":1,"service_tier":"default"}
			}`,
		},
		{
			name: "thinking state",
			response: `{
				"id":"msg_thinking", "type":"message", "role":"assistant", "model":"claude-contract",
				"content":[{"type":"thinking","thinking":"private","signature":"sig-1"}], "stop_reason":"end_turn",
				"usage":{"input_tokens":1,"output_tokens":1,"service_tier":"default"}
			}`,
			wantContinuation: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var response anthropic.Message
			if err := json.Unmarshal([]byte(test.response), &response); err != nil {
				t.Fatal(err)
			}
			lifted, err := profile.liftResponse(call, &response, "req")
			if err != nil {
				t.Fatal(err)
			}
			if (lifted.Continuation != nil) != test.wantContinuation {
				t.Fatalf("continuation = %#v, want present=%t", lifted.Continuation, test.wantContinuation)
			}
		})
	}
}

func TestLiftMapsTerminalReasonsAndRejectsInvalidProviderFacts(t *testing.T) {
	profile := mustBedrockProfile(t, "")
	call := provider.Call{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract", OperationKey: "bedrock-status", ServiceClass: llm.ServiceClassStandard}
	for _, test := range []struct {
		reason string
		want   llm.ResponseStatus
	}{
		{reason: "end_turn", want: llm.ResponseStatusCompleted},
		{reason: "stop_sequence", want: llm.ResponseStatusCompleted},
		{reason: "max_tokens", want: llm.ResponseStatusLength},
		{reason: "pause_turn", want: llm.ResponseStatusCompleted},
		{reason: "refusal", want: llm.ResponseStatusRefused},
	} {
		response := anthropic.Message{ID: "status-" + test.reason, Model: "claude-contract", StopReason: anthropic.StopReason(test.reason), Usage: anthropic.Usage{ServiceTier: anthropic.UsageServiceTier("default")}}
		if test.reason == "refusal" {
			response.StopDetails.Category = anthropic.RefusalStopDetailsCategoryCyber
		}
		got, err := profile.liftResponse(call, &response, "req")
		if err != nil || got.Status != test.want {
			t.Fatalf("reason %q = %#v, %v", test.reason, got, err)
		}
	}
	for _, test := range []struct {
		name     string
		response anthropic.Message
	}{
		{name: "tool stop without tool", response: anthropic.Message{ID: "missing-tool", Model: "claude-contract", StopReason: anthropic.StopReasonToolUse, Usage: anthropic.Usage{ServiceTier: anthropic.UsageServiceTier("default")}}},
		{name: "unknown stop reason", response: anthropic.Message{ID: "unknown-stop", Model: "claude-contract", StopReason: anthropic.StopReason("future_reason"), Usage: anthropic.Usage{ServiceTier: anthropic.UsageServiceTier("default")}}},
		{name: "unknown service tier", response: anthropic.Message{ID: "unknown-tier", Model: "claude-contract", StopReason: anthropic.StopReasonEndTurn, Usage: anthropic.Usage{ServiceTier: anthropic.UsageServiceTier("scale")}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := profile.liftResponse(call, &test.response, "req")
			var providerErr *provider.Error
			if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeProviderInvalidResponse || providerErr.Dispatch != provider.DispatchAccepted {
				t.Fatalf("invalid response error = %#v", err)
			}
		})
	}
}
