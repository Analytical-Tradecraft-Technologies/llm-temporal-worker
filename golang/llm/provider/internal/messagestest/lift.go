package messagestest

import (
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// Lift lifts a Messages response with the profile and call of the family
// under test.
type Lift func(response *anthropic.Message, requestID string) (llm.Response, error)

// LiftPreservesThinkingToolOrderUsageAndActualTier checks that a priority
// response keeps its thinking, text and tool blocks in order, its usage and
// provider facts, and a continuation named continuationHandle on family.
func LiftPreservesThinkingToolOrderUsageAndActualTier(t *testing.T, family provider.Family, continuationHandle string, lift Lift) {
	const thinkingRaw = `{"type":"thinking","thinking":"private","signature":"sig-1"}`
	const redactedRaw = `{"type":"redacted_thinking","data":"redacted-1"}`
	response := testassert.UnmarshalJSON[anthropic.Message](t, []byte(`{
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
    }`))
	lifted, err := lift(&response, "req-1")
	if err != nil {
		t.Fatal(err)
	}
	if lifted.Status != llm.ResponseStatusToolCalls || lifted.Service.Actual == nil || *lifted.Service.Actual != llm.ServiceClassPriority {
		t.Fatalf("status/service = %#v %#v", lifted.Status, lifted.Service)
	}
	if lifted.Provider.ResponseID != "msg_1" || lifted.Provider.RequestID != "req-1" || lifted.Provider.FinishReason != "tool_use" {
		t.Fatalf("provider facts = %#v", lifted.Provider)
	}
	if lifted.Route.ResolvedModel != "claude-resolved" || lifted.Route.APIFamily != string(family) {
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
	if lifted.Continuation == nil || lifted.Continuation.Handle != continuationHandle || len(lifted.Continuation.ProviderStates) != 2 {
		t.Fatalf("continuation = %#v", lifted.Continuation)
	}
	if string(lifted.Usage.ProviderRaw["cache_creation_input_tokens"]) != "2" {
		t.Fatalf("usage raw = %#v", lifted.Usage.ProviderRaw)
	}
}

// LiftEmitsContinuationOnlyForReplayableProviderState checks that only a
// response carrying thinking state gets a continuation. serviceTier is the
// usage tier label the family reports.
func LiftEmitsContinuationOnlyForReplayableProviderState(t *testing.T, serviceTier string, lift Lift) {
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
				"usage":{"input_tokens":1,"output_tokens":1,"service_tier":"` + serviceTier + `"}
			}`,
		},
		{
			name: "thinking state",
			response: `{
				"id":"msg_thinking", "type":"message", "role":"assistant", "model":"claude-contract",
				"content":[{"type":"thinking","thinking":"private","signature":"sig-1"}], "stop_reason":"end_turn",
				"usage":{"input_tokens":1,"output_tokens":1,"service_tier":"` + serviceTier + `"}
			}`,
			wantContinuation: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := testassert.UnmarshalJSON[anthropic.Message](t, []byte(test.response))
			lifted, err := lift(&response, "req")
			if err != nil {
				t.Fatal(err)
			}
			if (lifted.Continuation != nil) != test.wantContinuation {
				t.Fatalf("continuation = %#v, want present=%t", lifted.Continuation, test.wantContinuation)
			}
		})
	}
}
