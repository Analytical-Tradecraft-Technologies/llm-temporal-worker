package openaichat

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// A model turn with text between its tool calls, as Anthropic lifts it,
// lowers to one assistant message carrying both calls. Splitting it would put
// an assistant message between call A and its tool message, which Chat
// Completions rejects (#1354).
func TestCompileCoalescesInterleavedModelTurnIntoOneAssistantMessage(t *testing.T) {
	for _, strict := range []bool{true, false} {
		call, err := testAdapter(t).Compile(context.Background(), provider.CompileInput{
			Request: llm.Request{OperationKey: "op-interleaved", Model: "chat-model", Input: []llm.Item{
				llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "look both up"}}},
				llm.ToolCall{ID: "call-a", Name: "lookup", Arguments: json.RawMessage(`{"q":"a"}`)},
				llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "and the other"}}},
				llm.ToolCall{ID: "call-b", Name: "lookup", Arguments: json.RawMessage(`{"q":"b"}`)},
				llm.ToolResult{CallID: "call-a", Content: []llm.Part{llm.TextPart{Text: "result a"}}},
				llm.ToolResult{CallID: "call-b", Content: []llm.Part{llm.TextPart{Text: "result b"}}},
				llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "done"}}},
			}},
			Query:  provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"},
			Strict: strict,
		})
		if err != nil {
			t.Fatalf("strict=%t: %v", strict, err)
		}
		messages := marshalWire(t, call.SDKParams.(openaiChatParams))["messages"].([]any)
		if len(messages) != 5 {
			t.Fatalf("strict=%t messages = %#v", strict, messages)
		}
		assistant := messages[1].(map[string]any)
		calls, _ := assistant["tool_calls"].([]any)
		if assistant["role"] != "assistant" || len(calls) != 2 ||
			calls[0].(map[string]any)["id"] != "call-a" || calls[1].(map[string]any)["id"] != "call-b" {
			t.Fatalf("strict=%t assistant message = %#v", strict, assistant)
		}
		content, _ := assistant["content"].([]any)
		if len(content) != 1 || content[0].(map[string]any)["text"] != "and the other" {
			t.Fatalf("strict=%t assistant content = %#v", strict, assistant["content"])
		}
		for index, id := range []string{"call-a", "call-b"} {
			tool := messages[2+index].(map[string]any)
			if tool["role"] != "tool" || tool["tool_call_id"] != id {
				t.Fatalf("strict=%t tool message %d = %#v", strict, index, tool)
			}
		}
		// Text after the tool results starts the next assistant message.
		if final := messages[4].(map[string]any); final["role"] != "assistant" || final["tool_calls"] != nil {
			t.Fatalf("strict=%t final message = %#v", strict, final)
		}
	}
}
