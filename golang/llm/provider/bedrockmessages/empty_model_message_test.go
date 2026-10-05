package bedrockmessages

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// A lifted empty or content-filtered reply is a model message with no parts.
// Replaying it must not put an assistant message without content on the wire.
func TestCompileSkipsReplayedEmptyModelMessage(t *testing.T) {
	adapter := &Adapter{endpointID: "bedrock-prod", profile: mustBedrockProfile(t, "")}
	human := func(text string) llm.Message {
		return llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}}
	}
	// The empty model turn sits between two user turns and again next to a
	// model turn that was split into text and a tool call.
	request := llm.Request{OperationKey: "empty-model-replay", Model: "claude-contract", Input: []llm.Item{
		human("question"),
		llm.Message{Actor: llm.ActorModel},
		human("follow-up"),
		llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "looking"}}},
		llm.Message{Actor: llm.ActorModel, Content: []llm.Part{}},
		llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"sydney"}`)},
		llm.ToolResult{CallID: "call-1", Name: "lookup", Content: []llm.Part{llm.TextPart{Text: "found"}}},
	}, Tools: []llm.Tool{{Name: "lookup", InputSchema: json.RawMessage(`{"type":"object"}`)}}}
	for name, strict := range map[string]bool{"strict": true, "best effort": false} {
		t.Run(name, func(t *testing.T) {
			call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Query: provider.CapabilityQuery{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: request.Model}, Strict: strict})
			if err != nil {
				t.Fatalf("compile replayed empty model message: %v", err)
			}
			encoded, err := json.Marshal(call.SDKParams)
			if err != nil {
				t.Fatal(err)
			}
			var wire struct {
				Messages []struct {
					Role    string `json:"role"`
					Content []struct {
						Type string `json:"type"`
					} `json:"content"`
				} `json:"messages"`
			}
			if err := json.Unmarshal(encoded, &wire); err != nil {
				t.Fatal(err)
			}
			shape := make([]string, 0, len(wire.Messages))
			for _, message := range wire.Messages {
				if len(message.Content) == 0 {
					t.Fatalf("message without content reached the wire: %s", encoded)
				}
				shape = append(shape, message.Role+":"+message.Content[0].Type)
			}
			if got, want := strings.Join(shape, ","), "user:text,user:text,assistant:text,assistant:tool_use,user:tool_result"; got != want {
				t.Fatalf("messages = %s, want %s", got, want)
			}
		})
	}
}
