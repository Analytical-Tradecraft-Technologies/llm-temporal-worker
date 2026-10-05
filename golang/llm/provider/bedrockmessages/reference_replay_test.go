package bedrockmessages

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// A replayed transcript still carries the citations a provider such as Exa
// returned. They are output annotations with no wire form, so every later
// turn must compile without them instead of failing the only candidate.
func TestCompileSkipsReplayedReferenceAnnotations(t *testing.T) {
	adapter := &Adapter{endpointID: "bedrock-prod", profile: mustBedrockProfile(t, "")}
	human := func(text string) llm.Message {
		return llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}}
	}
	// The second response splits one model turn into text and a tool call and
	// carries a citation between the call and its result.
	request := llm.Request{OperationKey: "reference-replay", Model: "claude-contract", Input: []llm.Item{
		human("question"),
		llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "answer"}}},
		llm.Reference{URI: "https://example.com/cited-source", Metadata: map[string]json.RawMessage{"title": json.RawMessage(`"Cited source"`)}},
		llm.Reference{URI: "https://example.com/second-source"},
		human("follow-up"),
		llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "looking"}}},
		llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"sydney"}`)},
		llm.Reference{URI: "https://example.com/third-source"},
		llm.ToolResult{CallID: "call-1", Name: "lookup", Content: []llm.Part{llm.TextPart{Text: "found"}}},
	}, Tools: []llm.Tool{{Name: "lookup", InputSchema: json.RawMessage(`{"type":"object"}`)}}}
	for name, strict := range map[string]bool{"strict": true, "best effort": false} {
		t.Run(name, func(t *testing.T) {
			call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Query: provider.CapabilityQuery{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract"}, Strict: strict})
			if err != nil {
				t.Fatalf("compile replayed references: %v", err)
			}
			wire := marshalBedrockWire(t, call.SDKParams)
			messages := wire["messages"].([]any)
			roles := make([]string, 0, len(messages))
			for _, message := range messages {
				roles = append(roles, message.(map[string]any)["role"].(string))
			}
			if got, want := strings.Join(roles, ","), "user,assistant,user,assistant,assistant,user"; got != want {
				t.Fatalf("message roles = %s, want %s", got, want)
			}
			if encoded, err := json.Marshal(wire); err != nil || strings.Contains(string(encoded), "example.com") {
				t.Fatalf("reference reached the wire: %s (%v)", encoded, err)
			}
		})
	}
}
