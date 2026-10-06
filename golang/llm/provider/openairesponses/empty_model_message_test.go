package openairesponses

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// A lifted empty or content-filtered reply is a model message with no parts.
// Replaying it must not put an assistant message without content on the wire.
func TestCompileSkipsReplayedEmptyModelMessage(t *testing.T) {
	adapter := fixtureAdapterForProfile(t, responsesFixtureProfiles[0])
	human := func(text string) llm.Message {
		return llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}}
	}
	// The empty model turn sits between two user turns and again next to a
	// model turn that was split into text and a tool call.
	request := llm.Request{OperationKey: "empty-model-replay", Model: "gpt-contract", Input: []llm.Item{
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
			call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Query: provider.CapabilityQuery{EndpointID: "openai-fixture", Family: provider.FamilyOpenAIResponses, Model: request.Model}, Strict: strict})
			if err != nil {
				t.Fatalf("compile replayed empty model message: %v", err)
			}
			encoded, err := json.Marshal(call.SDKParams)
			if err != nil {
				t.Fatal(err)
			}
			var wire struct {
				Input []struct {
					Type    string            `json:"type"`
					Role    string            `json:"role"`
					Content []json.RawMessage `json:"content"`
				} `json:"input"`
			}
			if err := json.Unmarshal(encoded, &wire); err != nil {
				t.Fatal(err)
			}
			kinds := make([]string, 0, len(wire.Input))
			for _, item := range wire.Input {
				if item.Type == "message" && len(item.Content) == 0 {
					t.Fatalf("message without content reached the wire: %s", encoded)
				}
				kinds = append(kinds, item.Type+":"+item.Role)
			}
			if got, want := strings.Join(kinds, ","), "message:user,message:user,message:assistant,function_call:,function_call_output:"; got != want {
				t.Fatalf("input items = %s, want %s", got, want)
			}
		})
	}
}
