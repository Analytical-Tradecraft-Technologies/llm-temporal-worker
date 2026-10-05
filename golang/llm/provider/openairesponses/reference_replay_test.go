package openairesponses

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
	adapter := fixtureAdapterForProfile(t, responsesFixtureProfiles[0])
	human := func(text string) llm.Message {
		return llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}}
	}
	// The second response splits one model turn into text and a tool call and
	// carries a citation between the call and its result.
	request := llm.Request{OperationKey: "reference-replay", Model: "gpt-contract", Input: []llm.Item{
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
			call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Query: provider.CapabilityQuery{EndpointID: "openai-fixture", Family: provider.FamilyOpenAIResponses, Model: request.Model}, Strict: strict})
			if err != nil {
				t.Fatalf("compile replayed references: %v", err)
			}
			encoded, err := json.Marshal(call.SDKParams)
			if err != nil {
				t.Fatal(err)
			}
			var wire struct {
				Input []struct {
					Type string `json:"type"`
					Role string `json:"role"`
				} `json:"input"`
			}
			if err := json.Unmarshal(encoded, &wire); err != nil {
				t.Fatal(err)
			}
			kinds := make([]string, 0, len(wire.Input))
			for _, item := range wire.Input {
				kinds = append(kinds, item.Type+":"+item.Role)
			}
			if got, want := strings.Join(kinds, ","), "message:user,message:assistant,message:user,message:assistant,function_call:,function_call_output:"; got != want {
				t.Fatalf("input items = %s, want %s", got, want)
			}
			if strings.Contains(string(encoded), "example.com") {
				t.Fatalf("reference reached the wire: %s", encoded)
			}
		})
	}
}
