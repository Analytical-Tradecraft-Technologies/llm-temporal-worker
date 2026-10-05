package bedrockconverse

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// A replayed transcript still carries the citations a provider such as Exa
// returned. They are output annotations with no wire form, so every later
// turn must compile without them instead of failing the only candidate.
func TestCompileSkipsReplayedReferenceAnnotations(t *testing.T) {
	adapter := &Adapter{endpointID: "bedrock-prod", profile: DefaultProfile("nova")}
	human := func(text string) llm.Message {
		return llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}}
	}
	// The second response splits one model turn into text and a tool call and
	// carries a citation between the call and its result.
	request := llm.Request{OperationKey: "reference-replay", Model: "amazon.nova-pro-v1:0", Input: []llm.Item{
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
			call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Query: provider.CapabilityQuery{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockConverse, Model: request.Model}, Strict: strict})
			if err != nil {
				t.Fatalf("compile replayed references: %v", err)
			}
			params := call.SDKParams.(bedrockruntime.ConverseInput)
			roles := make([]string, 0, len(params.Messages))
			blocks := make([]int, 0, len(params.Messages))
			for _, message := range params.Messages {
				roles = append(roles, string(message.Role))
				blocks = append(blocks, len(message.Content))
			}
			// Converse requires strict alternation: the split model turn and
			// its tool call stay one assistant message.
			if got, want := strings.Join(roles, ","), "user,assistant,user,assistant,user"; got != want {
				t.Fatalf("message roles = %s, want %s", got, want)
			}
			if blocks[1] != 1 || blocks[3] != 2 || blocks[4] != 1 {
				t.Fatalf("message block counts = %v", blocks)
			}
		})
	}
}
