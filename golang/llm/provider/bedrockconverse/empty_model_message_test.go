package bedrockconverse

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
)

// A lifted empty or content-filtered reply is a model message with no parts.
// Replaying it must not put an assistant message without content on the wire.
func TestCompileSkipsReplayedEmptyModelMessage(t *testing.T) {
	adapter := &Adapter{endpointID: "bedrock-prod", profile: DefaultProfile("nova")}
	human := func(text string) llm.Message {
		return llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}}
	}
	// The empty model turn sits between two user turns and again next to a
	// model turn that was split into text and a tool call.
	request := llm.Request{OperationKey: "empty-model-replay", Model: "amazon.nova-pro-v1:0", Input: []llm.Item{
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
			call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Query: provider.CapabilityQuery{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockConverse, Model: request.Model}, Strict: strict})
			if err != nil {
				t.Fatalf("compile replayed empty model message: %v", err)
			}
			params := call.SDKParams.(bedrockruntime.ConverseInput)
			roles := make([]string, 0, len(params.Messages))
			blocks := make([]int, 0, len(params.Messages))
			for _, message := range params.Messages {
				roles = append(roles, string(message.Role))
				blocks = append(blocks, len(message.Content))
			}
			// Converse requires strict alternation: the two user turns merge
			// and the split model turn stays one assistant message.
			if got, want := strings.Join(roles, ","), "user,assistant,user"; got != want {
				t.Fatalf("message roles = %s, want %s", got, want)
			}
			if blocks[0] != 2 || blocks[1] != 2 || blocks[2] != 1 {
				t.Fatalf("message block counts = %v", blocks)
			}
		})
	}
}
