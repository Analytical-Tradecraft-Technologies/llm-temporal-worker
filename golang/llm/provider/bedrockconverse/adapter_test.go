package bedrockconverse

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

type fakeConverse struct {
	input  *bedrockruntime.ConverseInput
	output *bedrockruntime.ConverseOutput
	err    error
}

func (fake *fakeConverse) Converse(_ context.Context, input *bedrockruntime.ConverseInput, _ ...func(*bedrockruntime.Options)) (*bedrockruntime.ConverseOutput, error) {
	fake.input = input
	return fake.output, fake.err
}

func TestCompileAndInvokeConverse(t *testing.T) {
	fake := &fakeConverse{output: &bedrockruntime.ConverseOutput{
		Output: &types.ConverseOutputMemberMessage{Value: types.Message{Role: types.ConversationRoleAssistant, Content: []types.ContentBlock{
			&types.ContentBlockMemberText{Value: "Hello from Bedrock"},
		}}},
		StopReason:  types.StopReasonEndTurn,
		Usage:       &types.TokenUsage{InputTokens: aws.Int32(4), OutputTokens: aws.Int32(3)},
		ServiceTier: &types.ServiceTier{Type: types.ServiceTierTypePriority},
	}}
	adapter, err := New(&Client{converse: fake}, "bedrock-prod", DefaultProfile("nova"))
	if err != nil {
		t.Fatal(err)
	}
	request := llm.Request{OperationKey: "op-1", Model: "amazon.nova-lite-v1:0", ServiceClass: llm.ServiceClassPriority,
		Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "Hello"}}}}}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Query: provider.CapabilityQuery{Family: provider.FamilyBedrockConverse, EndpointID: "bedrock-prod", Model: request.Model}, Strict: true})
	if err != nil {
		t.Fatal(err)
	}
	params, ok := call.SDKParams.(bedrockruntime.ConverseInput)
	if !ok {
		t.Fatalf("SDK params type = %T", call.SDKParams)
	}
	if params.ModelId == nil || *params.ModelId != request.Model {
		t.Fatalf("model ID = %v, want %q", params.ModelId, request.Model)
	}
	if params.ServiceTier == nil || params.ServiceTier.Type != types.ServiceTierTypePriority {
		t.Fatalf("service tier = %#v, want priority", params.ServiceTier)
	}
	result, err := adapter.Invoke(context.Background(), call, provider.NopObserver{})
	if err != nil {
		t.Fatal(err)
	}
	if fake.input == nil || fake.input.ServiceTier.Type != types.ServiceTierTypePriority {
		t.Fatalf("invoked service tier = %#v, want priority", fake.input.ServiceTier)
	}
	if result.Response.Status != llm.ResponseStatusCompleted || result.Response.Usage.InputTokens != 4 || result.Response.Usage.OutputTokens != 3 {
		t.Fatalf("response = %#v", result.Response)
	}
	if len(result.Response.Output) != 1 {
		t.Fatalf("output length = %d, want one message", len(result.Response.Output))
	}
	message, ok := result.Response.Output[0].(llm.Message)
	if !ok || len(message.Content) != 1 || message.Content[0].(llm.TextPart).Text != "Hello from Bedrock" {
		t.Fatalf("output = %#v", result.Response.Output)
	}
}

func TestLowerToolCallUsesSmithyJSONDocument(t *testing.T) {
	profile, err := NewDefaultProfile("nova")
	if err != nil {
		t.Fatal(err)
	}
	request := llm.Request{Model: "nova", Input: []llm.Item{llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"city":"Sydney"}`)}}}
	input, err := lowerRequest(request, profile, string(types.ServiceTierTypeDefault), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(input.Messages) != 1 || len(input.Messages[0].Content) != 1 {
		t.Fatalf("lowered messages = %#v", input.Messages)
	}
	tool, ok := input.Messages[0].Content[0].(*types.ContentBlockMemberToolUse)
	if !ok || tool.Value.Input == nil {
		t.Fatalf("lowered tool block = %#v", input.Messages[0].Content[0])
	}
	encoded, err := tool.Value.Input.MarshalSmithyDocument()
	if err != nil || string(encoded) != `{"city":"Sydney"}` {
		t.Fatalf("tool input = %s, err=%v", encoded, err)
	}
}
