package openaichat

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	openai "github.com/openai/openai-go/v3"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestCompileLowersRolesMultimodalToolsAndStructuredOutput(t *testing.T) {
	adapter := testAdapter(t)
	maxTokens := 321
	temperature := 0.25
	request := llm.Request{
		OperationKey: "op-lower",
		Model:        "chat-model",
		ServiceClass: llm.ServiceClassPriority,
		Instructions: []llm.Instruction{
			{Level: llm.InstructionLevelPolicy, Kind: llm.InstructionKindText, Text: "policy"},
			{Level: llm.InstructionLevelApplication, Kind: llm.InstructionKindParts, Content: []llm.Part{llm.TextPart{Text: "developer"}}},
		},
		Input: []llm.Item{
			llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
				llm.TextPart{Text: "hello"},
				llm.ImagePart{URL: "https://example.test/image.png", MediaType: "image/png", Detail: "high"},
			}},
			llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "thinking"}}},
			llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"sydney"}`)},
			llm.ToolResult{CallID: "call-1", Content: []llm.Part{llm.JSONPart{Value: json.RawMessage(`{"ok":true}`)}}},
		},
		Tools: []llm.Tool{{
			Name:         "lookup",
			Description:  "look up a place",
			InputSchema:  json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}`),
			OutputSchema: json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`),
		}},
		ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNamed, Name: "lookup", Parallel: true},
		Output:     &llm.OutputSpec{MaxTokens: &maxTokens, Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Description: "answer object", Strict: true, Schema: json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"]}`)}},
		Sampling:   &llm.SamplingSpec{Temperature: &temperature, StopSequences: []string{"\n", "END"}},
		Extensions: map[string]json.RawMessage{"chat.contract": json.RawMessage(`{"provider_hint":"pinned"}`)},
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if call.Metadata.ProviderTier != "priority" || call.OperationKey != "op-lower" {
		t.Fatalf("call metadata = %#v", call)
	}
	params, ok := call.SDKParams.(openaiChatParams)
	if !ok {
		t.Fatalf("SDK params type = %T", call.SDKParams)
	}
	wire := marshalWire(t, params)
	if wire["model"] != "chat-model" || wire["service_tier"] != "priority" {
		t.Fatalf("wire identity = %#v", wire)
	}
	messages := wire["messages"].([]any)
	if messages[0].(map[string]any)["role"] != "system" || messages[1].(map[string]any)["role"] != "developer" {
		t.Fatalf("instruction roles = %#v", messages[:2])
	}
	user := messages[2].(map[string]any)
	parts := user["content"].([]any)
	if parts[0].(map[string]any)["type"] != "text" || parts[1].(map[string]any)["type"] != "image_url" {
		t.Fatalf("user parts = %#v", parts)
	}
	assistant := messages[3].(map[string]any)
	if assistant["role"] != "assistant" {
		t.Fatalf("assistant message = %#v", assistant)
	}
	toolCallMessage := messages[3].(map[string]any)
	if len(toolCallMessage["tool_calls"].([]any)) != 1 {
		t.Fatalf("tool call message = %#v", toolCallMessage)
	}
	toolResult := messages[4].(map[string]any)
	if toolResult["role"] != "tool" || toolResult["tool_call_id"] != "call-1" {
		t.Fatalf("tool result message = %#v", toolResult)
	}
	if wire["response_format"].(map[string]any)["type"] != "json_schema" {
		t.Fatalf("response format = %#v", wire["response_format"])
	}
	if wire["user"] != "pinned" {
		t.Fatalf("extension = %#v", wire["user"])
	}
	tools := wire["tools"].([]any)
	function := tools[0].(map[string]any)["function"].(map[string]any)
	if _, leaked := function["output_schema"]; leaked {
		t.Fatalf("caller-owned tool output schema leaked to Chat Completions: %#v", function)
	}
}

func TestCompileRejectsToolResultWithoutPrecedingCall(t *testing.T) {
	adapter := testAdapter(t)
	_, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{
			OperationKey: "op-order",
			Model:        "chat-model",
			Input: []llm.Item{llm.ToolResult{
				CallID:  "missing-call",
				Content: []llm.Part{llm.TextPart{Text: "result"}},
			}},
		},
		Query:  provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"},
		Strict: true,
	})
	if err == nil || !strings.Contains(err.Error(), "no preceding tool call") {
		t.Fatalf("tool ordering error = %v", err)
	}
}

// openaiChatParams is an alias kept in the test so the SDK type does not leak
// into provider-neutral assertions.
type openaiChatParams = openai.ChatCompletionNewParams
