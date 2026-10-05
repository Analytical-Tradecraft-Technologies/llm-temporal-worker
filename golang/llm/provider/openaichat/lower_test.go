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
		Tools:      []llm.Tool{{Name: "lookup", Description: "look up a place", InputSchema: json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}`)}},
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
	if wire["parallel_tool_calls"] != true {
		t.Fatalf("declared tools lost parallel control: %#v", wire)
	}
	choice, ok := wire["tool_choice"].(map[string]any)
	if !ok || choice["type"] != "function" {
		t.Fatalf("declared tools lost named choice: %#v", wire["tool_choice"])
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

// Chat tool messages carry only content and tool_call_id, so is_error is
// emulated with the documented text prefix in both portability modes.
func TestCompileEmulatesToolResultErrorWithTextPrefix(t *testing.T) {
	for _, strict := range []bool{true, false} {
		for _, isError := range []bool{true, false} {
			call, err := testAdapter(t).Compile(context.Background(), provider.CompileInput{
				Request: llm.Request{OperationKey: "op-tool-error", Model: "chat-model", Input: []llm.Item{
					llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "look it up"}}},
					llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"x"}`)},
					llm.ToolResult{CallID: "call-1", Content: []llm.Part{llm.TextPart{Text: "upstream timed out"}}, IsError: isError},
					llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "continue"}}},
				}},
				Query:  provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"},
				Strict: strict,
			})
			if err != nil {
				t.Fatalf("strict=%t is_error=%t: %v", strict, isError, err)
			}
			encoded, err := json.Marshal(call.SDKParams)
			if err != nil {
				t.Fatal(err)
			}
			want := `{"content":"upstream timed out","tool_call_id":"call-1","role":"tool"}`
			if isError {
				want = `{"content":"[is_error=true] The tool call failed; its output follows.\nupstream timed out","tool_call_id":"call-1","role":"tool"}`
			}
			if !strings.Contains(string(encoded), want) {
				t.Fatalf("strict=%t is_error=%t wire = %s, want message %s", strict, isError, encoded, want)
			}
			if !isError && strings.Contains(string(encoded), "is_error") {
				t.Fatalf("successful tool result gained an error marker: %s", encoded)
			}
		}
	}
}

// openaiChatParams is an alias kept in the test so the SDK type does not leak
// into provider-neutral assertions.
type openaiChatParams = openai.ChatCompletionNewParams

func TestCompilePreservesDisabledReasoning(t *testing.T) {
	for _, effort := range []llm.ReasoningEffort{"", llm.ReasoningEffortProviderDefault, llm.ReasoningEffortHigh} {
		t.Run(string(effort), func(t *testing.T) {
			adapter := testAdapter(t)
			call, err := adapter.Compile(context.Background(), provider.CompileInput{
				Request: llm.Request{OperationKey: "disabled-reasoning", Model: "chat-model", ServiceClass: llm.ServiceClassStandard,
					Input:     []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}},
					Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeDisabled, Effort: effort}},
				Query: provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"}, Strict: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			wire := marshalWire(t, call.SDKParams)
			if wire["reasoning_effort"] != "none" {
				t.Fatalf("disabled reasoning wire effort = %#v", wire["reasoning_effort"])
			}
		})
	}
}

func TestCompileOmitsToolControlsWithoutTools(t *testing.T) {
	for _, mode := range []llm.ToolChoiceMode{"", llm.ToolChoiceAuto, llm.ToolChoiceNone} {
		for _, parallel := range []bool{false, true} {
			request := llm.Request{
				OperationKey: "tool-less", Model: "chat-model",
				Input:      []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "Summarize this text"}}}},
				ToolPolicy: llm.ToolPolicy{Mode: mode, Parallel: parallel},
			}
			call, err := testAdapter(t).Compile(context.Background(), provider.CompileInput{
				Request: request, Query: provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"}, Strict: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			wire := marshalWire(t, call.SDKParams)
			for _, key := range []string{"tools", "tool_choice", "parallel_tool_calls"} {
				if value, ok := wire[key]; ok {
					t.Fatalf("mode=%q parallel=%v: unexpected %s=%#v", mode, parallel, key, value)
				}
			}
		}
	}
}

func TestLiftedRefusalReplaysAsAssistantRefusalPart(t *testing.T) {
	refused := openai.ChatCompletion{
		ID: "refused", Model: "chat-model", ServiceTier: openai.ChatCompletionServiceTierDefault,
		Choices: []openai.ChatCompletionChoice{{FinishReason: "stop", Message: openai.ChatCompletionMessage{Refusal: "I can't help with that."}}},
	}
	lifted, err := testProfile().liftResponse(provider.Call{ServiceClass: llm.ServiceClassStandard}, &refused, "req")
	if err != nil {
		t.Fatal(err)
	}
	input := append([]llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "question"}}}}, lifted.Output...)
	input = append(input, llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "a benign follow-up"}}})
	params, err := lowerRequest(llm.Request{Model: "chat-model", Input: input}, testProfile(), "default")
	if err != nil {
		t.Fatalf("replaying a lifted refusal: %v", err)
	}
	encoded, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		Messages []struct {
			Role    string           `json:"role"`
			Content []map[string]any `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatalf("wire = %s: %v", encoded, err)
	}
	if len(wire.Messages) != 3 || wire.Messages[1].Role != "assistant" || len(wire.Messages[1].Content) != 1 ||
		wire.Messages[1].Content[0]["type"] != "refusal" || wire.Messages[1].Content[0]["refusal"] != "I can't help with that." {
		t.Fatalf("assistant refusal wire = %s", encoded)
	}

	_, err = lowerRequest(llm.Request{Model: "chat-model", Input: []llm.Item{
		llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.ImagePart{URL: "https://example.test/a.png", MediaType: "image/png"}}},
	}}, testProfile(), "default")
	if err == nil || !strings.Contains(err.Error(), "assistant history") {
		t.Fatalf("assistant image error = %v", err)
	}
}

func TestAssistantHistoryRejectsNilPartWithoutPanicking(t *testing.T) {
	_, err := lowerRequest(llm.Request{Model: "chat-model", Input: []llm.Item{
		llm.Message{Actor: llm.ActorModel, Content: []llm.Part{nil}},
	}}, testProfile(), "default")
	if err == nil || !strings.Contains(err.Error(), "<nil>") {
		t.Fatalf("nil assistant part error = %v", err)
	}
}

// A successful result that already starts with the reserved prefix would be
// indistinguishable from a failed one, so strict mode rejects it.
func TestCompileStrictRejectsSuccessfulToolResultWithReservedPrefix(t *testing.T) {
	for _, strict := range []bool{true, false} {
		_, err := testAdapter(t).Compile(context.Background(), provider.CompileInput{
			Request: llm.Request{OperationKey: "op-tool-prefix", Model: "chat-model", Input: []llm.Item{
				llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"x"}`)},
				llm.ToolResult{CallID: "call-1", Name: "lookup", Content: []llm.Part{llm.TextPart{Text: "[is_error=true] The tool call "}, llm.TextPart{Text: "failed; its output follows.\nfine"}}},
			}},
			Query:  provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"},
			Strict: strict,
		})
		if strict && (err == nil || !strings.Contains(err.Error(), "reserved tool-error prefix")) {
			t.Fatalf("strict error = %v", err)
		}
		if !strict && err != nil {
			t.Fatalf("best-effort error = %v", err)
		}
	}
}
