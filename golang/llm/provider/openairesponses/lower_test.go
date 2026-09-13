package openairesponses

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/openai/openai-go/v3/responses"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func TestLoweringPreservesTypedInputAndControls(t *testing.T) {
	maxTokens := 128
	temperature := 0.2
	topP := 0.9
	request := llm.Request{
		APIVersion:   llm.APIVersion,
		OperationKey: "op-lower",
		Model:        "gpt-contract",
		ServiceClass: llm.ServiceClassEconomy,
		Instructions: []llm.Instruction{
			{Kind: llm.InstructionKindText, Level: llm.InstructionLevelPolicy, Text: "policy"},
			{Kind: llm.InstructionKindParts, Level: llm.InstructionLevelApplication, Content: []llm.Part{llm.TextPart{Text: "developer"}}},
		},
		Input: []llm.Item{
			llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
				llm.TextPart{Text: "hello"},
				llm.ImagePart{URL: "https://example.test/image.png", MediaType: "image/png", Detail: "high"},
				llm.DocumentPart{Bytes: []byte("pdf"), MediaType: "application/pdf", Title: "contract.pdf"},
			}},
			llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"x"}`)},
			llm.ToolResult{CallID: "call-1", Name: "lookup", Content: []llm.Part{llm.TextPart{Text: "result"}}},
		},
		Tools:      []llm.Tool{{Name: "lookup", Description: "find", InputSchema: json.RawMessage(`{"type":"object","properties":{}}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}},
		ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNamed, Name: "lookup", Parallel: true},
		Output:     &llm.OutputSpec{MaxTokens: &maxTokens, Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Description: "answer schema", Strict: true, Schema: json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"]}`)}},
		Sampling:   &llm.SamplingSpec{Temperature: &temperature, TopP: &topP},
		Reasoning:  &llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, Effort: llm.ReasoningEffortHigh, Summary: llm.ReasoningSummaryDetailed},
		Extensions: map[string]json.RawMessage{"openai.responses": json.RawMessage(`{"include":["reasoning.encrypted_content"],"store":false,"truncation":"auto"}`)},
	}
	params, err := lowerRequest(request, llm.ServiceClassEconomy)
	if err != nil {
		t.Fatal(err)
	}
	wire := marshalParams(t, params)
	if got := wire["model"]; got != "gpt-contract" {
		t.Fatalf("model = %#v", got)
	}
	if got := wire["service_tier"]; got != "flex" {
		t.Fatalf("service_tier = %#v, want flex", got)
	}
	if store, ok := wire["store"].(bool); !ok || store {
		t.Fatalf("store = %#v, want explicit false", wire["store"])
	}
	input, ok := wire["input"].([]any)
	if !ok || len(input) != 5 {
		t.Fatalf("input = %#v, want five typed items", wire["input"])
	}
	if input[0].(map[string]any)["role"] != "system" || input[1].(map[string]any)["role"] != "developer" {
		t.Fatalf("instruction order/roles not preserved: %#v %#v", input[0], input[1])
	}
	if input[2].(map[string]any)["role"] != "user" {
		t.Fatalf("message role = %#v", input[2].(map[string]any)["role"])
	}
	if input[3].(map[string]any)["type"] != "function_call" || input[4].(map[string]any)["type"] != "function_call_output" {
		t.Fatalf("tool items = %#v %#v", input[3], input[4])
	}
	if wire["tool_choice"].(map[string]any)["name"] != "lookup" {
		t.Fatalf("tool choice = %#v", wire["tool_choice"])
	}
	textConfig := wire["text"].(map[string]any)
	format := textConfig["format"].(map[string]any)
	if format["type"] != "json_schema" || format["strict"] != true {
		t.Fatalf("structured output = %#v", format)
	}
	if got := wire["include"].([]any)[0]; got != "reasoning.encrypted_content" {
		t.Fatalf("include extension = %#v", wire["include"])
	}
}

func TestLoweringMapsOnlyPublicServiceClasses(t *testing.T) {
	for _, test := range []struct {
		class llm.ServiceClass
		want  string
	}{
		{llm.ServiceClassEconomy, "flex"},
		{llm.ServiceClassStandard, "default"},
		{llm.ServiceClassPriority, "priority"},
	} {
		params, err := lowerRequest(llm.Request{Model: "gpt", OperationKey: "op"}, test.class)
		if err != nil {
			t.Fatalf("class %s: %v", test.class, err)
		}
		wire := marshalParams(t, params)
		if wire["service_tier"] != test.want {
			t.Errorf("class %s -> %#v, want %s", test.class, wire["service_tier"], test.want)
		}
		if strings.Contains(string(mustJSON(t, params)), "provider_default") {
			t.Errorf("public service class leaked provider_default: %s", mustJSON(t, params))
		}
	}
}

func TestLoweringRejectsLossyFields(t *testing.T) {
	topK := 4
	_, err := lowerRequest(llm.Request{Model: "gpt", OperationKey: "op", Sampling: &llm.SamplingSpec{TopK: &topK}}, llm.ServiceClassStandard)
	if err == nil || !strings.Contains(err.Error(), "sampling") {
		t.Fatalf("unsupported sampling error = %v", err)
	}
	_, err = lowerRequest(llm.Request{Model: "gpt", OperationKey: "op", Input: []llm.Item{llm.ProviderState{Provider: "x", EndpointFamily: "y", MediaType: "z"}}}, llm.ServiceClassStandard)
	if err == nil || !strings.Contains(err.Error(), "not accepted") {
		t.Fatalf("provider state error = %v", err)
	}
	_, err = lowerRequest(llm.Request{Model: "gpt", OperationKey: "op", Extensions: map[string]json.RawMessage{"other": json.RawMessage(`{}`)}}, llm.ServiceClassStandard)
	if err == nil || !strings.Contains(err.Error(), "namespace") {
		t.Fatalf("extension error = %v", err)
	}
}

func marshalParams(t *testing.T, params responses.ResponseNewParams) map[string]any {
	t.Helper()
	var wire map[string]any
	encoded, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	return wire
}

func mustJSON(t *testing.T, params responses.ResponseNewParams) []byte {
	t.Helper()
	encoded, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
