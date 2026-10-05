package bedrockmessages

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func TestLowerRequestMapsSystemToolsOutputSamplingAndReasoning(t *testing.T) {
	maxTokens := 2048
	temperature := 0.25
	topP := 0.8
	topK := 4
	reasoningBudget := 2048
	request := llm.Request{
		Model: "claude-contract",
		Instructions: []llm.Instruction{
			{Text: "follow the policy"},
			{Kind: llm.InstructionKindParts, Content: []llm.Part{llm.JSONPart{Value: []byte(`{"policy":"strict"}`)}}},
		},
		Input: []llm.Item{
			llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}},
		},
		Tools:      []llm.Tool{{Name: "lookup", Description: "Look up a city", InputSchema: []byte(`{"type":"object","properties":{"city":{"type":"string"}}}`)}},
		ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceRequired, Parallel: false},
		Output:     &llm.OutputSpec{MaxTokens: &maxTokens, Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Schema: []byte(`{"type":"object","properties":{"answer":{"type":"string"}}}`)}},
		Sampling:   &llm.SamplingSpec{Temperature: &temperature, TopP: &topP, TopK: &topK, StopSequences: []string{"END"}},
		Reasoning:  &llm.ReasoningSpec{Mode: llm.ReasoningModeAdaptive, Effort: llm.ReasoningEffortMedium, Summary: llm.ReasoningSummaryNone, TokenBudget: &reasoningBudget},
	}
	params, err := lowerRequest(request, mustBedrockProfile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	wire := marshalBedrockWire(t, params)
	if wire["model"] != "claude-contract" || wire["max_tokens"] != float64(maxTokens) {
		t.Fatalf("request identity = %#v", wire)
	}
	messages, ok := wire["messages"].([]any)
	if !ok || len(messages) != 1 || messages[0].(map[string]any)["role"] != "user" {
		t.Fatalf("messages = %#v", wire["messages"])
	}
	system, ok := wire["system"].([]any)
	if !ok || len(system) != 2 || system[0].(map[string]any)["text"] != "follow the policy" || system[1].(map[string]any)["text"] != `{"policy":"strict"}` {
		t.Fatalf("system = %#v", wire["system"])
	}
	tools, ok := wire["tools"].([]any)
	if !ok || len(tools) != 1 || tools[0].(map[string]any)["name"] != "lookup" {
		t.Fatalf("tools = %#v", wire["tools"])
	}
	choice, ok := wire["tool_choice"].(map[string]any)
	if !ok || choice["type"] != "any" || choice["disable_parallel_tool_use"] != true {
		t.Fatalf("tool choice = %#v", wire["tool_choice"])
	}
	outputConfig, ok := wire["output_config"].(map[string]any)
	if !ok || outputConfig["effort"] != "medium" {
		t.Fatalf("output config = %#v", wire["output_config"])
	}
	format, ok := outputConfig["format"].(map[string]any)
	if !ok || format["type"] != "json_schema" {
		t.Fatalf("output format = %#v", outputConfig["format"])
	}
	if wire["temperature"] != temperature || wire["top_p"] != topP || wire["top_k"] != float64(topK) {
		t.Fatalf("sampling = %#v", wire)
	}
	if got, ok := wire["stop_sequences"].([]any); !ok || len(got) != 1 || got[0] != "END" {
		t.Fatalf("stop sequences = %#v", wire["stop_sequences"])
	}
}

func TestLowerItemMapsToolAndMediaContent(t *testing.T) {
	tool, err := lowerItem(llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{"city":"sydney"}`)})
	if err != nil {
		t.Fatal(err)
	}
	toolContent := tool["content"].([]any)[0].(map[string]any)
	if tool["role"] != "assistant" || toolContent["type"] != "tool_use" || toolContent["id"] != "call-1" || toolContent["name"] != "lookup" {
		t.Fatalf("tool call = %#v", tool)
	}

	result, err := lowerItem(llm.ToolResult{CallID: "call-1", Content: []llm.Part{llm.TextPart{Text: "result"}}, IsError: true})
	if err != nil {
		t.Fatal(err)
	}
	resultContent := result["content"].([]any)[0].(map[string]any)
	if result["role"] != "user" || resultContent["type"] != "tool_result" || resultContent["tool_use_id"] != "call-1" || resultContent["is_error"] != true {
		t.Fatalf("tool result = %#v", result)
	}

	image, err := lowerPart(llm.ImagePart{Bytes: []byte("image-bytes"), MediaType: "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	imageSource := image.(map[string]any)["source"].(map[string]any)
	if imageSource["type"] != "base64" || imageSource["media_type"] != "image/png" || imageSource["data"] != base64.StdEncoding.EncodeToString([]byte("image-bytes")) {
		t.Fatalf("image = %#v", image)
	}

	document, err := lowerPart(llm.DocumentPart{Bytes: []byte("%PDF-1.7"), MediaType: "application/pdf", Title: "Report"})
	if err != nil {
		t.Fatal(err)
	}
	documentMap := document.(map[string]any)
	if documentMap["type"] != "document" || documentMap["title"] != "Report" || documentMap["source"].(map[string]any)["type"] != "base64" {
		t.Fatalf("document = %#v", document)
	}
}

func TestLowerRejectsUnsupportedProviderControlsAndMedia(t *testing.T) {
	seed := int64(7)
	invalidRequests := []struct {
		name    string
		request llm.Request
		want    string
	}{
		{name: "seed", request: llm.Request{Model: "claude", Sampling: &llm.SamplingSpec{Seed: &seed}}, want: "sampling field is not supported"},
		{name: "tool policy without tools", request: llm.Request{Model: "claude", ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceRequired}}, want: "requires at least one tool"},
		{name: "unsupported output", request: llm.Request{Model: "claude", Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKind("xml")}}}, want: "output format"},
	}
	for _, test := range invalidRequests {
		t.Run(test.name, func(t *testing.T) {
			_, err := lowerRequest(test.request, mustBedrockProfile(t, ""))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("lowerRequest() = %v, want substring %q", err, test.want)
			}
		})
	}
	invalidParts := []struct {
		name string
		part llm.Part
		want string
	}{
		{name: "image media", part: llm.ImagePart{Bytes: []byte("x"), MediaType: "image/svg+xml"}, want: "image media type"},
		{name: "image detail", part: llm.ImagePart{Bytes: []byte("x"), MediaType: "image/png", Detail: "high"}, want: "image detail"},
		{name: "document media", part: llm.DocumentPart{Bytes: []byte("x"), MediaType: "application/json"}, want: "document media type"},
		{name: "invalid JSON", part: llm.JSONPart{Value: []byte("{")}, want: "JSON part is invalid"},
	}
	for _, test := range invalidParts {
		t.Run(test.name, func(t *testing.T) {
			_, err := lowerPart(test.part)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("lowerPart() = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestLowerReasoningMapsSupportedModesAndRejectsInvalidBudgets(t *testing.T) {
	budget := 1024
	for _, test := range []struct {
		name      string
		reasoning llm.ReasoningSpec
		wantType  string
	}{
		{name: "disabled", reasoning: llm.ReasoningSpec{Mode: llm.ReasoningModeDisabled}, wantType: "disabled"},
		{name: "enabled", reasoning: llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, TokenBudget: &budget}, wantType: "enabled"},
		{name: "adaptive", reasoning: llm.ReasoningSpec{Mode: llm.ReasoningModeAdaptive}, wantType: "adaptive"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := lowerReasoning(test.reasoning, true)
			if err != nil || got["type"] != test.wantType {
				t.Fatalf("lowerReasoning() = %#v, %v", got, err)
			}
		})
	}
	for _, test := range []struct {
		name      string
		reasoning llm.ReasoningSpec
		want      string
	}{
		{name: "small budget", reasoning: llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, TokenBudget: func() *int { value := 512; return &value }()}, want: "at least 1024"},
		{name: "effort requires adaptive", reasoning: llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, Effort: llm.ReasoningEffortHigh, TokenBudget: &budget}, want: "requires adaptive"},
		{name: "unknown mode", reasoning: llm.ReasoningSpec{Mode: llm.ReasoningMode("future")}, want: "not supported"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := lowerReasoning(test.reasoning, true)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("lowerReasoning() = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestDefaultReasoningDoesNotOverrideProviderThinking(t *testing.T) {
	budget := 1024
	for _, test := range []struct {
		name      string
		reasoning *llm.ReasoningSpec
		wantType  string
	}{
		{name: "absent"},
		{name: "zero", reasoning: &llm.ReasoningSpec{}},
		{name: "mode and summary default", reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeProviderDefault, Summary: llm.ReasoningSummaryProviderDefault}},
		{name: "effort default", reasoning: &llm.ReasoningSpec{Effort: llm.ReasoningEffortProviderDefault}},
		{name: "all explicit defaults", reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeProviderDefault, Summary: llm.ReasoningSummaryProviderDefault, Effort: llm.ReasoningEffortProviderDefault}},
		{name: "adaptive with default effort", reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeAdaptive, Effort: llm.ReasoningEffortProviderDefault}, wantType: "adaptive"},
		{name: "budget with default effort", reasoning: &llm.ReasoningSpec{Effort: llm.ReasoningEffortProviderDefault, TokenBudget: &budget}, wantType: "enabled"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := llm.Request{OperationKey: "default-reasoning", Model: "claude-contract", Reasoning: test.reasoning}
			params, err := lowerRequest(request, mustBedrockProfile(t, ""))
			if err != nil {
				t.Fatal(err)
			}
			wire := marshalBedrockWire(t, params)
			thinking, exists := wire["thinking"]
			if test.wantType == "" {
				if exists {
					t.Fatalf("default reasoning emitted thinking = %#v", thinking)
				}
				if config, ok := wire["output_config"].(map[string]any); ok {
					if effort, exists := config["effort"]; exists {
						t.Fatalf("default effort emitted override = %#v", effort)
					}
				}
				return
			}
			config, ok := thinking.(map[string]any)
			if !ok || config["type"] != test.wantType {
				t.Fatalf("thinking = %#v, want %s", thinking, test.wantType)
			}
			if test.wantType == "enabled" && config["budget_tokens"] != float64(budget) {
				t.Fatalf("thinking budget = %#v", config)
			}
		})
	}
}

func TestLoweringSendsNonStrictToolsAndRejectsSchemalessJSONOutput(t *testing.T) {
	tools, err := lowerTools([]llm.Tool{{Name: "lookup", InputSchema: json.RawMessage(`{"type":"object","properties":{"q":{"type":"string","minLength":1}}}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, strict := tools[0].(map[string]any)["strict"]; strict {
		t.Fatalf("tool must not be forced strict: %#v", tools[0])
	}
	target := map[string]any{}
	err = lowerOutput(llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSON}}, target, true)
	if err == nil || !strings.Contains(err.Error(), "without a schema") || target["output_config"] != nil {
		t.Fatalf("schema-less JSON output = %v, %#v", err, target)
	}
}
