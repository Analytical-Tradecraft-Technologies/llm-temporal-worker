package openairesponses

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/openai/openai-go/v3/responses"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// function_call_output has no error field, so is_error is emulated with the
// documented text prefix in both portability modes.
func TestCompileEmulatesToolResultErrorWithTextPrefix(t *testing.T) {
	for _, strict := range []bool{true, false} {
		for _, isError := range []bool{true, false} {
			adapter := newFixtureAdapter(t, []byte(`{"id":"unused"}`))
			call, err := adapter.Compile(context.Background(), provider.CompileInput{
				Request: llm.Request{OperationKey: "op-tool-error", Model: "gpt-contract", Input: []llm.Item{
					llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "look it up"}}},
					llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"x"}`)},
					llm.ToolResult{CallID: "call-1", Name: "lookup", Content: []llm.Part{llm.TextPart{Text: "upstream timed out"}}, IsError: isError},
					llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "continue"}}},
				}},
				Query:  provider.CapabilityQuery{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract"},
				Strict: strict,
			})
			if err != nil {
				t.Fatalf("strict=%t is_error=%t: %v", strict, isError, err)
			}
			encoded, err := json.Marshal(call.SDKParams)
			if err != nil {
				t.Fatal(err)
			}
			want := `{"output":"upstream timed out","call_id":"call-1","type":"function_call_output"}`
			if isError {
				want = `{"output":"[is_error=true] The tool call failed; its output follows.\nupstream timed out","call_id":"call-1","type":"function_call_output"}`
			}
			if !strings.Contains(string(encoded), want) {
				t.Fatalf("strict=%t is_error=%t wire = %s, want item %s", strict, isError, encoded, want)
			}
			if !isError && strings.Contains(string(encoded), "is_error") {
				t.Fatalf("successful tool result gained an error marker: %s", encoded)
			}
		}
	}
}

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
		Tools:        []llm.Tool{{Name: "lookup", Description: "find", InputSchema: json.RawMessage(`{"type":"object","properties":{}}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}},
		ToolPolicy:   llm.ToolPolicy{Mode: llm.ToolChoiceNamed, Name: "lookup", Parallel: true},
		Output:       &llm.OutputSpec{MaxTokens: &maxTokens, Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Description: "answer schema", Strict: true, Schema: json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"]}`)}},
		Sampling:     &llm.SamplingSpec{Temperature: &temperature, TopP: &topP},
		Reasoning:    &llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, Effort: llm.ReasoningEffortHigh, Summary: llm.ReasoningSummaryDetailed},
		Continuation: &llm.Continuation{Handle: "openai-responses:resp-prev"},
		Extensions:   map[string]json.RawMessage{"openai.responses": json.RawMessage(`{"include":["reasoning.encrypted_content"],"store":false,"truncation":"auto"}`)},
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
	if got := wire["previous_response_id"]; got != "resp-prev" {
		t.Fatalf("previous_response_id = %#v", got)
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

func TestLoweringReasoningSummaryPreservesEffort(t *testing.T) {
	for _, test := range []struct {
		name        string
		mode        llm.ReasoningMode
		effort      llm.ReasoningEffort
		summary     llm.ReasoningSummary
		wantEffort  string
		wantSummary string
	}{
		{name: "none", mode: llm.ReasoningModeEnabled, effort: llm.ReasoningEffortHigh, summary: llm.ReasoningSummaryNone, wantEffort: "high"},
		{name: "omitted", mode: llm.ReasoningModeEnabled, effort: llm.ReasoningEffortHigh, wantEffort: "high"},
		{name: "provider default", mode: llm.ReasoningModeEnabled, effort: llm.ReasoningEffortHigh, summary: llm.ReasoningSummaryProviderDefault, wantEffort: "high"},
		{name: "auto", mode: llm.ReasoningModeEnabled, effort: llm.ReasoningEffortHigh, summary: llm.ReasoningSummaryAuto, wantEffort: "high", wantSummary: "auto"},
		{name: "concise", mode: llm.ReasoningModeEnabled, effort: llm.ReasoningEffortHigh, summary: llm.ReasoningSummaryConcise, wantEffort: "high", wantSummary: "concise"},
		{name: "detailed", mode: llm.ReasoningModeEnabled, effort: llm.ReasoningEffortHigh, summary: llm.ReasoningSummaryDetailed, wantEffort: "high", wantSummary: "detailed"},
		{name: "disabled", mode: llm.ReasoningModeDisabled, summary: llm.ReasoningSummaryNone, wantEffort: "none"},
		{name: "default effort", mode: llm.ReasoningModeEnabled, summary: llm.ReasoningSummaryNone},
	} {
		t.Run(test.name, func(t *testing.T) {
			params, err := lowerRequest(llm.Request{
				Model: "gpt", OperationKey: "op",
				Reasoning: &llm.ReasoningSpec{Mode: test.mode, Effort: test.effort, Summary: test.summary},
			}, llm.ServiceClassStandard)
			if err != nil {
				t.Fatal(err)
			}
			reasoning, ok := marshalParams(t, params)["reasoning"].(map[string]any)
			if !ok {
				t.Fatal("missing reasoning configuration")
			}
			for field, want := range map[string]string{"effort": test.wantEffort, "summary": test.wantSummary} {
				got, present := reasoning[field]
				if want == "" {
					if present {
						t.Errorf("%s = %#v, want omitted", field, got)
					}
				} else if got != want {
					t.Errorf("%s = %#v, want %q", field, got, want)
				}
			}
		})
	}
}

func TestReplayToolCallDoesNotInventProviderItemID(t *testing.T) {
	params, err := lowerRequest(llm.Request{Model: "gpt-contract", OperationKey: "replay", Input: []llm.Item{
		llm.ToolCall{ID: "call_abc", Name: "lookup", Arguments: json.RawMessage(`{"q":"x"}`)},
		llm.ToolResult{CallID: "call_abc", Name: "lookup", Content: []llm.Part{llm.TextPart{Text: "result"}}},
	}}, llm.ServiceClassStandard)
	if err != nil {
		t.Fatal(err)
	}
	input := marshalParams(t, params)["input"].([]any)
	call := input[0].(map[string]any)
	if _, exists := call["id"]; exists {
		t.Fatalf("invented provider item id: %#v", call)
	}
	if call["call_id"] != "call_abc" || input[1].(map[string]any)["call_id"] != "call_abc" {
		t.Fatalf("lost tool correlation: %#v", input)
	}
}

func TestReplayAssistantHistoryUsesOutputContentTypes(t *testing.T) {
	params, err := lowerRequest(llm.Request{Model: "gpt-contract", OperationKey: "replay", Input: []llm.Item{
		llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "question"}}},
		llm.Message{Actor: llm.ActorModel, Content: []llm.Part{
			llm.TextPart{Text: "answer"},
			llm.JSONPart{Value: json.RawMessage(`{"a":1}`)},
			llm.RefusalPart{Text: "declined", ProviderCode: "openai.refusal"},
		}},
		llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "follow-up"}}},
	}}, llm.ServiceClassStandard)
	if err != nil {
		t.Fatal(err)
	}
	input := marshalParams(t, params)["input"].([]any)
	if len(input) != 3 {
		t.Fatalf("input = %#v", input)
	}
	user := input[0].(map[string]any)["content"].([]any)[0].(map[string]any)
	if user["type"] != "input_text" {
		t.Fatalf("user content = %#v", user)
	}
	assistant := input[1].(map[string]any)
	if assistant["role"] != "assistant" {
		t.Fatalf("assistant message = %#v", assistant)
	}
	content := assistant["content"].([]any)
	if len(content) != 3 {
		t.Fatalf("assistant content = %#v", content)
	}
	for index, want := range []map[string]any{
		{"type": "output_text", "text": "answer"},
		{"type": "output_text", "text": `{"a":1}`},
		{"type": "refusal", "refusal": "declined"},
	} {
		part := content[index].(map[string]any)
		for field, value := range want {
			if part[field] != value {
				t.Fatalf("assistant part %d = %#v, want %s=%#v", index, part, field, value)
			}
		}
	}

	_, err = lowerRequest(llm.Request{Model: "gpt-contract", OperationKey: "replay", Input: []llm.Item{
		llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.ImagePart{URL: "https://example.test/a.png", MediaType: "image/png"}}},
	}}, llm.ServiceClassStandard)
	if err == nil || !strings.Contains(err.Error(), "assistant history") {
		t.Fatalf("assistant image error = %v", err)
	}
}

func TestAssistantHistoryRejectsNilPartWithoutPanicking(t *testing.T) {
	_, err := lowerRequest(llm.Request{Model: "gpt-contract", OperationKey: "replay", Input: []llm.Item{
		llm.Message{Actor: llm.ActorModel, Content: []llm.Part{nil}},
	}}, llm.ServiceClassStandard)
	if err == nil || !strings.Contains(err.Error(), "<nil>") {
		t.Fatalf("nil assistant part error = %v", err)
	}
}

// A successful result that already starts with the reserved prefix would be
// indistinguishable from a failed one, so strict mode rejects it.
func TestCompileStrictRejectsSuccessfulToolResultWithReservedPrefix(t *testing.T) {
	for _, strict := range []bool{true, false} {
		_, err := newFixtureAdapter(t, []byte(`{"id":"unused"}`)).Compile(context.Background(), provider.CompileInput{
			Request: llm.Request{OperationKey: "op-tool-prefix", Model: "gpt-contract", Input: []llm.Item{
				llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"x"}`)},
				llm.ToolResult{CallID: "call-1", Name: "lookup", Content: []llm.Part{llm.TextPart{Text: "[is_error=true] The tool call "}, llm.TextPart{Text: "failed; its output follows.\nfine"}}},
			}},
			Query:  provider.CapabilityQuery{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract"},
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
