// Package messagestest holds the compile tests that the Anthropic Messages and
// Bedrock Messages adapters share. Both lower requests through anthropicwire,
// so the same request must compile the same way on each; every function here
// runs one such test against the adapter that compile builds.
package messagestest

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// Compile compiles request on a fresh adapter of the family under test whose
// profile expects request.Model. strict selects strict portability.
type Compile func(t *testing.T, request llm.Request, strict bool) (provider.Call, error)

// ContractModel is the model the adapters' contract profiles expect.
const ContractModel = "claude-contract"

func intPtr(value int) *int { return &value }

func floatPtr(value float64) *float64 { return &value }

// PortabilityName suffixes a subtest name with its portability mode.
func PortabilityName(strict bool) string {
	if strict {
		return "/strict"
	}
	return "/best effort"
}

// Wire decodes the request body that SDK parameters would send.
func Wire(t *testing.T, params any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(encoded, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func isNonRetryableUnsupported(err error, want string) bool {
	var mapped *provider.Error
	return errors.As(err, &mapped) && mapped.Code == provider.CodeUnsupportedCapability && mapped.Phase == provider.PhaseCompile && mapped.Dispatch == provider.DispatchNotDispatched && mapped.Retry == provider.RetryNever && strings.Contains(mapped.SafeMessage, want)
}

// ManualThinkingCombinations checks that compile rejects the manual thinking
// combinations the provider refuses, before anything is dispatched, and
// accepts the documented ones.
func ManualThinkingCombinations(t *testing.T, compile Compile) {
	lookup := []llm.Tool{{Name: "lookup", InputSchema: json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}`)}}
	manual := func(budget int) *llm.ReasoningSpec {
		return &llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, TokenBudget: intPtr(budget)}
	}
	for _, test := range []struct {
		name      string
		request   llm.Request
		wantError string
	}{
		{name: "budget above max_tokens", request: llm.Request{Reasoning: manual(2048), Output: &llm.OutputSpec{MaxTokens: intPtr(1024)}}, wantError: "must be less than max_tokens"},
		{name: "budget equal to max_tokens", request: llm.Request{Reasoning: manual(2048), Output: &llm.OutputSpec{MaxTokens: intPtr(2048)}}, wantError: "must be less than max_tokens"},
		{name: "budget above default max_tokens", request: llm.Request{Reasoning: manual(2048)}, wantError: "must be less than max_tokens"},
		{name: "budget above max_tokens with tools", request: llm.Request{Reasoning: manual(2048), Output: &llm.OutputSpec{MaxTokens: intPtr(1024)}, Tools: lookup}, wantError: "must be less than max_tokens"},
		{name: "named tool choice", request: llm.Request{Reasoning: manual(2048), Output: &llm.OutputSpec{MaxTokens: intPtr(4096)}, Tools: lookup, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNamed, Name: "lookup"}}, wantError: "forced tool choice"},
		{name: "required tool choice", request: llm.Request{Reasoning: manual(2048), Output: &llm.OutputSpec{MaxTokens: intPtr(4096)}, Tools: lookup, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceRequired}}, wantError: "forced tool choice"},
		{name: "temperature", request: llm.Request{Reasoning: manual(2048), Output: &llm.OutputSpec{MaxTokens: intPtr(4096)}, Sampling: &llm.SamplingSpec{Temperature: floatPtr(0.25)}}, wantError: "temperature"},
		{name: "top_k", request: llm.Request{Reasoning: manual(2048), Output: &llm.OutputSpec{MaxTokens: intPtr(4096)}, Sampling: &llm.SamplingSpec{TopK: intPtr(5)}}, wantError: "top_k"},
		{name: "top_p below 0.95", request: llm.Request{Reasoning: manual(2048), Output: &llm.OutputSpec{MaxTokens: intPtr(4096)}, Sampling: &llm.SamplingSpec{TopP: floatPtr(0.5)}}, wantError: "top_p"},
		{name: "valid manual thinking", request: llm.Request{Reasoning: manual(2048), Output: &llm.OutputSpec{MaxTokens: intPtr(4096)}, Tools: lookup, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceAuto}}},
		{name: "manual thinking with documented top_p", request: llm.Request{Reasoning: manual(2048), Output: &llm.OutputSpec{MaxTokens: intPtr(4096)}, Sampling: &llm.SamplingSpec{TopP: floatPtr(0.95)}}},
		{name: "manual thinking with default temperature", request: llm.Request{Reasoning: manual(2048), Output: &llm.OutputSpec{MaxTokens: intPtr(4096)}, Sampling: &llm.SamplingSpec{Temperature: floatPtr(1)}}},
		{name: "manual thinking with no tool choice", request: llm.Request{Reasoning: manual(2048), Output: &llm.OutputSpec{MaxTokens: intPtr(4096)}, Tools: lookup, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNone}}},
		{name: "adaptive thinking with forced tool and sampling", request: llm.Request{Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeAdaptive}, Output: &llm.OutputSpec{MaxTokens: intPtr(1024)}, Tools: lookup, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNamed, Name: "lookup"}, Sampling: &llm.SamplingSpec{Temperature: floatPtr(0.25), TopK: intPtr(5)}}},
		{name: "forced tool and sampling without thinking", request: llm.Request{Output: &llm.OutputSpec{MaxTokens: intPtr(1024)}, Tools: lookup, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceRequired}, Sampling: &llm.SamplingSpec{Temperature: floatPtr(0.25), TopP: floatPtr(0.5)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := test.request
			request.OperationKey = "thinking-combination"
			request.Model = ContractModel
			request.Input = []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}
			_, err := compile(t, request, true)
			if test.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var mapped *provider.Error
			if err == nil || !strings.Contains(err.Error(), test.wantError) || !errors.As(err, &mapped) || mapped.Phase != provider.PhaseCompile || mapped.Dispatch != provider.DispatchNotDispatched {
				t.Fatalf("Compile() = %v, want compile rejection containing %q", err, test.wantError)
			}
		})
	}
}

func compileForModel(t *testing.T, compile Compile, model string, request llm.Request, strict bool) (provider.Call, error) {
	t.Helper()
	request.Model = model
	request.OperationKey = "model-sampling"
	return compile(t, request, strict)
}

// SamplingTheModelRefuses checks that current Claude models' refused
// sampling combinations are rejected at compile, before admission and
// dispatch (#1503).
func SamplingTheModelRefuses(t *testing.T, compile Compile) {
	temperature, defaultTemperature, topP := 0.4, 1.0, 0.9
	topK := 20
	both := &llm.SamplingSpec{Temperature: &temperature, TopP: &topP}
	for _, test := range []struct {
		model    string
		sampling *llm.SamplingSpec
		want     string
	}{
		{"claude-sonnet-5-5", &llm.SamplingSpec{Temperature: &defaultTemperature}, "Claude Sonnet 5.5 does not support temperature"},
		{"claude-sonnet-5-5", &llm.SamplingSpec{TopK: &topK}, "Claude Sonnet 5.5 does not support top_k"},
		{"claude-opus-5-5", &llm.SamplingSpec{Temperature: &temperature}, "Claude Opus 5.5 does not support temperature"},
		{"claude-opus-5", &llm.SamplingSpec{TopP: &topP}, "Claude Opus 5 does not support top_p"},
		{"claude-opus-4-8", both, "Claude Opus 4.8 does not support temperature"},
		{"claude-sonnet-5", &llm.SamplingSpec{Temperature: &temperature}, "Claude Sonnet 5 only supports temperature=1"},
		{"claude-sonnet-4-6", both, "Claude Sonnet 4.6 does not support combining temperature and top_p"},
		{"claude-sonnet-4-5-20250929", both, "Claude Sonnet 4.5 does not support combining temperature and top_p"},
		{"claude-opus-4-6", both, "Claude Opus 4.6 does not support combining temperature and top_p"},
		{"claude-opus-4-1-20250805", both, "Claude Opus 4.1 does not support combining temperature and top_p"},
	} {
		for _, strict := range []bool{false, true} {
			t.Run(test.model+PortabilityName(strict), func(t *testing.T) {
				call, err := compileForModel(t, compile, test.model, llm.Request{Sampling: test.sampling}, strict)
				if !isNonRetryableUnsupported(err, test.want) {
					t.Fatalf("Compile() = %v, want non-retryable unsupported capability containing %q", err, test.want)
				}
				if call.SDKParams != nil {
					t.Fatal("rejected request produced dispatch parameters")
				}
			})
		}
	}
}

// SamplingTheModelAccepts checks that sampling a model accepts reaches the
// wire unchanged.
func SamplingTheModelAccepts(t *testing.T, compile Compile) {
	temperature, defaultTemperature, topP := 0.4, 1.0, 0.9
	topK := 20
	for _, test := range []struct {
		model    string
		sampling llm.SamplingSpec
		want     map[string]any
	}{
		{"claude-sonnet-4-6", llm.SamplingSpec{Temperature: &temperature, TopK: &topK}, map[string]any{"temperature": temperature, "top_k": float64(topK)}},
		{"claude-opus-4-6", llm.SamplingSpec{TopP: &topP}, map[string]any{"top_p": topP}},
		{"claude-sonnet-5", llm.SamplingSpec{Temperature: &defaultTemperature}, map[string]any{"temperature": defaultTemperature}},
		{ContractModel, llm.SamplingSpec{Temperature: &temperature, TopP: &topP}, map[string]any{"temperature": temperature, "top_p": topP}},
	} {
		t.Run(test.model, func(t *testing.T) {
			call, err := compileForModel(t, compile, test.model, llm.Request{Sampling: &test.sampling}, true)
			if err != nil {
				t.Fatal(err)
			}
			wire := Wire(t, call.SDKParams)
			for field, value := range test.want {
				if wire[field] != value {
					t.Fatalf("%s = %#v, want %#v in %#v", field, wire[field], value, wire)
				}
			}
		})
	}
}

// ExtraHighEffortByModel checks that xhigh effort reaches the models that
// accept it and is refused as an unsupported capability elsewhere.
func ExtraHighEffortByModel(t *testing.T, compile Compile) {
	for _, model := range []string{"claude-opus-5-5", "claude-sonnet-5-5", "claude-opus-4-7"} {
		call, err := compileForModel(t, compile, model, llm.Request{Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeAdaptive, Effort: llm.ReasoningEffortExtraHigh}}, true)
		if err != nil {
			t.Fatalf("%s: %v", model, err)
		}
		wire := Wire(t, call.SDKParams)
		if config, _ := wire["output_config"].(map[string]any); config["effort"] != "xhigh" {
			t.Fatalf("%s: output_config = %#v, want effort xhigh", model, wire["output_config"])
		}
	}
	_, err := compileForModel(t, compile, "claude-sonnet-4-6", llm.Request{Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeAdaptive, Effort: llm.ReasoningEffortExtraHigh}}, true)
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeUnsupportedCapability || !strings.Contains(mapped.SafeMessage, "xhigh") {
		t.Fatalf("Sonnet 4.6 xhigh: Compile() = %v, want unsupported capability", err)
	}
}

func human(text string) llm.Message {
	return llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}}
}

// SkipsReplayedEmptyModelMessage checks that a replayed model message with
// no parts, as a lifted empty or content-filtered reply is, never puts an
// assistant message without content on the wire.
func SkipsReplayedEmptyModelMessage(t *testing.T, compile Compile) {
	// The empty model turn sits between two user turns and again next to a
	// model turn that was split into text and a tool call.
	request := llm.Request{OperationKey: "empty-model-replay", Model: ContractModel, Input: []llm.Item{
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
			call, err := compile(t, request, strict)
			if err != nil {
				t.Fatalf("compile replayed empty model message: %v", err)
			}
			encoded, err := json.Marshal(call.SDKParams)
			if err != nil {
				t.Fatal(err)
			}
			var wire struct {
				Messages []struct {
					Role    string `json:"role"`
					Content []struct {
						Type string `json:"type"`
					} `json:"content"`
				} `json:"messages"`
			}
			if err := json.Unmarshal(encoded, &wire); err != nil {
				t.Fatal(err)
			}
			shape := make([]string, 0, len(wire.Messages))
			for _, message := range wire.Messages {
				if len(message.Content) == 0 {
					t.Fatalf("message without content reached the wire: %s", encoded)
				}
				shape = append(shape, message.Role+":"+message.Content[0].Type)
			}
			if got, want := strings.Join(shape, ","), "user:text,user:text,assistant:text,assistant:tool_use,user:tool_result"; got != want {
				t.Fatalf("messages = %s, want %s", got, want)
			}
		})
	}
}

// SkipsReplayedReferenceAnnotations checks that the citations a replayed
// transcript still carries, which have no wire form, are left out instead of
// failing the only candidate.
func SkipsReplayedReferenceAnnotations(t *testing.T, compile Compile) {
	// The second response splits one model turn into text and a tool call and
	// carries a citation between the call and its result.
	request := llm.Request{OperationKey: "reference-replay", Model: ContractModel, Input: []llm.Item{
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
			call, err := compile(t, request, strict)
			if err != nil {
				t.Fatalf("compile replayed references: %v", err)
			}
			wire := Wire(t, call.SDKParams)
			messages := wire["messages"].([]any)
			roles := make([]string, 0, len(messages))
			for _, message := range messages {
				roles = append(roles, message.(map[string]any)["role"].(string))
			}
			if got, want := strings.Join(roles, ","), "user,assistant,user,assistant,assistant,user"; got != want {
				t.Fatalf("message roles = %s, want %s", got, want)
			}
			if encoded, err := json.Marshal(wire); err != nil || strings.Contains(string(encoded), "example.com") {
				t.Fatalf("reference reached the wire: %s (%v)", encoded, err)
			}
		})
	}
}
