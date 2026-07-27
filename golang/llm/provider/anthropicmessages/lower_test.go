package anthropicmessages

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestCompileLowersOrderedMessagesMultimodalToolsAndThinking(t *testing.T) {
	profile := testProfile()
	adapter := &Adapter{endpointID: "anthropic-prod", profile: mustProfile(t, profile)}
	maxTokens := 321
	temperature := 0.25
	topK := 5
	request := llm.Request{
		OperationKey: "anthropic-lower",
		Model:        "claude-contract",
		ServiceClass: llm.ServiceClassPriority,
		Portability:  llm.PortabilityBestEffort,
		Instructions: []llm.Instruction{
			{Level: llm.InstructionLevelPolicy, Kind: llm.InstructionKindText, Text: "policy"},
			{Level: llm.InstructionLevelApplication, Kind: llm.InstructionKindParts, Content: []llm.Part{
				llm.TextPart{Text: "application"}, llm.JSONPart{Value: json.RawMessage(`{"kind":"context"}`)},
			}},
		},
		Input: []llm.Item{
			llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
				llm.TextPart{Text: "hello"},
				llm.ImagePart{URL: "https://example.test/image.png", MediaType: "image/png"},
				llm.DocumentPart{Bytes: []byte("plain document"), MediaType: "text/plain", Title: "notes"},
			}},
			llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "prior"}}},
			llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"sydney"}`)},
			llm.ToolResult{CallID: "call-1", Content: []llm.Part{llm.JSONPart{Value: json.RawMessage(`{"ok":true}`)}}},
		},
		Tools:      []llm.Tool{{Name: "lookup", Description: "look up a place", InputSchema: json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}`)}},
		ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNamed, Name: "lookup", Parallel: true},
		Output:     &llm.OutputSpec{MaxTokens: &maxTokens, Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Strict: true, Schema: json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"]}`)}},
		Sampling:   &llm.SamplingSpec{Temperature: &temperature, TopK: &topK, StopSequences: []string{"END"}},
		Reasoning:  &llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, TokenBudget: intPtr(2048), Summary: llm.ReasoningSummaryNone},
		Extensions: map[string]json.RawMessage{"anthropic.contract": json.RawMessage(`{"container_id":"container-1"}`)},
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
		Strict:  false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if call.Metadata.ProviderTier != "auto" || call.OperationKey != request.OperationKey {
		t.Fatalf("call metadata = %#v", call)
	}
	wire := marshalWire(t, call.SDKParams)
	if wire["model"] != "claude-contract" || wire["service_tier"] != "auto" || wire["max_tokens"] != float64(maxTokens) {
		t.Fatalf("wire identity = %#v", wire)
	}
	system := wire["system"].([]any)
	if len(system) != 3 || system[0].(map[string]any)["text"] != "policy" || system[1].(map[string]any)["text"] != "application" || system[2].(map[string]any)["text"] != `{"kind":"context"}` {
		t.Fatalf("ordered system blocks = %#v", system)
	}
	messages := wire["messages"].([]any)
	if len(messages) != 4 {
		t.Fatalf("messages = %#v", messages)
	}
	if messages[0].(map[string]any)["role"] != "user" || messages[1].(map[string]any)["role"] != "assistant" {
		t.Fatalf("message roles = %#v", messages)
	}
	userParts := messages[0].(map[string]any)["content"].([]any)
	if userParts[0].(map[string]any)["type"] != "text" || userParts[1].(map[string]any)["type"] != "image" || userParts[2].(map[string]any)["type"] != "document" {
		t.Fatalf("multimodal parts = %#v", userParts)
	}
	if messages[2].(map[string]any)["content"].([]any)[0].(map[string]any)["type"] != "tool_use" {
		t.Fatalf("tool use message = %#v", messages[2])
	}
	toolResult := messages[3].(map[string]any)["content"].([]any)[0].(map[string]any)
	if toolResult["type"] != "tool_result" || toolResult["tool_use_id"] != "call-1" {
		t.Fatalf("tool result = %#v", toolResult)
	}
	if wire["tools"].([]any)[0].(map[string]any)["strict"] != true {
		t.Fatalf("strict tool schema = %#v", wire["tools"])
	}
	if wire["tool_choice"].(map[string]any)["type"] != "tool" {
		t.Fatalf("tool choice = %#v", wire["tool_choice"])
	}
	thinking := wire["thinking"].(map[string]any)
	if thinking["type"] != "enabled" || thinking["budget_tokens"] != float64(2048) || thinking["display"] != "omitted" {
		t.Fatalf("thinking = %#v", thinking)
	}
	if wire["output_config"].(map[string]any)["format"].(map[string]any)["type"] != "json_schema" {
		t.Fatalf("output config = %#v", wire["output_config"])
	}
	if wire["container"] != "container-1" {
		t.Fatalf("extension = %#v", wire["container"])
	}
}

func TestCompileStrictRejectsMixedInstructionLevels(t *testing.T) {
	adapter := &Adapter{endpointID: "anthropic-prod", profile: mustProfile(t, testProfile())}
	_, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{
			OperationKey: "anthropic-strict-instructions",
			Model:        "claude-contract",
			Portability:  llm.PortabilityStrict,
			Instructions: []llm.Instruction{
				{Level: llm.InstructionLevelPolicy, Text: "policy"},
				{Level: llm.InstructionLevelApplication, Text: "application"},
			},
		},
		Query:  provider.CapabilityQuery{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
		Strict: true,
	})
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("compile error = %T %v, want provider error", err, err)
	}
	if mapped.Phase != provider.PhaseCompile || mapped.Dispatch != provider.DispatchNotDispatched || !strings.Contains(mapped.SafeMessage, "instruction hierarchy") {
		t.Fatalf("compile error = %#v, want strict instruction hierarchy rejection", mapped)
	}
}

func TestCompileServiceTierPolicy(t *testing.T) {
	for _, test := range []struct {
		name        string
		class       llm.ServiceClass
		capacity    bool
		wantTier    string
		wantFailure string
	}{
		{name: "economy unsupported", class: llm.ServiceClassEconomy, capacity: true, wantFailure: "unsupported"},
		{name: "standard only", class: llm.ServiceClassStandard, capacity: true, wantTier: "standard_only"},
		{name: "priority requires capacity", class: llm.ServiceClassPriority, capacity: false, wantFailure: "priority_capacity"},
		{name: "priority auto", class: llm.ServiceClassPriority, capacity: true, wantTier: "auto"},
	} {
		t.Run(test.name, func(t *testing.T) {
			profile := testProfile()
			profile.PriorityCapacity = test.capacity
			adapter := &Adapter{endpointID: "anthropic-prod", profile: mustProfile(t, profile)}
			call, err := adapter.Compile(context.Background(), provider.CompileInput{
				Request: llm.Request{OperationKey: "tier-" + test.name, Model: "claude-contract", ServiceClass: test.class},
				Query:   provider.CapabilityQuery{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
				Strict:  true,
			})
			if test.wantFailure != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantFailure) {
					t.Fatalf("error = %v, want %q", err, test.wantFailure)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if call.Metadata.ProviderTier != test.wantTier {
				t.Fatalf("provider tier = %q, want %q", call.Metadata.ProviderTier, test.wantTier)
			}
		})
	}
}

func TestCompileRejectsUnsupportedMediaAndSampling(t *testing.T) {
	adapter := &Adapter{endpointID: "anthropic-prod", profile: mustProfile(t, testProfile())}
	_, err := lowerPart(llm.ImagePart{Blob: &llm.BlobRef{Digest: "sha256:blob", ByteLength: 5, MediaType: "image/png", Locator: "blob://blob-1"}})
	if err == nil || !strings.Contains(err.Error(), "blob-backed") {
		t.Fatalf("blob error = %v", err)
	}
	if _, err := lowerPart(llm.ImagePart{Bytes: []byte("image"), MediaType: "image/tiff"}); err == nil || !strings.Contains(err.Error(), "media type") {
		t.Fatalf("image media type error = %v", err)
	}
	if _, err := lowerPart(llm.ImagePart{URL: "https://example.test/image.png", MediaType: "image/png", Detail: "high"}); err == nil || !strings.Contains(err.Error(), "detail") {
		t.Fatalf("image detail error = %v", err)
	}
	base := llm.Request{OperationKey: "sampling-rejection", Model: "claude-contract"}
	seed := int64(1)
	base.Sampling = &llm.SamplingSpec{Seed: &seed}
	_, err = adapter.Compile(context.Background(), provider.CompileInput{Request: base, Query: provider.CapabilityQuery{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"}, Strict: true})
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("sampling error = %v", err)
	}
}

func TestCompileMapsAdaptiveReasoningEffortToOutputConfig(t *testing.T) {
	adapter := &Adapter{endpointID: "anthropic-prod", profile: mustProfile(t, testProfile())}
	request := llm.Request{
		OperationKey: "reasoning-effort",
		Model:        "claude-contract",
		Reasoning:    &llm.ReasoningSpec{Mode: llm.ReasoningModeAdaptive, Effort: llm.ReasoningEffortHigh},
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	wire := marshalWire(t, call.SDKParams)
	thinking, ok := wire["thinking"].(map[string]any)
	if !ok || thinking["type"] != "adaptive" {
		t.Fatalf("thinking = %#v", wire["thinking"])
	}
	outputConfig, ok := wire["output_config"].(map[string]any)
	if !ok || outputConfig["effort"] != "high" {
		t.Fatalf("output_config = %#v", wire["output_config"])
	}
}

func mustProfile(t *testing.T, profile Profile) Profile {
	t.Helper()
	validated, err := NewProfile(profile)
	if err != nil {
		t.Fatal(err)
	}
	return validated
}

func intPtr(value int) *int { return &value }
