package anthropicmessages

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func compileHaiku55(t *testing.T, request llm.Request, strict bool) (provider.Call, error) {
	t.Helper()
	if request.Model == "" {
		request.Model = "claude-haiku-5-5"
	}
	profile := testProfile()
	profile.ExpectedModel = request.Model
	profile.AllowedExtensions["anthropic.haiku55"] = ExtensionSpec{Fields: map[string]string{
		"temperature": "temperature", "top_p": "top_p", "top_k": "top_k", "thinking": "thinking",
	}}
	adapter := &Adapter{endpointID: "anthropic-test", profile: mustProfile(t, profile)}
	request.OperationKey = "haiku55-parameters"
	return adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: "anthropic-test", Family: provider.FamilyAnthropicMessages, Model: request.Model},
		Strict:  strict,
	})
}

func TestHaiku55RejectsUnsupportedParametersBeforeDispatch(t *testing.T) {
	temperature, defaultTemperature := 0.5, 1.0
	topP, defaultTopP := 1.0, 0.99
	topK := 0
	budget := 2048
	for _, test := range []struct {
		name    string
		request llm.Request
	}{
		{name: "priority", request: llm.Request{ServiceClass: llm.ServiceClassPriority}},
		{name: "enabled thinking", request: llm.Request{Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, TokenBudget: &budget}}},
		{name: "enabled without budget", request: llm.Request{Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled}}},
		{name: "modeless budget", request: llm.Request{Reasoning: &llm.ReasoningSpec{TokenBudget: &budget}}},
		{name: "adaptive budget", request: llm.Request{Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeAdaptive, TokenBudget: &budget}}},
		{name: "temperature", request: llm.Request{Sampling: &llm.SamplingSpec{Temperature: &temperature}}},
		{name: "top p", request: llm.Request{Sampling: &llm.SamplingSpec{TopP: &topP}}},
		{name: "top k", request: llm.Request{Sampling: &llm.SamplingSpec{TopK: &topK}}},
		{name: "combined defaults", request: llm.Request{Sampling: &llm.SamplingSpec{Temperature: &defaultTemperature, TopP: &defaultTopP}}},
		{name: "extension temperature", request: llm.Request{Extensions: map[string]json.RawMessage{"anthropic.haiku55": json.RawMessage(`{"temperature":0.5}`)}}},
		{name: "extension top p", request: llm.Request{Extensions: map[string]json.RawMessage{"anthropic.haiku55": json.RawMessage(`{"top_p":1}`)}}},
		{name: "extension top k", request: llm.Request{Extensions: map[string]json.RawMessage{"anthropic.haiku55": json.RawMessage(`{"top_k":0}`)}}},
		{name: "extension combined defaults", request: llm.Request{Extensions: map[string]json.RawMessage{"anthropic.haiku55": json.RawMessage(`{"temperature":1,"top_p":0.99}`)}}},
		{name: "extension enabled thinking", request: llm.Request{Extensions: map[string]json.RawMessage{"anthropic.haiku55": json.RawMessage(`{"thinking":{"type":"enabled","budget_tokens":2048}}`)}}},
		{name: "extension adaptive budget", request: llm.Request{Extensions: map[string]json.RawMessage{"anthropic.haiku55": json.RawMessage(`{"thinking":{"type":"adaptive","budget_tokens":2048}}`)}}},
		{name: "extension null thinking", request: llm.Request{Extensions: map[string]json.RawMessage{"anthropic.haiku55": json.RawMessage(`{"thinking":null}`)}}},
		{name: "mixed sampling and extension", request: llm.Request{Sampling: &llm.SamplingSpec{Temperature: &defaultTemperature}, Extensions: map[string]json.RawMessage{"anthropic.haiku55": json.RawMessage(`{"top_p":0.99}`)}}},
	} {
		for _, strict := range []bool{false, true} {
			t.Run(test.name+portabilityName(strict), func(t *testing.T) {
				call, err := compileHaiku55(t, test.request, strict)
				var mapped *provider.Error
				if !errors.As(err, &mapped) || mapped.Code != provider.CodeUnsupportedCapability || mapped.Phase != provider.PhaseCompile || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever || !strings.Contains(mapped.SafeMessage, "Haiku 5.5") {
					t.Fatalf("Compile() = %v, want non-retryable unsupported capability before dispatch", err)
				}
				if call.SDKParams != nil {
					t.Fatal("unsupported request produced dispatch parameters")
				}
			})
		}
	}
}

func TestHaiku55PreservesSupportedParameters(t *testing.T) {
	temperature, topP := 1.0, 0.99
	for _, test := range []struct {
		name    string
		request llm.Request
		check   func(*testing.T, map[string]any)
	}{
		{name: "defaults", check: func(t *testing.T, wire map[string]any) {
			for _, field := range []string{"temperature", "top_p", "top_k", "thinking"} {
				if _, exists := wire[field]; exists {
					t.Fatalf("defaults unexpectedly emitted %s", field)
				}
			}
		}},
		{name: "modeless effort", request: llm.Request{Reasoning: &llm.ReasoningSpec{Effort: llm.ReasoningEffortMedium}}, check: func(t *testing.T, wire map[string]any) {
			if wire["output_config"].(map[string]any)["effort"] != "medium" {
				t.Fatal(wire)
			}
		}},
		{name: "adaptive", request: llm.Request{Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeAdaptive, Effort: llm.ReasoningEffortHigh}}, check: func(t *testing.T, wire map[string]any) {
			if wire["thinking"].(map[string]any)["type"] != "adaptive" || wire["output_config"].(map[string]any)["effort"] != "high" {
				t.Fatal(wire)
			}
		}},
		{name: "disabled", request: llm.Request{Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeDisabled}}, check: func(t *testing.T, wire map[string]any) {
			if wire["thinking"].(map[string]any)["type"] != "disabled" {
				t.Fatal(wire)
			}
		}},
		{name: "temperature default", request: llm.Request{Sampling: &llm.SamplingSpec{Temperature: &temperature}}, check: func(t *testing.T, wire map[string]any) {
			if wire["temperature"] != temperature {
				t.Fatal(wire)
			}
		}},
		{name: "top p default", request: llm.Request{Sampling: &llm.SamplingSpec{TopP: &topP}}, check: func(t *testing.T, wire map[string]any) {
			if wire["top_p"] != topP {
				t.Fatal(wire)
			}
		}},
		{name: "extension adaptive", request: llm.Request{Extensions: map[string]json.RawMessage{"anthropic.haiku55": json.RawMessage(`{"thinking":{"type":"adaptive"}}`)}}, check: func(t *testing.T, wire map[string]any) {
			if wire["thinking"].(map[string]any)["type"] != "adaptive" {
				t.Fatal(wire)
			}
		}},
		{name: "extension temperature default", request: llm.Request{Extensions: map[string]json.RawMessage{"anthropic.haiku55": json.RawMessage(`{"temperature":1}`)}}, check: func(t *testing.T, wire map[string]any) {
			if wire["temperature"] != temperature {
				t.Fatal(wire)
			}
		}},
		{name: "extension top p default", request: llm.Request{Extensions: map[string]json.RawMessage{"anthropic.haiku55": json.RawMessage(`{"top_p":0.99}`)}}, check: func(t *testing.T, wire map[string]any) {
			if wire["top_p"] != topP {
				t.Fatal(wire)
			}
		}},
	} {
		for _, strict := range []bool{false, true} {
			t.Run(test.name+portabilityName(strict), func(t *testing.T) {
				call, err := compileHaiku55(t, test.request, strict)
				if err != nil {
					t.Fatal(err)
				}
				test.check(t, marshalWire(t, call.SDKParams))
			})
		}
	}
}

func TestHaiku55ConstraintsDoNotChangeOtherModels(t *testing.T) {
	temperature := 0.5
	topK := 5
	budget := 2048
	call, err := compileHaiku55(t, llm.Request{
		Model:        "claude-contract",
		ServiceClass: llm.ServiceClassPriority,
		Reasoning:    &llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, TokenBudget: &budget},
		Sampling:     &llm.SamplingSpec{Temperature: &temperature, TopK: &topK},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	wire := marshalWire(t, call.SDKParams)
	if wire["service_tier"] != "auto" || wire["temperature"] != temperature || wire["top_k"] != float64(topK) || wire["thinking"].(map[string]any)["type"] != "enabled" {
		t.Fatal(wire)
	}
}
