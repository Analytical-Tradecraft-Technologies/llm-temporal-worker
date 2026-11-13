package bedrockmessages

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func compileForModel(t *testing.T, model string, request llm.Request, strict bool) (provider.Call, error) {
	t.Helper()
	request.Model = model
	request.OperationKey = "model-sampling"
	profile := DefaultProfile("bedrock-contract")
	profile.CapabilityVersion = "bedrock-contract/v1"
	profile.Capabilities.Version = profile.CapabilityVersion
	profile.ExpectedModel = model
	validated, err := NewProfile(profile)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &Adapter{endpointID: "bedrock-test", profile: validated}
	return adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: "bedrock-test", Family: provider.FamilyBedrockMessages, Model: model},
		Strict:  strict,
	})
}

// Current Claude models refuse sampling combinations the request IR allows;
// each is rejected at compile, before admission and dispatch (#1503).
func TestCompileRejectsSamplingTheModelRefuses(t *testing.T) {
	temperature, defaultTemperature, topP := 0.4, 1.0, 0.9
	topK := 20
	both := &llm.SamplingSpec{Temperature: &temperature, TopP: &topP}
	for _, test := range []struct {
		model    string
		sampling *llm.SamplingSpec
		want     string
	}{
		{"us.anthropic.claude-sonnet-5-5", &llm.SamplingSpec{Temperature: &defaultTemperature}, "Claude Sonnet 5.5 does not support temperature"},
		{"us.anthropic.claude-sonnet-5-5", &llm.SamplingSpec{TopK: &topK}, "Claude Sonnet 5.5 does not support top_k"},
		{"anthropic.claude-opus-5-5", &llm.SamplingSpec{Temperature: &temperature}, "Claude Opus 5.5 does not support temperature"},
		{"anthropic.claude-opus-5", &llm.SamplingSpec{TopP: &topP}, "Claude Opus 5 does not support top_p"},
		{"global.anthropic.claude-opus-4-8", both, "Claude Opus 4.8 does not support temperature"},
		{"anthropic.claude-sonnet-5", &llm.SamplingSpec{Temperature: &temperature}, "Claude Sonnet 5 only supports temperature=1"},
		{"anthropic.claude-sonnet-4-6", both, "Claude Sonnet 4.6 does not support combining temperature and top_p"},
		{"us.anthropic.claude-sonnet-4-5-20250929-v1:0", both, "Claude Sonnet 4.5 does not support combining temperature and top_p"},
		{"anthropic.claude-opus-4-6-v1", both, "Claude Opus 4.6 does not support combining temperature and top_p"},
		{"us.anthropic.claude-opus-4-1-20250805-v1:0", both, "Claude Opus 4.1 does not support combining temperature and top_p"},
	} {
		for _, strict := range []bool{false, true} {
			t.Run(test.model+portabilityName(strict), func(t *testing.T) {
				call, err := compileForModel(t, test.model, llm.Request{Sampling: test.sampling}, strict)
				var mapped *provider.Error
				if !errors.As(err, &mapped) || mapped.Code != provider.CodeUnsupportedCapability || mapped.Phase != provider.PhaseCompile || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever || !strings.Contains(mapped.SafeMessage, test.want) {
					t.Fatalf("Compile() = %v, want non-retryable unsupported capability containing %q", err, test.want)
				}
				if call.SDKParams != nil {
					t.Fatal("rejected request produced dispatch parameters")
				}
			})
		}
	}
}

func TestCompileKeepsSamplingTheModelAccepts(t *testing.T) {
	temperature, defaultTemperature, topP := 0.4, 1.0, 0.9
	topK := 20
	for _, test := range []struct {
		model    string
		sampling llm.SamplingSpec
		want     map[string]any
	}{
		{"anthropic.claude-sonnet-4-6", llm.SamplingSpec{Temperature: &temperature, TopK: &topK}, map[string]any{"temperature": temperature, "top_k": float64(topK)}},
		{"anthropic.claude-opus-4-6-v1", llm.SamplingSpec{TopP: &topP}, map[string]any{"top_p": topP}},
		{"anthropic.claude-sonnet-5", llm.SamplingSpec{Temperature: &defaultTemperature}, map[string]any{"temperature": defaultTemperature}},
		{"claude-contract", llm.SamplingSpec{Temperature: &temperature, TopP: &topP}, map[string]any{"temperature": temperature, "top_p": topP}},
	} {
		t.Run(test.model, func(t *testing.T) {
			call, err := compileForModel(t, test.model, llm.Request{Sampling: &test.sampling}, true)
			if err != nil {
				t.Fatal(err)
			}
			wire := marshalBedrockWire(t, call.SDKParams)
			for field, value := range test.want {
				if wire[field] != value {
					t.Fatalf("%s = %#v, want %#v in %#v", field, wire[field], value, wire)
				}
			}
		})
	}
}

func TestCompileSendsExtraHighEffortToModelsThatAcceptIt(t *testing.T) {
	for _, model := range []string{"anthropic.claude-opus-5-5", "us.anthropic.claude-sonnet-5-5", "anthropic.claude-opus-4-7"} {
		call, err := compileForModel(t, model, llm.Request{Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeAdaptive, Effort: llm.ReasoningEffortExtraHigh}}, true)
		if err != nil {
			t.Fatalf("%s: %v", model, err)
		}
		wire := marshalBedrockWire(t, call.SDKParams)
		if config, _ := wire["output_config"].(map[string]any); config["effort"] != "xhigh" {
			t.Fatalf("%s: output_config = %#v, want effort xhigh", model, wire["output_config"])
		}
	}
	_, err := compileForModel(t, "anthropic.claude-sonnet-4-6", llm.Request{Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeAdaptive, Effort: llm.ReasoningEffortExtraHigh}}, true)
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeUnsupportedCapability || !strings.Contains(mapped.SafeMessage, "xhigh") {
		t.Fatalf("Sonnet 4.6 xhigh: Compile() = %v, want unsupported capability", err)
	}
}
