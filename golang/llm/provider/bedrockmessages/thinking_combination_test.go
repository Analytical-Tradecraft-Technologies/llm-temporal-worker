package bedrockmessages

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func intPtr(value int) *int { return &value }

func floatPtr(value float64) *float64 { return &value }

// Manual thinking fields are each valid on their own; the provider rejects
// some combinations, so compile must too, before anything is dispatched.
func TestCompileRejectsIncompatibleManualThinkingCombinations(t *testing.T) {
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
			request.Model = "claude-contract"
			request.Input = []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}
			adapter := &Adapter{endpointID: "bedrock-prod", profile: mustBedrockProfile(t, "")}
			_, err := adapter.Compile(context.Background(), provider.CompileInput{
				Request: request,
				Query:   provider.CapabilityQuery{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract"},
				Strict:  true,
			})
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
