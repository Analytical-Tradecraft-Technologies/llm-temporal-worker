package anthropicwire

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestCanonicalModelAcceptsProviderIDForms(t *testing.T) {
	for model, want := range map[string]string{
		"claude-sonnet-5-5":                                                     "claude-sonnet-5-5",
		"claude-opus-4-1-20250805":                                              "claude-opus-4-1",
		"claude-opus-4-5@20251101":                                              "claude-opus-4-5",
		"anthropic.claude-opus-5-5":                                             "claude-opus-5-5",
		"us.anthropic.claude-sonnet-4-5-20250929-v1:0":                          "claude-sonnet-4-5",
		"global.anthropic.claude-haiku-4-5-20251001-v1:0":                       "claude-haiku-4-5",
		"arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-sonnet-5": "claude-sonnet-5",
		"Claude-Opus-5":                                                         "claude-opus-5",
		"claude-sonnet-4":                                                       "claude-sonnet-4",
		"claude-contract":                                                       "claude-contract",
		"anthropic.claude-v2":                                                   "claude",
	} {
		if got := canonicalModel(model); got != want {
			t.Errorf("canonicalModel(%q) = %q, want %q", model, got, want)
		}
	}
}

func TestValidateModelSamplingFollowsThePerModelTable(t *testing.T) {
	type wire = map[string]any
	tests := []struct {
		model  string
		wire   wire
		reject string
	}{
		// Unknown models and models without a documented rule pass through.
		{model: "claude-contract", wire: wire{"temperature": 0.2, "top_p": 0.5, "top_k": 3}},
		{model: "claude-3-5-haiku-20241022", wire: wire{"temperature": 0.2, "top_p": 0.5}},
		// Claude 4.x before Opus 4.7: one of temperature and top_p.
		{model: "claude-opus-4-1-20250805", wire: wire{"temperature": 0.2, "top_p": 0.5}, reject: "Claude Opus 4.1 does not support combining temperature and top_p"},
		{model: "claude-sonnet-4-5", wire: wire{"temperature": 0.2, "top_p": 0.5}, reject: "Claude Sonnet 4.5 does not support combining"},
		{model: "claude-sonnet-4-6", wire: wire{"temperature": 0.2, "top_p": 0.5}, reject: "Claude Sonnet 4.6 does not support combining"},
		{model: "claude-opus-4-6", wire: wire{"temperature": 0.2, "top_p": 0.5}, reject: "Claude Opus 4.6 does not support combining"},
		{model: "claude-haiku-4-5", wire: wire{"temperature": 0.2, "top_p": 0.5}, reject: "Claude Haiku 4.5 does not support combining"},
		{model: "claude-sonnet-4-6", wire: wire{"temperature": 0.2, "top_k": 5}},
		{model: "claude-opus-4-6", wire: wire{"top_p": 0.5, "top_k": 5}},
		// Opus 4.7 and later, Sonnet 5.5, Fable and Mythos: no sampling field.
		{model: "claude-opus-4-7", wire: wire{"temperature": 1.0}, reject: "Claude Opus 4.7 does not support temperature"},
		{model: "claude-opus-5-5", wire: wire{"top_p": 0.99}, reject: "Claude Opus 5.5 does not support top_p"},
		{model: "claude-opus-5", wire: wire{"top_k": 1}, reject: "Claude Opus 5 does not support top_k"},
		{model: "claude-sonnet-5-5", wire: wire{"temperature": 1.0}, reject: "Claude Sonnet 5.5 does not support temperature"},
		{model: "claude-fable-5-2", wire: wire{"temperature": 0.5}, reject: "Claude Fable 5.2 does not support temperature"},
		{model: "claude-mythos-5-1", wire: wire{"top_p": 0.9}, reject: "Claude Mythos 5.1 does not support top_p"},
		{model: "claude-opus-5-5", wire: wire{"max_tokens": 10}},
		// Sonnet 5 and Haiku 5.5: defaults only.
		{model: "claude-sonnet-5", wire: wire{"temperature": 1.0}},
		{model: "claude-sonnet-5", wire: wire{"temperature": json.Number("1")}},
		{model: "claude-sonnet-5", wire: wire{"temperature": 0.2}, reject: "Claude Sonnet 5 only supports temperature=1"},
		{model: "claude-sonnet-5", wire: wire{"top_p": 0.99}, reject: "Claude Sonnet 5 does not support top_p"},
		{model: "claude-sonnet-5", wire: wire{"top_k": 1}, reject: "Claude Sonnet 5 does not support top_k"},
		{model: "claude-haiku-5-5", wire: wire{"top_p": 0.99}},
		{model: "claude-haiku-5-5", wire: wire{"top_p": 1.0}, reject: "Claude Haiku 5.5 only supports top_p=0.99"},
		{model: "claude-haiku-5-5", wire: wire{"temperature": 1.0, "top_p": 0.99}, reject: "Claude Haiku 5.5 does not support combining"},
	}
	for _, test := range tests {
		err := ValidateModelSampling(test.model, test.wire)
		if test.reject == "" {
			if err != nil {
				t.Errorf("%s %v: error = %v, want accepted", test.model, test.wire, err)
			}
			continue
		}
		var mapped *provider.Error
		if !errors.As(err, &mapped) || mapped.Code != provider.CodeUnsupportedCapability || mapped.Phase != provider.PhaseCompile || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever || !strings.Contains(mapped.SafeMessage, test.reject) {
			t.Errorf("%s %v: error = %v, want unsupported capability containing %q", test.model, test.wire, err, test.reject)
		}
	}
}

func TestLowerReasoningAcceptsExtraHighOnlyWhereTheModelDoes(t *testing.T) {
	for _, family := range []Family{Anthropic, Bedrock} {
		for _, model := range []string{"claude-opus-5-5", "anthropic.claude-sonnet-5-5", "claude-opus-4-7", "claude-haiku-5-5", "claude-fable-5-2"} {
			reasoning := llm.ReasoningSpec{Mode: llm.ReasoningModeAdaptive, Effort: llm.ReasoningEffortExtraHigh}
			thinking, err := family.LowerReasoning(model, reasoning, true)
			if err != nil || thinking["type"] != "adaptive" || ReasoningOutputEffort(reasoning) != "xhigh" {
				t.Errorf("%s %s: thinking = %#v, effort = %q, err = %v", family.Name, model, thinking, ReasoningOutputEffort(reasoning), err)
			}
		}
		for _, model := range []string{"claude-contract", "claude-opus-4-6", "claude-sonnet-4-6", "claude-haiku-4-5"} {
			_, err := family.LowerReasoning(model, llm.ReasoningSpec{Effort: llm.ReasoningEffortExtraHigh}, false)
			var mapped *provider.Error
			if !errors.As(err, &mapped) || mapped.Code != provider.CodeUnsupportedCapability || !strings.Contains(mapped.SafeMessage, "xhigh") {
				t.Errorf("%s %s: error = %v, want xhigh unsupported", family.Name, model, err)
			}
		}
	}
}
