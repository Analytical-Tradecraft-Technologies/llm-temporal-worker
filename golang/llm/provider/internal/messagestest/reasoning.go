package messagestest

import (
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func compileReasoning(t *testing.T, compile Compile, reasoning llm.ReasoningSpec, strict bool) (map[string]any, error) {
	t.Helper()
	call, err := compile(t, llm.Request{OperationKey: "v1-reasoning", Model: ContractModel, Reasoning: &reasoning, Output: &llm.OutputSpec{MaxTokens: intPtr(4096)}}, strict)
	if err != nil {
		return nil, err
	}
	return Wire(t, call.SDKParams), nil
}

// ModelessReasoningNeverEnablesThinking checks that the reasoning effort and
// summary the v1 contract sets, never the mode, neither turn thinking on nor
// make the request uncompilable.
func ModelessReasoningNeverEnablesThinking(t *testing.T, compile Compile) {
	for _, test := range []struct {
		name       string
		reasoning  llm.ReasoningSpec
		wantEffort string
	}{
		{name: "effort minimal", reasoning: llm.ReasoningSpec{Effort: llm.ReasoningEffortMinimal}, wantEffort: "low"},
		{name: "effort low", reasoning: llm.ReasoningSpec{Effort: llm.ReasoningEffortLow}, wantEffort: "low"},
		{name: "effort medium", reasoning: llm.ReasoningSpec{Effort: llm.ReasoningEffortMedium}, wantEffort: "medium"},
		{name: "effort high", reasoning: llm.ReasoningSpec{Effort: llm.ReasoningEffortHigh}, wantEffort: "high"},
		{name: "effort maximum", reasoning: llm.ReasoningSpec{Effort: llm.ReasoningEffortMaximum}, wantEffort: "max"},
		{name: "effort with explicit default mode", reasoning: llm.ReasoningSpec{Mode: llm.ReasoningModeProviderDefault, Effort: llm.ReasoningEffortHigh}, wantEffort: "high"},
		{name: "summary none", reasoning: llm.ReasoningSpec{Summary: llm.ReasoningSummaryNone}},
		{name: "summary auto", reasoning: llm.ReasoningSpec{Summary: llm.ReasoningSummaryAuto}},
		{name: "effort and summary none", reasoning: llm.ReasoningSpec{Effort: llm.ReasoningEffortHigh, Summary: llm.ReasoningSummaryNone}, wantEffort: "high"},
		{name: "explicit default effort", reasoning: llm.ReasoningSpec{Effort: llm.ReasoningEffortProviderDefault, Summary: llm.ReasoningSummaryNone}},
	} {
		for _, strict := range []bool{true, false} {
			t.Run(test.name+PortabilityName(strict), func(t *testing.T) {
				wire, err := compileReasoning(t, compile, test.reasoning, strict)
				if err != nil {
					t.Fatal(err)
				}
				if thinking, exists := wire["thinking"]; exists {
					t.Fatalf("modeless reasoning emitted thinking = %#v", thinking)
				}
				config, _ := wire["output_config"].(map[string]any)
				if test.wantEffort == "" {
					if effort, exists := config["effort"]; exists {
						t.Fatalf("output_config.effort = %#v, want absent", effort)
					}
					return
				}
				if config["effort"] != test.wantEffort {
					t.Fatalf("output_config = %#v, want effort %q", wire["output_config"], test.wantEffort)
				}
			})
		}
	}
}

// ReasoningSummaryMapsToThinkingDisplay checks how a reasoning summary maps
// to thinking.display, and that strict portability rejects one the family
// cannot express.
func ReasoningSummaryMapsToThinkingDisplay(t *testing.T, compile Compile) {
	for _, test := range []struct {
		name        string
		reasoning   llm.ReasoningSpec
		strict      bool
		wantType    string
		wantDisplay string
		wantError   string
	}{
		{name: "concise without mode best effort", reasoning: llm.ReasoningSpec{Summary: llm.ReasoningSummaryConcise}},
		{name: "detailed without mode best effort", reasoning: llm.ReasoningSpec{Effort: llm.ReasoningEffortHigh, Summary: llm.ReasoningSummaryDetailed}},
		{name: "concise without mode strict", reasoning: llm.ReasoningSpec{Summary: llm.ReasoningSummaryConcise}, strict: true, wantError: "reasoning summary"},
		{name: "detailed adaptive strict", reasoning: llm.ReasoningSpec{Mode: llm.ReasoningModeAdaptive, Summary: llm.ReasoningSummaryDetailed}, strict: true, wantError: "reasoning summary"},
		{name: "concise adaptive best effort", reasoning: llm.ReasoningSpec{Mode: llm.ReasoningModeAdaptive, Summary: llm.ReasoningSummaryConcise}, wantType: "adaptive", wantDisplay: "summarized"},
		{name: "detailed enabled best effort", reasoning: llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, TokenBudget: intPtr(2048), Summary: llm.ReasoningSummaryDetailed}, wantType: "enabled", wantDisplay: "summarized"},
		{name: "auto adaptive strict", reasoning: llm.ReasoningSpec{Mode: llm.ReasoningModeAdaptive, Summary: llm.ReasoningSummaryAuto}, strict: true, wantType: "adaptive", wantDisplay: "summarized"},
		{name: "none adaptive strict", reasoning: llm.ReasoningSpec{Mode: llm.ReasoningModeAdaptive, Summary: llm.ReasoningSummaryNone}, strict: true, wantType: "adaptive", wantDisplay: "omitted"},
		{name: "default adaptive strict", reasoning: llm.ReasoningSpec{Mode: llm.ReasoningModeAdaptive}, strict: true, wantType: "adaptive"},
		{name: "concise disabled best effort", reasoning: llm.ReasoningSpec{Mode: llm.ReasoningModeDisabled, Summary: llm.ReasoningSummaryConcise}, wantType: "disabled"},
		// Disabled wins over an effort, as on Chat and Responses (#1666).
		{name: "disabled with effort strict", reasoning: llm.ReasoningSpec{Mode: llm.ReasoningModeDisabled, Effort: llm.ReasoningEffortHigh}, strict: true, wantType: "disabled"},
		{name: "disabled with maximum effort strict", reasoning: llm.ReasoningSpec{Mode: llm.ReasoningModeDisabled, Effort: llm.ReasoningEffortMaximum}, strict: true, wantType: "disabled"},
	} {
		t.Run(test.name, func(t *testing.T) {
			wire, err := compileReasoning(t, compile, test.reasoning, test.strict)
			if test.wantError != "" {
				var mapped *provider.Error
				// Strict representability rejections are unsupported_capability.
				if err == nil || !strings.Contains(err.Error(), test.wantError) || !errors.As(err, &mapped) || (test.strict && mapped.Code != provider.CodeUnsupportedCapability) {
					t.Fatalf("Compile() = %v, want substring %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if test.wantType == "" {
				if thinking, exists := wire["thinking"]; exists {
					t.Fatalf("modeless summary emitted thinking = %#v", thinking)
				}
				return
			}
			thinking, _ := wire["thinking"].(map[string]any)
			if thinking["type"] != test.wantType {
				t.Fatalf("thinking = %#v, want type %q", wire["thinking"], test.wantType)
			}
			display, exists := thinking["display"]
			if test.wantDisplay == "" && exists || test.wantDisplay != "" && display != test.wantDisplay {
				t.Fatalf("thinking = %#v, want display %q", thinking, test.wantDisplay)
			}
		})
	}
}

// ExtraHighReturnsUnsupportedCapability checks that xhigh effort on the
// contract model is refused before dispatch in every reasoning mode.
func ExtraHighReturnsUnsupportedCapability(t *testing.T, compile Compile) {
	budget := 2048
	for _, mode := range []llm.ReasoningMode{"", llm.ReasoningModeProviderDefault, llm.ReasoningModeAdaptive, llm.ReasoningModeEnabled, llm.ReasoningModeDisabled} {
		for _, strict := range []bool{false, true} {
			t.Run(string(mode)+PortabilityName(strict), func(t *testing.T) {
				_, err := compileReasoning(t, compile, llm.ReasoningSpec{Mode: mode, Effort: llm.ReasoningEffortExtraHigh, TokenBudget: &budget}, strict)
				if !isNonRetryableUnsupported(err, "xhigh") {
					t.Fatalf("Compile() = %v, want non-retryable unsupported capability before dispatch", err)
				}
			})
		}
	}
}

// DisabledReasoningIgnoresEffort checks that disabled wins over an effort:
// thinking is disabled and no output_config.effort is sent, as Chat and
// Responses send effort none (#1666).
func DisabledReasoningIgnoresEffort(t *testing.T, compile Compile) {
	for _, strict := range []bool{true, false} {
		wire, err := compileReasoning(t, compile, llm.ReasoningSpec{Mode: llm.ReasoningModeDisabled, Effort: llm.ReasoningEffortHigh}, strict)
		if err != nil {
			t.Fatal(err)
		}
		if thinking, _ := wire["thinking"].(map[string]any); thinking["type"] != "disabled" {
			t.Fatalf("thinking = %#v, want disabled", wire["thinking"])
		}
		if config, _ := wire["output_config"].(map[string]any); config["effort"] != nil {
			t.Fatalf("output_config = %#v, want no effort", config)
		}
	}
}
