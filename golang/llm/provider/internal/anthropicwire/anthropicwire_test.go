package anthropicwire

import (
	"encoding/json"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestFamilyNamesItsEndpointInErrors(t *testing.T) {
	small := 512
	for _, test := range []struct {
		family Family
		err    func(Family) error
		want   string
	}{
		{Anthropic, func(f Family) error { return f.LowerSampling(llm.SamplingSpec{Seed: new(int64)}, map[string]any{}) }, "sampling field is not supported by Anthropic Messages"},
		{Bedrock, func(f Family) error { return f.LowerSampling(llm.SamplingSpec{Seed: new(int64)}, map[string]any{}) }, "sampling field is not supported by Bedrock Messages"},
		{Bedrock, func(f Family) error {
			_, err := f.LowerReasoning(llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled}, false)
			return err
		}, "enabled Bedrock thinking requires token_budget"},
		{Anthropic, func(f Family) error {
			_, err := f.LowerReasoning(llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, TokenBudget: &small}, false)
			return err
		}, "Anthropic thinking token_budget must be at least 1024"},
		{Bedrock, func(f Family) error {
			_, err := f.LowerReasoning(llm.ReasoningSpec{Effort: llm.ReasoningEffortExtraHigh}, false)
			return err
		}, "reasoning: xhigh reasoning effort is not supported by bedrockmessages"},
		{Bedrock, func(f Family) error {
			_, err := f.LowerPart(llm.RefusalPart{Text: "no"}, PartLowering{})
			return err
		}, "refusal part is not accepted as Bedrock input"},
	} {
		if err := test.err(test.family); err == nil || err.Error() != test.want {
			t.Errorf("%s: error = %v, want %q", test.family.Name, err, test.want)
		}
	}
}

func TestLowerPartUsesFamilyHooks(t *testing.T) {
	lowering := PartLowering{
		Image:    func(llm.ImagePart) (map[string]any, error) { return map[string]any{"type": "image"}, nil },
		Document: func(llm.DocumentPart) (map[string]any, error) { return map[string]any{"type": "document"}, nil },
		ProviderState: func(llm.ProviderStatePart) (json.RawMessage, error) {
			return json.RawMessage(`{"type":"thinking"}`), nil
		},
	}
	content, err := Anthropic.LowerParts([]llm.Part{llm.TextPart{Text: "hi"}, llm.ImagePart{}, llm.DocumentPart{}, llm.ProviderStatePart{}}, lowering)
	if err != nil || len(content) != 4 {
		t.Fatalf("LowerParts = %v, %v", content, err)
	}
	if content[1].(map[string]any)["type"] != "image" || content[2].(map[string]any)["type"] != "document" || string(content[3].(json.RawMessage)) != `{"type":"thinking"}` {
		t.Fatalf("hooks not used: %#v", content)
	}
	if _, err := Anthropic.LowerParts([]llm.Part{llm.TextPart{}, llm.JSONPart{Value: json.RawMessage(`{`)}}, lowering); err == nil || err.Error() != "part 1: JSON part is invalid" {
		t.Fatalf("invalid JSON part error = %v", err)
	}
}

func TestRequiredFeaturesCountsProviderState(t *testing.T) {
	got := RequiredFeatures(llm.Request{Input: []llm.Item{llm.ProviderState{}}})
	want := []provider.Feature{provider.FeatureText, provider.FeatureUsage, provider.FeatureContinuation, provider.FeatureReasoning}
	if len(got) != len(want) {
		t.Fatalf("RequiredFeatures = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("RequiredFeatures = %v, want %v", got, want)
		}
	}
}
