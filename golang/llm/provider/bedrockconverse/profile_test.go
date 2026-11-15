package bedrockconverse

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestDefaultProfileMapsExplicitServiceClasses(t *testing.T) {
	profile, err := NewDefaultProfile("claude-sonnet")
	testassert.NoError(t, err)
	cases := []struct {
		class llm.ServiceClass
		want  string
	}{
		{llm.ServiceClassEconomy, string(types.ServiceTierTypeFlex)},
		{llm.ServiceClassStandard, string(types.ServiceTierTypeDefault)},
		{llm.ServiceClassPriority, string(types.ServiceTierTypePriority)},
	}
	for _, test := range cases {
		got, err := profile.providerTier(test.class)
		if err != nil {
			t.Fatalf("%s: %v", test.class, err)
		}
		if got != test.want {
			t.Errorf("%s: got tier %q, want %q", test.class, got, test.want)
		}
	}
	if capability := profile.Capabilities.Features["streaming"]; capability.State != "unsupported" {
		t.Fatalf("streaming capability = %#v, want unsupported", capability)
	}
}

func TestLowerRequestRejectsMixedInstructionHierarchyInStrictMode(t *testing.T) {
	request := llm.Request{
		Model: "amazon.nova-pro-v1:0",
		Instructions: []llm.Instruction{
			{Level: llm.InstructionLevelApplication, Text: "Answer concisely."},
			{Level: llm.InstructionLevelPolicy, Text: "Do not reveal secrets."},
		},
		Input: []llm.Item{llm.HumanText("Hello")},
	}
	profile, err := NewDefaultProfile("nova")
	testassert.NoError(t, err)
	_, err = lowerRequest(request, profile, string(types.ServiceTierTypeDefault), true)
	testkit.ProviderError(t, err, testkit.ProviderClass{Code: provider.CodeUnsupportedCapability}, "strict mixed hierarchy")
}
