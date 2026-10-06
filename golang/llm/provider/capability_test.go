package provider_test

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestCapabilityResolution(t *testing.T) {
	set := provider.CapabilitySet{
		Version: "cap-v1",
		Features: map[provider.Feature]provider.Capability{
			provider.FeatureText:      {State: provider.CapabilityNative},
			provider.FeatureImage:     {State: provider.CapabilityEmulated, Transform: "image-url-to-data"},
			provider.FeatureToolCall:  {State: provider.CapabilityUnsupported, Reason: "not enabled"},
			provider.FeatureReasoning: {State: provider.CapabilityUnknown},
		},
	}
	for _, test := range []struct {
		feature provider.Feature
		strict  bool
		state   provider.CapabilityState
		err     bool
	}{
		{provider.FeatureText, true, provider.CapabilityNative, false},
		{provider.FeatureImage, true, provider.CapabilityEmulated, false},
		{provider.FeatureToolCall, true, provider.CapabilityUnsupported, true},
		{provider.FeatureReasoning, true, provider.CapabilityUnknown, true},
		{provider.FeatureReasoning, false, provider.CapabilityUnknown, false},
		{provider.FeatureStructuredOutput, false, provider.CapabilityUnknown, false},
	} {
		got, err := set.Resolve(test.feature, test.strict)
		if got.State != test.state || (err != nil) != test.err {
			t.Errorf("Resolve(%q, strict=%t) = %#v, %v; want %q, error=%t", test.feature, test.strict, got, err, test.state, test.err)
		}
	}
	if set.Version != "cap-v1" {
		t.Fatal("capability version was not retained")
	}
	if set.Supports(provider.FeatureText, true) == false || set.Supports(provider.FeatureReasoning, false) {
		t.Fatal("capability support predicate is incorrect")
	}
}

func TestCapabilityStatesAreClosed(t *testing.T) {
	if provider.CapabilityState("other").Valid() {
		t.Fatal("unknown capability state was accepted")
	}
}
