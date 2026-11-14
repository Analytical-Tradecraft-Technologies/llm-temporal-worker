package provider_test

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func completeFeatures() map[provider.Feature]provider.Capability {
	features := map[provider.Feature]provider.Capability{}
	for _, feature := range provider.AllFeatures() {
		features[feature] = provider.Capability{State: provider.CapabilityNative}
	}
	features[provider.FeatureStreaming] = provider.Capability{State: provider.CapabilityUnsupported}
	return features
}

func TestValidateProfileFeaturesNamesTheFamily(t *testing.T) {
	if err := provider.ValidateProfileFeatures("test family", "p", completeFeatures()); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		edit func(map[provider.Feature]provider.Capability)
		want string
	}{
		{"empty feature", func(f map[provider.Feature]provider.Capability) {
			f[""] = provider.Capability{State: provider.CapabilityNative}
		}, `test family profile "p" contains an empty capability feature`},
		{"invalid state", func(f map[provider.Feature]provider.Capability) {
			f[provider.FeatureText] = provider.Capability{State: "maybe"}
		}, `test family profile "p" capability "text" has invalid state "maybe"`},
		{"missing feature", func(f map[provider.Feature]provider.Capability) { delete(f, provider.FeatureUsage) }, `test family profile "p" must explicitly declare capability "usage"`},
		{"streaming", func(f map[provider.Feature]provider.Capability) {
			f[provider.FeatureStreaming] = provider.Capability{State: provider.CapabilityEmulated}
		}, `test family profile "p" cannot advertise streaming as "emulated": adapter does not implement OpenStream`},
	} {
		features := completeFeatures()
		test.edit(features)
		if err := provider.ValidateProfileFeatures("test family", "p", features); err == nil || err.Error() != test.want {
			t.Fatalf("%s: error = %v, want %q", test.name, err, test.want)
		}
	}
}

func TestCloneServiceMapsAreIndependentAndNeverNil(t *testing.T) {
	tiers := map[llm.ServiceClass]string{llm.ServiceClassStandard: "default"}
	tierCopy := provider.CloneServiceTiers(tiers)
	tierCopy[llm.ServiceClassStandard] = "changed"
	if tiers[llm.ServiceClassStandard] != "default" {
		t.Fatal("CloneServiceTiers shares its map")
	}
	classes := map[string]llm.ServiceClass{"default": llm.ServiceClassStandard}
	classCopy := provider.CloneActualServiceClasses(classes)
	delete(classCopy, "default")
	if len(classes) != 1 {
		t.Fatal("CloneActualServiceClasses shares its map")
	}
	if provider.CloneServiceTiers(nil) == nil || provider.CloneActualServiceClasses(nil) == nil {
		t.Fatal("a clone of nil is nil")
	}
}
