package provider

import (
	"fmt"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// ValidateProfileFeatures checks the capabilities an endpoint profile
// declares: each has a name and a valid state, every governed feature is
// declared, and streaming is not advertised, because no adapter that calls
// this implements OpenStream. family names the profile in errors, as in
// "anthropic messages".
func ValidateProfileFeatures(family, id string, features map[Feature]Capability) error {
	for feature, capability := range features {
		if feature == "" {
			return fmt.Errorf("%s profile %q contains an empty capability feature", family, id)
		}
		if !capability.State.Valid() {
			return fmt.Errorf("%s profile %q capability %q has invalid state %q", family, id, feature, capability.State)
		}
	}
	for _, feature := range AllFeatures() {
		if _, ok := features[feature]; !ok {
			return fmt.Errorf("%s profile %q must explicitly declare capability %q", family, id, feature)
		}
	}
	if streaming := features[FeatureStreaming]; streaming.State == CapabilityNative || streaming.State == CapabilityEmulated {
		return fmt.Errorf("%s profile %q cannot advertise streaming as %q: adapter does not implement OpenStream", family, id, streaming.State)
	}
	return nil
}

// CloneServiceTiers copies a profile's service-class to provider-tier map.
// The copy is never nil.
func CloneServiceTiers(values map[llm.ServiceClass]string) map[llm.ServiceClass]string {
	copy := make(map[llm.ServiceClass]string, len(values))
	for class, value := range values {
		copy[class] = value
	}
	return copy
}

// CloneActualServiceClasses copies a profile's provider-tier to
// service-class map. The copy is never nil.
func CloneActualServiceClasses(values map[string]llm.ServiceClass) map[string]llm.ServiceClass {
	copy := make(map[string]llm.ServiceClass, len(values))
	for tier, class := range values {
		copy[tier] = class
	}
	return copy
}
