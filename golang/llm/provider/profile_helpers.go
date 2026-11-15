package provider

import (
	"context"
	"fmt"
	"maps"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
)

// ValidateProfileIdentity checks the fields every endpoint profile validates
// the same way: a non-empty ID, an expected base URL clientconfig accepts, an
// expected model of at most 256 bytes, and one capability version declared by
// the profile or its capability set. family names the profile in errors, as
// in "anthropic messages".
func ValidateProfileIdentity(family, id, expectedBaseURL, expectedModel, capabilityVersion string, capabilities CapabilitySet) error {
	if id == "" {
		return fmt.Errorf("%s profile ID is required", family)
	}
	if expectedBaseURL != "" {
		if _, err := clientconfig.BaseURL(expectedBaseURL); err != nil {
			return fmt.Errorf("%s profile %q expected base URL: %w", family, id, err)
		}
	}
	if len(expectedModel) > 256 {
		return fmt.Errorf("%s profile %q expected model is too long", family, id)
	}
	version := ProfileCapabilityVersion(capabilityVersion, capabilities)
	if version == "" {
		return fmt.Errorf("%s profile %q capability version is required", family, id)
	}
	if capabilities.Version != "" && capabilities.Version != version {
		return fmt.Errorf("%s profile %q capability versions conflict", family, id)
	}
	return nil
}

// ProfileCapabilityVersion returns the capability version a profile declares,
// or else its capability set's.
func ProfileCapabilityVersion(declared string, capabilities CapabilitySet) string {
	if declared != "" {
		return declared
	}
	return capabilities.Version
}

// OneShotProfileCapabilities answers a capability query from the profile of
// an adapter that does not implement OpenStream. It checks the context and
// that the query names this family and endpoint, then returns a copy of
// capabilities carrying version, with streaming unsupported. label prefixes a
// query mismatch.
func OneShotProfileCapabilities(ctx context.Context, query CapabilityQuery, label string, family Family, endpointID string, capabilities CapabilitySet, version string) (CapabilitySet, error) {
	if err := ctx.Err(); err != nil {
		return CapabilitySet{}, err
	}
	if err := ValidateCapabilityQuery(query, label, family, endpointID); err != nil {
		return CapabilitySet{}, err
	}
	set := capabilities.Clone()
	set.Version = version
	set.Features[FeatureStreaming] = UnsupportedStreaming(set.Features[FeatureStreaming])
	return set, nil
}

// ValidateActualServiceClasses checks a profile's response-tier mapping: the
// class reported for a response without a tier, then every tier, by
// validTier, and the class it maps to.
func ValidateActualServiceClasses(family, id string, classes map[string]llm.ServiceClass, missing llm.ServiceClass, validTier func(string) bool) error {
	if missing != "" && !missing.Valid() {
		return fmt.Errorf("%s profile %q missing actual service class %q is invalid", family, id, missing)
	}
	for tier, class := range classes {
		if !validTier(tier) {
			return fmt.Errorf("%s profile %q actual provider tier %q is invalid", family, id, tier)
		}
		if !class.Valid() {
			return fmt.Errorf("%s profile %q actual provider tier %q maps to invalid class %q", family, id, tier, class)
		}
	}
	return nil
}

// ActualServiceClass maps the tier a response reported to a public class,
// using missing for a response without a tier. An unrecognized tier is not
// evidence of any class, so it yields nil rather than a guess or a failure:
// the response is already paid for, and the raw label is kept in the service
// facts for audit.
func ActualServiceClass(classes map[string]llm.ServiceClass, missing llm.ServiceClass, tier string) *llm.ServiceClass {
	class, ok := classes[tier]
	if tier == "" {
		class, ok = missing, missing != ""
	}
	if !ok {
		return nil
	}
	return &class
}

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
	maps.Copy(copy, values)
	return copy
}

// CloneActualServiceClasses copies a profile's provider-tier to
// service-class map. The copy is never nil.
func CloneActualServiceClasses(values map[string]llm.ServiceClass) map[string]llm.ServiceClass {
	copy := make(map[string]llm.ServiceClass, len(values))
	maps.Copy(copy, values)
	return copy
}
