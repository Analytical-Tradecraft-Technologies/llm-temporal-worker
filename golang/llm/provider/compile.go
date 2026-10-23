package provider

import (
	"fmt"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// The helpers in this file are the steps every adapter's Compile shares, so a
// request is normalized, capability-checked and sized the same way whatever
// endpoint family carries it. Each returns a compile-phase *Error as error.

// NormalizeCompileRequest normalizes the request and its service class, and
// checks the model against the capability query and, when pinnedModel is set,
// the profile's pinned model.
func NormalizeCompileRequest(request llm.Request, query CapabilityQuery, pinnedModel string) (llm.Request, llm.ServiceClass, error) {
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		return llm.Request{}, "", NewCompileError(err.Error())
	}
	serviceClass, err := llm.NormalizeServiceClass(normalized.ServiceClass)
	if err != nil {
		return llm.Request{}, "", NewCompileError(err.Error())
	}
	if err := llm.ValidateServiceClassFallbacks(serviceClass, normalized.ServiceClassFallbacks); err != nil {
		return llm.Request{}, "", NewCompileError(err.Error())
	}
	if query.Model != "" && query.Model != normalized.Model {
		return llm.Request{}, "", NewCompileError(fmt.Sprintf("model %q does not match capability query %q", normalized.Model, query.Model))
	}
	if pinnedModel != "" && pinnedModel != normalized.Model {
		return llm.Request{}, "", NewCompileError(fmt.Sprintf("model %q is not the pinned profile model %q", normalized.Model, pinnedModel))
	}
	return normalized, serviceClass, nil
}

// CompileCapabilities returns the capability set the caller supplied, or the
// adapter's own from load when the caller supplied none, with version as the
// fallback version.
func CompileCapabilities(set CapabilitySet, load func() (CapabilitySet, error), version string) (CapabilitySet, error) {
	if set.Version == "" && len(set.Features) == 0 {
		loaded, err := load()
		if err != nil {
			return CapabilitySet{}, NewCompileError(err.Error())
		}
		set = loaded
	}
	if set.Version == "" {
		set.Version = version
	}
	return set, nil
}

// RequireFeatures checks that set supports every feature natively or by
// emulation. extra, when set, adds a family-specific check that runs on each
// feature right after it resolves.
func RequireFeatures(set CapabilitySet, features []Feature, strict bool, extra func(Feature, Capability) error) error {
	for _, feature := range features {
		capability, err := set.Resolve(feature, strict)
		if err != nil {
			return NewUnsupportedFeatureError(feature, err.Error())
		}
		if capability.State != CapabilityNative && capability.State != CapabilityEmulated {
			return NewUnsupportedFeatureError(feature, fmt.Sprintf("capability %q is %s", feature, capability.State))
		}
		if extra != nil {
			if err := extra(feature, capability); err != nil {
				return err
			}
		}
	}
	return nil
}

// CompileMetadata completes the call metadata: the request digest unless the
// caller supplied one, the capability version and provider tier, whether
// opaque continuation state is required, and the canonical size estimate
// unless the caller supplied one.
func CompileMetadata(metadata CallMetadata, normalized llm.Request, capabilityVersion, providerTier string) (CallMetadata, error) {
	if metadata.SchemaDigest == ([32]byte{}) {
		digest, err := llm.RequestDigest(normalized)
		if err != nil {
			return CallMetadata{}, NewCompileError(err.Error())
		}
		metadata.SchemaDigest = digest
	}
	metadata.CapabilityVersion = capabilityVersion
	metadata.ProviderTier = providerTier
	metadata.OpaqueStateRequired = normalized.Continuation != nil
	if metadata.EstimatedBytes == 0 {
		canonical, err := CanonicalRequestBytes(normalized)
		if err != nil {
			return CallMetadata{}, NewCompileError(err.Error())
		}
		metadata.EstimatedBytes = len(canonical)
	}
	return metadata, nil
}

// Clone returns a copy of set whose feature map can be changed without
// touching the original.
func (set CapabilitySet) Clone() CapabilitySet {
	features := make(map[Feature]Capability, len(set.Features))
	for feature, capability := range set.Features {
		features[feature] = capability
	}
	set.Features = features
	return set
}
