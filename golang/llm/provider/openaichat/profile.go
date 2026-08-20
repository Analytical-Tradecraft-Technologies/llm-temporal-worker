package openaichat

import (
	"context"
	"encoding/json"
	"fmt"

	openai "github.com/openai/openai-go/v3"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
)

const (
	adapterName              = "openai.chat"
	defaultCapabilityVersion = "openai-chat/v1"
)

// ExtensionSpec describes the fields an endpoint profile permits in one
// namespaced extension. The map key is the semantic extension field and the
// value is the provider wire field. A blank wire name preserves the key.
//
// Extension values are still validated as JSON objects by the compiler; this
// type is deliberately only a field allow-list, not an arbitrary passthrough.
type ExtensionSpec struct {
	Fields map[string]string
}

// Profile is the immutable, endpoint-specific contract for a Chat
// Completions-compatible endpoint. Compatible endpoints must provide their
// own profile; the compiler never infers behavior from a hostname.
//
// ServiceTiers contains all three public classes. A present class with an
// empty provider value is explicitly unsupported. Omitting a class is an
// invalid profile, which prevents a provider default from becoming a public
// service class by accident.
type Profile struct {
	ID                        string
	CapabilityVersion         string
	Capabilities              provider.CapabilitySet
	ServiceTiers              map[llm.ServiceClass]string
	ActualServiceClasses      map[string]llm.ServiceClass
	MissingActualServiceClass llm.ServiceClass
	AllowedExtensions         map[string]ExtensionSpec
	// ExpectedBaseURL pins a profile to one exact normalized client endpoint.
	// Compatible behavior is never inferred from the URL; this is an identity
	// check that prevents accidentally attaching a profile to another service.
	ExpectedBaseURL string
	// ExpectedModel optionally pins a profile to one exact model/deployment.
	ExpectedModel string
	// WireDefaults are trusted, profile-owned root fields for compatible APIs.
	// Unknown fields are preserved on the official SDK parameter object instead
	// of being silently discarded during JSON union conversion.
	WireDefaults map[string]json.RawMessage
	// ReservedWireFields cannot be overridden by caller extensions. Profile
	// defaults are automatically reserved as well.
	ReservedWireFields map[string]struct{}
	// ResponseAugment may add profile-specific facts (for example citations or
	// provider-reported cost) after the common Chat response has been lifted.
	ResponseAugment           func(provider.Call, *openai.ChatCompletion, *llm.Response) error
	StructuredOutputTransform string
	directOpenAI              bool
}

// NewOpenAIProfile verifies and marks a profile for the direct OpenAI API.
// Generic and compatible profiles intentionally cannot opt into direct
// management APIs merely because their base URL happens to look OpenAI-like.
func NewOpenAIProfile(profile Profile) (Profile, error) {
	validated, err := NewProfile(profile)
	if err != nil {
		return Profile{}, err
	}
	validated.directOpenAI = true
	return validated, nil
}

// NewProfile validates and defensively copies a profile. The returned value is
// safe for concurrent use by an adapter and cannot be affected by mutations to
// the caller's maps.
func NewProfile(profile Profile) (Profile, error) {
	if err := profile.validate(); err != nil {
		return Profile{}, err
	}
	copy := profile
	copy.Capabilities = cloneCapabilities(profile.Capabilities)
	copy.Capabilities.Features[provider.FeatureContinuation] = unsupportedContinuationCapability(copy.Capabilities.Features[provider.FeatureContinuation])
	copy.Capabilities.Features[provider.FeatureStreaming] = unsupportedStreamingCapability(copy.Capabilities.Features[provider.FeatureStreaming])
	copy.ServiceTiers = cloneServiceTiers(profile.ServiceTiers)
	copy.ActualServiceClasses = cloneActualClasses(profile.ActualServiceClasses)
	copy.AllowedExtensions = cloneExtensions(profile.AllowedExtensions)
	copy.WireDefaults = cloneRawMessages(profile.WireDefaults)
	copy.ReservedWireFields = cloneStringSet(profile.ReservedWireFields)
	if copy.ExpectedBaseURL != "" {
		copy.ExpectedBaseURL, _ = clientconfig.BaseURL(copy.ExpectedBaseURL)
	}
	for wire := range copy.WireDefaults {
		copy.ReservedWireFields[wire] = struct{}{}
	}
	return copy, nil
}

func (profile Profile) validate() error {
	if profile.ID == "" {
		return fmt.Errorf("openai chat profile ID is required")
	}
	if profile.ExpectedBaseURL != "" {
		if _, err := clientconfig.BaseURL(profile.ExpectedBaseURL); err != nil {
			return fmt.Errorf("openai chat profile %q expected base URL: %w", profile.ID, err)
		}
	}
	if profile.ExpectedModel != "" && len(profile.ExpectedModel) > 256 {
		return fmt.Errorf("openai chat profile %q expected model is too long", profile.ID)
	}
	version := profile.CapabilityVersion
	if version == "" {
		version = profile.Capabilities.Version
	}
	if version == "" {
		return fmt.Errorf("openai chat profile %q capability version is required", profile.ID)
	}
	if profile.Capabilities.Version != "" && profile.Capabilities.Version != version {
		return fmt.Errorf("openai chat profile %q capability versions conflict", profile.ID)
	}
	for _, class := range publicServiceClasses() {
		value, ok := profile.ServiceTiers[class]
		if !ok {
			return fmt.Errorf("openai chat profile %q must declare service class %q", profile.ID, class)
		}
		if value != "" && !validProviderTier(value) {
			return fmt.Errorf("openai chat profile %q service class %q has invalid provider tier %q", profile.ID, class, value)
		}
	}
	for feature, capability := range profile.Capabilities.Features {
		if feature == "" {
			return fmt.Errorf("openai chat profile %q contains an empty capability feature", profile.ID)
		}
		if !capability.State.Valid() {
			return fmt.Errorf("openai chat profile %q capability %q has invalid state %q", profile.ID, feature, capability.State)
		}
	}
	for _, feature := range allFeatures() {
		if _, ok := profile.Capabilities.Features[feature]; !ok {
			return fmt.Errorf("openai chat profile %q must explicitly declare capability %q", profile.ID, feature)
		}
	}
	if streaming := profile.Capabilities.Features[provider.FeatureStreaming]; streaming.State == provider.CapabilityNative || streaming.State == provider.CapabilityEmulated {
		return fmt.Errorf("openai chat profile %q cannot advertise streaming as %q: adapter does not implement OpenStream", profile.ID, streaming.State)
	}
	if continuation := profile.Capabilities.Features[provider.FeatureContinuation]; continuation.State == provider.CapabilityNative || continuation.State == provider.CapabilityEmulated {
		return fmt.Errorf("openai chat profile %q cannot advertise continuation as %q: adapter does not implement continuation", profile.ID, continuation.State)
	}
	if profile.MissingActualServiceClass != "" && !profile.MissingActualServiceClass.Valid() {
		return fmt.Errorf("openai chat profile %q missing actual service class %q is invalid", profile.ID, profile.MissingActualServiceClass)
	}
	for providerTier, class := range profile.ActualServiceClasses {
		if !validProviderTier(providerTier) {
			return fmt.Errorf("openai chat profile %q actual provider tier %q is invalid", profile.ID, providerTier)
		}
		if !class.Valid() {
			return fmt.Errorf("openai chat profile %q actual provider tier %q maps to invalid class %q", profile.ID, providerTier, class)
		}
	}
	for namespace, spec := range profile.AllowedExtensions {
		if namespace == "" {
			return fmt.Errorf("openai chat profile %q contains an empty extension namespace", profile.ID)
		}
		for field, wire := range spec.Fields {
			if field == "" {
				return fmt.Errorf("openai chat profile %q extension %q contains an empty field", profile.ID, namespace)
			}
			if wire == "" {
				continue
			}
			if wire == "model" || wire == "messages" || wire == "service_tier" {
				return fmt.Errorf("openai chat profile %q extension %q cannot override %q", profile.ID, namespace, wire)
			}
		}
	}
	for wire, raw := range profile.WireDefaults {
		if wire == "" {
			return fmt.Errorf("openai chat profile %q contains an empty wire default", profile.ID)
		}
		if wire == "model" || wire == "messages" || wire == "service_tier" {
			return fmt.Errorf("openai chat profile %q cannot default %q", profile.ID, wire)
		}
		if !json.Valid(raw) {
			return fmt.Errorf("openai chat profile %q wire default %q is invalid JSON", profile.ID, wire)
		}
	}
	for wire := range profile.ReservedWireFields {
		if wire == "" {
			return fmt.Errorf("openai chat profile %q contains an empty reserved wire field", profile.ID)
		}
		if wire == "model" || wire == "messages" || wire == "service_tier" {
			return fmt.Errorf("openai chat profile %q cannot reserve %q", profile.ID, wire)
		}
	}
	return nil
}

func (profile Profile) capabilityVersion() string {
	if profile.CapabilityVersion != "" {
		return profile.CapabilityVersion
	}
	return profile.Capabilities.Version
}

func (profile Profile) capabilities(ctx context.Context, query provider.CapabilityQuery, endpointID string) (provider.CapabilitySet, error) {
	if err := ctx.Err(); err != nil {
		return provider.CapabilitySet{}, err
	}
	if query.Family != "" && query.Family != provider.FamilyOpenAIChat {
		return provider.CapabilitySet{}, fmt.Errorf("openai chat profile %q: capability family %q does not match %q", profile.ID, query.Family, provider.FamilyOpenAIChat)
	}
	if query.EndpointID != "" && query.EndpointID != endpointID {
		return provider.CapabilitySet{}, fmt.Errorf("openai chat profile %q: capability endpoint %q does not match %q", profile.ID, query.EndpointID, endpointID)
	}
	set := cloneCapabilities(profile.Capabilities)
	set.Version = profile.capabilityVersion()
	set.Features[provider.FeatureContinuation] = unsupportedContinuationCapability(set.Features[provider.FeatureContinuation])
	set.Features[provider.FeatureStreaming] = unsupportedStreamingCapability(set.Features[provider.FeatureStreaming])
	return set, nil
}

func (profile Profile) providerTier(class llm.ServiceClass) (string, error) {
	value, ok := profile.ServiceTiers[class]
	if !ok {
		return "", fmt.Errorf("service class %q is not declared by profile %q", class, profile.ID)
	}
	if value == "" {
		return "", fmt.Errorf("service class %q is unsupported by profile %q", class, profile.ID)
	}
	return value, nil
}

func (profile Profile) actualClass(providerTier string) (*llm.ServiceClass, error) {
	if providerTier == "" {
		if profile.MissingActualServiceClass != "" {
			class := profile.MissingActualServiceClass
			return &class, nil
		}
		return nil, fmt.Errorf("provider response omitted service tier")
	}
	class, ok := profile.ActualServiceClasses[providerTier]
	if !ok {
		return nil, fmt.Errorf("provider returned unsupported service tier %q", providerTier)
	}
	return &class, nil
}

func publicServiceClasses() []llm.ServiceClass {
	return []llm.ServiceClass{llm.ServiceClassEconomy, llm.ServiceClassStandard, llm.ServiceClassPriority}
}

func unsupportedStreamingCapability(capability provider.Capability) provider.Capability {
	capability.State = provider.CapabilityUnsupported
	capability.Transform = ""
	if capability.Reason == "" {
		capability.Reason = "adapter does not implement OpenStream"
	}
	return capability
}

func unsupportedContinuationCapability(capability provider.Capability) provider.Capability {
	capability.State = provider.CapabilityUnsupported
	capability.Transform = ""
	if capability.Reason == "" {
		capability.Reason = "adapter does not implement continuation"
	}
	return capability
}

func allFeatures() []provider.Feature {
	return []provider.Feature{
		provider.FeatureText,
		provider.FeatureImage,
		provider.FeatureDocument,
		provider.FeatureToolCall,
		provider.FeatureStructuredOutput,
		provider.FeatureReasoning,
		provider.FeatureContinuation,
		provider.FeatureStreaming,
		provider.FeatureUsage,
	}
}

func validProviderTier(value string) bool {
	return value != "" && len(value) <= 128
}

func cloneCapabilities(set provider.CapabilitySet) provider.CapabilitySet {
	features := set.Features
	set.Features = make(map[provider.Feature]provider.Capability, len(set.Features))
	for feature, capability := range features {
		set.Features[feature] = capability
	}
	return set
}

func cloneServiceTiers(values map[llm.ServiceClass]string) map[llm.ServiceClass]string {
	copy := make(map[llm.ServiceClass]string, len(values))
	for class, value := range values {
		copy[class] = value
	}
	return copy
}

func cloneActualClasses(values map[string]llm.ServiceClass) map[string]llm.ServiceClass {
	copy := make(map[string]llm.ServiceClass, len(values))
	for tier, class := range values {
		copy[tier] = class
	}
	return copy
}

func cloneExtensions(values map[string]ExtensionSpec) map[string]ExtensionSpec {
	copy := make(map[string]ExtensionSpec, len(values))
	for namespace, spec := range values {
		fields := make(map[string]string, len(spec.Fields))
		for field, wire := range spec.Fields {
			fields[field] = wire
		}
		copy[namespace] = ExtensionSpec{Fields: fields}
	}
	return copy
}

func cloneRawMessages(values map[string]json.RawMessage) map[string]json.RawMessage {
	copy := make(map[string]json.RawMessage, len(values))
	for key, value := range values {
		copy[key] = append(json.RawMessage(nil), value...)
	}
	return copy
}

func cloneStringSet(values map[string]struct{}) map[string]struct{} {
	copy := make(map[string]struct{}, len(values))
	for key := range values {
		copy[key] = struct{}{}
	}
	return copy
}

func extensionObject(raw json.RawMessage) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	if fields == nil {
		return nil, fmt.Errorf("extension must be a JSON object")
	}
	return fields, nil
}
