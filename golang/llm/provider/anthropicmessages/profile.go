package anthropicmessages

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
)

const (
	adapterName              = "anthropic.messages"
	defaultCapabilityVersion = "anthropic-messages/v1"
	defaultMaxTokens         = int64(1024)
)

// ExtensionSpec describes the fields an Anthropic Messages profile permits in
// one namespaced extension. The map key is the semantic extension field and
// the value is the provider wire field. A blank wire name preserves the key.
// Extensions are intentionally profile-owned rather than arbitrary JSON
// passthroughs.
type ExtensionSpec struct {
	Fields map[string]string
}

// Profile is the immutable, endpoint-specific contract for direct Anthropic
// Messages. Compatible gateways and AWS paths use their own profiles rather
// than inferring behavior from a hostname.
//
// ServiceTiers must declare all three public classes. An empty provider value
// is an explicit unsupported class; there is no public provider-default class.
type Profile struct {
	ID                        string
	CapabilityVersion         string
	Capabilities              provider.CapabilitySet
	ServiceTiers              map[llm.ServiceClass]string
	ActualServiceClasses      map[string]llm.ServiceClass
	MissingActualServiceClass llm.ServiceClass
	AllowedExtensions         map[string]ExtensionSpec
	ExpectedBaseURL           string
	ExpectedModel             string
	// PriorityCapacity records that this endpoint/profile is allowed to use
	// Anthropic's priority capacity. A priority request lowers to service_tier
	// "auto" only when this is true.
	PriorityCapacity bool
	// DefaultMaxTokens supplies Anthropic's required max_tokens field when the
	// semantic request did not provide an output limit.
	DefaultMaxTokens int64
}

// NewProfile validates and defensively copies a profile. The result is safe
// for concurrent use by an adapter and is unaffected by caller map mutation.
func NewProfile(profile Profile) (Profile, error) {
	if err := profile.validate(); err != nil {
		return Profile{}, err
	}
	copy := profile
	copy.Capabilities = cloneCapabilities(profile.Capabilities)
	copy.Capabilities.Features[provider.FeatureStreaming] = unsupportedStreamingCapability(copy.Capabilities.Features[provider.FeatureStreaming])
	copy.ServiceTiers = cloneServiceTiers(profile.ServiceTiers)
	copy.ActualServiceClasses = cloneActualClasses(profile.ActualServiceClasses)
	copy.AllowedExtensions = cloneExtensions(profile.AllowedExtensions)
	if copy.ExpectedBaseURL != "" {
		// validate() already accepted this value; retaining the normalized value
		// makes the adapter identity comparison exact.
		copy.ExpectedBaseURL, _ = clientconfig.BaseURL(copy.ExpectedBaseURL)
	}
	if copy.DefaultMaxTokens == 0 {
		copy.DefaultMaxTokens = defaultMaxTokens
	}
	return copy, nil
}

// DefaultProfile returns the direct Anthropic contract used by examples and
// small deployments. Production route registries should still pin an endpoint
// URL/model and may replace any capability with a verified narrower value.
func DefaultProfile(id string) Profile {
	features := make(map[provider.Feature]provider.Capability, len(allFeatures()))
	for _, feature := range allFeatures() {
		features[feature] = provider.Capability{State: provider.CapabilityNative}
	}
	features[provider.FeatureStreaming] = unsupportedStreamingCapability(features[provider.FeatureStreaming])
	return Profile{
		ID:                id,
		CapabilityVersion: defaultCapabilityVersion,
		Capabilities:      provider.CapabilitySet{Version: defaultCapabilityVersion, Features: features},
		ServiceTiers: map[llm.ServiceClass]string{
			llm.ServiceClassEconomy:  "",
			llm.ServiceClassStandard: "standard_only",
			llm.ServiceClassPriority: "auto",
		},
		ActualServiceClasses: map[string]llm.ServiceClass{
			"standard": llm.ServiceClassStandard,
			"priority": llm.ServiceClassPriority,
			"batch":    llm.ServiceClassEconomy,
		},
		PriorityCapacity: true,
	}
}

// NewDefaultProfile validates and returns DefaultProfile(id).
func NewDefaultProfile(id string) (Profile, error) { return NewProfile(DefaultProfile(id)) }

func (profile Profile) validate() error {
	if profile.ID == "" {
		return fmt.Errorf("anthropic messages profile ID is required")
	}
	if profile.ExpectedBaseURL != "" {
		if _, err := clientconfig.BaseURL(profile.ExpectedBaseURL); err != nil {
			return fmt.Errorf("anthropic messages profile %q expected base URL: %w", profile.ID, err)
		}
	}
	if profile.ExpectedModel != "" && len(profile.ExpectedModel) > 256 {
		return fmt.Errorf("anthropic messages profile %q expected model is too long", profile.ID)
	}
	version := profile.CapabilityVersion
	if version == "" {
		version = profile.Capabilities.Version
	}
	if version == "" {
		return fmt.Errorf("anthropic messages profile %q capability version is required", profile.ID)
	}
	if profile.Capabilities.Version != "" && profile.Capabilities.Version != version {
		return fmt.Errorf("anthropic messages profile %q capability versions conflict", profile.ID)
	}
	for _, class := range publicServiceClasses() {
		value, ok := profile.ServiceTiers[class]
		if !ok {
			return fmt.Errorf("anthropic messages profile %q must declare service class %q", profile.ID, class)
		}
		if value != "" && !validProviderTier(value) {
			return fmt.Errorf("anthropic messages profile %q service class %q has invalid provider tier %q", profile.ID, class, value)
		}
	}
	for feature, capability := range profile.Capabilities.Features {
		if feature == "" {
			return fmt.Errorf("anthropic messages profile %q contains an empty capability feature", profile.ID)
		}
		if !capability.State.Valid() {
			return fmt.Errorf("anthropic messages profile %q capability %q has invalid state %q", profile.ID, feature, capability.State)
		}
	}
	for _, feature := range allFeatures() {
		if _, ok := profile.Capabilities.Features[feature]; !ok {
			return fmt.Errorf("anthropic messages profile %q must explicitly declare capability %q", profile.ID, feature)
		}
	}
	if streaming := profile.Capabilities.Features[provider.FeatureStreaming]; streaming.State == provider.CapabilityNative || streaming.State == provider.CapabilityEmulated {
		return fmt.Errorf("anthropic messages profile %q cannot advertise streaming as %q: adapter does not implement OpenStream", profile.ID, streaming.State)
	}
	if profile.MissingActualServiceClass != "" && !profile.MissingActualServiceClass.Valid() {
		return fmt.Errorf("anthropic messages profile %q missing actual service class %q is invalid", profile.ID, profile.MissingActualServiceClass)
	}
	for providerTier, class := range profile.ActualServiceClasses {
		if !validProviderTier(providerTier) {
			return fmt.Errorf("anthropic messages profile %q actual provider tier %q is invalid", profile.ID, providerTier)
		}
		if !class.Valid() {
			return fmt.Errorf("anthropic messages profile %q actual provider tier %q maps to invalid class %q", profile.ID, providerTier, class)
		}
	}
	for namespace, spec := range profile.AllowedExtensions {
		if namespace == "" {
			return fmt.Errorf("anthropic messages profile %q contains an empty extension namespace", profile.ID)
		}
		for field, wire := range spec.Fields {
			if field == "" {
				return fmt.Errorf("anthropic messages profile %q extension %q contains an empty field", profile.ID, namespace)
			}
			if wire == "model" || wire == "messages" || wire == "service_tier" {
				return fmt.Errorf("anthropic messages profile %q extension %q cannot override %q", profile.ID, namespace, wire)
			}
		}
	}
	if profile.DefaultMaxTokens < 0 {
		return fmt.Errorf("anthropic messages profile %q default max_tokens must not be negative", profile.ID)
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
	if query.Family != "" && query.Family != provider.FamilyAnthropicMessages {
		return provider.CapabilitySet{}, fmt.Errorf("anthropic messages profile %q: capability family %q does not match %q", profile.ID, query.Family, provider.FamilyAnthropicMessages)
	}
	if query.EndpointID != "" && query.EndpointID != endpointID {
		return provider.CapabilitySet{}, fmt.Errorf("anthropic messages profile %q: capability endpoint %q does not match %q", profile.ID, query.EndpointID, endpointID)
	}
	set := cloneCapabilities(profile.Capabilities)
	set.Version = profile.capabilityVersion()
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
	if class == llm.ServiceClassPriority && value == "auto" && !profile.PriorityCapacity {
		return "", fmt.Errorf("priority service class requires priority_capacity for profile %q", profile.ID)
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

func validProviderTier(value string) bool { return value != "" && len(value) <= 128 }

func cloneCapabilities(set provider.CapabilitySet) provider.CapabilitySet {
	features := set.Features
	set.Features = make(map[provider.Feature]provider.Capability, len(features))
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

func cloneRawMap(values map[string]json.RawMessage) map[string]json.RawMessage {
	copy := make(map[string]json.RawMessage, len(values))
	for key, value := range values {
		copy[key] = append(json.RawMessage(nil), value...)
	}
	return copy
}
