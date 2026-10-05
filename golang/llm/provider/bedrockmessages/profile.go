package bedrockmessages

import (
	"context"
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
)

const (
	adapterName              = "bedrock.messages"
	defaultCapabilityVersion = "bedrock-anthropic/v1"
	defaultMaxTokens         = int64(1024)
)

// Profile is an immutable Bedrock Anthropic Messages contract. Bedrock tier
// names are kept here, at the provider boundary; callers only see economy,
// standard, and priority.
type Profile struct {
	ID                        string
	CapabilityVersion         string
	Capabilities              provider.CapabilitySet
	ServiceTiers              map[llm.ServiceClass]string
	ActualServiceClasses      map[string]llm.ServiceClass
	MissingActualServiceClass llm.ServiceClass
	ExpectedBaseURL           string
	ExpectedModel             string
	DefaultMaxTokens          int64
}

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
			llm.ServiceClassEconomy:  "flex",
			llm.ServiceClassStandard: "default",
			llm.ServiceClassPriority: "priority",
		},
		ActualServiceClasses: map[string]llm.ServiceClass{
			"flex":     llm.ServiceClassEconomy,
			"default":  llm.ServiceClassStandard,
			"priority": llm.ServiceClassPriority,
		},
	}
}

func NewDefaultProfile(id string) (Profile, error) { return NewProfile(DefaultProfile(id)) }

func NewProfile(profile Profile) (Profile, error) {
	if err := profile.validate(); err != nil {
		return Profile{}, err
	}
	copy := profile
	copy.Capabilities = cloneCapabilities(profile.Capabilities)
	copy.Capabilities.Features[provider.FeatureStreaming] = unsupportedStreamingCapability(copy.Capabilities.Features[provider.FeatureStreaming])
	copy.ServiceTiers = cloneServiceTiers(profile.ServiceTiers)
	copy.ActualServiceClasses = cloneActualClasses(profile.ActualServiceClasses)
	if copy.ExpectedBaseURL != "" {
		copy.ExpectedBaseURL, _ = clientconfig.BaseURL(copy.ExpectedBaseURL)
	}
	if copy.DefaultMaxTokens == 0 {
		copy.DefaultMaxTokens = defaultMaxTokens
	}
	return copy, nil
}

func (profile Profile) validate() error {
	if profile.ID == "" {
		return fmt.Errorf("bedrock messages profile ID is required")
	}
	if profile.ExpectedBaseURL != "" {
		if _, err := clientconfig.BaseURL(profile.ExpectedBaseURL); err != nil {
			return fmt.Errorf("bedrock messages profile %q expected base URL: %w", profile.ID, err)
		}
	}
	if profile.ExpectedModel != "" && len(profile.ExpectedModel) > 256 {
		return fmt.Errorf("bedrock messages profile %q expected model is too long", profile.ID)
	}
	version := profile.CapabilityVersion
	if version == "" {
		version = profile.Capabilities.Version
	}
	if version == "" {
		return fmt.Errorf("bedrock messages profile %q capability version is required", profile.ID)
	}
	if profile.Capabilities.Version != "" && profile.Capabilities.Version != version {
		return fmt.Errorf("bedrock messages profile %q capability versions conflict", profile.ID)
	}
	supported := 0
	for _, class := range publicServiceClasses() {
		value, ok := profile.ServiceTiers[class]
		if !ok {
			return fmt.Errorf("bedrock messages profile %q must declare service class %q", profile.ID, class)
		}
		// An empty tier marks a class the endpoint does not offer, as in the
		// Chat and Anthropic profiles; providerTier then reports it unsupported.
		if value == "" {
			continue
		}
		if !validProviderTier(value) {
			return fmt.Errorf("bedrock messages profile %q service class %q has invalid provider tier %q", profile.ID, class, value)
		}
		supported++
	}
	if supported == 0 {
		return fmt.Errorf("bedrock messages profile %q must support at least one service class", profile.ID)
	}
	for feature, capability := range profile.Capabilities.Features {
		if feature == "" {
			return fmt.Errorf("bedrock messages profile %q contains an empty capability feature", profile.ID)
		}
		if !capability.State.Valid() {
			return fmt.Errorf("bedrock messages profile %q capability %q has invalid state %q", profile.ID, feature, capability.State)
		}
	}
	for _, feature := range allFeatures() {
		if _, ok := profile.Capabilities.Features[feature]; !ok {
			return fmt.Errorf("bedrock messages profile %q must explicitly declare capability %q", profile.ID, feature)
		}
	}
	if streaming := profile.Capabilities.Features[provider.FeatureStreaming]; streaming.State == provider.CapabilityNative || streaming.State == provider.CapabilityEmulated {
		return fmt.Errorf("bedrock messages profile %q cannot advertise streaming as %q: adapter does not implement OpenStream", profile.ID, streaming.State)
	}
	if profile.MissingActualServiceClass != "" && !profile.MissingActualServiceClass.Valid() {
		return fmt.Errorf("bedrock messages profile %q missing actual service class %q is invalid", profile.ID, profile.MissingActualServiceClass)
	}
	for tier, class := range profile.ActualServiceClasses {
		if !validProviderTier(tier) {
			return fmt.Errorf("bedrock messages profile %q actual provider tier %q is invalid", profile.ID, tier)
		}
		if !class.Valid() {
			return fmt.Errorf("bedrock messages profile %q actual provider tier %q maps to invalid class %q", profile.ID, tier, class)
		}
	}
	if profile.DefaultMaxTokens < 0 {
		return fmt.Errorf("bedrock messages profile %q default max_tokens must not be negative", profile.ID)
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
	if query.Family != "" && query.Family != provider.FamilyBedrockMessages {
		return provider.CapabilitySet{}, fmt.Errorf("bedrock messages profile %q: capability family %q does not match %q", profile.ID, query.Family, provider.FamilyBedrockMessages)
	}
	if query.EndpointID != "" && query.EndpointID != endpointID {
		return provider.CapabilitySet{}, fmt.Errorf("bedrock messages profile %q: capability endpoint %q does not match %q", profile.ID, query.EndpointID, endpointID)
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
	if !validProviderTier(value) {
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
	switch value {
	case "flex", "default", "priority":
		return true
	default:
		return false
	}
}

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
