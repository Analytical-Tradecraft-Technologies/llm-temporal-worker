package bedrockconverse

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
)

const (
	adapterName              = "bedrock.converse"
	defaultCapabilityVersion = "bedrock-converse/v1"
	defaultMaxTokens         = int32(1024)
)

// Profile describes one explicitly configured Bedrock Converse model family.
// Model IDs stay opaque strings because Bedrock adds foundation, inference
// profile, provisioned-throughput, and ARN identifiers independently of this
// library's release cycle.
type Profile struct {
	ID                        string
	CapabilityVersion         string
	Capabilities              provider.CapabilitySet
	ServiceTiers              map[llm.ServiceClass]string
	ActualServiceClasses      map[string]llm.ServiceClass
	MissingActualServiceClass llm.ServiceClass
	ExpectedBaseURL           string
	ExpectedModel             string
	DefaultMaxTokens          int32
}

func DefaultProfile(id string) Profile {
	features := make(map[provider.Feature]provider.Capability, len(allFeatures()))
	for _, feature := range allFeatures() {
		state := provider.CapabilityUnsupported
		reason := "not verified for the generic Bedrock Converse boundary"
		if feature == provider.FeatureText || feature == provider.FeatureToolCall || feature == provider.FeatureUsage {
			state, reason = provider.CapabilityNative, ""
		}
		features[feature] = provider.Capability{State: state, Reason: reason}
	}
	return Profile{
		ID:                id,
		CapabilityVersion: defaultCapabilityVersion,
		Capabilities:      provider.CapabilitySet{Version: defaultCapabilityVersion, Features: features},
		ServiceTiers: map[llm.ServiceClass]string{
			llm.ServiceClassEconomy:  string(types.ServiceTierTypeFlex),
			llm.ServiceClassStandard: string(types.ServiceTierTypeDefault),
			llm.ServiceClassPriority: string(types.ServiceTierTypePriority),
		},
		ActualServiceClasses: map[string]llm.ServiceClass{
			string(types.ServiceTierTypeFlex):     llm.ServiceClassEconomy,
			string(types.ServiceTierTypeDefault):  llm.ServiceClassStandard,
			string(types.ServiceTierTypePriority): llm.ServiceClassPriority,
		},
		MissingActualServiceClass: llm.ServiceClassStandard,
	}
}

func NewDefaultProfile(id string) (Profile, error) { return NewProfile(DefaultProfile(id)) }

func NewProfile(profile Profile) (Profile, error) {
	if err := profile.validate(); err != nil {
		return Profile{}, err
	}
	copy := profile
	copy.Capabilities = cloneCapabilities(profile.Capabilities)
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
		return fmt.Errorf("bedrock converse profile ID is required")
	}
	if profile.ExpectedBaseURL != "" {
		if _, err := clientconfig.BaseURL(profile.ExpectedBaseURL); err != nil {
			return fmt.Errorf("bedrock converse profile %q expected base URL: %w", profile.ID, err)
		}
	}
	if len(profile.ExpectedModel) > 256 {
		return fmt.Errorf("bedrock converse profile %q expected model is too long", profile.ID)
	}
	version := profile.CapabilityVersion
	if version == "" {
		version = profile.Capabilities.Version
	}
	if version == "" {
		return fmt.Errorf("bedrock converse profile %q capability version is required", profile.ID)
	}
	if profile.Capabilities.Version != "" && profile.Capabilities.Version != version {
		return fmt.Errorf("bedrock converse profile %q capability versions conflict", profile.ID)
	}
	for _, class := range publicServiceClasses() {
		value, ok := profile.ServiceTiers[class]
		if !ok || !validServiceTier(value) {
			return fmt.Errorf("bedrock converse profile %q service class %q has invalid provider tier %q", profile.ID, class, value)
		}
	}
	for feature, capability := range profile.Capabilities.Features {
		if feature == "" || !capability.State.Valid() {
			return fmt.Errorf("bedrock converse profile %q has invalid capability %q", profile.ID, feature)
		}
	}
	for _, feature := range allFeatures() {
		if _, ok := profile.Capabilities.Features[feature]; !ok {
			return fmt.Errorf("bedrock converse profile %q must explicitly declare capability %q", profile.ID, feature)
		}
	}
	for tier, class := range profile.ActualServiceClasses {
		if !validServiceTier(tier) {
			return fmt.Errorf("bedrock converse profile %q actual provider tier %q is invalid", profile.ID, tier)
		}
		if !class.Valid() {
			return fmt.Errorf("bedrock converse profile %q actual provider tier %q maps to invalid class %q", profile.ID, tier, class)
		}
	}
	if profile.Capabilities.Features[provider.FeatureStreaming].State != provider.CapabilityUnsupported {
		return fmt.Errorf("bedrock converse profile %q cannot advertise streaming: Converse adapter is one-shot", profile.ID)
	}
	if profile.MissingActualServiceClass != "" && !profile.MissingActualServiceClass.Valid() {
		return fmt.Errorf("bedrock converse profile %q missing actual service class %q is invalid", profile.ID, profile.MissingActualServiceClass)
	}
	if profile.DefaultMaxTokens < 0 {
		return fmt.Errorf("bedrock converse profile %q default max_tokens must not be negative", profile.ID)
	}
	return nil
}

func (profile Profile) capabilities(ctx context.Context, query provider.CapabilityQuery, endpointID string) (provider.CapabilitySet, error) {
	if err := ctx.Err(); err != nil {
		return provider.CapabilitySet{}, err
	}
	if query.Family != "" && query.Family != provider.FamilyBedrockConverse {
		return provider.CapabilitySet{}, fmt.Errorf("bedrock converse profile %q: capability family %q does not match %q", profile.ID, query.Family, provider.FamilyBedrockConverse)
	}
	if query.EndpointID != "" && query.EndpointID != endpointID {
		return provider.CapabilitySet{}, fmt.Errorf("bedrock converse profile %q: capability endpoint %q does not match %q", profile.ID, query.EndpointID, endpointID)
	}
	return cloneCapabilities(profile.Capabilities), nil
}

func (profile Profile) providerTier(class llm.ServiceClass) (string, error) {
	tier, ok := profile.ServiceTiers[class]
	if !ok || !validServiceTier(tier) {
		return "", fmt.Errorf("service class %q is unsupported by profile %q", class, profile.ID)
	}
	return tier, nil
}

func (profile Profile) actualClass(tier string) (*llm.ServiceClass, error) {
	if tier == "" {
		if profile.MissingActualServiceClass == "" {
			return nil, fmt.Errorf("provider response omitted service tier")
		}
		class := profile.MissingActualServiceClass
		return &class, nil
	}
	class, ok := profile.ActualServiceClasses[tier]
	if !ok {
		return nil, fmt.Errorf("provider returned unsupported service tier %q", tier)
	}
	return &class, nil
}

func publicServiceClasses() []llm.ServiceClass {
	return []llm.ServiceClass{llm.ServiceClassEconomy, llm.ServiceClassStandard, llm.ServiceClassPriority}
}

func validServiceTier(value string) bool {
	switch types.ServiceTierType(value) {
	case types.ServiceTierTypeFlex, types.ServiceTierTypeDefault, types.ServiceTierTypePriority:
		return true
	default:
		return false
	}
}

func allFeatures() []provider.Feature {
	return []provider.Feature{provider.FeatureText, provider.FeatureImage, provider.FeatureDocument, provider.FeatureToolCall, provider.FeatureStructuredOutput, provider.FeatureReasoning, provider.FeatureContinuation, provider.FeatureStreaming, provider.FeatureUsage}
}

func cloneCapabilities(set provider.CapabilitySet) provider.CapabilitySet {
	features := make(map[provider.Feature]provider.Capability, len(set.Features))
	for feature, capability := range set.Features {
		features[feature] = capability
	}
	set.Features = features
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
