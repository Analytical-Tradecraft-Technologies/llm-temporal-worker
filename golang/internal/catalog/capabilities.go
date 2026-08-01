package catalog

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

type capabilityDocument struct {
	Version  string                           `yaml:"version"`
	Entries  []capabilityEntryDocument        `yaml:"entries"`
	Profiles map[string]capabilityProfileFile `yaml:"profiles"`
}

// capabilityEntryDocument is the versioned entry shape documented for
// production catalogs. Profiles in the local fixture use the profiles map
// shape below; both are intentionally decoded with known fields only.
type capabilityEntryDocument struct {
	ID         string                         `yaml:"id"`
	Family     string                         `yaml:"family"`
	Model      modelMatcher                   `yaml:"model"`
	VerifiedAt time.Time                      `yaml:"verified_at"`
	Features   map[string]capabilityClaimFile `yaml:"features"`
	Limits     capabilityLimitsFile           `yaml:"limits"`
}

type capabilityProfileFile struct {
	Family         string   `yaml:"family"`
	Model          string   `yaml:"model"`
	Input          []string `yaml:"input"`
	Output         []string `yaml:"output"`
	ServiceClasses []string `yaml:"service_classes"`
	MaxContext     int64    `yaml:"max_context_tokens"`
	MaxOutput      int64    `yaml:"max_output_tokens"`
}

type modelMatcher struct {
	Exact string `yaml:"exact"`
}

type capabilityClaimFile struct {
	Level     string `yaml:"level"`
	Transform string `yaml:"transform"`
	Reason    string `yaml:"reason"`
	MaxBytes  int64  `yaml:"max_bytes"`
	Pinned    bool   `yaml:"pinned"`
	Dialect   string `yaml:"dialect"`
}

type capabilityLimitsFile struct {
	ContextTokens int64 `yaml:"context_tokens"`
	OutputTokens  int64 `yaml:"output_tokens"`
}

func compileCapabilities(document capabilityDocument) (map[string]CapabilityProfile, error) {
	if err := validateIdentifier(document.Version, "version"); err != nil {
		return nil, err
	}
	if len(document.Entries) == 0 && len(document.Profiles) == 0 {
		return nil, fmt.Errorf("entries or profiles must not be empty")
	}
	if len(document.Entries) > 0 && len(document.Profiles) > 0 {
		return nil, fmt.Errorf("entries and profiles cannot both be configured")
	}
	profiles := make(map[string]CapabilityProfile)
	if len(document.Entries) > 0 {
		for index, entry := range document.Entries {
			profile, err := compileCapabilityEntry(document.Version, index, entry)
			if err != nil {
				return nil, err
			}
			if _, exists := profiles[profile.ID]; exists {
				return nil, fmt.Errorf("duplicate capability profile ID %q", profile.ID)
			}
			profiles[profile.ID] = profile
		}
		return profiles, nil
	}
	for id, fileProfile := range document.Profiles {
		profile, err := compileCapabilityProfile(document.Version, id, fileProfile)
		if err != nil {
			return nil, err
		}
		if _, exists := profiles[id]; exists {
			return nil, fmt.Errorf("duplicate capability profile ID %q", id)
		}
		profiles[id] = profile
	}
	return profiles, nil
}

func compileCapabilityEntry(version string, index int, entry capabilityEntryDocument) (CapabilityProfile, error) {
	path := fmt.Sprintf("entries[%d]", index)
	if err := validateIdentifier(entry.ID, path+".id"); err != nil {
		return CapabilityProfile{}, err
	}
	family := endpointFamily(entry.Family)
	if !family.Valid() {
		return CapabilityProfile{}, fmt.Errorf("%s.family %q is unsupported", path, entry.Family)
	}
	if err := validateIdentifier(entry.Model.Exact, path+".model.exact"); err != nil {
		return CapabilityProfile{}, err
	}
	if !entry.VerifiedAt.IsZero() && entry.VerifiedAt.Location() == nil {
		return CapabilityProfile{}, fmt.Errorf("%s.verified_at must include a timezone", path)
	}
	if err := entry.Limits.validate(path + ".limits"); err != nil {
		return CapabilityProfile{}, err
	}
	serviceClasses, serviceClassesDeclared, err := compileEntryServiceClasses(entry.Features, path+".features")
	if err != nil {
		return CapabilityProfile{}, err
	}
	set, err := compileClaims(version, entry.Features, path+".features")
	if err != nil {
		return CapabilityProfile{}, err
	}
	return CapabilityProfile{
		ID:                     entry.ID,
		Family:                 family,
		Model:                  entry.Model.Exact,
		VerifiedAt:             entry.VerifiedAt,
		Set:                    set,
		ServiceClasses:         serviceClasses,
		ServiceClassesDeclared: serviceClassesDeclared,
	}, nil
}

func compileCapabilityProfile(version, id string, profile capabilityProfileFile) (CapabilityProfile, error) {
	if err := validateIdentifier(id, "profiles.id"); err != nil {
		return CapabilityProfile{}, err
	}
	family := endpointFamily(profile.Family)
	if !family.Valid() {
		return CapabilityProfile{}, fmt.Errorf("profiles.%s.family %q is unsupported", id, profile.Family)
	}
	if err := validateIdentifier(profile.Model, "profiles."+id+".model"); err != nil {
		return CapabilityProfile{}, err
	}
	if profile.MaxContext < 0 || profile.MaxOutput < 0 {
		return CapabilityProfile{}, fmt.Errorf("profiles.%s limits must not be negative", id)
	}
	features := make(map[string]capabilityClaimFile, len(profile.Input)+len(profile.Output))
	inputSeen := make(map[string]struct{}, len(profile.Input))
	for index, name := range profile.Input {
		canonical, err := profileFeature(name, fmt.Sprintf("profiles.%s.input[%d]", id, index))
		if err != nil {
			return CapabilityProfile{}, err
		}
		if _, exists := inputSeen[canonical]; exists {
			return CapabilityProfile{}, fmt.Errorf("profiles.%s repeats feature %q", id, name)
		}
		inputSeen[canonical] = struct{}{}
		features[canonical] = capabilityClaimFile{Level: "native"}
	}
	outputSeen := make(map[string]struct{}, len(profile.Output))
	for index, name := range profile.Output {
		canonical, err := profileFeature(name, fmt.Sprintf("profiles.%s.output[%d]", id, index))
		if err != nil {
			return CapabilityProfile{}, err
		}
		if _, exists := outputSeen[canonical]; exists {
			return CapabilityProfile{}, fmt.Errorf("profiles.%s repeats feature %q", id, name)
		}
		outputSeen[canonical] = struct{}{}
		features[canonical] = capabilityClaimFile{Level: "native"}
	}
	serviceClasses := make([]llm.ServiceClass, 0, len(profile.ServiceClasses))
	seenClasses := make(map[string]struct{}, len(profile.ServiceClasses))
	for index, value := range profile.ServiceClasses {
		class, err := validateServiceClass(value, fmt.Sprintf("profiles.%s.service_classes[%d]", id, index))
		if err != nil {
			return CapabilityProfile{}, err
		}
		if _, exists := seenClasses[value]; exists {
			return CapabilityProfile{}, fmt.Errorf("profiles.%s repeats service class %q", id, value)
		}
		seenClasses[value] = struct{}{}
		serviceClasses = append(serviceClasses, class)
	}
	sort.Slice(serviceClasses, func(i, j int) bool { return serviceClasses[i] < serviceClasses[j] })
	set, err := compileClaims(version, features, "profiles."+id+".features")
	if err != nil {
		return CapabilityProfile{}, err
	}
	return CapabilityProfile{
		ID:                     id,
		Family:                 family,
		Model:                  profile.Model,
		Set:                    set,
		ServiceClasses:         serviceClasses,
		ServiceClassesDeclared: profile.ServiceClasses != nil,
	}, nil
}

func compileEntryServiceClasses(claims map[string]capabilityClaimFile, path string) ([]llm.ServiceClass, bool, error) {
	classes := make([]llm.ServiceClass, 0, 3)
	seen := make(map[llm.ServiceClass]struct{}, 3)
	declared := false
	for name, claim := range claims {
		class, ok := serviceClassFeature(name)
		if !ok {
			continue
		}
		declared = true
		state := provider.CapabilityState(claim.Level)
		if !state.Valid() {
			return nil, false, fmt.Errorf("%s.%s.level %q is unsupported", path, name, claim.Level)
		}
		if state == provider.CapabilityEmulated && strings.TrimSpace(claim.Transform) == "" {
			return nil, false, fmt.Errorf("%s.%s.transform is required for emulated capability", path, name)
		}
		if state == provider.CapabilityUnsupported {
			continue
		}
		if _, exists := seen[class]; exists {
			return nil, false, fmt.Errorf("%s maps conflicting claims to service class %q", path, class)
		}
		seen[class] = struct{}{}
		classes = append(classes, class)
	}
	sort.Slice(classes, func(i, j int) bool { return classes[i] < classes[j] })
	return classes, declared, nil
}

func (limits capabilityLimitsFile) validate(path string) error {
	if limits.ContextTokens < 0 || limits.OutputTokens < 0 {
		return fmt.Errorf("%s values must not be negative", path)
	}
	return nil
}

func compileClaims(version string, claims map[string]capabilityClaimFile, path string) (provider.CapabilitySet, error) {
	if len(claims) == 0 {
		return provider.CapabilitySet{}, fmt.Errorf("%s must not be empty", path)
	}
	features := make(map[provider.Feature]provider.Capability, len(claims))
	for name, claim := range claims {
		feature, keep, err := providerFeature(name, path+"."+name)
		if err != nil {
			return provider.CapabilitySet{}, err
		}
		if !keep {
			continue
		}
		state := provider.CapabilityState(claim.Level)
		if !state.Valid() {
			return provider.CapabilitySet{}, fmt.Errorf("%s.level %q is unsupported", path+"."+name, claim.Level)
		}
		if state == provider.CapabilityEmulated && strings.TrimSpace(claim.Transform) == "" {
			return provider.CapabilitySet{}, fmt.Errorf("%s.transform is required for emulated capability", path+"."+name)
		}
		if claim.MaxBytes < 0 {
			return provider.CapabilitySet{}, fmt.Errorf("%s.max_bytes must not be negative", path+"."+name)
		}
		if existing, exists := features[feature]; exists {
			if existing.State != state || existing.Transform != claim.Transform || existing.Reason != claim.Reason {
				return provider.CapabilitySet{}, fmt.Errorf("%s maps conflicting claims to provider feature %q", path, feature)
			}
			continue
		}
		features[feature] = provider.Capability{State: state, Transform: claim.Transform, Reason: claim.Reason}
	}
	if len(features) == 0 {
		return provider.CapabilitySet{}, fmt.Errorf("%s contains no provider capabilities", path)
	}
	return provider.CapabilitySet{Version: version, Features: features}, nil
}

// providerFeature accepts the documented dotted claim names and the compact
// names used by the local fixture. `reference` is validated but intentionally
// omitted because the provider port has no external-reference feature.
func providerFeature(name, path string) (provider.Feature, bool, error) {
	switch name {
	case "text", "input.text":
		return provider.FeatureText, true, nil
	case "image", "input.image":
		return provider.FeatureImage, true, nil
	case "document", "input.document":
		return provider.FeatureDocument, true, nil
	case "reference", "input.reference":
		return "", false, nil
	case "tool_call", "tools.auto", "tools.required", "tools.parallel", "output.tool_call":
		return provider.FeatureToolCall, true, nil
	case "structured_output", "output.json_schema":
		return provider.FeatureStructuredOutput, true, nil
	case "reasoning":
		return provider.FeatureReasoning, true, nil
	case "continuation", "continuation.response_id":
		return provider.FeatureContinuation, true, nil
	case "streaming":
		return provider.FeatureStreaming, true, nil
	case "usage", "stream.typed_usage":
		return provider.FeatureUsage, true, nil
	case "service.economy", "service.standard", "service.priority":
		return "", false, nil
	default:
		return "", false, fmt.Errorf("%s has unsupported feature %q", path, name)
	}
}

func serviceClassFeature(name string) (llm.ServiceClass, bool) {
	switch name {
	case "service.economy":
		return llm.ServiceClassEconomy, true
	case "service.standard":
		return llm.ServiceClassStandard, true
	case "service.priority":
		return llm.ServiceClassPriority, true
	default:
		return "", false
	}
}

func profileFeature(name, path string) (string, error) {
	if strings.TrimSpace(name) != name || name == "" {
		return "", fmt.Errorf("%s must be a non-empty feature name", path)
	}
	if _, _, err := providerFeature(name, path); err != nil {
		return "", err
	}
	return name, nil
}
