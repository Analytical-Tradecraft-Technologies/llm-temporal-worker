package provider

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

var registryProfileIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

// Registration is the immutable, non-secret contract for one configured
// adapter profile. The registry never discovers profiles from hostnames or
// provider defaults: Family and ProfileID are explicit configuration keys.
//
// SDKParameterTypeName identifies the concrete official-SDK parameter value
// that the adapter's Compile method returns in Call.SDKParams;
// SDKParameterCheck verifies it without exporting that SDK type. CompileProbe
// is a local, side-effect-free probe used to verify that declaration against
// the adapter. ClientFactory must construct the already-configured SDK client
// without making a provider request; the registry invokes it once during
// registration to prove construction succeeds and does not retain the client.
type Registration struct {
	Family       Family
	ProfileID    string
	Adapter      Adapter
	Capabilities CapabilitySet
	ServiceTiers map[llm.ServiceClass]string
	// SDKParameterTypeName is a redacted, human-readable type identifier. The
	// concrete SDK type remains private to the adapter package.
	SDKParameterTypeName string
	// SDKParameterCheck is supplied by the adapter package and recognizes its
	// own concrete SDK parameter value without exporting that type here.
	SDKParameterCheck func(any) bool
	CompileProbe      func(context.Context, Adapter) (Call, error)
	ClientFactory     func() (any, error)
}

// Registry is a deterministic collection of validated adapter profiles. A
// Registry is safe for concurrent lookups after construction; registration is
// intentionally only supported through Register before the registry is shared.
type Registry struct {
	entries map[registryKey]Registration
}

type registryKey struct {
	family    Family
	profileID string
}

// NewRegistry validates and registers each supplied profile. Registration is
// ordered so a duplicate key cannot silently replace an earlier profile.
func NewRegistry(registrations ...Registration) (*Registry, error) {
	registry := &Registry{entries: make(map[registryKey]Registration, len(registrations))}
	for _, registration := range registrations {
		if err := registry.Register(registration); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

// Register validates one profile and adds it to the registry. The caller must
// finish registration before sharing the Registry between goroutines.
func (registry *Registry) Register(registration Registration) error {
	if registry == nil {
		return fmt.Errorf("provider registry is nil")
	}
	if registry.entries == nil {
		registry.entries = make(map[registryKey]Registration)
	}
	if err := validateRegistryKey(registration.Family, registration.ProfileID); err != nil {
		return err
	}
	key := registryKey{family: registration.Family, profileID: registration.ProfileID}
	if _, exists := registry.entries[key]; exists {
		return fmt.Errorf("provider registry profile %q for family %q is already registered", registration.ProfileID, registration.Family)
	}
	if err := validateRegistration(registration); err != nil {
		return err
	}
	// The registration owns copies of maps so mutable configuration supplied by
	// a caller cannot change the contract after validation.
	registration.Capabilities = cloneCapabilities(registration.Capabilities)
	registration.ServiceTiers = cloneServiceTiers(registration.ServiceTiers)
	registry.entries[key] = registration
	return nil
}

// Lookup returns the validated registration for an explicit family/profile
// pair. The returned maps are independent copies.
func (registry *Registry) Lookup(family Family, profileID string) (Registration, bool) {
	if registry == nil {
		return Registration{}, false
	}
	registration, ok := registry.entries[registryKey{family: family, profileID: profileID}]
	if !ok {
		return Registration{}, false
	}
	registration.Capabilities = cloneCapabilities(registration.Capabilities)
	registration.ServiceTiers = cloneServiceTiers(registration.ServiceTiers)
	return registration, true
}

// Adapter resolves an explicit family/profile pair without exposing registry
// internals to the engine. It returns a safe error that contains no SDK or
// credential material.
func (registry *Registry) Adapter(family Family, profileID string) (Adapter, error) {
	registration, ok := registry.Lookup(family, profileID)
	if !ok {
		return nil, fmt.Errorf("provider adapter profile %q for family %q is not registered", profileID, family)
	}
	return registration.Adapter, nil
}

// ModelLister resolves the optional provider management capability for an
// explicit adapter profile. A missing capability is reported as unsupported;
// the registry never probes a provider or synthesizes an inventory page.
func (registry *Registry) ModelLister(family Family, profileID string) (ModelLister, error) {
	adapter, err := registry.Adapter(family, profileID)
	if err != nil {
		return nil, err
	}
	lister, ok := adapter.(ModelLister)
	if !ok {
		return nil, fmt.Errorf("%w: provider adapter profile %q for family %q does not support model inventory", ErrModelInventoryUnsupported, profileID, family)
	}
	return lister, nil
}

// Registrations returns a stable family/profile-sorted snapshot for inventory
// and startup diagnostics. It never includes a constructed SDK client.
func (registry *Registry) Registrations() []Registration {
	if registry == nil {
		return nil
	}
	result := make([]Registration, 0, len(registry.entries))
	for _, registration := range registry.entries {
		registration.Capabilities = cloneCapabilities(registration.Capabilities)
		registration.ServiceTiers = cloneServiceTiers(registration.ServiceTiers)
		result = append(result, registration)
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Family != result[right].Family {
			return result[left].Family < result[right].Family
		}
		return result[left].ProfileID < result[right].ProfileID
	})
	return result
}

func validateRegistration(registration Registration) error {
	if err := validateRegistryKey(registration.Family, registration.ProfileID); err != nil {
		return err
	}
	if isNilAdapter(registration.Adapter) {
		return fmt.Errorf("provider registry profile %q has no concrete adapter", registration.ProfileID)
	}
	if strings.TrimSpace(registration.SDKParameterTypeName) == "" || registration.SDKParameterCheck == nil {
		return fmt.Errorf("provider registry profile %q has no concrete SDK parameter type check", registration.ProfileID)
	}
	if registration.CompileProbe == nil {
		return fmt.Errorf("provider registry profile %q compile probe is required", registration.ProfileID)
	}
	if registration.ClientFactory == nil {
		return fmt.Errorf("provider registry profile %q client factory is required", registration.ProfileID)
	}
	capabilities, err := validateCapabilities(registration.Capabilities)
	if err != nil {
		return fmt.Errorf("provider registry profile %q: %w", registration.ProfileID, err)
	}
	registration.Capabilities = capabilities
	adapterCapabilities, err := registration.Adapter.Capabilities(context.Background(), CapabilityQuery{Family: registration.Family})
	if err != nil {
		return fmt.Errorf("provider registry profile %q adapter capability query failed", registration.ProfileID)
	}
	adapterCapabilities, err = validateCapabilities(adapterCapabilities)
	if err != nil {
		return fmt.Errorf("provider registry profile %q adapter capabilities are invalid", registration.ProfileID)
	}
	if !capabilitiesEqual(registration.Capabilities, adapterCapabilities) {
		return fmt.Errorf("provider registry profile %q capabilities do not match the concrete adapter", registration.ProfileID)
	}
	if err := validateServiceTiers(registration.ServiceTiers); err != nil {
		return fmt.Errorf("provider registry profile %q: %w", registration.ProfileID, err)
	}
	call, err := registration.CompileProbe(context.Background(), registration.Adapter)
	if err != nil {
		return fmt.Errorf("provider registry profile %q compile probe failed", registration.ProfileID)
	}
	if call.Family != registration.Family {
		return fmt.Errorf("provider registry profile %q compile probe returned family %q", registration.ProfileID, call.Family)
	}
	if isNilAny(call.SDKParams) {
		return fmt.Errorf("provider registry profile %q compile probe returned no SDK parameters", registration.ProfileID)
	}
	if !registration.SDKParameterCheck(call.SDKParams) {
		return fmt.Errorf("provider registry profile %q compile probe returned unsupported SDK parameter type", registration.ProfileID)
	}
	client, err := registration.ClientFactory()
	if err != nil || isNilAny(client) {
		// Do not include the factory error: SDK constructors may include endpoint
		// or authentication details that are not safe for startup diagnostics.
		return fmt.Errorf("provider registry profile %q client construction failed", registration.ProfileID)
	}
	return nil
}

func validateRegistryKey(family Family, profileID string) error {
	if !family.Valid() {
		return fmt.Errorf("provider registry family %q is invalid", family)
	}
	if !registryProfileIDPattern.MatchString(profileID) {
		return fmt.Errorf("provider registry profile ID must be lowercase hyphenated identifier")
	}
	return nil
}

func validateCapabilities(set CapabilitySet) (CapabilitySet, error) {
	if strings.TrimSpace(set.Version) == "" {
		return CapabilitySet{}, fmt.Errorf("capability version is required")
	}
	if set.Features == nil {
		return CapabilitySet{}, fmt.Errorf("capability features are required")
	}
	knownFeatures := make(map[Feature]struct{}, len(registryFeatures()))
	for _, feature := range registryFeatures() {
		knownFeatures[feature] = struct{}{}
		capability, ok := set.Features[feature]
		if !ok {
			return CapabilitySet{}, fmt.Errorf("capability %q is not declared", feature)
		}
		if !capability.State.Valid() {
			return CapabilitySet{}, fmt.Errorf("capability %q has invalid state", feature)
		}
		if feature == FeatureStreaming && (capability.State == CapabilityNative || capability.State == CapabilityEmulated) {
			return CapabilitySet{}, fmt.Errorf("streaming capability cannot be advertised by a one-shot adapter")
		}
	}
	for feature := range set.Features {
		if _, ok := knownFeatures[feature]; !ok {
			return CapabilitySet{}, fmt.Errorf("capability %q is not governed", feature)
		}
	}
	return set, nil
}

func validateServiceTiers(tiers map[llm.ServiceClass]string) error {
	if tiers == nil {
		return fmt.Errorf("service tier mappings are required")
	}
	for _, class := range []llm.ServiceClass{llm.ServiceClassEconomy, llm.ServiceClassStandard, llm.ServiceClassPriority} {
		value, ok := tiers[class]
		if !ok {
			return fmt.Errorf("service class %q is not declared", class)
		}
		if value != "" && strings.TrimSpace(value) != value {
			return fmt.Errorf("service class %q has non-canonical provider tier", class)
		}
	}
	for class := range tiers {
		if !class.Valid() {
			return fmt.Errorf("service tier mapping contains invalid public class")
		}
	}
	return nil
}

func registryFeatures() []Feature {
	return []Feature{FeatureText, FeatureImage, FeatureDocument, FeatureToolCall, FeatureStructuredOutput, FeatureReasoning, FeatureContinuation, FeatureStreaming, FeatureUsage}
}

func isNilAdapter(adapter Adapter) bool { return isNilAny(adapter) }

func isNilAny(value any) bool {
	if value == nil {
		return true
	}
	reflectValue := reflect.ValueOf(value)
	switch reflectValue.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflectValue.IsNil()
	default:
		return false
	}
}

func cloneCapabilities(set CapabilitySet) CapabilitySet {
	copy := set
	if set.Features != nil {
		copy.Features = make(map[Feature]Capability, len(set.Features))
		for feature, capability := range set.Features {
			copy.Features[feature] = capability
		}
	}
	return copy
}

func capabilitiesEqual(left, right CapabilitySet) bool {
	if left.Version != right.Version || len(left.Features) != len(right.Features) {
		return false
	}
	for feature, capability := range left.Features {
		if capability != right.Features[feature] {
			return false
		}
	}
	return true
}

func cloneServiceTiers(tiers map[llm.ServiceClass]string) map[llm.ServiceClass]string {
	if tiers == nil {
		return nil
	}
	copy := make(map[llm.ServiceClass]string, len(tiers))
	for class, tier := range tiers {
		copy[class] = tier
	}
	return copy
}
