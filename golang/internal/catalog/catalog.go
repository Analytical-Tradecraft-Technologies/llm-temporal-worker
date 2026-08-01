// Package catalog loads the immutable, operator-supplied capability and price
// catalogs referenced by config.Config.
//
// Catalog files are treated as data, not executable configuration.  The file
// bytes are bounded and authenticated with the SHA-256 digest in the config
// reference before YAML decoding starts.  Decoding is strict: unknown fields,
// duplicate keys, duplicate identities, and malformed values fail closed.
package catalog

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	yaml "go.yaml.in/yaml/v4"
)

// DefaultMaxBytes bounds one catalog document.  A catalog is configuration,
// so a large file is rejected before YAML allocates an unbounded structure.
const DefaultMaxBytes = 4 << 20

// Options controls the bounded file reader.  MaxBytes must be positive and
// cannot exceed DefaultMaxBytes; the upper bound prevents a caller from
// accidentally defeating the safety invariant.
type Options struct {
	MaxBytes int
}

func (options Options) maxBytes() (int, error) {
	if options.MaxBytes == 0 {
		return DefaultMaxBytes, nil
	}
	if options.MaxBytes < 1 || options.MaxBytes > DefaultMaxBytes {
		return 0, fmt.Errorf("catalog max bytes must be between 1 and %d", DefaultMaxBytes)
	}
	return options.MaxBytes, nil
}

// CapabilityProfile is the compiled form of one capability profile entry.
// Metadata remains available to callers that need to match a profile against
// an endpoint/model; Set is the provider-neutral capability contract consumed
// by adapters.
type CapabilityProfile struct {
	ID                     string
	Family                 provider.Family
	Model                  string
	VerifiedAt             time.Time
	Set                    provider.CapabilitySet
	ServiceClasses         []llm.ServiceClass
	ServiceClassesDeclared bool
}

// CapabilityCatalog is one verified capability document. Profiles are keyed
// by their immutable IDs.
type CapabilityCatalog struct {
	Version  string
	Profiles map[string]CapabilityProfile
	Digest   [32]byte
}

// PricingCatalog is one verified price document compiled by pricing's exact
// decimal arithmetic. Catalog is safe to publish as an immutable snapshot.
type PricingCatalog struct {
	ID      string
	Version string
	Catalog pricing.Catalog
	Digest  [32]byte
}

// Bundle is the complete set of catalogs referenced by one worker config.
// The maps are keyed by profile ID and price-catalog ID so endpoint references
// can be checked before a runtime snapshot is published.
type Bundle struct {
	Capabilities map[string]CapabilityProfile
	Pricing      map[string]PricingCatalog
}

// LoadCapabilities reads and compiles one configured capability catalog.
func LoadCapabilities(ref config.CatalogRef) (CapabilityCatalog, error) {
	return LoadCapabilitiesWithOptions(ref, Options{})
}

// LoadCapabilitiesWithOptions is LoadCapabilities with an explicit bounded
// reader limit.
func LoadCapabilitiesWithOptions(ref config.CatalogRef, options Options) (CapabilityCatalog, error) {
	data, digest, err := readVerified(ref, options)
	if err != nil {
		return CapabilityCatalog{}, err
	}
	var document capabilityDocument
	if err := decodeStrict(data, &document); err != nil {
		return CapabilityCatalog{}, fmt.Errorf("capability catalog %q: %w", ref.File, err)
	}
	profiles, err := compileCapabilities(document)
	if err != nil {
		return CapabilityCatalog{}, fmt.Errorf("capability catalog %q: %w", ref.File, err)
	}
	return CapabilityCatalog{Version: document.Version, Profiles: profiles, Digest: digest}, nil
}

// LoadPricing reads and compiles one configured price catalog.
func LoadPricing(ref config.CatalogRef) (PricingCatalog, error) {
	return LoadPricingWithOptions(ref, Options{})
}

// LoadPricingWithOptions is LoadPricing with an explicit bounded reader
// limit.
func LoadPricingWithOptions(ref config.CatalogRef, options Options) (PricingCatalog, error) {
	data, digest, err := readVerified(ref, options)
	if err != nil {
		return PricingCatalog{}, err
	}
	var document pricingDocument
	if err := decodeStrict(data, &document); err != nil {
		return PricingCatalog{}, fmt.Errorf("pricing catalog %q: %w", ref.File, err)
	}
	compiled, err := compilePricing(document)
	if err != nil {
		return PricingCatalog{}, fmt.Errorf("pricing catalog %q: %w", ref.File, err)
	}
	return PricingCatalog{ID: compiled.ID, Version: document.Version, Catalog: compiled.Catalog, Digest: digest}, nil
}

// Load resolves every catalog reference in cfg and verifies every endpoint
// reference. It intentionally accepts a Config value rather than loading the
// worker's main YAML file, so callers can choose when and how config.Load is
// performed and can atomically publish the resulting bundle.
func Load(cfg config.Config) (Bundle, error) {
	return LoadWithOptions(cfg, Options{})
}

// LoadWithOptions is Load with an explicit bounded reader limit.
func LoadWithOptions(cfg config.Config, options Options) (Bundle, error) {
	if _, err := options.maxBytes(); err != nil {
		return Bundle{}, err
	}
	bundle := Bundle{
		Capabilities: make(map[string]CapabilityProfile),
		Pricing:      make(map[string]PricingCatalog),
	}
	for index, ref := range cfg.Capabilities.Catalogs {
		catalog, err := LoadCapabilitiesWithOptions(ref, options)
		if err != nil {
			return Bundle{}, fmt.Errorf("capabilities.catalogs[%d]: %w", index, err)
		}
		for id, profile := range catalog.Profiles {
			if _, exists := bundle.Capabilities[id]; exists {
				return Bundle{}, fmt.Errorf("duplicate capability profile ID %q across catalogs", id)
			}
			bundle.Capabilities[id] = profile
		}
	}
	for index, ref := range cfg.Pricing.Catalogs {
		catalog, err := LoadPricingWithOptions(ref, options)
		if err != nil {
			return Bundle{}, fmt.Errorf("pricing.catalogs[%d]: %w", index, err)
		}
		if _, exists := bundle.Pricing[catalog.ID]; exists {
			return Bundle{}, fmt.Errorf("duplicate pricing catalog ID %q across catalogs", catalog.ID)
		}
		bundle.Pricing[catalog.ID] = catalog
	}
	if err := verifyEndpointReferences(cfg, bundle); err != nil {
		return Bundle{}, err
	}
	return bundle, nil
}

func verifyEndpointReferences(cfg config.Config, bundle Bundle) error {
	for _, endpointID := range sortedEndpointIDs(cfg.Endpoints) {
		endpoint := cfg.Endpoints[endpointID]
		profile, ok := bundle.Capabilities[endpoint.CapabilityProfile]
		if !ok {
			return fmt.Errorf("endpoint %q references missing capability profile %q", endpointID, endpoint.CapabilityProfile)
		}
		if endpointFamily(endpoint.Family) != profile.Family {
			return fmt.Errorf("endpoint %q family %q does not match capability profile %q family %q", endpointID, endpoint.Family, endpoint.CapabilityProfile, profile.Family)
		}
		catalog, ok := bundle.Pricing[endpoint.PriceCatalog]
		if !ok {
			return fmt.Errorf("endpoint %q references missing price catalog %q", endpointID, endpoint.PriceCatalog)
		}
		if err := verifyPriceCatalogReference(endpointID, endpoint, catalog.Catalog); err != nil {
			return err
		}
	}
	return nil
}

func verifyPriceCatalogReference(endpointID string, endpoint config.EndpointConfig, catalog pricing.Catalog) error {
	// A price catalog may contain many endpoint/model entries. The endpoint
	// reference itself is considered valid when at least one entry identifies
	// the configured endpoint family and endpoint ID. Model routes are checked
	// by the route planner once a concrete model is selected.
	for _, entry := range catalog.Entries {
		if entry.EndpointID == endpointID && entry.Family == string(endpointFamily(endpoint.Family)) {
			return nil
		}
	}
	return fmt.Errorf("endpoint %q has no price entry in catalog %q for family %q", endpointID, endpoint.PriceCatalog, endpointFamily(endpoint.Family))
}

func sortedEndpointIDs(endpoints map[string]config.EndpointConfig) []string {
	ids := make([]string, 0, len(endpoints))
	for id := range endpoints {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func readVerified(ref config.CatalogRef, options Options) ([]byte, [32]byte, error) {
	var zero [32]byte
	maxBytes, err := options.maxBytes()
	if err != nil {
		return nil, zero, err
	}
	if ref.File == "" || !filepath.IsAbs(ref.File) {
		return nil, zero, fmt.Errorf("catalog file must be an absolute path")
	}
	if len(ref.SHA256) != sha256.Size*2 {
		return nil, zero, fmt.Errorf("catalog %q SHA-256 digest must be %d hex characters", ref.File, sha256.Size*2)
	}
	expected, err := hex.DecodeString(ref.SHA256)
	if err != nil {
		return nil, zero, fmt.Errorf("catalog %q SHA-256 digest: %w", ref.File, err)
	}
	file, err := os.Open(ref.File)
	if err != nil {
		return nil, zero, fmt.Errorf("open catalog %q: %w", ref.File, err)
	}
	defer file.Close()
	if info, statErr := file.Stat(); statErr == nil && info.Size() > int64(maxBytes) {
		return nil, zero, fmt.Errorf("catalog %q exceeds %d bytes", ref.File, maxBytes)
	}
	data, err := io.ReadAll(io.LimitReader(file, int64(maxBytes)+1))
	if err != nil {
		return nil, zero, fmt.Errorf("read catalog %q: %w", ref.File, err)
	}
	if len(data) > maxBytes {
		return nil, zero, fmt.Errorf("catalog %q exceeds %d bytes", ref.File, maxBytes)
	}
	digest := sha256.Sum256(data)
	if subtle.ConstantTimeCompare(digest[:], expected) != 1 {
		return nil, zero, fmt.Errorf("catalog %q SHA-256 digest mismatch: got %s", ref.File, hex.EncodeToString(digest[:]))
	}
	return data, digest, nil
}

func decodeStrict(data []byte, out any) error {
	if len(bytes.TrimSpace(data)) == 0 {
		return fmt.Errorf("document is empty")
	}
	if err := yaml.Load(data, out, yaml.WithKnownFields(), yaml.WithUniqueKeys(), yaml.WithSingleDocument()); err != nil {
		return fmt.Errorf("strict YAML: %w", err)
	}
	return nil
}

func endpointFamily(value string) provider.Family {
	switch value {
	case "azure_openai_responses":
		return provider.FamilyOpenAIResponses
	case "azure_openai_chat":
		return provider.FamilyOpenAIChat
	case "anthropic_aws_messages":
		return provider.FamilyAnthropicMessages
	case "bedrock_anthropic_messages":
		return provider.FamilyBedrockMessages
	case "bedrock_converse":
		return provider.FamilyBedrockConverse
	default:
		return provider.Family(value)
	}
}

func validateIdentifier(value, field string) error {
	if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\r\n") {
		return fmt.Errorf("%s must be a non-empty single-line identifier", field)
	}
	return nil
}

func validateServiceClass(value string, field string) (llm.ServiceClass, error) {
	switch value {
	case string(llm.ServiceClassEconomy), string(llm.ServiceClassStandard), string(llm.ServiceClassPriority):
		return llm.ServiceClass(value), nil
	default:
		return "", fmt.Errorf("%s must be economy, standard, or priority", field)
	}
}
