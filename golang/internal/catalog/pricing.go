package catalog

import (
	"fmt"
	"strings"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/pricing"
	yaml "go.yaml.in/yaml/v4"
)

type pricingDocument struct {
	Version string             `yaml:"version"`
	ID      string             `yaml:"id"`
	Entries []pricingEntryFile `yaml:"entries"`
}

type pricingEntryFile struct {
	Provider       string       `yaml:"provider"`
	EndpointID     string       `yaml:"endpoint_id"`
	Endpoint       string       `yaml:"endpoint"`
	Family         string       `yaml:"family"`
	EndpointFamily string       `yaml:"endpoint_family"`
	Region         string       `yaml:"region"`
	Model          string       `yaml:"model"`
	ProviderTier   string       `yaml:"provider_tier"`
	ServiceClass   string       `yaml:"service_class"`
	Input          decimalValue `yaml:"input_per_million"`
	Output         decimalValue `yaml:"output_per_million"`
	CacheRead      decimalValue `yaml:"cache_read_per_million"`
	CacheWrite     decimalValue `yaml:"cache_write_per_million"`
	Reasoning      decimalValue `yaml:"reasoning_per_million"`
	PerRequest     decimalValue `yaml:"per_request"`
	// MaxPromptTokens is the largest prompt the quote covers, for providers
	// that bill a higher tier above a prompt-size threshold.
	MaxPromptTokens int64     `yaml:"max_prompt_tokens"`
	EffectiveFrom   time.Time `yaml:"effective_from"`
	EffectiveUntil  time.Time `yaml:"effective_until"`
	Source          string    `yaml:"source"`
	Provenance      string    `yaml:"provenance"`
	Version         string    `yaml:"version"`
}

type decimalValue struct {
	value pricing.DecimalUSD
	set   bool
}

func (decimal *decimalValue) UnmarshalYAML(node *yaml.Node) error {
	if decimal == nil {
		return fmt.Errorf("decimal target is nil")
	}
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return fmt.Errorf("price must be a quoted decimal string")
	}
	parsed, err := pricing.ParseDecimalUSD(node.Value)
	if err != nil {
		return err
	}
	decimal.value = parsed
	decimal.set = true
	return nil
}

func compilePricing(document pricingDocument) (PricingCatalog, error) {
	if err := validateIdentifier(document.Version, "version"); err != nil {
		return PricingCatalog{}, err
	}
	id := strings.TrimSpace(document.ID)
	if id == "" {
		// The local fixture predates the explicit id field and uses version as
		// its immutable catalog reference. Keep that shape deterministic.
		id = document.Version
	}
	if err := validateIdentifier(id, "id"); err != nil {
		return PricingCatalog{}, err
	}
	if len(document.Entries) == 0 {
		return PricingCatalog{}, fmt.Errorf("entries must not be empty")
	}
	entries := make([]pricing.Entry, 0, len(document.Entries))
	seen := make(map[string]struct{}, len(document.Entries))
	for index, fileEntry := range document.Entries {
		entry, err := compilePricingEntry(document, id, index, fileEntry)
		if err != nil {
			return PricingCatalog{}, err
		}
		key := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s", entry.Provider, entry.Family, entry.EndpointID, entry.Region, entry.Model, entry.ProviderTier, entry.EffectiveFrom.UTC().Format(time.RFC3339Nano))
		if _, exists := seen[key]; exists {
			return PricingCatalog{}, fmt.Errorf("entries[%d] duplicates pricing identity", index)
		}
		seen[key] = struct{}{}
		entries = append(entries, entry)
	}
	compiled, err := pricing.CompileUSD(document.Version, entries)
	if err != nil {
		return PricingCatalog{}, err
	}
	return PricingCatalog{ID: id, Catalog: compiled}, nil
}

func compilePricingEntry(document pricingDocument, catalogID string, index int, fileEntry pricingEntryFile) (pricing.Entry, error) {
	path := fmt.Sprintf("entries[%d]", index)
	providerName := strings.TrimSpace(fileEntry.Provider)
	endpointID := strings.TrimSpace(fileEntry.EndpointID)
	endpointAlias := strings.TrimSpace(fileEntry.Endpoint)
	if endpointID != "" && endpointAlias != "" && endpointID != endpointAlias {
		return pricing.Entry{}, fmt.Errorf("%s.endpoint_id and endpoint disagree", path)
	}
	if endpointID == "" {
		endpointID = endpointAlias
	}
	if providerName == "" {
		// The local fixture's endpoint name is also its provider identity. A
		// production document should set provider explicitly so accounting is
		// not coupled to a deployment name.
		providerName = endpointID
	}
	if err := validateIdentifier(providerName, path+".provider"); err != nil {
		return pricing.Entry{}, err
	}
	if err := validateIdentifier(endpointID, path+".endpoint_id"); err != nil {
		return pricing.Entry{}, err
	}
	familyValue := strings.TrimSpace(fileEntry.Family)
	familyAlias := strings.TrimSpace(fileEntry.EndpointFamily)
	if familyValue != "" && familyAlias != "" && endpointFamily(familyValue) != endpointFamily(familyAlias) {
		return pricing.Entry{}, fmt.Errorf("%s.family and endpoint_family disagree", path)
	}
	if familyValue == "" {
		familyValue = familyAlias
	}
	family := endpointFamily(familyValue)
	if !family.Valid() {
		return pricing.Entry{}, fmt.Errorf("%s.family %q is unsupported", path, familyValue)
	}
	if err := validateIdentifier(strings.TrimSpace(fileEntry.Region), path+".region"); err != nil {
		return pricing.Entry{}, err
	}
	if err := validateIdentifier(strings.TrimSpace(fileEntry.Model), path+".model"); err != nil {
		return pricing.Entry{}, err
	}
	providerTier := strings.TrimSpace(fileEntry.ProviderTier)
	if fileEntry.ServiceClass != "" {
		class, err := validateServiceClass(fileEntry.ServiceClass, path+".service_class")
		if err != nil {
			return pricing.Entry{}, err
		}
		if providerTier == "" {
			providerTier = string(class)
		}
	}
	if err := validateIdentifier(providerTier, path+".provider_tier"); err != nil {
		return pricing.Entry{}, err
	}
	if fileEntry.MaxPromptTokens < 0 {
		return pricing.Entry{}, fmt.Errorf("%s.max_prompt_tokens must not be negative", path)
	}
	if !fileEntry.EffectiveFrom.IsZero() && !fileEntry.EffectiveUntil.IsZero() && !fileEntry.EffectiveUntil.After(fileEntry.EffectiveFrom) {
		return pricing.Entry{}, fmt.Errorf("%s effective interval is empty", path)
	}
	provenance := strings.TrimSpace(fileEntry.Provenance)
	source := strings.TrimSpace(fileEntry.Source)
	if provenance != "" && source != "" && provenance != source {
		return pricing.Entry{}, fmt.Errorf("%s.provenance and source disagree", path)
	}
	if provenance == "" {
		provenance = source
	}
	entryVersion := strings.TrimSpace(fileEntry.Version)
	if entryVersion == "" {
		entryVersion = catalogID
	}
	unknownComponents := make([]pricing.PriceComponent, 0, 6)
	if !fileEntry.Input.set {
		unknownComponents = append(unknownComponents, pricing.PriceComponentInput)
	}
	if !fileEntry.Output.set {
		unknownComponents = append(unknownComponents, pricing.PriceComponentOutput)
	}
	if !fileEntry.CacheRead.set {
		unknownComponents = append(unknownComponents, pricing.PriceComponentCacheRead)
	}
	if !fileEntry.CacheWrite.set {
		unknownComponents = append(unknownComponents, pricing.PriceComponentCacheWrite)
	}
	if !fileEntry.Reasoning.set {
		unknownComponents = append(unknownComponents, pricing.PriceComponentReasoning)
	}
	if !fileEntry.PerRequest.set {
		unknownComponents = append(unknownComponents, pricing.PriceComponentPerRequest)
	}
	return pricing.Entry{
		Provider:          providerName,
		Family:            string(family),
		EndpointID:        endpointID,
		Region:            fileEntry.Region,
		Model:             fileEntry.Model,
		ProviderTier:      providerTier,
		Prices:            pricing.UnitPrices{InputPerMillion: fileEntry.Input.value, OutputPerMillion: fileEntry.Output.value, CacheReadPerMillion: fileEntry.CacheRead.value, CacheWritePerMillion: fileEntry.CacheWrite.value, ReasoningPerMillion: fileEntry.Reasoning.value, PerRequest: fileEntry.PerRequest.value},
		UnknownComponents: unknownComponents,
		MaxPromptTokens:   fileEntry.MaxPromptTokens,
		EffectiveFrom:     fileEntry.EffectiveFrom,
		EffectiveUntil:    fileEntry.EffectiveUntil,
		Provenance:        provenance,
		Version:           entryVersion,
	}, nil
}
