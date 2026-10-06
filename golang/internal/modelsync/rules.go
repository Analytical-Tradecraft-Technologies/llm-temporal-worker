package modelsync

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/mfow/llm-temporal-worker/golang/pricing"
	yaml "go.yaml.in/yaml/v4"
)

// RulesVersion is the only accepted rules document version.
const RulesVersion = "model-sync-rules/v1"

//go:embed rules/default.yaml
var defaultRules []byte

// DefaultRulesDocument returns the built-in rules file shipped in the binary.
func DefaultRulesDocument() []byte { return append([]byte(nil), defaultRules...) }

// Model ID transforms from the OpenRouter name (after the provider prefix) to
// the provider's own model ID.
const (
	ModelIDVerbatim     = "verbatim"
	ModelIDDotsToDashes = "dots_to_dashes"
)

// RulesDocument is one rules file.
type RulesDocument struct {
	Version string `yaml:"version"`
	// Exclude lists path.Match patterns of OpenRouter IDs that are never
	// routable, on OpenRouter or directly.
	Exclude   []string                 `yaml:"exclude"`
	Providers map[string]ProviderRules `yaml:"providers"`
}

// ProviderRules declares one provider's direct models. Every direct model is
// listed explicitly with its prices, so direct routes never depend on a
// fetched price.
type ProviderRules struct {
	Prefix string `yaml:"prefix"`
	// ModelID derives the provider model ID from the OpenRouter ID when a
	// model omits model.
	ModelID string `yaml:"model_id"`
	// Models are keyed by OpenRouter ID (or, for models OpenRouter does not
	// list, an ID under Prefix).
	Models map[string]DirectModel `yaml:"models"`
}

// DirectModel is one model a provider's direct endpoints serve.
type DirectModel struct {
	// Model is the provider model ID; empty applies the ModelID transform.
	Model string `yaml:"model"`
	// ContextTokens bounds input plus reserved output; zero is unspecified.
	ContextTokens int64 `yaml:"context_tokens"`
	OutputTokens  int64 `yaml:"output_tokens"`
	// Exclude removes the direct route; an OpenRouter route remains.
	Exclude bool `yaml:"exclude"`
	// Prices are keyed by the endpoint's service-class provider_value. A
	// class whose tier has no price is not offered on the direct route.
	Prices map[string]TierPrices `yaml:"prices"`
}

// TierPrices are exact per-million (and per-request) USD decimals. Every
// component is required: a direct model is never partially priced.
// Reasoning is billed inside output on every supported family.
type TierPrices struct {
	Input      string `yaml:"input_per_million"`
	Output     string `yaml:"output_per_million"`
	CacheRead  string `yaml:"cache_read_per_million"`
	CacheWrite string `yaml:"cache_write_per_million"`
	PerRequest string `yaml:"per_request"`
}

// Rules is the merged, validated rule set.
type Rules struct {
	Exclude   []string
	Providers map[string]ProviderRules
	// Digest identifies the merged rule set; it versions extra-model prices.
	Digest string
}

// ParseRules strictly decodes one rules file.
func ParseRules(data []byte) (RulesDocument, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return RulesDocument{}, fmt.Errorf("model-sync rules document is empty")
	}
	var document RulesDocument
	if err := yaml.Load(data, &document, yaml.WithKnownFields(), yaml.WithUniqueKeys()); err != nil {
		return RulesDocument{}, fmt.Errorf("model-sync rules: strict YAML: %w", err)
	}
	if document.Version != RulesVersion {
		return RulesDocument{}, fmt.Errorf("model-sync rules version %q is not %q", document.Version, RulesVersion)
	}
	return document, nil
}

// MergeRules layers documents in order over each other. Exclusions
// accumulate; a provider's scalar fields are replaced when a later document
// sets them, and its maps are merged key by key.
func MergeRules(documents ...RulesDocument) (Rules, error) {
	merged := Rules{Providers: map[string]ProviderRules{}}
	digest := sha256.New()
	for _, document := range documents {
		encoded, err := yaml.Marshal(document)
		if err != nil {
			return Rules{}, err
		}
		digest.Write(encoded)
		merged.Exclude = append(merged.Exclude, document.Exclude...)
		names := make([]string, 0, len(document.Providers))
		for name := range document.Providers {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			layer := document.Providers[name]
			current := merged.Providers[name]
			if layer.Prefix != "" {
				current.Prefix = layer.Prefix
			}
			if layer.ModelID != "" {
				current.ModelID = layer.ModelID
			}
			current.Models = mergeModels(current.Models, layer.Models)
			merged.Providers[name] = current
		}
	}
	merged.Digest = hex.EncodeToString(digest.Sum(nil))
	if err := merged.validate(); err != nil {
		return Rules{}, err
	}
	return merged, nil
}

func mergeMap[V any](base, layer map[string]V) map[string]V {
	if len(base) == 0 && len(layer) == 0 {
		return nil
	}
	result := make(map[string]V, len(base)+len(layer))
	for key, value := range base {
		result[key] = value
	}
	for key, value := range layer {
		result[key] = value
	}
	return result
}

// mergeModels layers model rules field by field, so an override can change
// one price or limit without restating the model. Prices merge per tier.
func mergeModels(base, layer map[string]DirectModel) map[string]DirectModel {
	result := mergeMap(base, nil)
	if result == nil && len(layer) > 0 {
		result = make(map[string]DirectModel, len(layer))
	}
	for id, model := range layer {
		current, exists := result[id]
		if !exists {
			result[id] = model
			continue
		}
		if model.Model != "" {
			current.Model = model.Model
		}
		if model.ContextTokens != 0 {
			current.ContextTokens = model.ContextTokens
		}
		if model.OutputTokens != 0 {
			current.OutputTokens = model.OutputTokens
		}
		current.Exclude = model.Exclude
		current.Prices = mergeMap(current.Prices, model.Prices)
		result[id] = current
	}
	return result
}

func (rules Rules) validate() error {
	for _, pattern := range rules.Exclude {
		if _, err := path.Match(pattern, ""); err != nil || pattern == "" {
			return fmt.Errorf("model-sync rules exclude pattern %q is invalid", pattern)
		}
	}
	prefixes := map[string]string{}
	for name, provider := range rules.Providers {
		if err := validateIdentifier(name); err != nil {
			return fmt.Errorf("model-sync rules provider: %w", err)
		}
		if !strings.HasSuffix(provider.Prefix, "/") || validateIdentifier(provider.Prefix) != nil || strings.Count(provider.Prefix, "/") != 1 {
			return fmt.Errorf("model-sync rules provider %q prefix must be one path segment ending in /", name)
		}
		if other, duplicate := prefixes[provider.Prefix]; duplicate {
			return fmt.Errorf("model-sync rules providers %q and %q share prefix %q", other, name, provider.Prefix)
		}
		prefixes[provider.Prefix] = name
		switch provider.ModelID {
		case ModelIDVerbatim, ModelIDDotsToDashes:
		default:
			return fmt.Errorf("model-sync rules provider %q model_id must be %s or %s", name, ModelIDVerbatim, ModelIDDotsToDashes)
		}
		for id, model := range provider.Models {
			if !strings.HasPrefix(id, provider.Prefix) || validateIdentifier(id) != nil {
				return fmt.Errorf("model-sync rules provider %q model %q must start with %q", name, id, provider.Prefix)
			}
			if model.Model != "" && validateIdentifier(model.Model) != nil {
				return fmt.Errorf("model-sync rules provider %q model %q mapping is invalid", name, id)
			}
			if model.ContextTokens < 0 || model.OutputTokens < 0 {
				return fmt.Errorf("model-sync rules provider %q model %q token limits must not be negative", name, id)
			}
			if !model.Exclude && len(model.Prices) == 0 {
				return fmt.Errorf("model-sync rules provider %q model %q has no prices", name, id)
			}
			for tier, prices := range model.Prices {
				if validateIdentifier(tier) != nil {
					return fmt.Errorf("model-sync rules provider %q model %q tier %q is invalid", name, id, tier)
				}
				if _, err := prices.unitPrices(); err != nil {
					return fmt.Errorf("model-sync rules provider %q model %q tier %q: %w", name, id, tier, err)
				}
			}
		}
	}
	return nil
}

func (prices TierPrices) unitPrices() (pricing.UnitPrices, error) {
	var result pricing.UnitPrices
	fields := []struct {
		name   string
		value  string
		target *pricing.DecimalUSD
	}{
		{"input_per_million", prices.Input, &result.InputPerMillion},
		{"output_per_million", prices.Output, &result.OutputPerMillion},
		{"cache_read_per_million", prices.CacheRead, &result.CacheReadPerMillion},
		{"cache_write_per_million", prices.CacheWrite, &result.CacheWritePerMillion},
		{"per_request", prices.PerRequest, &result.PerRequest},
	}
	for _, field := range fields {
		parsed, err := pricing.ParseDecimalUSD(field.value)
		if err != nil {
			return pricing.UnitPrices{}, fmt.Errorf("price %s: %w", field.name, err)
		}
		*field.target = parsed
	}
	result.ReasoningPerMillion = pricing.MustDecimalUSD("0")
	return result, nil
}

// Excluded reports whether an OpenRouter ID is excluded from routing.
func (rules Rules) Excluded(id string) bool {
	for _, pattern := range rules.Exclude {
		if matched, _ := path.Match(pattern, id); matched {
			return true
		}
	}
	return false
}

// Direct returns the direct model rule for OpenRouter ID id with its provider
// model ID resolved, or false when the provider does not serve it directly.
func (rules Rules) Direct(providerName, id string) (DirectModel, bool) {
	provider, ok := rules.Providers[providerName]
	if !ok || !strings.HasPrefix(id, provider.Prefix) {
		return DirectModel{}, false
	}
	model, listed := provider.Models[id]
	if !listed || model.Exclude {
		return DirectModel{}, false
	}
	if model.Model == "" {
		name := strings.TrimPrefix(id, provider.Prefix)
		if provider.ModelID == ModelIDDotsToDashes {
			name = strings.ReplaceAll(name, ".", "-")
		}
		model.Model = name
	}
	return model, true
}
