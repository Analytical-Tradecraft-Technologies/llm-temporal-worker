// Package modelsync makes the models OpenRouter lists routable under their
// OpenRouter IDs, priced from OpenRouter's published endpoint prices.
//
// One worker at a time fetches OpenRouter's model and endpoint lists into a
// Document and publishes it to shared state storage. Every worker installs the
// latest published Document into its engine snapshot, combining it with the
// model-sync rules to derive OpenRouter routes and, for the providers whose
// endpoints are configured, direct routes that share the OpenRouter model ID.
//
// The Document holds only what OpenRouter's API returned. Rules and endpoint
// configuration are applied by every worker when the Document is installed, so
// a configuration change never requires a refetch.
package modelsync

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

// DocumentSchema versions the stored Document. A reader rejects any other
// value instead of guessing at a different shape.
const DocumentSchema = "openrouter-catalog/v1"

// Document bounds. OpenRouter lists a few hundred models with a handful of
// endpoints each; the bounds leave room for growth while keeping one stored
// value small enough to move through Redis in a single command.
const (
	MaxDocumentBytes  = 8 << 20
	MaxModels         = 4096
	MaxEndpoints      = 64
	MaxOverrides      = 8
	MaxIdentifierSize = 256
	maxListEntries    = 64
)

// Document is one complete OpenRouter catalog observation.
type Document struct {
	Schema    string    `json:"schema"`
	FetchedAt time.Time `json:"fetched_at"`
	Models    []Model   `json:"models"`
}

// Model is one OpenRouter model and the upstream endpoints serving it.
type Model struct {
	ID                  string     `json:"id"`
	ContextLength       int64      `json:"context_length,omitempty"`
	MaxCompletionTokens int64      `json:"max_completion_tokens,omitempty"`
	InputModalities     []string   `json:"input_modalities,omitempty"`
	SupportedParameters []string   `json:"supported_parameters,omitempty"`
	Endpoints           []Endpoint `json:"endpoints"`
}

// Endpoint is one upstream provider endpoint OpenRouter can route a model to.
// Tag is OpenRouter's stable endpoint identifier, for example "openai" or
// "openai/flex".
type Endpoint struct {
	Tag                 string  `json:"tag"`
	ContextLength       int64   `json:"context_length,omitempty"`
	MaxCompletionTokens int64   `json:"max_completion_tokens,omitempty"`
	MaxPromptTokens     int64   `json:"max_prompt_tokens,omitempty"`
	Pricing             Pricing `json:"pricing"`
}

// Pricing holds OpenRouter's per-token (and per-request) USD decimal strings
// exactly as published. An empty string means OpenRouter did not list the
// component.
type Pricing struct {
	Prompt            string            `json:"prompt"`
	Completion        string            `json:"completion"`
	InputCacheRead    string            `json:"input_cache_read,omitempty"`
	InputCacheWrite   string            `json:"input_cache_write,omitempty"`
	InputCacheWrite1h string            `json:"input_cache_write_1h,omitempty"`
	Request           string            `json:"request,omitempty"`
	Overrides         []PricingOverride `json:"overrides,omitempty"`
}

// PricingOverride replaces prices for prompts of at least MinPromptTokens.
// Components it omits keep the base price.
type PricingOverride struct {
	MinPromptTokens   int64  `json:"min_prompt_tokens"`
	Prompt            string `json:"prompt,omitempty"`
	Completion        string `json:"completion,omitempty"`
	InputCacheRead    string `json:"input_cache_read,omitempty"`
	InputCacheWrite   string `json:"input_cache_write,omitempty"`
	InputCacheWrite1h string `json:"input_cache_write_1h,omitempty"`
}

// Normalize sorts the Document into its canonical order so equal content has
// equal bytes and digest.
func (document *Document) Normalize() {
	document.FetchedAt = document.FetchedAt.UTC()
	sort.Slice(document.Models, func(i, j int) bool { return document.Models[i].ID < document.Models[j].ID })
	for index := range document.Models {
		model := &document.Models[index]
		sort.Strings(model.InputModalities)
		sort.Strings(model.SupportedParameters)
		sort.Slice(model.Endpoints, func(i, j int) bool { return model.Endpoints[i].Tag < model.Endpoints[j].Tag })
		for endpointIndex := range model.Endpoints {
			overrides := model.Endpoints[endpointIndex].Pricing.Overrides
			sort.Slice(overrides, func(i, j int) bool { return overrides[i].MinPromptTokens < overrides[j].MinPromptTokens })
		}
	}
}

// Validate checks the Document's shape and bounds. It never consults the
// clock: freshness is a caller decision.
func (document Document) Validate() error {
	if document.Schema != DocumentSchema {
		return fmt.Errorf("model catalog schema %q is not %q", document.Schema, DocumentSchema)
	}
	if document.FetchedAt.IsZero() {
		return fmt.Errorf("model catalog fetched_at is required")
	}
	if len(document.Models) == 0 || len(document.Models) > MaxModels {
		return fmt.Errorf("model catalog must list between 1 and %d models", MaxModels)
	}
	for index, model := range document.Models {
		if index > 0 && document.Models[index-1].ID >= model.ID {
			return fmt.Errorf("model catalog models are not sorted and unique at %q", model.ID)
		}
		if err := model.validate(); err != nil {
			return fmt.Errorf("model %q: %w", model.ID, err)
		}
	}
	return nil
}

func (model Model) validate() error {
	if err := validateIdentifier(model.ID); err != nil {
		return err
	}
	if model.ContextLength < 0 || model.MaxCompletionTokens < 0 {
		return fmt.Errorf("token limits must not be negative")
	}
	if len(model.InputModalities) > maxListEntries || len(model.SupportedParameters) > maxListEntries {
		return fmt.Errorf("model lists more than %d modalities or parameters", maxListEntries)
	}
	for _, value := range append(append([]string(nil), model.InputModalities...), model.SupportedParameters...) {
		if err := validateIdentifier(value); err != nil {
			return err
		}
	}
	if len(model.Endpoints) == 0 || len(model.Endpoints) > MaxEndpoints {
		return fmt.Errorf("model must list between 1 and %d endpoints", MaxEndpoints)
	}
	for index, endpoint := range model.Endpoints {
		if index > 0 && model.Endpoints[index-1].Tag >= endpoint.Tag {
			return fmt.Errorf("endpoints are not sorted and unique at %q", endpoint.Tag)
		}
		if err := endpoint.validate(); err != nil {
			return fmt.Errorf("endpoint %q: %w", endpoint.Tag, err)
		}
	}
	return nil
}

func (endpoint Endpoint) validate() error {
	if err := validateIdentifier(endpoint.Tag); err != nil {
		return err
	}
	if endpoint.ContextLength < 0 || endpoint.MaxCompletionTokens < 0 || endpoint.MaxPromptTokens < 0 {
		return fmt.Errorf("token limits must not be negative")
	}
	return endpoint.Pricing.validate()
}

func (prices Pricing) validate() error {
	if prices.Prompt == "" || prices.Completion == "" {
		return fmt.Errorf("prompt and completion prices are required")
	}
	for _, value := range []string{prices.Prompt, prices.Completion, prices.InputCacheRead, prices.InputCacheWrite, prices.InputCacheWrite1h, prices.Request} {
		if err := validatePrice(value); err != nil {
			return err
		}
	}
	if len(prices.Overrides) > MaxOverrides {
		return fmt.Errorf("more than %d price overrides", MaxOverrides)
	}
	for index, override := range prices.Overrides {
		if override.MinPromptTokens <= 0 {
			return fmt.Errorf("price override min_prompt_tokens must be positive")
		}
		if index > 0 && prices.Overrides[index-1].MinPromptTokens >= override.MinPromptTokens {
			return fmt.Errorf("price overrides are not sorted and unique")
		}
		for _, value := range []string{override.Prompt, override.Completion, override.InputCacheRead, override.InputCacheWrite, override.InputCacheWrite1h} {
			if err := validatePrice(value); err != nil {
				return err
			}
		}
	}
	return nil
}

// validatePrice accepts an empty (unlisted) price or an exact non-negative
// decimal that can be scaled to a per-million price.
func validatePrice(value string) error {
	if value == "" {
		return nil
	}
	_, err := perMillion(value)
	return err
}

func validateIdentifier(value string) error {
	if value == "" || len(value) > MaxIdentifierSize || strings.TrimSpace(value) != value {
		return fmt.Errorf("identifier %q must be 1-%d bytes without surrounding space", value, MaxIdentifierSize)
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return fmt.Errorf("identifier %q must be printable ASCII without spaces", value)
		}
	}
	return nil
}

// Encode returns the canonical JSON bytes of a valid Document.
func (document Document) Encode() ([]byte, error) {
	document.Normalize()
	if err := document.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	if len(encoded) > MaxDocumentBytes {
		return nil, fmt.Errorf("model catalog exceeds %d bytes", MaxDocumentBytes)
	}
	return encoded, nil
}

// DecodeDocument strictly decodes and validates stored Document bytes.
func DecodeDocument(data []byte) (Document, error) {
	if len(data) == 0 || len(data) > MaxDocumentBytes {
		return Document{}, fmt.Errorf("model catalog must be between 1 and %d bytes", MaxDocumentBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var document Document
	if err := decoder.Decode(&document); err != nil {
		return Document{}, fmt.Errorf("decode model catalog: %w", err)
	}
	if decoder.More() {
		return Document{}, fmt.Errorf("decode model catalog: trailing data")
	}
	if err := document.Validate(); err != nil {
		return Document{}, err
	}
	return document, nil
}

// Digest is the lowercase hex SHA-256 of canonical Document bytes.
func Digest(encoded []byte) string {
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// perMillion converts OpenRouter's per-token USD decimal to the exact
// per-million decimal the price catalog uses, by moving the decimal point.
func perMillion(perToken string) (pricing.DecimalUSD, error) {
	parsed, err := pricing.ParseDecimalUSD(perToken)
	if err != nil {
		return pricing.DecimalUSD{}, err
	}
	canonical := parsed.CanonicalString()
	whole, fraction, _ := strings.Cut(canonical, ".")
	for len(fraction) < 6 {
		fraction += "0"
	}
	whole, fraction = whole+fraction[:6], fraction[6:]
	whole = strings.TrimLeft(whole, "0")
	if whole == "" {
		whole = "0"
	}
	scaled := whole
	if fraction != "" {
		scaled += "." + fraction
	}
	return pricing.ParseDecimalUSD(scaled)
}
