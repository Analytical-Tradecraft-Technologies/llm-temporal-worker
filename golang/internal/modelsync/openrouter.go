package modelsync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultOpenRouterBaseURL is OpenRouter's public API root.
	DefaultOpenRouterBaseURL = "https://openrouter.ai/api/v1"
	maxResponseBytes         = 16 << 20
	defaultFetchConcurrency  = 8
	// maxEndpointFailureRatio bounds how many per-model endpoint lookups may
	// fail before the whole fetch is rejected. A rejected fetch keeps the
	// previously published Document, so an OpenRouter incident never shrinks
	// the routable model set by more than this share.
	maxEndpointFailureRatio = 0.1
)

// Fetcher reads OpenRouter's public model and endpoint lists.
type Fetcher struct {
	// Client must already enforce the deployment's outbound policy.
	Client  *http.Client
	BaseURL string
	// APIKey is optional; OpenRouter's model lists are public.
	APIKey      string
	Concurrency int
	Clock       func() time.Time
}

// Fetch returns a complete, normalized Document. Models whose output is not
// text-only (image and audio generators), models OpenRouter lists without a
// usable price (for example its own variable-priced routers) and alias or
// variant IDs (prefixed with "~" or containing ":") are skipped.
func (fetcher Fetcher) Fetch(ctx context.Context) (Document, error) {
	if fetcher.Client == nil {
		return Document{}, fmt.Errorf("OpenRouter fetch requires an HTTP client")
	}
	clock := fetcher.Clock
	if clock == nil {
		clock = time.Now
	}
	listed, err := fetcher.listModels(ctx)
	if err != nil {
		return Document{}, err
	}
	concurrency := fetcher.Concurrency
	if concurrency <= 0 {
		concurrency = defaultFetchConcurrency
	}
	type result struct {
		model Model
		ok    bool
		err   error
	}
	results := make([]result, len(listed))
	work := make(chan int)
	var group sync.WaitGroup
	for range concurrency {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range work {
				endpoints, err := fetcher.listEndpoints(ctx, listed[index].ID)
				if err != nil {
					results[index] = result{err: err}
					continue
				}
				model := listed[index]
				model.Endpoints = endpoints
				results[index] = result{model: model, ok: len(endpoints) > 0}
			}
		}()
	}
	for index := range listed {
		if ctx.Err() != nil {
			break
		}
		work <- index
	}
	close(work)
	group.Wait()
	if err := ctx.Err(); err != nil {
		return Document{}, err
	}
	document := Document{Schema: DocumentSchema, FetchedAt: clock().UTC(), Models: make([]Model, 0, len(listed))}
	failures := 0
	for _, value := range results {
		switch {
		case value.err != nil:
			failures++
		case value.ok:
			document.Models = append(document.Models, value.model)
		}
	}
	if len(listed) > 0 && float64(failures) > float64(len(listed))*maxEndpointFailureRatio {
		return Document{}, fmt.Errorf("OpenRouter endpoint lookup failed for %d of %d models", failures, len(listed))
	}
	if len(document.Models) > MaxModels {
		return Document{}, fmt.Errorf("OpenRouter lists more than %d priced models", MaxModels)
	}
	document.Normalize()
	if err := document.Validate(); err != nil {
		return Document{}, err
	}
	return document, nil
}

type wireModel struct {
	ID            string `json:"id"`
	ContextLength int64  `json:"context_length"`
	Architecture  struct {
		InputModalities  []string `json:"input_modalities"`
		OutputModalities []string `json:"output_modalities"`
	} `json:"architecture"`
	TopProvider struct {
		MaxCompletionTokens int64 `json:"max_completion_tokens"`
	} `json:"top_provider"`
	SupportedParameters []string                   `json:"supported_parameters"`
	Pricing             map[string]json.RawMessage `json:"pricing"`
}

func (fetcher Fetcher) listModels(ctx context.Context) ([]Model, error) {
	var body struct {
		Data []wireModel `json:"data"`
	}
	if err := fetcher.get(ctx, "/models", &body); err != nil {
		return nil, fmt.Errorf("list OpenRouter models: %w", err)
	}
	models := make([]Model, 0, len(body.Data))
	seen := make(map[string]struct{}, len(body.Data))
	for _, wire := range body.Data {
		if !syncableModelID(wire.ID) || !textOutputOnly(wire.Architecture.OutputModalities) {
			continue
		}
		if _, duplicate := seen[wire.ID]; duplicate {
			continue
		}
		// The model-level price is only a filter here: a model OpenRouter
		// cannot price (for example a router quoted at -1) is never routable.
		if _, err := decodePricing(wire.Pricing); err != nil {
			continue
		}
		model := Model{ID: wire.ID, ContextLength: wire.ContextLength, MaxCompletionTokens: wire.TopProvider.MaxCompletionTokens,
			InputModalities: boundedIdentifiers(wire.Architecture.InputModalities), SupportedParameters: boundedIdentifiers(wire.SupportedParameters)}
		if model.ContextLength < 0 || model.MaxCompletionTokens < 0 {
			continue
		}
		seen[wire.ID] = struct{}{}
		models = append(models, model)
	}
	if len(models) == 0 {
		return nil, fmt.Errorf("list OpenRouter models: no priced models")
	}
	return models, nil
}

type wireEndpoint struct {
	Tag                 string                     `json:"tag"`
	ContextLength       int64                      `json:"context_length"`
	MaxCompletionTokens *int64                     `json:"max_completion_tokens"`
	MaxPromptTokens     *int64                     `json:"max_prompt_tokens"`
	Pricing             map[string]json.RawMessage `json:"pricing"`
}

func (fetcher Fetcher) listEndpoints(ctx context.Context, modelID string) ([]Endpoint, error) {
	segments := strings.Split(modelID, "/")
	for index, segment := range segments {
		segments[index] = url.PathEscape(segment)
	}
	var body struct {
		Data struct {
			Endpoints []wireEndpoint `json:"endpoints"`
		} `json:"data"`
	}
	if err := fetcher.get(ctx, "/models/"+strings.Join(segments, "/")+"/endpoints", &body); err != nil {
		return nil, err
	}
	endpoints := make([]Endpoint, 0, len(body.Data.Endpoints))
	seen := make(map[string]struct{}, len(body.Data.Endpoints))
	for _, wire := range body.Data.Endpoints {
		if validateIdentifier(wire.Tag) != nil {
			continue
		}
		if _, duplicate := seen[wire.Tag]; duplicate {
			continue
		}
		prices, err := decodePricing(wire.Pricing)
		if err != nil {
			continue
		}
		endpoint := Endpoint{Tag: wire.Tag, ContextLength: wire.ContextLength, Pricing: prices}
		if wire.MaxCompletionTokens != nil {
			endpoint.MaxCompletionTokens = *wire.MaxCompletionTokens
		}
		if wire.MaxPromptTokens != nil {
			endpoint.MaxPromptTokens = *wire.MaxPromptTokens
		}
		if endpoint.validate() != nil {
			continue
		}
		seen[wire.Tag] = struct{}{}
		endpoints = append(endpoints, endpoint)
		if len(endpoints) == MaxEndpoints {
			break
		}
	}
	return endpoints, nil
}

// textOutputOnly keeps models whose only output is text. Image, audio and
// other media generators are never routable: the unified response carries
// text, and their media pricing has no catalog component. A model that only
// accepts media as input is an ordinary chat model and stays.
func textOutputOnly(modalities []string) bool {
	return len(modalities) == 1 && modalities[0] == "text"
}

// syncableModelID excludes OpenRouter alias ("~vendor/...") and variant
// ("vendor/model:batch") IDs: they repeat a base model under different
// routing or billing semantics that the catalog does not model.
func syncableModelID(id string) bool {
	return validateIdentifier(id) == nil && strings.Contains(id, "/") && !strings.HasPrefix(id, "~") && !strings.Contains(id, ":")
}

func boundedIdentifiers(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if validateIdentifier(value) == nil && len(result) < maxListEntries {
			result = append(result, value)
		}
	}
	return result
}

// decodePricing reads OpenRouter's price object, which publishes decimals as
// strings. A number is accepted only when its literal is a plain decimal, so
// no price passes through binary floating point.
func decodePricing(raw map[string]json.RawMessage) (Pricing, error) {
	read := func(name string) (string, error) {
		value, present := raw[name]
		if !present || string(value) == "null" {
			return "", nil
		}
		return decimalLiteral(value)
	}
	var prices Pricing
	var err error
	fields := []struct {
		name   string
		target *string
	}{
		{"prompt", &prices.Prompt}, {"completion", &prices.Completion}, {"input_cache_read", &prices.InputCacheRead},
		{"input_cache_write", &prices.InputCacheWrite}, {"input_cache_write_1h", &prices.InputCacheWrite1h}, {"request", &prices.Request},
	}
	for _, field := range fields {
		if *field.target, err = read(field.name); err != nil {
			return Pricing{}, fmt.Errorf("price %s: %w", field.name, err)
		}
	}
	if overrides, present := raw["overrides"]; present && string(overrides) != "null" {
		var wire []map[string]json.RawMessage
		if err := json.Unmarshal(overrides, &wire); err != nil {
			return Pricing{}, fmt.Errorf("price overrides: %w", err)
		}
		for _, item := range wire {
			var override PricingOverride
			if err := json.Unmarshal(item["min_prompt_tokens"], &override.MinPromptTokens); err != nil {
				return Pricing{}, fmt.Errorf("price override min_prompt_tokens: %w", err)
			}
			targets := []struct {
				name   string
				target *string
			}{
				{"prompt", &override.Prompt}, {"completion", &override.Completion}, {"input_cache_read", &override.InputCacheRead},
				{"input_cache_write", &override.InputCacheWrite}, {"input_cache_write_1h", &override.InputCacheWrite1h},
			}
			for _, field := range targets {
				value, present := item[field.name]
				if !present || string(value) == "null" {
					continue
				}
				if *field.target, err = decimalLiteral(value); err != nil {
					return Pricing{}, fmt.Errorf("price override %s: %w", field.name, err)
				}
			}
			prices.Overrides = append(prices.Overrides, override)
		}
	}
	if err := prices.validateUnsorted(); err != nil {
		return Pricing{}, err
	}
	return prices, nil
}

// validateUnsorted validates fetched prices before Normalize orders overrides.
func (prices Pricing) validateUnsorted() error {
	sorted := prices
	sorted.Overrides = append([]PricingOverride(nil), prices.Overrides...)
	sort.Slice(sorted.Overrides, func(i, j int) bool { return sorted.Overrides[i].MinPromptTokens < sorted.Overrides[j].MinPromptTokens })
	return sorted.validate()
}

func decimalLiteral(raw json.RawMessage) (string, error) {
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text, validatePrice(text)
	}
	literal := strings.TrimSpace(string(raw))
	if literal == "" || strings.ContainsAny(literal, "eE+-") {
		return "", fmt.Errorf("price is not a non-negative plain decimal")
	}
	return literal, validatePrice(literal)
}

var errUnexpectedStatus = errors.New("unexpected OpenRouter status")

func (fetcher Fetcher) get(ctx context.Context, path string, target any) error {
	base := strings.TrimRight(fetcher.BaseURL, "/")
	if base == "" {
		base = DefaultOpenRouterBaseURL
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	if fetcher.APIKey != "" {
		request.Header.Set("Authorization", "Bearer "+fetcher.APIKey)
	}
	response, err := fetcher.Client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%w %d", errUnexpectedStatus, response.StatusCode)
	}
	if len(data) > maxResponseBytes {
		return fmt.Errorf("OpenRouter response exceeds %d bytes", maxResponseBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode OpenRouter response: %w", err)
	}
	return nil
}
