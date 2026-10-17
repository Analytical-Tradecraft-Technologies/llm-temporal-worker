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
	// previously published Document; in an accepted fetch, a model whose
	// lookup failed keeps its previously published entry.
	maxEndpointFailureRatio = 0.1
	// endpointLookupAttempts bounds attempts per endpoint lookup when
	// OpenRouter answers 429 or 5xx.
	endpointLookupAttempts    = 3
	defaultEndpointRetryDelay = 500 * time.Millisecond
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
	// RetryDelay is the first wait before retrying a lookup OpenRouter
	// answered with 429 or 5xx; it doubles per attempt. Zero uses 500ms.
	RetryDelay time.Duration
}

// Fetch returns a complete, normalized Document. Models whose output is not
// text-only (image and audio generators), models OpenRouter lists without a
// usable price (for example its own variable-priced routers) and alias or
// variant IDs (prefixed with "~" or containing ":") are skipped. A listed
// model whose endpoint lookup still fails after retries keeps its entry from
// previous, the last published Document (empty when none is published).
func (fetcher Fetcher) Fetch(ctx context.Context, previous Document) (Document, error) {
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
	published := make(map[string]Model, len(previous.Models))
	for _, model := range previous.Models {
		published[model.ID] = model
	}
	failures := 0
	for index, value := range results {
		switch {
		case value.err != nil:
			failures++
			// A transient lookup failure must not drop a routable model for a
			// whole refresh period; only a model /models no longer lists or
			// whose lookup returns no usable endpoint is removed.
			if model, found := published[listed[index].ID]; found {
				document.Models = append(document.Models, model)
			}
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
	path := "/models/" + strings.Join(segments, "/") + "/endpoints"
	delay := fetcher.RetryDelay
	if delay <= 0 {
		delay = defaultEndpointRetryDelay
	}
	for attempt := 1; ; attempt++ {
		err := fetcher.get(ctx, path, &body)
		if err == nil {
			break
		}
		var status statusError
		if attempt == endpointLookupAttempts || !errors.As(err, &status) || !status.transient() {
			return nil, err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		delay *= 2
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
	discount, err := read("discount")
	if err != nil {
		return Pricing{}, fmt.Errorf("price discount: %w", err)
	}
	if scaled, err := perMillion(discount); err == nil && scaled.CanonicalString() != "0" {
		prices.Discount = discount
	}
	// A conditional price cannot supply an unknown unconditional base rate.
	if err := prices.validate(); err != nil {
		return Pricing{}, err
	}
	if overrides, present := raw["overrides"]; present && string(overrides) != "null" {
		var wire []map[string]json.RawMessage
		if err := json.Unmarshal(overrides, &wire); err != nil {
			return Pricing{}, fmt.Errorf("price overrides: %w", err)
		}
		if len(wire) > MaxOverrides {
			return Pricing{}, fmt.Errorf("more than %d price overrides", MaxOverrides)
		}
		var scheduled PricingOverride
		var scheduledRequest string
		for _, item := range wire {
			var override PricingOverride
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
			_, days := item["utc_days"]
			_, start := item["utc_start"]
			_, end := item["utc_end"]
			if days || start || end {
				if err := validatePriceSchedule(item); err != nil {
					return Pricing{}, err
				}
				if rawMin, present := item["min_prompt_tokens"]; present {
					if err := json.Unmarshal(rawMin, &override.MinPromptTokens); err != nil || override.MinPromptTokens <= 0 {
						return Pricing{}, fmt.Errorf("scheduled price min_prompt_tokens must be positive")
					}
				}
				// Reservations must remain safe when a queued request crosses a
				// price window. Cover every window rather than consulting a clock.
				scheduled = maxOverridePrices(scheduled, override)
				if rawRequest, present := item["request"]; present && string(rawRequest) != "null" {
					request, err := decimalLiteral(rawRequest)
					if err != nil || validateRequestPrice(request) != nil {
						return Pricing{}, fmt.Errorf("invalid scheduled request price")
					}
					scheduledRequest = maxPrice(scheduledRequest, request)
				}
				continue
			}
			if err := json.Unmarshal(item["min_prompt_tokens"], &override.MinPromptTokens); err != nil {
				return Pricing{}, fmt.Errorf("price override min_prompt_tokens: %w", err)
			}
			if request, present := item["request"]; present && string(request) != "null" {
				return Pricing{}, fmt.Errorf("long-prompt request price overrides are not supported")
			}
			prices.Overrides = append(prices.Overrides, override)
		}
		prices.Prompt = maxPrice(prices.Prompt, scheduled.Prompt)
		prices.Completion = maxPrice(prices.Completion, scheduled.Completion)
		prices.InputCacheRead = maxPrice(prices.InputCacheRead, scheduled.InputCacheRead)
		prices.InputCacheWrite = maxPrice(prices.InputCacheWrite, scheduled.InputCacheWrite)
		prices.InputCacheWrite1h = maxPrice(prices.InputCacheWrite1h, scheduled.InputCacheWrite1h)
		prices.Request = maxPrice(prices.Request, scheduledRequest)
		for index := range prices.Overrides {
			prices.Overrides[index] = maxOverridePrices(prices.Overrides[index], scheduled)
		}
	}
	if err := prices.validateUnsorted(); err != nil {
		return Pricing{}, err
	}
	return prices, nil
}

// maxOverridePrices preserves the first override's threshold and covers every
// listed token-price component. Omitted components still inherit the base.
func maxOverridePrices(left, right PricingOverride) PricingOverride {
	left.Prompt = maxPrice(left.Prompt, right.Prompt)
	left.Completion = maxPrice(left.Completion, right.Completion)
	left.InputCacheRead = maxPrice(left.InputCacheRead, right.InputCacheRead)
	left.InputCacheWrite = maxPrice(left.InputCacheWrite, right.InputCacheWrite)
	left.InputCacheWrite1h = maxPrice(left.InputCacheWrite1h, right.InputCacheWrite1h)
	return left
}

func validatePriceSchedule(item map[string]json.RawMessage) error {
	if raw, present := item["utc_days"]; present {
		var days []string
		if err := json.Unmarshal(raw, &days); err != nil || len(days) == 0 || len(days) > 7 {
			return fmt.Errorf("price schedule utc_days must list 1-7 weekdays")
		}
		seen := make(map[string]bool, len(days))
		for _, day := range days {
			switch day {
			case "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday":
			default:
				return fmt.Errorf("invalid price schedule weekday")
			}
			if seen[day] {
				return fmt.Errorf("duplicate price schedule weekday")
			}
			seen[day] = true
		}
	}
	start, hasStart := item["utc_start"]
	end, hasEnd := item["utc_end"]
	if hasStart != hasEnd {
		return fmt.Errorf("price schedule requires both utc_start and utc_end")
	}
	if hasStart {
		for _, raw := range []json.RawMessage{start, end} {
			var hhmm int
			if string(raw) == "null" || json.Unmarshal(raw, &hhmm) != nil || hhmm < 0 || hhmm > 2359 || hhmm%100 >= 60 {
				return fmt.Errorf("price schedule time must be a UTC HHMM integer")
			}
		}
	}
	return nil
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

// statusError reports a non-200 OpenRouter response.
type statusError struct{ code int }

func (err statusError) Error() string { return fmt.Sprintf("%v %d", errUnexpectedStatus, err.code) }

func (err statusError) Unwrap() error { return errUnexpectedStatus }

// transient reports whether a retry may succeed: rate limiting or a server
// error.
func (err statusError) transient() bool {
	return err.code == http.StatusTooManyRequests || err.code >= 500
}

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
		return statusError{code: response.StatusCode}
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
