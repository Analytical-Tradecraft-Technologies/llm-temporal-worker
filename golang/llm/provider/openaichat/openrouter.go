package openaichat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
)

const openRouterBaseURL = "https://openrouter.ai/api/v1/"

type OpenRouterClientConfig struct {
	BaseURL     string
	APIKey      string
	HTTPReferer string
	Title       string
	HTTPClient  *http.Client
}

func NewOpenRouterClient(config OpenRouterClientConfig) (*Client, error) {
	baseURL, err := clientconfig.BaseURL(config.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("openrouter chat: %w", err)
	}
	if baseURL != openRouterBaseURL {
		return nil, fmt.Errorf("openrouter chat: base URL must be exactly %q", openRouterBaseURL)
	}
	if err := clientconfig.Secret("OpenRouter API key", config.APIKey); err != nil {
		return nil, fmt.Errorf("openrouter chat: %w", err)
	}
	if config.HTTPClient == nil {
		return nil, fmt.Errorf("openrouter chat: HTTP client is required")
	}
	options := []option.RequestOption{
		option.WithAPIKey(config.APIKey),
		option.WithBaseURL(baseURL),
		option.WithHTTPClient(config.HTTPClient),
		option.WithMaxRetries(0),
	}
	if strings.TrimSpace(config.HTTPReferer) != "" {
		options = append(options, option.WithHeader("HTTP-Referer", config.HTTPReferer))
	}
	if strings.TrimSpace(config.Title) != "" {
		options = append(options, option.WithHeader("X-OpenRouter-Title", config.Title))
	}
	return &Client{sdk: openai.NewClient(options...), baseURL: baseURL}, nil
}

type OpenRouterProfileConfig struct {
	ID                        string
	CapabilityVersion         string
	BaseURL                   string
	Model                     string
	Capabilities              provider.CapabilitySet
	ServiceTiers              map[llm.ServiceClass]string
	ActualServiceClasses      map[string]llm.ServiceClass
	MissingActualServiceClass llm.ServiceClass
	ProviderOrder             []string
	// SelectUpstream lets OpenRouter choose (and fail over between) upstream
	// providers instead of pinning ProviderOrder. Budget admission must then
	// price the route at its most expensive upstream; model sync does.
	SelectUpstream    bool
	AllowFallbacks    bool
	RequireParameters bool
	AllowedExtensions map[string]ExtensionSpec
}

func NewOpenRouterProfile(config OpenRouterProfileConfig) (Profile, error) {
	baseURL, err := clientconfig.BaseURL(config.BaseURL)
	if err != nil {
		return Profile{}, fmt.Errorf("openrouter chat profile: %w", err)
	}
	if baseURL != openRouterBaseURL {
		return Profile{}, fmt.Errorf("openrouter chat profile: base URL must be exactly %q", openRouterBaseURL)
	}
	if config.SelectUpstream {
		return newOpenRouterUpstreamProfile(config, baseURL)
	}
	if len(config.ProviderOrder) == 0 {
		return Profile{}, fmt.Errorf("openrouter chat profile: provider order is required")
	}
	seen := make(map[string]struct{}, len(config.ProviderOrder))
	order := make([]string, len(config.ProviderOrder))
	for index, name := range config.ProviderOrder {
		name = strings.TrimSpace(name)
		if name == "" {
			return Profile{}, fmt.Errorf("openrouter chat profile: provider order entry %d is empty", index)
		}
		if _, ok := seen[name]; ok {
			return Profile{}, fmt.Errorf("openrouter chat profile: provider order contains duplicate %q", name)
		}
		seen[name] = struct{}{}
		order[index] = name
	}
	if config.AllowFallbacks {
		return Profile{}, fmt.Errorf("openrouter chat profile: allow_fallbacks must be false")
	}
	if !config.RequireParameters {
		return Profile{}, fmt.Errorf("openrouter chat profile: require_parameters must be true")
	}
	providerField := map[string]any{
		"order":              order,
		"allow_fallbacks":    false,
		"require_parameters": true,
	}
	providerRaw, err := json.Marshal(providerField)
	if err != nil {
		return Profile{}, fmt.Errorf("openrouter chat profile: provider defaults: %w", err)
	}
	allowed := cloneExtensions(config.AllowedExtensions)
	if allowed == nil {
		allowed = map[string]ExtensionSpec{}
	}
	if _, exists := allowed["openrouter"]; !exists {
		allowed["openrouter"] = ExtensionSpec{Fields: map[string]string{
			"provider_order":     "provider",
			"allow_fallbacks":    "provider",
			"require_parameters": "provider",
		}}
	}
	return NewProfile(Profile{
		// The developer role is not accepted by every API version or
		// compatible server; system is.
		ApplicationInstructionRole: "system",
		ID:                         config.ID,
		CapabilityVersion:          config.CapabilityVersion,
		Capabilities:               config.Capabilities,
		ServiceTiers:               config.ServiceTiers,
		ActualServiceClasses:       config.ActualServiceClasses,
		MissingActualServiceClass:  config.MissingActualServiceClass,
		AllowedExtensions:          allowed,
		ExpectedBaseURL:            baseURL,
		ExpectedModel:              config.Model,
		WireDefaults: map[string]json.RawMessage{
			"provider": providerRaw,
		},
		ReservedWireFields: map[string]struct{}{"provider": {}},
		ResponseAugment:    augmentOpenRouter,
		ResponseError:      openRouterResponseError,
	})
}

// newOpenRouterUpstreamProfile builds the profile for an endpoint on which
// OpenRouter selects the upstream provider. Only require_parameters is sent,
// so OpenRouter never routes to an upstream that would drop a request field.
// Callers cannot steer the upstream through the openrouter extension.
func newOpenRouterUpstreamProfile(config OpenRouterProfileConfig, baseURL string) (Profile, error) {
	if len(config.ProviderOrder) != 0 {
		return Profile{}, fmt.Errorf("openrouter chat profile: provider order must be empty when OpenRouter selects the upstream")
	}
	if !config.RequireParameters {
		return Profile{}, fmt.Errorf("openrouter chat profile: require_parameters must be true")
	}
	providerRaw, err := json.Marshal(map[string]any{"require_parameters": true})
	if err != nil {
		return Profile{}, fmt.Errorf("openrouter chat profile: provider defaults: %w", err)
	}
	allowed := cloneExtensions(config.AllowedExtensions)
	delete(allowed, "openrouter")
	return NewProfile(Profile{
		ApplicationInstructionRole: "system",
		ID:                         config.ID,
		CapabilityVersion:          config.CapabilityVersion,
		Capabilities:               config.Capabilities,
		ServiceTiers:               config.ServiceTiers,
		ActualServiceClasses:       config.ActualServiceClasses,
		MissingActualServiceClass:  config.MissingActualServiceClass,
		AllowedExtensions:          allowed,
		ExpectedBaseURL:            baseURL,
		ExpectedModel:              config.Model,
		WireDefaults:               map[string]json.RawMessage{"provider": providerRaw},
		ReservedWireFields:         map[string]struct{}{"provider": {}},
		ResponseAugment:            augmentOpenRouter,
		ResponseError:              openRouterResponseError,
	})
}

func NewOpenRouterAdapter(client *Client, endpointID string, config OpenRouterProfileConfig) (*Adapter, error) {
	profile, err := NewOpenRouterProfile(config)
	if err != nil {
		return nil, err
	}
	return New(client, endpointID, profile)
}

func augmentOpenRouter(call provider.Call, response *openai.ChatCompletion, lifted *llm.Response) error {
	if response == nil || lifted == nil {
		return fmt.Errorf("openrouter response is empty")
	}
	if lifted.Provider.Raw == nil {
		lifted.Provider.Raw = map[string]json.RawMessage{}
	}
	if lifted.Usage.ProviderRaw == nil {
		lifted.Usage.ProviderRaw = map[string]json.RawMessage{}
	}
	if call.Metadata.WebSearch {
		lifted.Usage.ProviderRaw["web_search_calls"] = json.RawMessage("null")
		lifted.Cost.Status = llm.CostStatusUnknown
	}
	lifted.Output = append(lifted.Output, llm.WebSearchReferences([]byte(response.RawJSON()))...)
	lifted.Provider.GenerationID = response.ID
	fields, err := rawResponseObject(response)
	if err != nil {
		return err
	}
	if generation, ok := fields["generation_id"]; ok {
		var value string
		if err := json.Unmarshal(generation, &value); err != nil || value == "" {
			return fmt.Errorf("openrouter generation_id is invalid")
		}
		lifted.Provider.GenerationID = value
		addRawFact(lifted.Provider.Raw, "generation_id", generation)
	}
	usageRaw, ok := fields["usage"]
	if !ok {
		return nil
	}
	addRawFact(lifted.Provider.Raw, "openrouter_usage", usageRaw)
	addRawFact(lifted.Usage.ProviderRaw, "openrouter_usage", usageRaw)
	var usage map[string]json.RawMessage
	if err := json.Unmarshal(usageRaw, &usage); err != nil || usage == nil {
		return fmt.Errorf("openrouter usage metadata is invalid")
	}
	// The search count is recorded but never settles the cost: the auto
	// engine bills at the selected engine's or native provider's rate, which
	// the response does not report. Only OpenRouter's total (usage.cost,
	// below) makes a search call's cost known.
	if call.Metadata.WebSearch {
		if calls, ok := openRouterWebSearchRequests(usage); ok {
			lifted.Usage.ProviderRaw["web_search_calls"], _ = json.Marshal(calls)
		}
	}
	if costRaw, ok := usage["cost"]; ok {
		if err := responseAugmentCost(lifted, costRaw, "openrouter_cost", "openrouter_reported"); err != nil {
			return err
		}
	}
	if details, ok := usage["cost_details"]; ok {
		addRawFact(lifted.Provider.Raw, "openrouter_cost_details", details)
		addRawFact(lifted.Usage.ProviderRaw, "openrouter_cost_details", details)
	}
	addOpenRouterBYOKCost(lifted, usage)
	_ = call
	return nil
}

// openRouterWebSearchRequests reads the hosted search count OpenRouter reports
// as usage.server_tool_use.web_search_requests or, for some upstreams,
// usage.server_tool_use_details.web_search_requests. Both objects count the
// same searches, so the count is never summed: when both are present they must
// agree. A malformed, negative or disagreeing count is unknown.
func openRouterWebSearchRequests(usage map[string]json.RawMessage) (int64, bool) {
	var count int64
	found := false
	for _, field := range []string{"server_tool_use", "server_tool_use_details"} {
		raw, present := usage[field]
		if !present || string(raw) == "null" {
			continue
		}
		var server struct {
			WebSearchRequests *int64 `json:"web_search_requests"`
		}
		if json.Unmarshal(raw, &server) != nil {
			return 0, false
		}
		if server.WebSearchRequests == nil {
			continue
		}
		value := *server.WebSearchRequests
		if value < 0 || (found && value != count) {
			return 0, false
		}
		count, found = value, true
	}
	return count, found
}

// addOpenRouterBYOKCost settles a BYOK call at OpenRouter's fee plus the
// upstream inference cost. usage.cost is only what the OpenRouter account is
// charged; with BYOK the upstream provider bills inference to the operator's
// own key, and OpenRouter reports that amount as
// cost_details.upstream_inference_cost. Only usage.is_byok selects this:
// non-BYOK responses also carry upstream_inference_cost, already included in
// usage.cost. A BYOK response without both amounts, or an is_byok marker that
// is not a boolean, keeps the cost unknown, so the reservation is charged
// rather than refunded.
func addOpenRouterBYOKCost(lifted *llm.Response, usage map[string]json.RawMessage) {
	raw, ok := usage["is_byok"]
	if !ok {
		return
	}
	switch strings.TrimSpace(string(raw)) {
	case "false":
		return
	case "true":
	default:
		lifted.Cost = llm.Cost{Status: llm.CostStatusUnknown}
		return
	}
	var details struct {
		UpstreamInferenceCost json.RawMessage `json:"upstream_inference_cost"`
	}
	if raw, ok := usage["cost_details"]; ok {
		_ = json.Unmarshal(raw, &details)
	}
	upstream, err := parseDecimalUSD(details.UpstreamInferenceCost, "openrouter_upstream_inference_cost")
	if err != nil || lifted.Cost.ActualCostUSD == nil {
		lifted.Cost = llm.Cost{Status: llm.CostStatusUnknown}
		return
	}
	total, err := lifted.Cost.ActualCostUSD.Add(upstream)
	if err != nil {
		lifted.Cost = llm.Cost{Status: llm.CostStatusUnknown}
		return
	}
	lifted.Cost.ActualCostUSD = &total
}

// openRouterResponseError maps the failures OpenRouter reports with HTTP 200:
// a top-level error object when the upstream provider failed before
// generating, and a choice whose finish_reason is "error" when it failed
// mid-generation. The error code is the upstream HTTP status, so it goes
// through the same status table as an HTTP error response. A top-level error
// carries no generation and is classified like the HTTP error; a failed
// choice may already have been billed, so it stays accepted and is never
// retried automatically.
func openRouterResponseError(call provider.Call, response *openai.ChatCompletion, requestID string) *provider.Error {
	fields, err := rawResponseObject(response)
	if err != nil {
		return nil
	}
	if raw, ok := fields["error"]; ok && string(raw) != "null" {
		return openRouterBodyError(call, requestID, raw, provider.PhaseDispatch, false)
	}
	var choices []map[string]json.RawMessage
	if raw, ok := fields["choices"]; !ok || json.Unmarshal(raw, &choices) != nil {
		return nil
	}
	for _, choice := range choices {
		var finish string
		if raw, ok := choice["finish_reason"]; ok {
			_ = json.Unmarshal(raw, &finish)
		}
		raw, hasError := choice["error"]
		if hasError && string(raw) == "null" {
			hasError = false
		}
		if finish == "error" || hasError {
			mapped := openRouterBodyError(call, requestID, raw, provider.PhaseLift, true)
			mapped.Provider.ResponseID = response.ID
			return mapped
		}
	}
	return nil
}

func openRouterBodyError(call provider.Call, requestID string, raw json.RawMessage, phase provider.Phase, accepted bool) *provider.Error {
	status := openRouterErrorStatus(raw)
	code, retry, dispatch, safe := classifyStatus(status)
	if accepted {
		dispatch, retry, safe = provider.DispatchAccepted, provider.RetryNever, "provider failed during generation"
	}
	mapped := provider.NewError(code, phase, dispatch, retry, safe+" (reported in a successful response body)")
	mapped.OperationID = call.OperationKey
	mapped.Provider.RequestID = requestID
	mapped.SafeDetails = map[string]string{"provider": "openrouter", "status": strconv.Itoa(status)}
	return mapped
}

// openRouterErrorStatus reads the upstream HTTP status from an OpenRouter
// error object. A missing or non-status code is treated as an upstream
// failure (502).
func openRouterErrorStatus(raw json.RawMessage) int {
	var body struct {
		Code json.RawMessage `json:"code"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &body) != nil {
		return http.StatusBadGateway
	}
	var number int
	if json.Unmarshal(body.Code, &number) == nil && number >= 400 && number <= 599 {
		return number
	}
	var text string
	if json.Unmarshal(body.Code, &text) == nil {
		if parsed, err := strconv.Atoi(text); err == nil && parsed >= 400 && parsed <= 599 {
			return parsed
		}
	}
	return http.StatusBadGateway
}
