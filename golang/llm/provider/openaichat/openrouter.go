package openaichat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
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
	AllowFallbacks            bool
	RequireParameters         bool
	AllowedExtensions         map[string]ExtensionSpec
	// SupportedParameters is the pinned endpoint's OpenRouter
	// `supported_parameters` listing. It derives the wire shape (max_tokens
	// when listed, parallel_tool_calls only when listed, never store) and
	// closes the optional parameters a request may send.
	SupportedParameters []string
	// ReasoningEfforts maps public efforts to OpenRouter's unified
	// reasoning.effort values. An unmapped effort is refused before dispatch.
	ReasoningEfforts map[llm.ReasoningEffort]string
	// ModelAliases are documented dated revisions ("permaslugs") of the pinned
	// model that OpenRouter may echo as the response model.
	ModelAliases []string
}

// openRouterServiceTiers are the documented Chat Completions service_tier
// values that OpenRouter also reports back. The request alias "fast" is
// reported as "priority", so it is refused to keep the mapping one-to-one.
var openRouterServiceTiers = map[string]struct{}{"default": {}, "flex": {}, "priority": {}}

// openRouterDefaultServiceTier is the tier OpenRouter serves when a request
// names none; non-default tier endpoints are opt-in only.
const openRouterDefaultServiceTier = "default"

// openRouterParameters is the documented OpenRouter request-parameter
// vocabulary used in model endpoint `supported_parameters` listings.
// `store` is deliberately absent: OpenRouter's Chat request schema has no
// such field, and storage is governed by provider.data_collection instead.
var openRouterParameters = map[string]struct{}{
	"temperature": {}, "top_p": {}, "top_k": {}, "frequency_penalty": {}, "presence_penalty": {},
	"repetition_penalty": {}, "min_p": {}, "top_a": {}, "seed": {}, "max_tokens": {},
	"max_completion_tokens": {}, "logit_bias": {}, "logprobs": {}, "top_logprobs": {},
	"response_format": {}, "structured_outputs": {}, "stop": {}, "tools": {}, "tool_choice": {},
	"parallel_tool_calls": {}, "include_reasoning": {}, "reasoning": {}, "reasoning_effort": {},
	"web_search_options": {}, "verbosity": {},
}

func NewOpenRouterProfile(config OpenRouterProfileConfig) (Profile, error) {
	baseURL, err := clientconfig.BaseURL(config.BaseURL)
	if err != nil {
		return Profile{}, fmt.Errorf("openrouter chat profile: %w", err)
	}
	if baseURL != openRouterBaseURL {
		return Profile{}, fmt.Errorf("openrouter chat profile: base URL must be exactly %q", openRouterBaseURL)
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
	shape, supported, err := openRouterWireShape(config)
	if err != nil {
		return Profile{}, fmt.Errorf("openrouter chat profile: %w", err)
	}
	providerField := map[string]any{
		"order":              order,
		"allow_fallbacks":    false,
		"require_parameters": true,
		"data_collection":    "deny",
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
		ID:                        config.ID,
		CapabilityVersion:         config.CapabilityVersion,
		Capabilities:              config.Capabilities,
		ServiceTiers:              config.ServiceTiers,
		ActualServiceClasses:      config.ActualServiceClasses,
		MissingActualServiceClass: config.MissingActualServiceClass,
		AllowedExtensions:         allowed,
		ExpectedBaseURL:           baseURL,
		ExpectedModel:             config.Model,
		WireDefaults: map[string]json.RawMessage{
			"provider": providerRaw,
		},
		ReservedWireFields:  map[string]struct{}{"provider": {}},
		WireShape:           shape,
		SupportedParameters: supported,
		ResponseModel:       ResponseModelPolicy{Pinned: true, Aliases: config.ModelAliases},
		ResponseAugment:     augmentOpenRouter,
	})
}

// openRouterWireShape derives the request shape from the pinned endpoint's
// documented parameters and service tiers.
func openRouterWireShape(config OpenRouterProfileConfig) (WireShape, map[string]struct{}, error) {
	if len(config.SupportedParameters) == 0 {
		return WireShape{}, nil, fmt.Errorf("supported_parameters of the pinned endpoint are required")
	}
	supported := make(map[string]struct{}, len(config.SupportedParameters))
	for _, parameter := range config.SupportedParameters {
		if _, known := openRouterParameters[parameter]; !known {
			return WireShape{}, nil, fmt.Errorf("supported parameter %q is not a documented OpenRouter parameter", parameter)
		}
		if _, duplicate := supported[parameter]; duplicate {
			return WireShape{}, nil, fmt.Errorf("supported parameter %q is repeated", parameter)
		}
		supported[parameter] = struct{}{}
	}
	shape := WireShape{
		OutputTokenLimitField: OutputTokenLimitFieldMaxCompletionTokens,
		Store:                 DefaultFalseFieldUnsupported,
		// OpenRouter documents parallel_tool_calls with default true, and the
		// pinned upstreams run tool calls in parallel when it is absent.
		ParallelToolCalls: DefaultTrueFieldUnsupported,
		ReasoningEffortField:  ReasoningEffortFieldObject,
		ReasoningEfforts:      map[llm.ReasoningEffort]string{},
	}
	if _, ok := supported["max_tokens"]; ok {
		shape.OutputTokenLimitField = OutputTokenLimitFieldMaxTokens
	}
	if _, ok := supported["parallel_tool_calls"]; ok {
		shape.ParallelToolCalls = DefaultFalseFieldSupported
	}
	if len(config.ReasoningEfforts) > 0 {
		if _, ok := supported["reasoning"]; !ok {
			return WireShape{}, nil, fmt.Errorf("reasoning efforts require the pinned endpoint to support %q", "reasoning")
		}
	}
	for effort, value := range config.ReasoningEfforts {
		shape.ReasoningEfforts[effort] = value
	}
	var defaultClass llm.ServiceClass
	supportedClasses := 0
	for _, class := range publicServiceClasses() {
		tier := config.ServiceTiers[class]
		if tier == "" {
			continue
		}
		if _, documented := openRouterServiceTiers[tier]; !documented {
			return WireShape{}, nil, fmt.Errorf("service class %q provider tier %q is not a documented OpenRouter tier (default, flex, priority)", class, tier)
		}
		supportedClasses++
		if tier == openRouterDefaultServiceTier {
			defaultClass = class
		}
	}
	for tier := range config.ActualServiceClasses {
		if _, documented := openRouterServiceTiers[tier]; !documented {
			return WireShape{}, nil, fmt.Errorf("actual provider tier %q is not a documented OpenRouter tier (default, flex, priority)", tier)
		}
	}
	if config.MissingActualServiceClass != "" && (defaultClass == "" || config.MissingActualServiceClass != defaultClass) {
		// Non-default tiers are opt-in and reported, so a response without a
		// tier can be attributed only to the class served by the default tier.
		return WireShape{}, nil, fmt.Errorf("missing service tier class %q must be the class mapped to the %q tier", config.MissingActualServiceClass, openRouterDefaultServiceTier)
	}
	// A request that names no tier is never routed to a non-default tier, so
	// a default-only endpoint omits service_tier instead of sending a field the
	// pinned upstream does not list.
	shape.OmitServiceTier = supportedClasses == 1 && defaultClass != ""
	if shape.OmitServiceTier && config.MissingActualServiceClass == "" {
		return WireShape{}, nil, fmt.Errorf("a default-tier-only endpoint omits service_tier and must declare missing_service_tier %q for responses that report no tier", defaultClass)
	}
	return shape, supported, nil
}

// OpenRouterEndpoint is the typed form of an openai_chat endpoint's
// `extensions.openrouter` block.
type OpenRouterEndpoint struct {
	ProviderOrder       []string
	SupportedParameters []string
	ReasoningEfforts    map[llm.ReasoningEffort]string
	MissingServiceTier  llm.ServiceClass
	ModelAliases        []string
}

// ParseOpenRouterEndpoint decodes `extensions.openrouter` from a YAML or JSON
// configuration map. Unknown fields are refused, and the optional
// allow_fallbacks and require_parameters markers may only restate the pinned
// values false and true.
func ParseOpenRouterEndpoint(values map[string]any) (OpenRouterEndpoint, error) {
	var endpoint OpenRouterEndpoint
	for field, value := range values {
		var err error
		switch field {
		case "provider_order":
			endpoint.ProviderOrder, err = openRouterStrings(field, value)
		case "supported_parameters":
			endpoint.SupportedParameters, err = openRouterStrings(field, value)
		case "model_aliases":
			endpoint.ModelAliases, err = openRouterStrings(field, value)
		case "reasoning_efforts":
			endpoint.ReasoningEfforts, err = openRouterReasoningEfforts(value)
		case "missing_service_tier":
			text, ok := value.(string)
			if !ok || !llm.ServiceClass(text).Valid() {
				err = fmt.Errorf("missing_service_tier must be economy, standard or priority")
			}
			endpoint.MissingServiceTier = llm.ServiceClass(text)
		case "allow_fallbacks":
			if pinned, ok := value.(bool); !ok || pinned {
				err = fmt.Errorf("allow_fallbacks must be false; hidden provider fallback is prohibited")
			}
		case "require_parameters":
			if pinned, ok := value.(bool); !ok || !pinned {
				err = fmt.Errorf("require_parameters must be true")
			}
		default:
			err = fmt.Errorf("field %q is not supported", field)
		}
		if err != nil {
			return OpenRouterEndpoint{}, fmt.Errorf("openrouter extension: %w", err)
		}
	}
	return endpoint, nil
}

// ProfileConfig binds the endpoint block to one endpoint's identity. The
// actual-tier mapping is the inverse of the request tiers because OpenRouter
// reports the same default/flex/priority vocabulary it accepts.
func (endpoint OpenRouterEndpoint) ProfileConfig(id, baseURL, model string, capabilities provider.CapabilitySet, serviceTiers map[llm.ServiceClass]string) OpenRouterProfileConfig {
	actual := make(map[string]llm.ServiceClass, len(serviceTiers))
	for class, tier := range serviceTiers {
		if tier != "" {
			actual[tier] = class
		}
	}
	return OpenRouterProfileConfig{
		ID:                        id,
		CapabilityVersion:         capabilities.Version,
		BaseURL:                   baseURL,
		Model:                     model,
		Capabilities:              capabilities,
		ServiceTiers:              serviceTiers,
		ActualServiceClasses:      actual,
		MissingActualServiceClass: endpoint.MissingServiceTier,
		ProviderOrder:             endpoint.ProviderOrder,
		AllowFallbacks:            false,
		RequireParameters:         true,
		SupportedParameters:       endpoint.SupportedParameters,
		ReasoningEfforts:          endpoint.ReasoningEfforts,
		ModelAliases:              endpoint.ModelAliases,
	}
}

// ValidateOpenRouterEndpoint applies every OpenRouter profile check that does
// not depend on a capability catalog, so configuration validation refuses an
// invalid effort mapping or wire combination before a worker starts.
func ValidateOpenRouterEndpoint(id, baseURL string, values map[string]any, serviceTiers map[llm.ServiceClass]string) error {
	endpoint, err := ParseOpenRouterEndpoint(values)
	if err != nil {
		return err
	}
	capabilities := provider.CapabilitySet{Version: "config-validation", Features: make(map[provider.Feature]provider.Capability, len(allFeatures()))}
	for _, feature := range allFeatures() {
		capabilities.Features[feature] = provider.Capability{State: provider.CapabilityUnknown}
	}
	_, err = NewOpenRouterProfile(endpoint.ProfileConfig(id, baseURL, "", capabilities, serviceTiers))
	return err
}

func openRouterStrings(field string, value any) ([]string, error) {
	var items []any
	switch typed := value.(type) {
	case []any:
		items = typed
	case []string:
		items = make([]any, len(typed))
		for index, item := range typed {
			items[index] = item
		}
	default:
		return nil, fmt.Errorf("%s must be an array of strings", field)
	}
	result := make([]string, len(items))
	for index, item := range items {
		text, ok := item.(string)
		if !ok || strings.TrimSpace(text) == "" || strings.TrimSpace(text) != text {
			return nil, fmt.Errorf("%s entry %d must be a non-empty trimmed string", field, index)
		}
		result[index] = text
	}
	return result, nil
}

func openRouterReasoningEfforts(value any) (map[llm.ReasoningEffort]string, error) {
	var entries map[string]any
	switch typed := value.(type) {
	case map[string]any:
		entries = typed
	case map[string]string:
		entries = make(map[string]any, len(typed))
		for key, item := range typed {
			entries[key] = item
		}
	default:
		return nil, fmt.Errorf("reasoning_efforts must map public efforts to OpenRouter reasoning.effort values")
	}
	result := make(map[llm.ReasoningEffort]string, len(entries))
	for effort, item := range entries {
		text, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("reasoning_efforts.%s must be a string", effort)
		}
		result[llm.ReasoningEffort(effort)] = text
	}
	return result, nil
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
	// The API reference types usage.cost as number|null. A null cost is an
	// unreported receipt, not an invalid response; the runtime then applies
	// its catalog-cost policy instead of rejecting a billed completion.
	if costRaw, ok := usage["cost"]; ok && string(costRaw) != "null" {
		if err := responseAugmentCost(lifted, costRaw, "openrouter_cost", "openrouter_reported"); err != nil {
			return err
		}
	}
	if details, ok := usage["cost_details"]; ok {
		addRawFact(lifted.Provider.Raw, "openrouter_cost_details", details)
		addRawFact(lifted.Usage.ProviderRaw, "openrouter_cost_details", details)
	}
	_ = call
	return nil
}
