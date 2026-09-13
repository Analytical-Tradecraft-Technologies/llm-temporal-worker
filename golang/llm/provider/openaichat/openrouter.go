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
	WireShape                 WireShape
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
		ReservedWireFields: map[string]struct{}{"provider": {}},
		WireShape:          config.WireShape,
		ResponseAugment:    augmentOpenRouter,
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
	if costRaw, ok := usage["cost"]; ok {
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
