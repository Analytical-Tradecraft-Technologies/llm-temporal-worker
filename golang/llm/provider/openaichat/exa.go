package openaichat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
)

const exaBaseURL = "https://api.exa.ai/"

type ExaClientConfig struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

func NewExaClient(config ExaClientConfig) (*Client, error) {
	baseURL, err := clientconfig.BaseURL(config.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("exa chat: %w", err)
	}
	if baseURL != exaBaseURL {
		return nil, fmt.Errorf("exa chat: base URL must be exactly %q", exaBaseURL)
	}
	if err := clientconfig.Secret("Exa API key", config.APIKey); err != nil {
		return nil, fmt.Errorf("exa chat: %w", err)
	}
	if config.HTTPClient == nil {
		return nil, fmt.Errorf("exa chat: HTTP client is required")
	}
	return &Client{
		sdk: openai.NewClient(
			option.WithBaseURL(baseURL),
			option.WithHTTPClient(config.HTTPClient),
			option.WithHeaderDel("Authorization"),
			option.WithHeader("x-api-key", config.APIKey),
			option.WithMaxRetries(0),
		),
		baseURL: baseURL,
	}, nil
}

type ExaProfileConfig struct {
	ID                        string
	CapabilityVersion         string
	BaseURL                   string
	Model                     string
	Capabilities              provider.CapabilitySet
	ServiceTiers              map[llm.ServiceClass]string
	ActualServiceClasses      map[string]llm.ServiceClass
	MissingActualServiceClass llm.ServiceClass
	AllowedExtensions         map[string]ExtensionSpec
}

func NewExaProfile(config ExaProfileConfig) (Profile, error) {
	baseURL, err := clientconfig.BaseURL(config.BaseURL)
	if err != nil {
		return Profile{}, fmt.Errorf("exa chat profile: %w", err)
	}
	if baseURL != exaBaseURL {
		return Profile{}, fmt.Errorf("exa chat profile: base URL must be exactly %q", exaBaseURL)
	}
	model := strings.TrimSpace(config.Model)
	if model == "" {
		model = "exa"
	}
	textBody, err := json.Marshal(map[string]any{"text": true})
	if err != nil {
		return Profile{}, fmt.Errorf("exa chat profile: extra body: %w", err)
	}
	allowed := cloneExtensions(config.AllowedExtensions)
	if allowed == nil {
		allowed = map[string]ExtensionSpec{}
	}
	if _, exists := allowed["exa"]; !exists {
		allowed["exa"] = ExtensionSpec{Fields: map[string]string{"text": "extra_body"}}
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
		ExpectedModel:             model,
		WireDefaults: map[string]json.RawMessage{
			"extra_body": textBody,
		},
		ReservedWireFields: map[string]struct{}{"extra_body": {}},
		ResponseAugment:    augmentExa,
	})
}

func NewExaAdapter(client *Client, endpointID string, config ExaProfileConfig) (*Adapter, error) {
	profile, err := NewExaProfile(config)
	if err != nil {
		return nil, err
	}
	return New(client, endpointID, profile)
}

func augmentExa(call provider.Call, response *openai.ChatCompletion, lifted *llm.Response) error {
	if response == nil || lifted == nil {
		return fmt.Errorf("exa response is empty")
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
	var costRaw json.RawMessage
	if value, ok := fields["costDollars"]; ok {
		costRaw = value
	}
	if value, ok := fields["cost_dollars"]; ok {
		if costRaw != nil {
			return fmt.Errorf("exa response contains duplicate cost fields")
		}
		costRaw = value
	}
	if costRaw != nil {
		var costFields map[string]json.RawMessage
		if err := json.Unmarshal(costRaw, &costFields); err != nil || costFields == nil {
			return fmt.Errorf("exa costDollars is invalid")
		}
		total, ok := costFields["total"]
		if !ok {
			return fmt.Errorf("exa costDollars.total is missing")
		}
		if err := responseAugmentCost(lifted, total, "exa_cost_dollars", "exa_reported"); err != nil {
			return err
		}
		addRawFact(lifted.Provider.Raw, "costDollars", costRaw)
		addRawFact(lifted.Usage.ProviderRaw, "costDollars", costRaw)
	}
	// Exa documents citations on the selected message; earlier responses
	// carried them at the root. Prefer the message and fall back to the root.
	message, err := exaSelectedMessageFields(response)
	if err != nil {
		return err
	}
	for _, key := range []string{"results", "sources", "citations"} {
		value, ok := message[key]
		if !ok {
			value, ok = fields[key]
		}
		if !ok || string(value) == "null" {
			continue
		}
		references, err := exaReferences(value, key)
		if err != nil {
			return err
		}
		lifted.Output = append(lifted.Output, references...)
		addRawFact(lifted.Provider.Raw, "exa_"+key, value)
	}
	_ = call
	return nil
}

func exaSelectedMessageFields(response *openai.ChatCompletion) (map[string]json.RawMessage, error) {
	if len(response.Choices) == 0 {
		return map[string]json.RawMessage{}, nil
	}
	raw := strings.TrimSpace(response.Choices[0].Message.RawJSON())
	if raw == "" {
		return map[string]json.RawMessage{}, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil || fields == nil {
		return nil, fmt.Errorf("exa response message is invalid")
	}
	return fields, nil
}

func exaReferences(raw json.RawMessage, field string) ([]llm.Item, error) {
	var entries []json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("exa %s is invalid", field)
	}
	result := make([]llm.Item, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for index, entry := range entries {
		var value map[string]json.RawMessage
		if err := json.Unmarshal(entry, &value); err != nil || value == nil {
			return nil, fmt.Errorf("exa %s[%d] is invalid", field, index)
		}
		uriRaw := value["url"]
		if len(uriRaw) == 0 {
			uriRaw = value["uri"]
		}
		if len(uriRaw) == 0 {
			uriRaw = value["link"]
		}
		var uri string
		if err := json.Unmarshal(uriRaw, &uri); err != nil || !validReferenceURI(uri) {
			return nil, fmt.Errorf("exa %s[%d] has an invalid URL", field, index)
		}
		if _, exists := seen[uri]; exists {
			continue
		}
		seen[uri] = struct{}{}
		metadata := map[string]json.RawMessage{}
		for _, key := range []string{"id", "title", "score"} {
			if value, ok := value[key]; ok {
				metadata[key] = append(json.RawMessage(nil), value...)
			}
		}
		result = append(result, llm.Reference{URI: uri, Metadata: metadata})
	}
	return result, nil
}

func validReferenceURI(value string) bool {
	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed.Host == "" {
		return false
	}
	return parsed.Scheme == "http" || parsed.Scheme == "https"
}
