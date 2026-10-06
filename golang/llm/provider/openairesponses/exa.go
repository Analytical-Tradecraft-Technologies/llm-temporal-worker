package openairesponses

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

const (
	exaBaseURL = "https://api.exa.ai/"
	// ExaAgentModel is the model Exa serves on its Responses endpoint: the
	// Agent API.
	ExaAgentModel = "exa-agent"
	// exaAgentDefaultEffort is sent when the caller sets no effort. Exa's own
	// default, auto, is metered up to a $5 cap; a fixed effort bills one flat
	// price per request, which the price catalog can bound.
	exaAgentDefaultEffort = llm.ReasoningEffortMedium
)

// ExaClientConfig contains the resolved values for Exa's Responses endpoint.
type ExaClientConfig struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

// NewExaClient constructs the Responses client for Exa's Agent API. Exa
// authenticates with x-api-key rather than a bearer token.
func NewExaClient(config ExaClientConfig) (*Client, error) {
	baseURL, err := clientconfig.BaseURL(config.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("exa responses: %w", err)
	}
	if baseURL != exaBaseURL {
		return nil, fmt.Errorf("exa responses: base URL must be exactly %q", exaBaseURL)
	}
	if err := clientconfig.Secret("Exa API key", config.APIKey); err != nil {
		return nil, fmt.Errorf("exa responses: %w", err)
	}
	if config.HTTPClient == nil {
		return nil, fmt.Errorf("exa responses: HTTP client is required")
	}
	return &Client{sdk: openai.NewClient(
		option.WithBaseURL(baseURL),
		option.WithHTTPClient(config.HTTPClient),
		option.WithHeaderDel("Authorization"),
		option.WithHeader("x-api-key", config.APIKey),
		option.WithMaxRetries(0),
	)}, nil
}

// WithExaAgent adapts the request and response to Exa's Agent API: only the
// exa-agent model is accepted, service_tier and OpenAI storage fields are
// not sent, every request carries a fixed effort no higher than medium, and
// Exa's reported costDollars.total settles the call.
func WithExaAgent() AdapterOption {
	return func(adapter *Adapter) {
		adapter.exaAgent = true
		adapter.omitServiceTier = true
		adapter.storageDenied = false
	}
}

// exaAgentRequest rewrites the lowered request for the Agent API.
func exaAgentRequest(requestMap map[string]any, request llm.Request) error {
	if request.Model != ExaAgentModel {
		return fmt.Errorf("exa responses serves only model %q", ExaAgentModel)
	}
	effort := exaAgentDefaultEffort
	if request.Reasoning != nil {
		if request.Reasoning.TokenBudget != nil {
			return fmt.Errorf("reasoning token_budget is not supported by the Exa Agent API")
		}
		switch request.Reasoning.Effort {
		case "", llm.ReasoningEffortProviderDefault:
		case llm.ReasoningEffortMinimal, llm.ReasoningEffortLow, llm.ReasoningEffortMedium:
			effort = request.Reasoning.Effort
		default:
			// Exa rejects high and above on a synchronous request, and the
			// price catalog bounds only the synchronous efforts.
			return fmt.Errorf("reasoning effort %q is not supported by a synchronous Exa Agent request; use minimal, low or medium", request.Reasoning.Effort)
		}
	}
	requestMap["reasoning"] = map[string]any{"effort": string(effort)}
	// OpenAI storage controls have no Exa equivalent.
	delete(requestMap, "store")
	delete(requestMap, "include")
	return nil
}

// exaAgentCost reads Exa's reported cost from the raw response body.
func exaAgentCost(raw string) (*pricing.USD, error) {
	if raw == "" {
		return nil, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return nil, fmt.Errorf("exa response is not a JSON object")
	}
	costRaw, present := fields["costDollars"]
	if alternate, ok := fields["cost_dollars"]; ok {
		if present {
			return nil, fmt.Errorf("exa response contains duplicate cost fields")
		}
		costRaw, present = alternate, true
	}
	if !present || string(costRaw) == "null" {
		return nil, nil
	}
	var cost map[string]json.RawMessage
	if err := json.Unmarshal(costRaw, &cost); err != nil || cost == nil {
		return nil, fmt.Errorf("exa costDollars is invalid")
	}
	total, ok := cost["total"]
	if !ok {
		return nil, fmt.Errorf("exa costDollars.total is missing")
	}
	var text string
	if err := json.Unmarshal(total, &text); err != nil {
		text = string(total)
	}
	amount, err := pricing.ParseUSD(text)
	if err != nil {
		return nil, fmt.Errorf("exa costDollars.total: %w", err)
	}
	return &amount, nil
}
