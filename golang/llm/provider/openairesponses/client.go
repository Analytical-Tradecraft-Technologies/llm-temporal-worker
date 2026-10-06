package openairesponses

import (
	"fmt"
	"net/http"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/responses"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
)

// ClientConfig contains only resolved values for one adapter endpoint. The
// caller owns secret resolution; this package never reads provider secrets
// from process environment variables.
type ClientConfig struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

// Client owns the official OpenAI SDK client for the Responses endpoint.
// SDK types stay private to this adapter package.
type Client struct {
	sdk          openai.Client
	directOpenAI bool
}

func NewClient(config ClientConfig) (*Client, error) {
	baseURL, err := clientconfig.BaseURL(config.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("openai responses: %w", err)
	}
	if err := clientconfig.Secret("OpenAI API key", config.APIKey); err != nil {
		return nil, fmt.Errorf("openai responses: %w", err)
	}
	if config.HTTPClient == nil {
		return nil, fmt.Errorf("openai responses: HTTP client is required")
	}
	if clientconfig.LoopbackHTTP(baseURL) {
		// The SDK serves credentialed plain HTTP only from its own direct
		// transport, bypassing the guarded client. Send the key from the guarded
		// client instead, and build only the services used, without the SDK's
		// environment credential defaults that would re-enable that check.
		options := []option.RequestOption{
			option.WithBaseURL(baseURL),
			option.WithHTTPClient(clientconfig.LoopbackBearerClient(config.HTTPClient, config.APIKey)),
			option.WithMaxRetries(0),
		}
		return &Client{sdk: openai.Client{Options: options, Responses: responses.NewResponseService(options...), Models: openai.NewModelService(options...)}, directOpenAI: true}, nil
	}
	options := []option.RequestOption{
		option.WithAPIKey(config.APIKey),
		option.WithBaseURL(baseURL),
		option.WithHTTPClient(config.HTTPClient),
		option.WithMaxRetries(0),
	}
	return &Client{sdk: openai.NewClient(options...), directOpenAI: true}, nil
}
