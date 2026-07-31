package openairesponses

import (
	"fmt"
	"net/http"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
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
	sdk openai.Client
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
	return &Client{sdk: openai.NewClient(
		option.WithAPIKey(config.APIKey),
		option.WithBaseURL(baseURL),
		option.WithHTTPClient(config.HTTPClient),
		option.WithMaxRetries(0),
	)}, nil
}
