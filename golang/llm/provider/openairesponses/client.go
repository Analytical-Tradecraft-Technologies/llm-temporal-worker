package openairesponses

import (
	"net/http"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/responses"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/openaiwire"
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
	baseURL, err := clientconfig.KeyedClient("openai responses", config.BaseURL, "", "OpenAI API key", config.APIKey, config.HTTPClient)
	if err != nil {
		return nil, err
	}
	options, loopback := openaiwire.ClientOptions(baseURL, config.APIKey, config.HTTPClient)
	if loopback {
		return &Client{sdk: openai.Client{Options: options, Responses: responses.NewResponseService(options...), Models: openai.NewModelService(options...)}, directOpenAI: true}, nil
	}
	return &Client{sdk: openai.NewClient(options...), directOpenAI: true}, nil
}
