package openaichat

import (
	"fmt"
	"net/http"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
)

type ClientConfig struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

// Client owns the official OpenAI SDK client for a profiled Chat endpoint.
type Client struct {
	sdk            openai.Client
	baseURL        string
	requestOptions []option.RequestOption
}

func NewClient(config ClientConfig) (*Client, error) {
	baseURL, err := clientconfig.BaseURL(config.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("openai chat: %w", err)
	}
	if err := clientconfig.Secret("OpenAI API key", config.APIKey); err != nil {
		return nil, fmt.Errorf("openai chat: %w", err)
	}
	if config.HTTPClient == nil {
		return nil, fmt.Errorf("openai chat: HTTP client is required")
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
		return &Client{sdk: openai.Client{Options: options, Chat: openai.NewChatService(options...), Models: openai.NewModelService(options...)}, baseURL: baseURL}, nil
	}
	options := []option.RequestOption{
		option.WithAPIKey(config.APIKey),
		option.WithBaseURL(baseURL),
		option.WithHTTPClient(config.HTTPClient),
		option.WithMaxRetries(0),
	}
	return &Client{sdk: openai.NewClient(options...), baseURL: baseURL}, nil
}

func (client *Client) options() []option.RequestOption {
	if client == nil || len(client.requestOptions) == 0 {
		return nil
	}
	return append([]option.RequestOption(nil), client.requestOptions...)
}
