package openaichat

import (
	"fmt"
	"net/http"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
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
	options := []option.RequestOption{
		option.WithAPIKey(config.APIKey),
		option.WithBaseURL(baseURL),
		option.WithHTTPClient(config.HTTPClient),
		option.WithMaxRetries(0),
	}
	if clientconfig.LoopbackHTTP(baseURL) {
		// The SDK refuses credentials over HTTP unless explicitly allowed, and
		// then serves loopback requests from its own direct transport.
		options = append(options, option.WithUnsafeAllowHTTP())
	}
	return &Client{sdk: openai.NewClient(options...), baseURL: baseURL}, nil
}

func (client *Client) options() []option.RequestOption {
	if client == nil || len(client.requestOptions) == 0 {
		return nil
	}
	return append([]option.RequestOption(nil), client.requestOptions...)
}
