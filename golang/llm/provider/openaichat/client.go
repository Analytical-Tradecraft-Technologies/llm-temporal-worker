package openaichat

import (
	"net/http"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/openaiwire"
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
	baseURL, err := clientconfig.KeyedClient("openai chat", config.BaseURL, "", "OpenAI API key", config.APIKey, config.HTTPClient)
	if err != nil {
		return nil, err
	}
	options, loopback := openaiwire.ClientOptions(baseURL, config.APIKey, config.HTTPClient)
	if loopback {
		return &Client{sdk: openai.Client{Options: options, Chat: openai.NewChatService(options...), Models: openai.NewModelService(options...)}, baseURL: baseURL}, nil
	}
	return &Client{sdk: openai.NewClient(options...), baseURL: baseURL}, nil
}

func (client *Client) options() []option.RequestOption {
	if client == nil || len(client.requestOptions) == 0 {
		return nil
	}
	return append([]option.RequestOption(nil), client.requestOptions...)
}
