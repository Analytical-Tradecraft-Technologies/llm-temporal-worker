package bedrockmessages

import (
	"context"
	"fmt"
	"net/http"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/bedrock"
	"github.com/anthropics/anthropic-sdk-go/option"
	awssdk "github.com/aws/aws-sdk-go-v2/aws"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
)

type ClientConfig struct {
	BaseURL    string
	HTTPClient *http.Client
	AWSConfig  awssdk.Config
}

// Client owns the official Anthropic SDK Bedrock middleware and client. AWS
// signing is delegated to the SDK; credentials are never copied into a
// provider-neutral request or configuration value.
type Client struct {
	sdk      anthropic.Client
	messages messageService
	baseURL  string
}

type messageService interface {
	New(context.Context, anthropic.MessageNewParams, ...option.RequestOption) (*anthropic.Message, error)
}

func NewClient(ctx context.Context, config ClientConfig) (*Client, error) {
	if ctx == nil {
		return nil, fmt.Errorf("bedrock messages: context is required")
	}
	baseURL := ""
	if config.BaseURL != "" {
		validated, err := clientconfig.BaseURL(config.BaseURL)
		if err != nil {
			return nil, fmt.Errorf("bedrock messages: %w", err)
		}
		baseURL = validated
	}
	if config.HTTPClient == nil {
		return nil, fmt.Errorf("bedrock messages: HTTP client is required")
	}
	if config.AWSConfig.Region == "" {
		return nil, fmt.Errorf("bedrock messages: AWS region is required")
	}
	opts := []option.RequestOption{
		option.WithoutEnvironmentDefaults(),
		option.WithHTTPClient(config.HTTPClient),
		option.WithRequestTimeout(clientconfig.MessagesRequestTimeout(config.HTTPClient)),
		option.WithMaxRetries(0),
		bedrock.WithConfig(config.AWSConfig),
	}
	if baseURL != "" {
		// Keep the SDK's Bedrock request rewriting/signing while allowing a
		// loopback endpoint in contract tests.
		opts = append(opts, option.WithBaseURL(baseURL))
	}
	sdk := anthropic.NewClient(opts...)
	return &Client{sdk: sdk, messages: &sdk.Messages, baseURL: baseURL}, nil
}
