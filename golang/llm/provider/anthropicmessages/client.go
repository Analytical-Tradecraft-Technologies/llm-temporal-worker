package anthropicmessages

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	anthropicaws "github.com/anthropics/anthropic-sdk-go/aws"
	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
)

type ClientConfig struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

// Client owns the official Anthropic Messages SDK client. Environment-based
// credential discovery is disabled so configuration snapshots remain the
// source of truth.
type Client struct {
	sdk      anthropic.Client
	messages messageService
	baseURL  string
}

type messageService interface {
	New(context.Context, anthropic.MessageNewParams, ...option.RequestOption) (*anthropic.Message, error)
}

func NewClient(config ClientConfig) (*Client, error) {
	baseURL, err := clientconfig.BaseURL(config.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("anthropic messages: %w", err)
	}
	if err := clientconfig.Secret("Anthropic API key", config.APIKey); err != nil {
		return nil, fmt.Errorf("anthropic messages: %w", err)
	}
	if config.HTTPClient == nil {
		return nil, fmt.Errorf("anthropic messages: HTTP client is required")
	}
	sdk := anthropic.NewClient(
		option.WithoutEnvironmentDefaults(),
		option.WithAPIKey(config.APIKey),
		option.WithBaseURL(baseURL),
		option.WithHTTPClient(config.HTTPClient),
		option.WithRequestTimeout(clientconfig.MessagesRequestTimeout(config.HTTPClient)),
		option.WithMaxRetries(0),
	)
	return &Client{sdk: sdk, messages: &sdk.Messages, baseURL: baseURL}, nil
}

// AWSClientConfig supplies explicit configuration for the Anthropic AWS
// gateway. Authentication and signing remain inside the official SDK client;
// neither credentials nor resolved auth headers enter provider calls.
type AWSClientConfig struct {
	BaseURL    string
	HTTPClient *http.Client
	AWSConfig  anthropicaws.ClientConfig
}

// MarshalJSON intentionally exposes only non-secret AWS routing metadata. The
// SDK's credential provider and signing state remain process-local.
func (config AWSClientConfig) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		BaseURL       string `json:"base_url,omitempty"`
		AWSRegion     string `json:"aws_region,omitempty"`
		AWSProfile    string `json:"aws_profile,omitempty"`
		WorkspaceID   string `json:"workspace_id,omitempty"`
		SkipAuth      bool   `json:"skip_auth,omitempty"`
		HasAPIKey     bool   `json:"has_api_key,omitempty"`
		HasAccessKey  bool   `json:"has_access_key,omitempty"`
		HasSessionKey bool   `json:"has_session_key,omitempty"`
	}{
		BaseURL:       config.BaseURL,
		AWSRegion:     config.AWSConfig.AWSRegion,
		AWSProfile:    config.AWSConfig.AWSProfile,
		WorkspaceID:   config.AWSConfig.WorkspaceID,
		SkipAuth:      config.AWSConfig.SkipAuth,
		HasAPIKey:     config.AWSConfig.APIKey != "",
		HasAccessKey:  config.AWSConfig.AWSAccessKey != "",
		HasSessionKey: config.AWSConfig.AWSSessionToken != "",
	})
}

// NewAWSClient constructs an Anthropic AWS gateway client using the SDK's
// supported authentication middleware. Routing identity is always explicit so
// ambient region, workspace, and base-URL environment defaults cannot change
// a configured endpoint. Authentication selection remains available to direct
// SDK callers; production route construction uses NewAWSGatewayClient.
func NewAWSClient(ctx context.Context, config AWSClientConfig) (*Client, error) {
	if ctx == nil {
		return nil, fmt.Errorf("anthropic AWS messages: context is required")
	}
	if config.HTTPClient == nil {
		return nil, fmt.Errorf("anthropic AWS messages: HTTP client is required")
	}
	if strings.TrimSpace(config.BaseURL) == "" {
		return nil, fmt.Errorf("anthropic AWS messages: AWS gateway base URL is required")
	}
	if strings.TrimSpace(config.AWSConfig.AWSRegion) == "" || strings.TrimSpace(config.AWSConfig.AWSRegion) != config.AWSConfig.AWSRegion {
		return nil, fmt.Errorf("anthropic AWS messages: AWS region is required")
	}
	if strings.TrimSpace(config.AWSConfig.WorkspaceID) == "" || strings.TrimSpace(config.AWSConfig.WorkspaceID) != config.AWSConfig.WorkspaceID {
		return nil, fmt.Errorf("anthropic AWS messages: AWS workspace ID is required")
	}
	if config.AWSConfig.BaseURL != "" {
		return nil, fmt.Errorf("anthropic AWS messages: base URL must be supplied through AWSClientConfig.BaseURL")
	}
	baseURL, err := clientconfig.BaseURL(config.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("anthropic AWS messages: %w", err)
	}
	config.AWSConfig.BaseURL = baseURL
	options := []option.RequestOption{
		option.WithoutEnvironmentDefaults(),
		option.WithHTTPClient(config.HTTPClient),
		option.WithRequestTimeout(clientconfig.MessagesRequestTimeout(config.HTTPClient)),
		option.WithMaxRetries(0),
	}
	client, err := anthropicaws.NewClient(ctx, config.AWSConfig, options...)
	if err != nil {
		return nil, fmt.Errorf("anthropic AWS messages: %w", err)
	}
	return &Client{messages: &client.Messages, baseURL: baseURL}, nil
}

// NewAWSGatewayClient is the production route constructor. It accepts only
// the official SDK's default AWS credential chain, which is workload-identity
// aware. API-key/static-credential modes and the SDK's AWS API-key fallback
// are rejected before a request client is constructed.
func NewAWSGatewayClient(ctx context.Context, config AWSClientConfig) (*Client, error) {
	if config.AWSConfig.SkipAuth || config.AWSConfig.APIKey != "" || config.AWSConfig.AWSAccessKey != "" || config.AWSConfig.AWSSecretAccessKey != "" || config.AWSConfig.AWSSessionToken != "" || config.AWSConfig.AWSProfile != "" {
		return nil, fmt.Errorf("anthropic AWS gateway: aws_default_chain authentication is required")
	}
	if os.Getenv("ANTHROPIC_AWS_API_KEY") != "" {
		return nil, fmt.Errorf("anthropic AWS gateway: ANTHROPIC_AWS_API_KEY is not permitted when aws_default_chain is configured")
	}
	return NewAWSClient(ctx, config)
}
