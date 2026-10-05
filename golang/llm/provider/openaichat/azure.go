package openaichat

import (
	"fmt"
	"net/http"
	"strings"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/azure"
	"github.com/openai/openai-go/v3/option"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
)

type AzureClientConfig struct {
	Endpoint   string
	APIVersion string
	APIKey     string
	HTTPClient *http.Client
}

// NewAzureClient uses the official Azure middleware for deployment path
// rewriting and Api-Key authentication. The SDK retry policy remains fixed at
// zero so operation-level retry/ledger code remains authoritative.
func NewAzureClient(config AzureClientConfig) (*Client, error) {
	endpoint, err := clientconfig.BaseURL(config.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("azure chat: %w", err)
	}
	if strings.TrimSpace(config.APIVersion) == "" {
		return nil, fmt.Errorf("azure chat: API version is required")
	}
	if err := clientconfig.Secret("Azure OpenAI API key", config.APIKey); err != nil {
		return nil, fmt.Errorf("azure chat: %w", err)
	}
	if config.HTTPClient == nil {
		return nil, fmt.Errorf("azure chat: HTTP client is required")
	}
	options := []option.RequestOption{
		azure.WithEndpoint(endpoint, config.APIVersion),
		azure.WithAPIKey(config.APIKey),
		option.WithHTTPClient(config.HTTPClient),
		option.WithMaxRetries(0),
	}
	if clientconfig.LoopbackHTTP(endpoint) {
		// Azure enforces its own HTTPS-only credential policy; loopback HTTP
		// needs the Azure opt-in and is served by the SDK's direct transport.
		options = append(options, azure.WithUnsafeAllowHTTP())
	}
	return &Client{sdk: openai.NewClient(options...), baseURL: endpoint}, nil
}

type AzureProfileConfig struct {
	ID                        string
	CapabilityVersion         string
	BaseURL                   string
	Deployment                string
	Capabilities              provider.CapabilitySet
	ServiceTiers              map[llm.ServiceClass]string
	ActualServiceClasses      map[string]llm.ServiceClass
	MissingActualServiceClass llm.ServiceClass
	AllowedExtensions         map[string]ExtensionSpec
}

func NewAzureProfile(config AzureProfileConfig) (Profile, error) {
	baseURL, err := clientconfig.BaseURL(config.BaseURL)
	if err != nil {
		return Profile{}, fmt.Errorf("azure chat profile: %w", err)
	}
	if strings.TrimSpace(config.Deployment) == "" {
		return Profile{}, fmt.Errorf("azure chat profile: deployment is required")
	}
	return NewProfile(Profile{
		// The developer role is not accepted by every API version or
		// compatible server; system is.
		ApplicationInstructionRole: "system",
		ID:                         config.ID,
		CapabilityVersion:          config.CapabilityVersion,
		Capabilities:               config.Capabilities,
		ServiceTiers:               config.ServiceTiers,
		ActualServiceClasses:       config.ActualServiceClasses,
		MissingActualServiceClass:  config.MissingActualServiceClass,
		// The Azure OpenAI Chat Completions specification defines no
		// service_tier, on the request or the response.
		OmitServiceTier:   true,
		AllowedExtensions: config.AllowedExtensions,
		ExpectedBaseURL:   baseURL,
		ExpectedModel:     config.Deployment,
	})
}

func NewAzureAdapter(client *Client, endpointID string, config AzureProfileConfig) (*Adapter, error) {
	profile, err := NewAzureProfile(config)
	if err != nil {
		return nil, err
	}
	return New(client, endpointID, profile)
}

// Aliases keep the provider name explicit at call sites that construct more
// than one OpenAI-compatible client.
func NewAzureOpenAIClient(config AzureClientConfig) (*Client, error) {
	return NewAzureClient(config)
}

func NewAzureOpenAIProfile(config AzureProfileConfig) (Profile, error) {
	return NewAzureProfile(config)
}

func NewAzureOpenAIAdapter(client *Client, endpointID string, config AzureProfileConfig) (*Adapter, error) {
	return NewAzureAdapter(client, endpointID, config)
}
