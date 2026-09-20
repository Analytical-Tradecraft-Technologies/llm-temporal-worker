package openairesponses

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/azure"
	"github.com/openai/openai-go/v3/option"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
)

// AzureClientConfig contains the resolved values for one Azure OpenAI
// Responses endpoint. The caller owns secret resolution; this package never
// reads provider credentials from process environment variables.
type AzureClientConfig struct {
	Endpoint   string
	APIVersion string
	APIKey     string
	HTTPClient *http.Client
}

// AzureTokenClientConfig contains the resolved values for an Azure OpenAI
// Responses endpoint authenticated with an Azure token credential.
type AzureTokenClientConfig struct {
	Endpoint        string
	APIVersion      string
	TokenCredential azcore.TokenCredential
	HTTPClient      *http.Client
}

// NewAzureClient constructs an OpenAI Responses client with the official
// Azure endpoint and API-key middleware. The Azure SDK currently recognizes
// deployment routes for Chat Completions, but not the Responses route. Azure's
// Responses API is exposed at /openai/v1/responses, so the small path shim
// below extends the official middleware without changing request semantics or
// leaking SDK types from this package.
func NewAzureClient(config AzureClientConfig) (*Client, error) {
	endpoint, err := validateAzureConfig(config.Endpoint, config.APIVersion, config.HTTPClient)
	if err != nil {
		return nil, err
	}
	if err := clientconfig.Secret("Azure OpenAI API key", config.APIKey); err != nil {
		return nil, fmt.Errorf("azure responses: %w", err)
	}
	return &Client{sdk: openai.NewClient(
		azure.WithEndpoint(endpoint, config.APIVersion),
		azure.WithAPIKey(config.APIKey),
		option.WithHTTPClient(config.HTTPClient),
		option.WithMaxRetries(0),
		option.WithMiddleware(azureResponsesPathMiddleware),
	)}, nil
}

// NewAzureTokenClient constructs an OpenAI Responses client with the official
// Azure endpoint and token-credential middleware. It is intentionally separate
// from NewAzureClient so an auth mode cannot silently fall back to an API key
// or an environment-derived credential.
func NewAzureTokenClient(config AzureTokenClientConfig) (*Client, error) {
	endpoint, err := validateAzureConfig(config.Endpoint, config.APIVersion, config.HTTPClient)
	if err != nil {
		return nil, err
	}
	if !validTokenCredential(config.TokenCredential) {
		return nil, fmt.Errorf("azure responses: token credential is required")
	}
	return &Client{sdk: openai.NewClient(
		azure.WithEndpoint(endpoint, config.APIVersion),
		azure.WithTokenCredential(config.TokenCredential),
		option.WithHTTPClient(config.HTTPClient),
		option.WithMaxRetries(0),
		option.WithMiddleware(azureResponsesPathMiddleware),
	)}, nil
}

func validTokenCredential(credential azcore.TokenCredential) bool {
	if credential == nil {
		return false
	}
	value := reflect.ValueOf(credential)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return !value.IsNil()
	default:
		return true
	}
}

func validateAzureConfig(rawEndpoint, apiVersion string, httpClient *http.Client) (string, error) {
	endpoint, err := clientconfig.BaseURL(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("azure responses: %w", err)
	}
	if strings.TrimSpace(apiVersion) == "" {
		return "", fmt.Errorf("azure responses: API version is required")
	}
	if httpClient == nil {
		return "", fmt.Errorf("azure responses: HTTP client is required")
	}
	return endpoint, nil
}

// azureResponsesPathMiddleware fills the one route gap in the official Azure
// middleware: the SDK's deployment substitution table does not yet include
// /responses. The Responses API uses the Azure v1 path and keeps the model
// deployment in the JSON body, so no model or request fields are rewritten.
func azureResponsesPathMiddleware(request *http.Request, next option.MiddlewareNext) (*http.Response, error) {
	if request != nil && request.URL != nil && (request.URL.Path == "/openai/responses" || strings.HasPrefix(request.URL.Path, "/openai/responses/")) {
		request.URL.Path = strings.Replace(request.URL.Path, "/openai/responses", "/openai/v1/responses", 1)
		request.URL.RawPath = ""
	}
	return next(request)
}

// NewAzureAdapter constructs an adapter for one Azure Responses endpoint.
// Endpoint and capability identity remain provider-neutral at the adapter
// boundary; only the client construction uses Azure-specific middleware.
func NewAzureAdapter(client *Client, endpointID, capabilityVersion string) (*BackgroundAdapter, error) {
	base, err := New(client, endpointID, capabilityVersion)
	if err != nil {
		return nil, err
	}
	return &BackgroundAdapter{base}, nil
}

// Explicit aliases keep the provider name visible at call sites that compose
// multiple OpenAI-compatible clients.
func NewAzureOpenAIClient(config AzureClientConfig) (*Client, error) {
	return NewAzureClient(config)
}

func NewAzureOpenAITokenClient(config AzureTokenClientConfig) (*Client, error) {
	return NewAzureTokenClient(config)
}

func NewAzureOpenAIAdapter(client *Client, endpointID, capabilityVersion string) (*BackgroundAdapter, error) {
	return NewAzureAdapter(client, endpointID, capabilityVersion)
}
