package openaiwire

import (
	"fmt"
	"net/http"

	"github.com/openai/openai-go/v3/option"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
)

// ExaBaseURL is the only base URL Exa's OpenAI-compatible endpoints accept.
const ExaBaseURL = "https://api.exa.ai/"

// ExaClientOptions validates an Exa endpoint's settings and returns its base
// URL and the OpenAI SDK options that reach it. Exa authenticates with
// x-api-key rather than a bearer token, and the SDK must not retry. label
// names the endpoint in errors, as in "exa chat".
func ExaClientOptions(label, rawBaseURL, apiKey string, httpClient *http.Client) (string, []option.RequestOption, error) {
	baseURL, err := clientconfig.BaseURL(rawBaseURL)
	if err != nil {
		return "", nil, fmt.Errorf("%s: %w", label, err)
	}
	if baseURL != ExaBaseURL {
		return "", nil, fmt.Errorf("%s: base URL must be exactly %q", label, ExaBaseURL)
	}
	if err := clientconfig.Secret("Exa API key", apiKey); err != nil {
		return "", nil, fmt.Errorf("%s: %w", label, err)
	}
	if httpClient == nil {
		return "", nil, fmt.Errorf("%s: HTTP client is required", label)
	}
	return baseURL, []option.RequestOption{
		option.WithBaseURL(baseURL),
		option.WithHTTPClient(httpClient),
		option.WithHeaderDel("Authorization"),
		option.WithHeader("x-api-key", apiKey),
		option.WithMaxRetries(0),
	}, nil
}
