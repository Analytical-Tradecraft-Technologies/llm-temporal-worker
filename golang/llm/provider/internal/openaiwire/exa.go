package openaiwire

import (
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
	baseURL, err := clientconfig.KeyedClient(label, rawBaseURL, ExaBaseURL, "Exa API key", apiKey, httpClient)
	if err != nil {
		return "", nil, err
	}
	return baseURL, []option.RequestOption{
		option.WithBaseURL(baseURL),
		option.WithHTTPClient(httpClient),
		option.WithHeaderDel("Authorization"),
		option.WithHeader("x-api-key", apiKey),
		option.WithMaxRetries(0),
	}, nil
}

// ClientOptions returns the SDK options of a client that sends key as a bearer
// token to baseURL. loopback reports a plain-HTTP loopback baseURL: the SDK
// serves credentialed plain HTTP only from its own direct transport, bypassing
// the guarded client, so the options send the key from the guarded client
// instead, and the caller must build only the services it uses, without the
// SDK's environment credential defaults that would re-enable that check.
func ClientOptions(baseURL, key string, httpClient *http.Client) (options []option.RequestOption, loopback bool) {
	if clientconfig.LoopbackHTTP(baseURL) {
		return []option.RequestOption{
			option.WithBaseURL(baseURL),
			option.WithHTTPClient(clientconfig.LoopbackBearerClient(httpClient, key)),
			option.WithMaxRetries(0),
		}, true
	}
	return []option.RequestOption{
		option.WithAPIKey(key),
		option.WithBaseURL(baseURL),
		option.WithHTTPClient(httpClient),
		option.WithMaxRetries(0),
	}, false
}
