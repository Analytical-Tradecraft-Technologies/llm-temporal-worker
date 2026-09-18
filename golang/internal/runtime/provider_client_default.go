//go:build !llmtw_joined_fixture

package runtime

import (
	"net/http"

	"github.com/mfow/llm-temporal-worker/golang/config"
)

func newProviderEgressHTTPClient(base *http.Client, endpoint config.EndpointConfig, maxResponseBytes int, resolver ProviderEgressResolver, dial ProviderEgressDialContext) (*http.Client, error) {
	return newGuardedProviderEgressHTTPClient(base, endpoint, maxResponseBytes, resolver, dial)
}
