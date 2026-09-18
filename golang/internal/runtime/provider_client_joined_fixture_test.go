//go:build llmtw_joined_fixture

package runtime

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestJoinedFixtureProviderClientIsExactAndHostPinned(t *testing.T) {
	endpoint := config.EndpointConfig{
		Family:        "openai_chat",
		BaseURL:       joinedFixtureProviderBaseURL,
		OutboundHosts: []string{"joined-smoke"},
		Timeout:       config.Duration(5 * time.Second),
	}
	client, err := newProviderEgressHTTPClient(nil, endpoint, 1024, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := client.Transport.(*joinedFixtureRoundTripper)
	if !ok {
		t.Fatalf("fixture transport = %T", client.Transport)
	}
	request, err := http.NewRequest(http.MethodPost, "https://not-joined-smoke/v1/chat/completions", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = transport.RoundTrip(request); !errors.Is(err, provider.ErrProviderEgressDenied) {
		t.Fatalf("cross-host request error = %v", err)
	}

	endpoint.Family = "openai_responses"
	if _, err = newProviderEgressHTTPClient(nil, endpoint, 1024, nil, nil); !errors.Is(err, provider.ErrProviderEgressDenied) {
		t.Fatalf("wrong-family fixture error = %v", err)
	}
}

func TestJoinedFixtureBuildRetainsProductionGuardForOtherEndpoints(t *testing.T) {
	endpoint := config.EndpointConfig{
		Family:        "openai_chat",
		BaseURL:       "https://api.example.test/v1",
		OutboundHosts: []string{"api.example.test"},
		Timeout:       config.Duration(5 * time.Second),
	}
	client, err := newProviderEgressHTTPClient(nil, endpoint, 1024, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := client.Transport.(*providerEgressRoundTripper); !ok {
		t.Fatalf("non-fixture transport = %T", client.Transport)
	}
}
