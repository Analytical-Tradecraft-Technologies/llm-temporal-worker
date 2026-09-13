//go:build llmtw_joined_fixture

package runtime

import (
	"errors"
	"net/http"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
)

const joinedFixtureProviderBaseURL = "https://joined-smoke/v1"

type joinedFixtureRoundTripper struct {
	policy           *providerEgressPolicy
	next             http.RoundTripper
	maxResponseBytes int64
}

// newProviderEgressHTTPClient keeps the production egress guard for every
// endpoint except the one exact HTTPS service compiled into the isolated
// joined-smoke image. This build tag is never part of a release worker image;
// it permits the deterministic Compose fixture to remain network-local without
// adding a runtime switch that could weaken production SSRF controls.
func newProviderEgressHTTPClient(base *http.Client, endpoint config.EndpointConfig, maxResponseBytes int, resolver ProviderEgressResolver, dial ProviderEgressDialContext) (*http.Client, error) {
	if endpoint.BaseURL != joinedFixtureProviderBaseURL {
		return newGuardedProviderEgressHTTPClient(base, endpoint, maxResponseBytes, resolver, dial)
	}
	if endpoint.Family != "openai_chat" || len(endpoint.OutboundHosts) != 1 || endpoint.OutboundHosts[0] != "joined-smoke" {
		return nil, deniedProviderEgress("invalid_joined_fixture_policy")
	}
	if maxResponseBytes <= 0 {
		return nil, errors.New("provider response byte limit must be positive")
	}
	allowedHosts, err := configuredProviderHosts(endpoint)
	if err != nil {
		return nil, err
	}
	transport, err := cloneProviderTransport(base)
	if err != nil {
		return nil, err
	}
	timeout := boundedProviderRequestTimeout(time.Duration(endpoint.Timeout))
	transport.Proxy = nil
	transport.DialTLSContext = nil
	transport.DialTLS = nil
	transport.TLSHandshakeTimeout = boundedProviderConnectTimeout(timeout)
	transport.ResponseHeaderTimeout = timeout
	transport.ExpectContinueTimeout = minDuration(time.Second, boundedProviderConnectTimeout(timeout))
	transport.DisableCompression = true
	return &http.Client{
		Transport: &joinedFixtureRoundTripper{
			policy:           &providerEgressPolicy{allowedHosts: allowedHosts},
			next:             transport,
			maxResponseBytes: int64(maxResponseBytes),
		},
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, nil
}

func (transport *joinedFixtureRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if transport == nil || transport.policy == nil || transport.next == nil {
		return nil, deniedProviderEgress("invalid_joined_fixture_transport")
	}
	if err := transport.policy.authorizeRequest(request); err != nil {
		recordProviderEgressDenied(request, err)
		return nil, err
	}
	response, err := transport.next.RoundTrip(request)
	if err != nil || response == nil {
		return response, err
	}
	return boundProviderResponse(response, transport.maxResponseBytes)
}
