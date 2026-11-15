package contract

import (
	"errors"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"io"
	"net/http"
	"strings"
	"testing"
)

type closeTrackingRoundTripper struct {
	closed bool
}

func (transport *closeTrackingRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("unexpected request")
}

func (transport *closeTrackingRoundTripper) CloseIdleConnections() {
	transport.closed = true
}

func TestRetryServerStartsEmpty(t *testing.T) {
	server := NewRetryServer(t)
	t.Cleanup(server.Close)
	if server.URL == "" || server.Count() != 0 {
		t.Fatalf("retry server = %#v", server)
	}
}

func TestRetryServerReturnsRetryableContractResponseAndCountsCalls(t *testing.T) {
	server := NewRetryServer(t)
	t.Cleanup(server.Close)
	for attempt := 1; attempt <= 2; attempt++ {
		request, err := http.NewRequest(http.MethodPost, server.URL, strings.NewReader(`{"request":"payload"}`))
		testassert.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		response, err := server.HTTPClient.Do(request)
		testassert.NoError(t, err)
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		testassert.NoError(t, err)
		if response.StatusCode != http.StatusInternalServerError || response.Header.Get("Content-Type") != "application/json" || string(body) != `{"error":{"message":"retryable contract failure"}}` {
			t.Fatalf("response = status %d, headers %#v, body %q", response.StatusCode, response.Header, body)
		}
		if response.Request != request {
			t.Fatal("response did not retain the originating request")
		}
		if got := server.Count(); got != int64(attempt) {
			t.Fatalf("Count() after attempt %d = %d", attempt, got)
		}
	}
}

func TestRetryServerNilLifecycleMethodsAreSafe(t *testing.T) {
	var server *RetryServer
	server.Close()
	if got := server.Count(); got != 0 {
		t.Fatalf("nil Count() = %d", got)
	}
	(&RetryServer{}).Close()
}

func TestRetryServerCloseClosesIdleConnections(t *testing.T) {
	transport := &closeTrackingRoundTripper{}
	server := &RetryServer{HTTPClient: &http.Client{Transport: transport}}

	server.Close()

	if !transport.closed {
		t.Fatal("Close did not close idle connections")
	}
}
