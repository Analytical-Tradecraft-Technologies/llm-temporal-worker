package openairesponses

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/openai/openai-go/v3/responses"
	"github.com/openai/openai-go/v3/shared"
)

// The SDK only sends credentials over plain HTTP to loopback endpoints through
// its own direct transport, so the configured HTTP client is not consulted.
func TestNewClientSendsCredentialsToLoopbackHTTPThroughSDKTransport(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("test environment does not allow a loopback listener: %v", err)
	}
	var requests atomic.Int32
	server := &httptest.Server{Listener: listener, Config: &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/v1/responses" {
			t.Errorf("request path = %q, want /v1/responses", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer loopback-key" {
			t.Errorf("Authorization = %q, want bearer loopback key", got)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"resp_1","object":"response","created_at":1700000000,"model":"gpt-loopback","status":"completed","output":[]}`)
	})}}
	server.Start()
	t.Cleanup(server.Close)
	unused := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("configured HTTP client must not carry loopback HTTP requests")
	})}
	client, err := NewClient(ClientConfig{BaseURL: server.URL + "/v1", APIKey: "loopback-key", HTTPClient: unused})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.sdk.Responses.New(context.Background(), responses.ResponseNewParams{Model: shared.ResponsesModel("gpt-loopback")}); err != nil {
		t.Fatal(err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("loopback requests = %d, want 1", got)
	}
}
