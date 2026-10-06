package openaichat

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/shared"
)

// A loopback development endpoint is still reached through the configured,
// guarded HTTP client (egress policy, response-size limit, pre-dispatch
// evidence), which sends the key itself; the SDK's environment credentials do
// not switch it back to the SDK's own transport (#1087).
func TestNewClientSendsLoopbackHTTPThroughTheGuardedClient(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "environment-key")
	var requests atomic.Int32
	server := newLoopbackHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("request path = %q, want /v1/chat/completions", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer loopback-key" {
			t.Errorf("Authorization = %q, want bearer loopback key", got)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"chatcmpl-1","object":"chat.completion","created":1700000000,"model":"gpt-loopback","choices":[]}`)
	}))
	var guarded atomic.Int32
	configured := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		guarded.Add(1)
		return http.DefaultTransport.RoundTrip(request)
	})}
	client, err := NewClient(ClientConfig{BaseURL: server.URL + "/v1", APIKey: "loopback-key", HTTPClient: configured})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.sdk.Chat.Completions.New(context.Background(), openai.ChatCompletionNewParams{Model: shared.ChatModel("gpt-loopback"), Messages: []openai.ChatCompletionMessageParamUnion{openai.UserMessage("hi")}}); err != nil {
		t.Fatal(err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("loopback requests = %d, want 1", got)
	}
	if got := guarded.Load(); got != 1 {
		t.Fatalf("guarded client carried %d requests, want 1", got)
	}
}

func TestNewAzureClientSendsCredentialsToLoopbackHTTPThroughSDKTransport(t *testing.T) {
	var requests atomic.Int32
	server := newLoopbackHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/openai/deployments/deployment-a/chat/completions" {
			t.Errorf("request path = %q, want Azure deployment chat path", r.URL.Path)
		}
		if got := r.Header.Get("Api-Key"); got != "loopback-azure-key" {
			t.Errorf("Api-Key = %q, want loopback Azure key", got)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"chatcmpl-1","object":"chat.completion","created":1700000000,"model":"deployment-a","choices":[]}`)
	}))
	client, err := NewAzureClient(AzureClientConfig{Endpoint: server.URL, APIVersion: "2025-01-01", APIKey: "loopback-azure-key", HTTPClient: rejectingHTTPClient()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.sdk.Chat.Completions.New(context.Background(), openai.ChatCompletionNewParams{Model: shared.ChatModel("deployment-a"), Messages: []openai.ChatCompletionMessageParamUnion{openai.UserMessage("hi")}}); err != nil {
		t.Fatal(err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("loopback requests = %d, want 1", got)
	}
}

func newLoopbackHTTPServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("test environment does not allow a loopback listener: %v", err)
	}
	server := &httptest.Server{Listener: listener, Config: &http.Server{Handler: handler}}
	server.Start()
	t.Cleanup(server.Close)
	return server
}

func rejectingHTTPClient() *http.Client {
	return &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("configured HTTP client must not carry loopback HTTP requests")
	})}
}
