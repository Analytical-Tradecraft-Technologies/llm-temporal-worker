package openairesponses

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/openai/openai-go/v3/packages/param"
	"github.com/openai/openai-go/v3/responses"
	"github.com/openai/openai-go/v3/shared"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/contract"
)

func TestSDKRetriesDisabled(t *testing.T) {
	server := contract.NewRetryServer(t)
	client, err := NewClient(ClientConfig{BaseURL: server.URL, APIKey: "test-key", HTTPClient: server.HTTPClient})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.sdk.Responses.New(context.Background(), responses.ResponseNewParams{
		Model: shared.ResponsesModel("gpt-contract"),
		Input: responses.ResponseNewParamsInputUnion{OfString: param.NewOpt("hello")},
	})
	if err == nil {
		t.Fatal("expected retryable response error")
	}
	if got := server.Count(); got != 1 {
		t.Fatalf("request count = %d, want 1", got)
	}
}

func TestClientValidatesResolvedConfig(t *testing.T) {
	for _, config := range []ClientConfig{
		{BaseURL: "https://api.openai.com/v1", APIKey: "", HTTPClient: http.DefaultClient},
		{BaseURL: "https://api.openai.com/v1", APIKey: "key", HTTPClient: nil},
		{BaseURL: "http://untrusted.example", APIKey: "key", HTTPClient: http.DefaultClient},
	} {
		if _, err := NewClient(config); err == nil {
			t.Errorf("NewClient(%#v) unexpectedly succeeded", config)
		}
	}
}

func TestClientHonorsInjectedRedirectPolicy(t *testing.T) {
	var requests atomic.Int32
	server := newLoopbackTLSServer(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		http.Redirect(response, request, "/provider-redirect-target", http.StatusFound)
	}))

	httpClient := server.Client()
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	client, err := NewClient(ClientConfig{BaseURL: server.URL, APIKey: "test-key", HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.sdk.Responses.New(context.Background(), responses.ResponseNewParams{Model: shared.ResponsesModel("gpt-contract")})
	if err == nil {
		t.Fatal("expected the SDK to surface the redirect response")
	}
	if got, want := requests.Load(), int32(1); got != want {
		t.Fatalf("provider requests = %d, want %d without redirect follow", got, want)
	}
}

func newLoopbackTLSServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("test environment does not allow a loopback listener: %v", err)
	}
	server := &httptest.Server{Listener: listener, Config: &http.Server{Handler: handler}}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}
