package openaichat

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestListModelsNormalizesAndPagesDirectOpenAIModels(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Fatalf("request path = %q, want /v1/models", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Fatalf("authorization = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"object":"list","data":[{"id":"zeta","object":"model","created":1700000000,"owned_by":"openai"},{"id":"alpha","object":"model","created":1700000001,"owned_by":"openai"},{"id":"beta","object":"model","created":1700000002,"owned_by":"openai"}]}`)
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{BaseURL: server.URL + "/v1", APIKey: "test-key", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "openai-direct", testProfile())
	if err != nil {
		t.Fatal(err)
	}
	first, err := adapter.ListModels(nil, provider.ModelListQuery{EndpointID: "openai-direct", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if first.Complete || first.NextCursor != "2" || modelIDs(first.Models) != "alpha,beta" {
		t.Fatalf("first page = %#v, want alpha,beta with cursor 2", first)
	}
	second, err := adapter.ListModels(context.Background(), provider.ModelListQuery{EndpointID: "openai-direct", Cursor: first.NextCursor, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !second.Complete || second.NextCursor != "" || modelIDs(second.Models) != "zeta" {
		t.Fatalf("second page = %#v, want zeta complete", second)
	}
	if got := second.Models[0].CreatedAt.Unix(); got != 1700000000 {
		t.Fatalf("created timestamp = %d", got)
	}
}

func TestListModelsRedactsProviderErrors(t *testing.T) {
	const secret = "sensitive-provider-response"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, secret, http.StatusBadGateway)
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{BaseURL: server.URL + "/v1", APIKey: "test-key", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "openai-direct", testProfile())
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.ListModels(context.Background(), provider.ModelListQuery{EndpointID: "openai-direct", Limit: 1})
	if err == nil || !strings.Contains(err.Error(), "provider model inventory request failed") || strings.Contains(err.Error(), secret) {
		t.Fatalf("redacted error = %v", err)
	}
}

func TestListModelsUnsupportedWithoutDirectCapability(t *testing.T) {
	adapter := &Adapter{client: &Client{}}
	_, err := adapter.ListModels(context.Background(), provider.ModelListQuery{EndpointID: "compatible", Limit: 1})
	if !errors.Is(err, provider.ErrModelInventoryUnsupported) {
		t.Fatalf("error = %v, want unsupported", err)
	}
}

func modelIDs(models []provider.Model) string {
	ids := make([]string, len(models))
	for i, model := range models {
		ids[i] = model.ProviderModelID
	}
	return strings.Join(ids, ",")
}
