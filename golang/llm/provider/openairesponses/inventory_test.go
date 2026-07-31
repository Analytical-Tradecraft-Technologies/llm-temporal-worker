package openairesponses

import (
	"context"
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
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"object":"list","data":[{"id":"zeta","object":"model","created":1700000000,"owned_by":"openai"},{"id":"alpha","object":"model","created":1700000001,"owned_by":"openai"},{"id":"beta","object":"model","created":1700000002,"owned_by":"openai"}]}`)
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{BaseURL: server.URL + "/v1", APIKey: "test-key", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewOpenAIAdapter(client, "openai-responses", "responses-contract/v1")
	if err != nil {
		t.Fatal(err)
	}
	first, err := adapter.ListModels(nil, provider.ModelListQuery{EndpointID: "openai-responses", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if first.Complete || !strings.HasSuffix(first.NextCursor, ":2") || modelIDs(first.Models) != "alpha,beta" {
		t.Fatalf("first page = %#v, want alpha,beta with cursor 2", first)
	}
	second, err := adapter.ListModels(context.Background(), provider.ModelListQuery{EndpointID: "openai-responses", Cursor: first.NextCursor, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !second.Complete || second.NextCursor != "" || modelIDs(second.Models) != "zeta" {
		t.Fatalf("second page = %#v, want zeta complete", second)
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
	adapter, err := NewOpenAIAdapter(client, "openai-responses", "responses-contract/v1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.ListModels(context.Background(), provider.ModelListQuery{EndpointID: "openai-responses", Limit: 1})
	if err == nil || !strings.Contains(err.Error(), "provider model inventory request failed") || strings.Contains(err.Error(), secret) {
		t.Fatalf("redacted error = %v", err)
	}
}

func TestListModelsUnsupportedWithoutDirectCapability(t *testing.T) {
	if _, ok := any(&Adapter{}).(provider.ModelLister); ok {
		t.Fatal("compatible adapter unexpectedly satisfies provider.ModelLister")
	}
}

func TestListModelsRejectsEndpointMismatch(t *testing.T) {
	adapter, err := NewOpenAIAdapter(&Client{directOpenAI: true}, "openai-responses", "responses-contract/v1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.ListModels(context.Background(), provider.ModelListQuery{EndpointID: "other", Limit: 1})
	if err == nil || !strings.Contains(err.Error(), "endpoint does not match") {
		t.Fatalf("mismatched endpoint error = %v", err)
	}
}

func modelIDs(models []provider.Model) string {
	ids := make([]string, len(models))
	for i, model := range models {
		ids[i] = model.ProviderModelID
	}
	return strings.Join(ids, ",")
}
