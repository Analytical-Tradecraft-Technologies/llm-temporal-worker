package openairesponses

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestListModelsNormalizesAndPagesDirectOpenAIModels(t *testing.T) {
	adaptertest.ListModelsNormalizesAndPagesDirectOpenAIModels(t, "openai-responses", newInventoryLister)
}

func TestListModelsRedactsProviderErrors(t *testing.T) {
	const secret = "sensitive-provider-response"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, secret, http.StatusBadGateway)
	}))
	defer server.Close()
	adapter := newInventoryAdapter(t, server)
	_, err := adapter.ListModels(context.Background(), provider.ModelListQuery{EndpointID: "openai-responses", Limit: 1})
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
	testassert.NoError(t, err)
	_, err = adapter.ListModels(context.Background(), provider.ModelListQuery{EndpointID: "other", Limit: 1})
	testassert.ErrorContains(t, err, "endpoint does not match")
}

// newInventoryAdapter builds the "openai-responses" model-listing adapter
// over server's /v1 API.
func newInventoryAdapter(t *testing.T, server *httptest.Server) *ModelListerAdapter {
	t.Helper()
	client, err := NewClient(ClientConfig{BaseURL: server.URL + "/v1", APIKey: "test-key", HTTPClient: server.Client()})
	testassert.NoError(t, err)
	adapter, err := NewOpenAIAdapter(client, "openai-responses", "responses-contract/v1")
	testassert.NoError(t, err)
	return adapter
}

func newInventoryLister(t *testing.T, server *httptest.Server) provider.ModelLister {
	return newInventoryAdapter(t, server)
}
