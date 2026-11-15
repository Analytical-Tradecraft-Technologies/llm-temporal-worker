package openaichat

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestListModelsNormalizesAndPagesDirectOpenAIModels(t *testing.T) {
	adaptertest.ListModelsNormalizesAndPagesDirectOpenAIModels(t, "openai-direct", newTestKeyInventoryLister)
}

func TestListModelsRedactsProviderErrors(t *testing.T) {
	const secret = "sensitive-provider-response"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, secret, http.StatusBadGateway)
	}))
	defer server.Close()
	adapter := newInventoryAdapter(t, server, "test-key")
	_, err := adapter.ListModels(context.Background(), provider.ModelListQuery{EndpointID: "openai-direct", Limit: 1})
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
	profile, err := NewOpenAIProfile(testProfile())
	testassert.NoError(t, err)
	adapter, err := NewOpenAIAdapter(&Client{}, "openai-direct", profile)
	testassert.NoError(t, err)
	_, err = adapter.ListModels(context.Background(), provider.ModelListQuery{EndpointID: "other", Limit: 1})
	testassert.ErrorContains(t, err, "endpoint does not match")
}

func TestListModelsRejectsChangedSnapshotCursor(t *testing.T) {
	calls := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		if calls == 1 {
			io.WriteString(w, `{"object":"list","data":[{"id":"alpha","created":1,"owned_by":"openai"},{"id":"beta","created":2,"owned_by":"openai"}]}`)
			return
		}
		io.WriteString(w, `{"object":"list","data":[{"id":"alpha","created":1,"owned_by":"openai"},{"id":"changed","created":2,"owned_by":"openai"}]}`)
	}))
	defer server.Close()
	adapter := newInventoryAdapter(t, server, "key")
	first, err := adapter.ListModels(context.Background(), provider.ModelListQuery{EndpointID: "openai-direct", Limit: 1})
	testassert.NoError(t, err)
	_, err = adapter.ListModels(context.Background(), provider.ModelListQuery{EndpointID: "openai-direct", Cursor: first.NextCursor, Limit: 1})
	testassert.ErrorContains(t, err, "snapshot changed")
}

// newInventoryAdapter builds the "openai-direct" model-listing adapter over
// server's /v1 API with apiKey.
func newInventoryAdapter(t *testing.T, server *httptest.Server, apiKey string) *ModelListerAdapter {
	t.Helper()
	client, err := NewClient(ClientConfig{BaseURL: server.URL + "/v1", APIKey: apiKey, HTTPClient: server.Client()})
	testassert.NoError(t, err)
	profile, err := NewOpenAIProfile(testProfile())
	testassert.NoError(t, err)
	adapter, err := NewOpenAIAdapter(client, "openai-direct", profile)
	testassert.NoError(t, err)
	return adapter
}

func newTestKeyInventoryLister(t *testing.T, server *httptest.Server) provider.ModelLister {
	return newInventoryAdapter(t, server, "test-key")
}
