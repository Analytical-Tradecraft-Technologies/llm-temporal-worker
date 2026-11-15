package adaptertest

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// ModelIDs joins the provider model IDs of models with commas, in order.
func ModelIDs(models []provider.Model) string {
	ids := make([]string, len(models))
	for i, model := range models {
		ids[i] = model.ProviderModelID
	}
	return strings.Join(ids, ",")
}

// NewModelLister builds the model-listing adapter under test over server's
// /v1 API with the API key "test-key".
type NewModelLister func(t *testing.T, server *httptest.Server) provider.ModelLister

// ListModelsNormalizesAndPagesDirectOpenAIModels checks that an OpenAI
// /v1/models listing is authenticated, sorted by ID, paged by cursor and
// keeps each model's creation time.
func ListModelsNormalizesAndPagesDirectOpenAIModels(t *testing.T, endpointID string, newLister NewModelLister) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	adapter := newLister(t, server)
	first, err := adapter.ListModels(nil, provider.ModelListQuery{EndpointID: endpointID, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if first.Complete || !strings.HasSuffix(first.NextCursor, ":2") || ModelIDs(first.Models) != "alpha,beta" {
		t.Fatalf("first page = %#v, want alpha,beta with cursor 2", first)
	}
	second, err := adapter.ListModels(context.Background(), provider.ModelListQuery{EndpointID: endpointID, Cursor: first.NextCursor, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !second.Complete || second.NextCursor != "" || ModelIDs(second.Models) != "zeta" {
		t.Fatalf("second page = %#v, want zeta complete", second)
	}
	if got := second.Models[0].CreatedAt.Unix(); got != 1700000000 {
		t.Fatalf("created timestamp = %d", got)
	}
}
