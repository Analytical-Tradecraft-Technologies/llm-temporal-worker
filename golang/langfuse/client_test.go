package langfuse

import (
	"context"
	"encoding/json"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestExportCarriesCallerIdentityAndProvider(t *testing.T) {
	var body string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != "public" || p != "secret" {
			t.Error("missing project authentication")
		}
		if r.URL.Path != "/api/public/otel/v1/traces" || r.Header.Get("x-langfuse-ingestion-version") != "4" {
			t.Error("incorrect ingestion protocol")
		}
		var v any
		if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
			t.Error(err)
		}
		b, _ := json.Marshal(v)
		body = string(b)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	c, err := NewClient(server.URL, "public", "secret", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	op := Operation{TraceID: strings.Repeat("1", 32), SpanID: strings.Repeat("2", 16), Kind: "generate", StartedAt: now, EndedAt: now.Add(time.Second), Context: llm.RequestContext{Tenant: "tenant-a", Project: "project-a", Actor: "client-a", Tags: map[string]string{"application": "forecaster"}}, Generations: []Generation{{ID: strings.Repeat("3", 16), StartedAt: now, EndedAt: now.Add(time.Second), Model: "gpt-test", Provider: "openrouter", Endpoint: "router", Input: map[string]any{"prompt": "hello"}, Output: map[string]any{"answer": "world"}}}}
	if err := c.Export(context.Background(), op); err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"langfuse.user.id", "client-a", "langfuse.observation.metadata.tenant", "tenant-a", "project-a", "forecaster", "openrouter", "hello", "world"} {
		if !strings.Contains(body, s) {
			t.Errorf("payload missing %s", s)
		}
	}
}

func TestExportRejectsPartialSuccessAndNeverLeaksResponse(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"partialSuccess":{"rejectedSpans":"1","errorMessage":"private response"}}`))
	}))
	defer server.Close()
	c, _ := NewClient(server.URL, "public", "secret", server.Client())
	err := c.Export(context.Background(), Operation{TraceID: strings.Repeat("1", 32), SpanID: strings.Repeat("2", 16), StartedAt: time.Now(), EndedAt: time.Now()})
	if err == nil || strings.Contains(err.Error(), "private response") {
		t.Fatalf("unsafe or absent failure: %v", err)
	}
}

func TestExportAuthenticationRejectionIsPermanent(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }))
	defer server.Close()
	c, _ := NewClient(server.URL, "public", "secret", server.Client())
	err := c.Export(context.Background(), Operation{})
	if err != ErrRejected {
		t.Fatalf("authentication failure must be permanent: %v", err)
	}
}
