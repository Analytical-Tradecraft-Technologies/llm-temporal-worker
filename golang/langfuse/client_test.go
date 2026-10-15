package langfuse

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
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

func TestExportBreakerFailsFastThenProbes(t *testing.T) {
	var calls atomic.Int32
	var healthy atomic.Bool
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if !healthy.Load() {
			w.WriteHeader(503)
			return
		}
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	c, _ := NewClient(server.URL, "public", "secret", server.Client())
	now := time.Unix(1000, 0)
	c.now = func() time.Time { return now }
	op := Operation{TraceID: strings.Repeat("1", 32), SpanID: strings.Repeat("2", 16), StartedAt: now, EndedAt: now}
	for i := 0; i < breakerThreshold; i++ {
		if err := c.Export(context.Background(), op); !errors.Is(err, ErrExport) || errors.Is(err, ErrUnavailable) {
			t.Fatalf("attempt %d: want a delivery failure, got %v", i, err)
		}
	}
	if err := c.Export(context.Background(), op); !errors.Is(err, ErrUnavailable) || !errors.Is(err, ErrExport) {
		t.Fatalf("open breaker must fail fast: %v", err)
	}
	if calls.Load() != breakerThreshold {
		t.Fatalf("open breaker reached Langfuse: %d calls", calls.Load())
	}
	// A failed probe after the cooldown re-opens the breaker.
	now = now.Add(breakerCooldown)
	if err := c.Export(context.Background(), op); errors.Is(err, ErrUnavailable) || !errors.Is(err, ErrExport) {
		t.Fatalf("probe must reach Langfuse: %v", err)
	}
	if err := c.Export(context.Background(), op); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("failed probe must re-open the breaker: %v", err)
	}
	// A successful probe closes it.
	healthy.Store(true)
	now = now.Add(breakerCooldown)
	for i := 0; i < 2; i++ {
		if err := c.Export(context.Background(), op); err != nil {
			t.Fatalf("closed breaker export %d: %v", i, err)
		}
	}
}

func TestExportLimitsRequestsWaitingOnLangfuse(t *testing.T) {
	release := make(chan struct{})
	var arrived sync.WaitGroup
	arrived.Add(maxInFlight)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrived.Done()
		<-release
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	c, _ := NewClient(server.URL, "public", "secret", server.Client())
	op := Operation{TraceID: strings.Repeat("1", 32), SpanID: strings.Repeat("2", 16), StartedAt: time.Now(), EndedAt: time.Now()}
	results := make(chan error, maxInFlight)
	for i := 0; i < maxInFlight; i++ {
		go func() { results <- c.Export(context.Background(), op) }()
	}
	arrived.Wait()
	start := time.Now()
	if err := c.Export(context.Background(), op); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("export beyond the in-flight limit must fail fast: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("over-limit export waited %v", elapsed)
	}
	close(release)
	for i := 0; i < maxInFlight; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}
