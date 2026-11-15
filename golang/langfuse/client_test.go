package langfuse

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExportCarriesCallerIdentityAndProvider(t *testing.T) {
	var body string
	c := newTestClient(t, 0, func(w http.ResponseWriter, r *http.Request) {
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
	})
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
	c := newTestClient(t, 0, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"partialSuccess":{"rejectedSpans":"1","errorMessage":"private response"}}`))
	})
	err := c.Export(context.Background(), minimalOperation(time.Now()))
	if !errors.Is(err, ErrRejected) || errors.Is(err, ErrExport) || strings.Contains(err.Error(), "private response") {
		t.Fatalf("partial success must be a safe, permanent rejection: %v", err)
	}
}

func TestExportAuthenticationRejectionIsPermanent(t *testing.T) {
	c := newTestClient(t, 0, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) })
	err := c.Export(context.Background(), Operation{})
	if err != ErrRejected {
		t.Fatalf("authentication failure must be permanent: %v", err)
	}
}

func TestExportBreakerFailsFastThenProbes(t *testing.T) {
	var calls atomic.Int32
	var healthy atomic.Bool
	c := newTestClient(t, 0, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if !healthy.Load() {
			w.WriteHeader(503)
			return
		}
		w.Write([]byte(`{}`))
	})
	now := time.Unix(1000, 0)
	c.now = func() time.Time { return now }
	op := minimalOperation(now)
	for i := 0; i < breakerThreshold; i++ {
		if err := c.Export(context.Background(), op); !errors.Is(err, ErrExport) || errors.Is(err, ErrUnavailable) {
			t.Fatalf("attempt %d: want a delivery failure, got %v", i, err)
		}
	}
	if err := c.Export(context.Background(), op); !errors.Is(err, ErrUnavailable) || !errors.Is(err, ErrExport) || errors.Is(err, ErrSaturated) {
		t.Fatalf("open breaker must fail fast as unavailable, not saturated: %v", err)
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

func TestExportLocalRejectionDoesNotResetBreaker(t *testing.T) {
	var calls atomic.Int32
	c := newTestClient(t, 0, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(503)
	})
	op := minimalOperation(time.Now())
	oversized := op
	oversized.Input = strings.Repeat("x", maxBody)
	for i := 0; i < breakerThreshold-1; i++ {
		c.Export(context.Background(), op)
	}
	if err := c.Export(context.Background(), oversized); !errors.Is(err, ErrOversized) || !errors.Is(err, ErrRejected) || errors.Is(err, ErrExport) {
		t.Fatalf("oversized payload must be rejected locally: %v", err)
	}
	c.Export(context.Background(), op)
	if err := c.Export(context.Background(), op); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("local rejection reset the breaker: %v", err)
	}
	if calls.Load() != breakerThreshold {
		t.Fatalf("Langfuse received %d calls, want %d", calls.Load(), breakerThreshold)
	}
}

func TestExportLimitsRequestsWaitingOnLangfuse(t *testing.T) {
	for _, tc := range []struct{ configured, want int }{{0, defaultMaxInFlight}, {3, 3}} {
		t.Run(strconv.Itoa(tc.configured), func(t *testing.T) {
			release := make(chan struct{})
			var arrived sync.WaitGroup
			arrived.Add(tc.want)
			c := newTestClient(t, tc.configured, func(w http.ResponseWriter, r *http.Request) {
				arrived.Done()
				<-release
				w.Write([]byte(`{}`))
			})
			op := minimalOperation(time.Now())
			results := make(chan error, tc.want)
			for i := 0; i < tc.want; i++ {
				go func() { results <- c.Export(context.Background(), op) }()
			}
			arrived.Wait()
			start := time.Now()
			if err := c.Export(context.Background(), op); !errors.Is(err, ErrSaturated) || !errors.Is(err, ErrUnavailable) {
				t.Fatalf("export beyond the in-flight limit must fail fast as saturated: %v", err)
			}
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("over-limit export waited %v", elapsed)
			}
			close(release)
			for i := 0; i < tc.want; i++ {
				if err := <-results; err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// A burst larger than the in-flight limit against a healthy Langfuse delivers
// every trace once callers retry ErrSaturated after a backoff, as the export
// Activity does, while requests on the wire never exceed the limit (#1459).
func TestExportBurstDeliversEveryTraceWithinLimit(t *testing.T) {
	const limit, burst = 2, 5
	var inFlight, peak, delivered atomic.Int32
	c := newTestClient(t, limit, func(w http.ResponseWriter, r *http.Request) {
		n := inFlight.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(100 * time.Millisecond)
		inFlight.Add(-1)
		delivered.Add(1)
		w.Write([]byte(`{}`))
	})
	op := minimalOperation(time.Now())
	var saturated atomic.Int32
	results := make(chan error, burst)
	for i := 0; i < burst; i++ {
		go func() {
			for {
				err := c.Export(context.Background(), op)
				if !errors.Is(err, ErrSaturated) {
					results <- err
					return
				}
				saturated.Add(1)
				time.Sleep(20 * time.Millisecond)
			}
		}()
	}
	for i := 0; i < burst; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if delivered.Load() != burst || peak.Load() > limit || saturated.Load() == 0 {
		t.Fatalf("delivered %d, peak in flight %d (limit %d), saturated refusals %d", delivered.Load(), peak.Load(), limit, saturated.Load())
	}
	// Saturation says nothing about Langfuse's health.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failures != 0 {
		t.Fatalf("saturation counted %d breaker failures", c.failures)
	}
}

// Only a transport or server failure says Langfuse is unhealthy. Throttling,
// rejected spans and the caller's own cancellation neither open the breaker
// nor hide earlier failures from it (#1670).
func TestExportBreakerCountsOnlyOutages(t *testing.T) {
	var status atomic.Int32
	arrived := make(chan struct{})
	release := make(chan struct{})
	c := newTestClient(t, 0, func(w http.ResponseWriter, r *http.Request) {
		switch code := int(status.Load()); code {
		case 0:
			// Hold the request until the test has canceled it, then drop the
			// connection. Returning normally would send an implicit 200, which
			// the client could read as success if it arrived before the
			// cancellation did.
			io.Copy(io.Discard, r.Body)
			arrived <- struct{}{}
			<-release
			panic(http.ErrAbortHandler)
		default:
			w.WriteHeader(code)
		}
	})
	op := minimalOperation(time.Now())
	for _, code := range []int32{429, 408} {
		status.Store(code)
		for i := 0; i < breakerThreshold+1; i++ {
			if err := c.Export(context.Background(), op); !errors.Is(err, ErrThrottled) || !errors.Is(err, ErrExport) || errors.Is(err, ErrUnavailable) {
				t.Fatalf("%d attempt %d: want throttled, got %v", code, i, err)
			}
		}
	}
	status.Store(0)
	for i := 0; i < breakerThreshold+1; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-arrived
			cancel()
		}()
		err := c.Export(ctx, op)
		release <- struct{}{}
		cancel()
		if !errors.Is(err, ErrExport) || errors.Is(err, ErrUnavailable) {
			t.Fatalf("canceled attempt %d: got %v", i, err)
		}
	}
	// Two outages, then neutral answers, then one more outage opens it: the
	// neutral answers neither reset nor add to the count.
	status.Store(503)
	for i := 0; i < breakerThreshold-1; i++ {
		c.Export(context.Background(), op)
	}
	status.Store(429)
	c.Export(context.Background(), op)
	status.Store(503)
	c.Export(context.Background(), op)
	if err := c.Export(context.Background(), op); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("throttling hid earlier failures from the breaker: %v", err)
	}
}

func TestExportClassifiesRedirectsAndPartialSuccess(t *testing.T) {
	var body atomic.Value
	var status atomic.Int32
	c := newTestClient(t, 0, func(w http.ResponseWriter, r *http.Request) {
		if code := int(status.Load()); code != 200 {
			w.Header().Set("Location", "https://elsewhere.example/")
			w.WriteHeader(code)
			return
		}
		w.Write([]byte(body.Load().(string)))
	})
	op := minimalOperation(time.Now())
	for _, code := range []int32{301, 302, 307, 308} {
		status.Store(code)
		if err := c.Export(context.Background(), op); !errors.Is(err, ErrRedirected) || !errors.Is(err, ErrRejected) {
			t.Fatalf("%d: want redirected, got %v", code, err)
		}
	}
	status.Store(200)
	for response, want := range map[string]error{
		`{"partialSuccess":{"rejectedSpans":"3"}}`:                          ErrRejected,
		`{"partialSuccess":{"rejectedSpans":0,"errorMessage":"a warning"}}`: nil,
		`{"partialSuccess":{}}`:                                             nil,
		`{}`:                                                                nil,
	} {
		body.Store(response)
		if err := c.Export(context.Background(), op); !errors.Is(err, want) || (want == nil && err != nil) {
			t.Fatalf("%s: got %v, want %v", response, err, want)
		}
	}
}

// newTestClient serves handler over TLS for the test's lifetime and returns a
// client authenticated as the "public"/"secret" project.
func newTestClient(t *testing.T, maxInFlight int, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	c, err := NewClient(server.URL, "public", "secret", server.Client(), maxInFlight)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// minimalOperation is a valid operation with fixed IDs and no generations.
func minimalOperation(at time.Time) Operation {
	return Operation{TraceID: strings.Repeat("1", 32), SpanID: strings.Repeat("2", 16), StartedAt: at, EndedAt: at}
}
