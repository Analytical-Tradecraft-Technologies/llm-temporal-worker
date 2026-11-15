package adaptertest

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// LoopbackJSONServer serves body as JSON on a loopback listener, reporting
// any request for another path or without header set to want, and counts the
// requests it receives. The test is skipped where no loopback listener can be
// opened.
func LoopbackJSONServer(t *testing.T, path, header, want, body string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	requests := &atomic.Int32{}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("test environment does not allow a loopback listener: %v", err)
	}
	server := &httptest.Server{Listener: listener, Config: &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != path {
			t.Errorf("request path = %q, want %s", r.URL.Path, path)
		}
		if got := r.Header.Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, body)
	})}}
	server.Start()
	t.Cleanup(server.Close)
	return server, requests
}

// RejectingHTTPClient fails every request, for a client that must send
// loopback HTTP through the SDK's own transport instead.
func RejectingHTTPClient() *http.Client {
	return &http.Client{Transport: RoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("configured HTTP client must not carry loopback HTTP requests")
	})}
}

// SendsLoopbackThroughGuardedClient checks that a loopback development
// endpoint is still reached through the configured, guarded HTTP client
// (egress policy, response-size limit, pre-dispatch evidence), which sends
// the key itself, and that the SDK's environment credentials do not switch it
// back to the SDK's own transport (#1087). send builds a client for baseURL
// with the key "loopback-key" over httpClient and sends one request, which
// the server answers with body at path.
func SendsLoopbackThroughGuardedClient(t *testing.T, path, body string, send func(baseURL string, httpClient *http.Client) error) {
	t.Setenv("OPENAI_API_KEY", "environment-key")
	server, requests := LoopbackJSONServer(t, path, "Authorization", "Bearer loopback-key", body)
	var guarded atomic.Int32
	configured := &http.Client{Transport: RoundTripFunc(func(request *http.Request) (*http.Response, error) {
		guarded.Add(1)
		return http.DefaultTransport.RoundTrip(request)
	})}
	if err := send(server.URL+"/v1", configured); err != nil {
		t.Fatal(err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("loopback requests = %d, want 1", got)
	}
	if got := guarded.Load(); got != 1 {
		t.Fatalf("guarded client carried %d requests, want 1", got)
	}
}
