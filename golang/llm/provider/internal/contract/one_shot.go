package contract

import (
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

// RetryServer is an in-memory retryable HTTP endpoint. Keeping the transport
// in-process makes the contract test work in restricted CI sandboxes while
// still exercising the SDK's real request and retry machinery.
type RetryServer struct {
	URL        string
	HTTPClient *http.Client
	Calls      atomic.Int64
}

func NewRetryServer(t testing.TB) *RetryServer {
	t.Helper()
	server := &RetryServer{}
	server.URL = "https://127.0.0.1/contract"
	server.HTTPClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		server.Calls.Add(1)
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"retryable contract failure"}}`)),
			Request:    request,
		}, nil
	})}
	return server
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func (server *RetryServer) Close() {
	if server != nil && server.HTTPClient != nil {
		if transport, ok := server.HTTPClient.Transport.(interface{ CloseIdleConnections() }); ok {
			transport.CloseIdleConnections()
		}
	}
}

func (server *RetryServer) Count() int64 {
	if server == nil {
		return 0
	}
	return server.Calls.Load()
}
