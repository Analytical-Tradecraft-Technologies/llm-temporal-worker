// Package testkit holds fixtures and checks that tests in several package
// areas share and that need the module's own types, so they cannot live in
// the dependency-free testassert package. Only tests and other test-support
// packages may import it.
package testkit

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// WaitForEvent fails the test immediately unless event delivers or closes
// within a second; description names the event in the failure.
func WaitForEvent(t testing.TB, event <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-event:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

// LoopbackListener opens a TCP listener on 127.0.0.1, skipping the test where
// the environment does not allow one.
func LoopbackListener(t testing.TB) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("test environment does not allow a loopback listener: %v", err)
	}
	return listener
}

// LoopbackTLSServer serves handler over TLS on a LoopbackListener until the
// test ends.
func LoopbackTLSServer(t testing.TB, handler http.Handler) *httptest.Server {
	t.Helper()
	server := &httptest.Server{Listener: LoopbackListener(t), Config: &http.Server{Handler: handler}}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}

// RoundTripFunc is an http.RoundTripper backed by a function.
type RoundTripFunc func(*http.Request) (*http.Response, error)

// RoundTrip calls function.
func (function RoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

// ProviderClass is the classification a provider error must carry. An empty
// field is not checked.
type ProviderClass struct {
	Code     provider.Code
	Phase    provider.Phase
	Dispatch provider.DispatchCertainty
	Retry    provider.RetryDisposition
}

// String lists the fields want sets, as "code=… phase=…".
func (want ProviderClass) String() string {
	var fields []string
	for _, field := range []struct{ name, value string }{
		{"code", string(want.Code)},
		{"phase", string(want.Phase)},
		{"dispatch", string(want.Dispatch)},
		{"retry", string(want.Retry)},
	} {
		if field.value != "" {
			fields = append(fields, field.name+"="+field.value)
		}
	}
	return strings.Join(fields, " ")
}

// matches reports whether got carries every field want sets.
func (want ProviderClass) matches(got *provider.Error) bool {
	return (want.Code == "" || got.Code == want.Code) &&
		(want.Phase == "" || got.Phase == want.Phase) &&
		(want.Dispatch == "" || got.Dispatch == want.Dispatch) &&
		(want.Retry == "" || got.Retry == want.Retry)
}

// ProviderError fails the test immediately unless err is, or wraps, a
// non-nil *provider.Error classified as want, and returns that error for
// further checks. The optional context names the step under test; when
// given, the failure message starts with "<context>: ".
func ProviderError(t testing.TB, err error, want ProviderClass, context ...string) *provider.Error {
	t.Helper()
	var mapped *provider.Error
	if ok := errors.As(err, &mapped); !ok || mapped == nil {
		got := any(err)
		if ok {
			got = nil // a nil *provider.Error, which formats as ""
		}
		fatal(t, context, "error = %v, want a provider error with %s", got, want)
		return nil
	}
	if !want.matches(mapped) {
		fatal(t, context, "error = %#v, want a provider error with %s", *mapped, want)
	}
	return mapped
}

// fatal fails t with the formatted message, prefixed by the context parts
// joined with ": ".
func fatal(t testing.TB, context []string, format string, args ...any) {
	t.Helper()
	message := fmt.Sprintf(format, args...)
	if len(context) > 0 {
		message = strings.Join(context, ": ") + ": " + message
	}
	t.Fatalf("%s", message)
}
