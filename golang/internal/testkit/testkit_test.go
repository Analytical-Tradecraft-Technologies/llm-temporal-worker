package testkit

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

type recorder struct {
	testing.TB
	failed  bool
	message string
}

func (r *recorder) Helper() {}

func (r *recorder) Fatalf(format string, args ...any) {
	r.failed = true
	r.message = fmt.Sprintf(format, args...)
}

func TestRoundTripFuncCallsTheFunction(t *testing.T) {
	want := &http.Response{StatusCode: http.StatusTeapot}
	request, err := http.NewRequest(http.MethodGet, "https://example.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	var seen *http.Request
	got, err := RoundTripFunc(func(request *http.Request) (*http.Response, error) {
		seen = request
		return want, nil
	}).RoundTrip(request)
	if got != want || err != nil || seen != request {
		t.Fatalf("RoundTrip() = %v, %v (saw %v), want the function's response for the request", got, err, seen)
	}
}

func TestWaitForEvent(t *testing.T) {
	closed := make(chan struct{})
	close(closed)
	r := &recorder{}
	WaitForEvent(r, closed, "closed channel")
	if r.failed {
		t.Fatalf("closed channel failed: %q", r.message)
	}
	WaitForEvent(r, make(chan struct{}), "silent channel")
	if want := "timed out waiting for silent channel"; r.message != want {
		t.Fatalf("message = %q, want %q", r.message, want)
	}
}

func TestLoopbackTLSServerServesTheHandler(t *testing.T) {
	server := LoopbackTLSServer(t, http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusTeapot)
	}))
	if !strings.HasPrefix(server.URL, "https://127.0.0.1:") {
		t.Fatalf("server URL = %q, want a loopback TLS address", server.URL)
	}
	response, err := server.Client().Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusTeapot {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusTeapot)
	}
}

func TestProviderError(t *testing.T) {
	denial := provider.NewError(provider.CodePermissionDenied, provider.PhaseDispatch, provider.DispatchNotDispatched, provider.RetryNever, "denied")
	full := ProviderClass{Code: provider.CodePermissionDenied, Phase: provider.PhaseDispatch, Dispatch: provider.DispatchNotDispatched, Retry: provider.RetryNever}
	for _, test := range []struct {
		name        string
		err         error
		want        ProviderClass
		context     []string
		wantMessage string // "" when the check must pass
	}{
		{name: "every field", err: denial, want: full},
		{name: "wrapped", err: fmt.Errorf("plan: %w", denial), want: ProviderClass{Code: provider.CodePermissionDenied}},
		{name: "unset fields are not checked", err: denial, want: ProviderClass{Retry: provider.RetryNever}},
		{name: "nil error", err: nil, want: full, wantMessage: "error = <nil>, want a provider error with code=permission_denied phase=dispatch dispatch=not_dispatched retry=never"},
		{name: "nil provider error", err: (*provider.Error)(nil), want: full, context: []string{"Invoke"}, wantMessage: "Invoke: error = <nil>, want a provider error with code=permission_denied phase=dispatch dispatch=not_dispatched retry=never"},
		{name: "other error", err: errors.New("boom"), want: ProviderClass{Dispatch: provider.DispatchAmbiguous}, wantMessage: "error = boom, want a provider error with dispatch=ambiguous"},
		{
			name: "other classification", err: denial, want: ProviderClass{Code: provider.CodePermissionDenied, Retry: provider.RetryNextRoute}, context: []string{"case", "Invoke"},
			wantMessage: fmt.Sprintf("case: Invoke: error = %#v, want a provider error with code=permission_denied retry=next_route", *denial),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &recorder{}
			got := ProviderError(r, test.err, test.want, test.context...)
			if wantFail := test.wantMessage != ""; r.failed != wantFail {
				t.Fatalf("failed = %v, want %v (message %q)", r.failed, wantFail, r.message)
			}
			if r.message != test.wantMessage {
				t.Fatalf("message = %q, want %q", r.message, test.wantMessage)
			}
			if !r.failed && got == nil {
				t.Fatal("ProviderError returned nil for a matching error")
			}
		})
	}
}
