// Package adaptertest holds the transport fakes and checks that the provider
// adapter tests share. Each check takes the adapter-specific step, such as
// building a client or mapping an SDK error, as a function, so the same
// contract is asserted the same way on every adapter.
package adaptertest

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// RoundTripFunc is an http.RoundTripper backed by a function.
type RoundTripFunc func(*http.Request) (*http.Response, error)

// RoundTrip calls function.
func (function RoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

// JSONResponse answers request with status, a JSON content type, the extra
// headers given as name/value pairs, and body.
func JSONResponse(request *http.Request, status int, body string, headers ...string) *http.Response {
	header := http.Header{"Content-Type": []string{"application/json"}}
	for index := 0; index+1 < len(headers); index += 2 {
		header.Set(headers[index], headers[index+1])
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}
}

// panickingBody panics when the SDK decodes the response, as an SDK
// deserializer meeting an unknown union member does.
type panickingBody struct{}

func (panickingBody) Read([]byte) (int, error) { panic("slice bounds out of range") }
func (panickingBody) Close() error             { return nil }

// PanickingTransport answers every request with status and a JSON content
// type whose body panics when read.
func PanickingTransport(status int) RoundTripFunc {
	return func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: panickingBody{}, Request: request}, nil
	}
}

// RecoveredDecodePanic checks that err, from an Invoke whose successful
// response body panicked while the SDK decoded it, is an accepted,
// never-retried invalid response for operationKey.
func RecoveredDecodePanic(t *testing.T, err error, operationKey string) {
	t.Helper()
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Dispatch != provider.DispatchAccepted || mapped.Retry != provider.RetryNever || mapped.Code != provider.CodeProviderInvalidResponse || mapped.OperationID != operationKey {
		t.Fatalf("Invoke() error = %#v, want an accepted, never-retried invalid response", err)
	}
}

// Observer counts the dispatch observations an adapter reports and keeps
// the last response metadata.
type Observer struct {
	Before   int
	Headers  int
	Progress int
	Metadata provider.ResponseMetadata
}

// BeforePossibleWrite counts a pre-write mark.
func (observer *Observer) BeforePossibleWrite(context.Context) error {
	observer.Before++
	return nil
}

// AfterResponseHeaders counts response headers and keeps their metadata.
func (observer *Observer) AfterResponseHeaders(_ context.Context, metadata provider.ResponseMetadata) error {
	observer.Headers++
	observer.Metadata = metadata
	return nil
}

// OnProgress counts a progress report.
func (observer *Observer) OnProgress(context.Context, provider.Progress) {
	observer.Progress++
}

// RedirectTransport answers every request with a 307 redirect to a URL
// carrying a continuation secret and the JSON body, counting requests in
// calls.
func RedirectTransport(calls *int, body string) RoundTripFunc {
	return func(request *http.Request) (*http.Response, error) {
		*calls++
		return JSONResponse(request, http.StatusTemporaryRedirect, body, "Location", "https://redirect.example/continuation-secret"), nil
	}
}

// AmbiguousRedirect checks that err, from an Invoke answered with a
// redirect, is an ambiguous, never-retried provider-unavailable error, and
// returns it.
func AmbiguousRedirect(t *testing.T, err error) *provider.Error {
	t.Helper()
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("Invoke() error = %T %v, want *provider.Error", err, err)
	}
	if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever {
		t.Fatalf("mapped redirect = %#v, want ambiguous non-retriable provider-unavailable", mapped)
	}
	return mapped
}

// PreDialDeadlineTransport holds every request until its context ends, as a
// dial that never completes would, then records the context error as a
// pre-dispatch failure. It counts requests and reads of their bodies.
type PreDialDeadlineTransport struct {
	started   chan struct{}
	once      sync.Once
	Calls     int
	BodyReads int
}

// NewPreDialDeadlineTransport returns a transport no request has reached.
func NewPreDialDeadlineTransport() *PreDialDeadlineTransport {
	return &PreDialDeadlineTransport{started: make(chan struct{})}
}

// RoundTrip blocks until the request context ends.
func (transport *PreDialDeadlineTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.Calls++
	if request.Body != nil {
		request.Body = &countingReadCloser{ReadCloser: request.Body, reads: &transport.BodyReads}
	}
	transport.once.Do(func() { close(transport.started) })
	<-request.Context().Done()
	provider.RecordPreDispatchContext(request.Context(), request.Context().Err())
	return nil, request.Context().Err()
}

type countingReadCloser struct {
	io.ReadCloser
	reads *int
}

func (reader *countingReadCloser) Read(data []byte) (int, error) {
	*reader.reads++
	return reader.ReadCloser.Read(data)
}

// CallerDeadlineBeforeDial runs invoke under a caller deadline that expires
// once transport is reached, and checks that Invoke reports it as a
// certified pre-dispatch deadline that is never retried, after exactly one
// request whose body was never read.
func CallerDeadlineBeforeDial(t *testing.T, transport *PreDialDeadlineTransport, invoke func(context.Context) error) {
	t.Helper()
	deadline := &preDialDeadlineContext{done: make(chan struct{})}
	result := make(chan error, 1)
	go func() { result <- invoke(deadline) }()
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("provider transport was not reached")
	}
	close(deadline.done)
	var invokeErr error
	select {
	case invokeErr = <-result:
	case <-time.After(time.Second):
		t.Fatal("Invoke() did not return after the pre-dial deadline")
	}
	var mapped *provider.Error
	if !errors.As(invokeErr, &mapped) {
		t.Fatalf("Invoke() error = %T %v, want *provider.Error", invokeErr, invokeErr)
	}
	if mapped.Code != provider.CodeDeadlineExceeded || mapped.Phase != provider.PhaseDispatch || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever {
		t.Fatalf("mapped error = %#v, want non-retryable pre-dispatch caller deadline", mapped)
	}
	if !errors.Is(mapped, provider.ErrProviderPreDispatch) || !errors.Is(mapped, context.DeadlineExceeded) {
		t.Fatalf("mapped error = %v, want certified pre-dispatch caller deadline", mapped)
	}
	if transport.Calls != 1 || transport.BodyReads != 0 {
		t.Fatalf("pre-dial transport calls/body reads = %d/%d, want 1/0", transport.Calls, transport.BodyReads)
	}
}

// preDialDeadlineContext is a caller context with a distant deadline that
// the test expires on demand by closing done.
type preDialDeadlineContext struct {
	done chan struct{}
}

func (ctx *preDialDeadlineContext) Deadline() (time.Time, bool) {
	return time.Now().Add(time.Hour), true
}

func (ctx *preDialDeadlineContext) Done() <-chan struct{} { return ctx.done }

func (ctx *preDialDeadlineContext) Err() error {
	select {
	case <-ctx.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (*preDialDeadlineContext) Value(any) any { return nil }
