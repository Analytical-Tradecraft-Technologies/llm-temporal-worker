package adaptertest

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// ProviderNeutralFailures checks how mapError classifies the failures every
// adapter sees whatever its SDK: an egress denial and a certified
// pre-dispatch failure move to the next route undispatched and keep their
// marker, and a cancellation in the transport stays ambiguous while
// dispatchContextError keeps one before dispatch undispatched.
func ProviderNeutralFailures(t *testing.T, mapError func(error) *provider.Error, dispatchContextError func(error) *provider.Error) {
	for _, test := range []struct {
		name   string
		marker error
		lost   string
	}{
		{name: "egress denial", marker: provider.ErrProviderEgressDenied, lost: "mapped error did not preserve the egress marker"},
		{name: "certified pre-dispatch", marker: provider.ErrProviderPreDispatch, lost: "mapped error did not preserve the pre-dispatch marker"},
	} {
		t.Run(test.name, func(t *testing.T) {
			mapped := mapError(test.marker)
			if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute {
				t.Fatalf("mapped = %#v", mapped)
			}
			if !errors.Is(mapped, test.marker) {
				t.Fatal(test.lost)
			}
		})
	}
	t.Run("canceled transport", func(t *testing.T) {
		mapped := mapError(context.Canceled)
		if mapped.Code != provider.CodeCanceled || mapped.Dispatch != provider.DispatchAmbiguous {
			t.Fatalf("cancellation classification = %#v", mapped)
		}
		if before := dispatchContextError(context.Canceled); before.Dispatch != provider.DispatchNotDispatched {
			t.Fatal("preflight cancellation became ambiguous")
		}
	})
}

// HTTPStatuses checks how mapStatus, which maps an SDK error for an HTTP
// status and response, classifies the statuses whose meaning is common to
// every HTTP provider. The response is nil when the status carries no
// headers.
func HTTPStatuses(t *testing.T, mapStatus func(status int, response *http.Response) *provider.Error) {
	t.Run("redirect is ambiguous", func(t *testing.T) {
		mapped := mapStatus(http.StatusTemporaryRedirect, &http.Response{Header: http.Header{"Location": []string{"https://redirect.example/secret"}}})
		if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever {
			t.Fatalf("mapped redirect = %#v, want ambiguous non-retriable provider-unavailable", mapped)
		}
	})
	t.Run("retry after delay", func(t *testing.T) {
		mapped := mapStatus(http.StatusTooManyRequests, &http.Response{Header: http.Header{"Retry-After": []string{"2"}}})
		if got, want := mapped.RetryAfter, 2*time.Second; got != want {
			t.Fatalf("retry after = %s, want %s", got, want)
		}
		if got, want := mapped.SafeDetails["retry_after"], "2"; got != want {
			t.Fatalf("safe retry after = %q, want %q", got, want)
		}
	})
	t.Run("gateway timeouts remain ambiguous", func(t *testing.T) {
		for _, status := range []int{http.StatusGatewayTimeout, provider.StatusCloudflareTimeout} {
			mapped := mapStatus(status, nil)
			if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetrySameOperation {
				t.Fatalf("status %d became a rejection: %#v", status, mapped)
			}
		}
	})
	t.Run("server errors remain rejected", func(t *testing.T) {
		for _, status := range []int{500, 502, 503, 529} {
			mapped := mapStatus(status, nil)
			if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchRejected || mapped.Retry != provider.RetrySameOperation {
				t.Fatalf("status %d = %#v, want a retryable rejection", status, mapped)
			}
		}
	})
	// An account out of credit is an operator-side route failure: it must
	// not surface as the caller's invalid_argument, which route health
	// ignores.
	t.Run("payment required is a route-level denial", func(t *testing.T) {
		mapped := mapStatus(http.StatusPaymentRequired, nil)
		if mapped.Code != provider.CodePermissionDenied || mapped.Dispatch != provider.DispatchRejected || mapped.Retry != provider.RetryNever {
			t.Fatalf("402 = %#v, want a rejected permission denial", mapped)
		}
		if got, want := mapped.SafeDetails["status"], "402"; got != want {
			t.Fatalf("safe status = %q, want %q", got, want)
		}
	})
}
