package openairesponses

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestMapAPIErrorProducesSafeCommonFacts(t *testing.T) {
	apiErr := &openai.Error{
		Code:       "rate_limit_exceeded",
		Message:    "secret provider body",
		StatusCode: http.StatusTooManyRequests,
		Response:   &http.Response{Header: http.Header{"X-Request-Id": []string{"req-error"}, "Retry-After": []string{"2"}}},
	}
	mapped := mapError(apiErr)
	if mapped.Code != provider.CodeProviderRateLimited || mapped.Dispatch != provider.DispatchRejected || mapped.Retry != provider.RetryAfter {
		t.Fatalf("mapped = %#v", mapped)
	}
	if mapped.Provider.RequestID != "req-error" || mapped.SafeDetails["provider_code"] != "rate_limit_exceeded" {
		t.Fatalf("provider facts = %#v", mapped)
	}
	encoded, err := json.Marshal(mapped)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "secret provider body") {
		t.Fatalf("raw provider message leaked: %s", encoded)
	}
}

func TestMapAPIErrorMapsRetryAfterDelay(t *testing.T) {
	mapped := mapAPIError(&openai.Error{
		StatusCode: http.StatusTooManyRequests,
		Response:   &http.Response{Header: http.Header{"Retry-After": []string{"2"}}},
	})
	if got, want := mapped.RetryAfter, 2*time.Second; got != want {
		t.Fatalf("retry after = %s, want %s", got, want)
	}
	if got, want := mapped.SafeDetails["retry_after"], "2"; got != want {
		t.Fatalf("safe retry after = %q, want %q", got, want)
	}
}

func TestMapAPIErrorTreatsRedirectResponseAsAmbiguous(t *testing.T) {
	mapped := mapAPIError(&openai.Error{
		StatusCode: http.StatusTemporaryRedirect,
		Response:   &http.Response{Header: http.Header{"Location": []string{"https://redirect.example/secret"}}},
	})
	if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever {
		t.Fatalf("mapped redirect = %#v, want ambiguous non-retriable provider-unavailable", mapped)
	}
}

func TestMapErrorClassifiesContextAndTransportFailures(t *testing.T) {
	for _, test := range []struct {
		err  error
		code provider.Code
	}{
		{context.Canceled, provider.CodeCanceled},
		{context.DeadlineExceeded, provider.CodeDeadlineExceeded},
		{errors.New("connection reset"), provider.CodeProviderUnavailable},
	} {
		mapped := mapError(test.err)
		if mapped.Code != test.code {
			t.Errorf("%v -> %s, want %s", test.err, mapped.Code, test.code)
		}
	}
}

func TestMapErrorClassifiesEgressDenialBeforeDispatch(t *testing.T) {
	mapped := mapError(provider.ErrProviderEgressDenied)
	if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute {
		t.Fatalf("mapped = %#v", mapped)
	}
	if !errors.Is(mapped, provider.ErrProviderEgressDenied) {
		t.Fatal("mapped error did not preserve the egress marker")
	}
}

func TestMapErrorClassifiesCertifiedPreDispatchAvailability(t *testing.T) {
	mapped := mapError(provider.ErrProviderPreDispatch)
	if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute {
		t.Fatalf("mapped = %#v", mapped)
	}
	if !errors.Is(mapped, provider.ErrProviderPreDispatch) {
		t.Fatal("mapped error did not preserve the pre-dispatch marker")
	}
}

func TestMapErrorRedactsTransportCause(t *testing.T) {
	transportCause := errors.New("Authorization: Bearer provider-secret; prompt=private-content; continuation=opaque-handle; body=provider-raw")
	mapped := mapError(transportCause)
	encoded, err := json.Marshal(mapped)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"provider-secret", "private-content", "opaque-handle", "provider-raw"} {
		if strings.Contains(string(encoded), raw) || strings.Contains(mapped.Error(), raw) {
			t.Fatalf("safe provider error leaked %q: %s", raw, encoded)
		}
	}
	if !errors.Is(mapped, transportCause) {
		t.Fatal("mapped error did not preserve the local diagnostic cause")
	}
}

func TestGatewayTimeoutsRemainAmbiguous(t *testing.T) {
	for _, status := range []int{http.StatusGatewayTimeout, provider.StatusCloudflareTimeout} {
		mapped := mapAPIError(&openai.Error{StatusCode: status})
		if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetrySameOperation {
			t.Fatalf("status %d became a rejection: %#v", status, mapped)
		}
	}
}

func TestServerErrorsRemainRejected(t *testing.T) {
	for _, status := range []int{500, 502, 503, 529} {
		mapped := mapAPIError(&openai.Error{StatusCode: status})
		if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchRejected || mapped.Retry != provider.RetrySameOperation {
			t.Fatalf("status %d = %#v, want a retryable rejection", status, mapped)
		}
	}
}

// An account out of credit is an operator-side route failure: it must not
// surface as the caller's invalid_argument, which route health ignores.
func TestPaymentRequiredIsARouteLevelDenial(t *testing.T) {
	mapped := mapAPIError(&openai.Error{StatusCode: http.StatusPaymentRequired})
	if mapped.Code != provider.CodePermissionDenied || mapped.Dispatch != provider.DispatchRejected || mapped.Retry != provider.RetryNever {
		t.Fatalf("402 = %#v, want a rejected permission denial", mapped)
	}
	if got, want := mapped.SafeDetails["status"], "402"; got != want {
		t.Fatalf("safe status = %q, want %q", got, want)
	}
}
