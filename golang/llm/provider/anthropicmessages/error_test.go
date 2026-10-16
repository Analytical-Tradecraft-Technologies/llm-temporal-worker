package anthropicmessages

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestMapErrorClassifiesEgressDenialBeforeDispatch(t *testing.T) {
	mapped := mapError(provider.ErrProviderEgressDenied, "anthropic_messages")
	if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute {
		t.Fatalf("mapped = %#v", mapped)
	}
	if !errors.Is(mapped, provider.ErrProviderEgressDenied) {
		t.Fatal("mapped error did not preserve the egress marker")
	}
}

func TestMapErrorClassifiesCertifiedPreDispatchAvailability(t *testing.T) {
	mapped := mapError(provider.ErrProviderPreDispatch, "anthropic_messages")
	if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute {
		t.Fatalf("mapped = %#v", mapped)
	}
	if !errors.Is(mapped, provider.ErrProviderPreDispatch) {
		t.Fatal("mapped error did not preserve the pre-dispatch marker")
	}
}

func TestMapAPIErrorTreatsRedirectResponseAsAmbiguous(t *testing.T) {
	mapped := mapAPIError(&anthropic.Error{
		StatusCode: http.StatusTemporaryRedirect,
		Response:   &http.Response{Header: http.Header{"Location": []string{"https://redirect.example/secret"}}},
	}, "anthropic-profile")
	if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever {
		t.Fatalf("mapped redirect = %#v, want ambiguous non-retriable provider-unavailable", mapped)
	}
}

func TestMapAPIErrorMapsRetryAfterDelay(t *testing.T) {
	mapped := mapAPIError(&anthropic.Error{
		StatusCode: http.StatusTooManyRequests,
		Response:   &http.Response{Header: http.Header{"Retry-After": []string{"2"}}},
	}, "anthropic-profile")
	if got, want := mapped.RetryAfter, 2*time.Second; got != want {
		t.Fatalf("retry after = %s, want %s", got, want)
	}
	if got, want := mapped.SafeDetails["retry_after"], "2"; got != want {
		t.Fatalf("safe retry after = %q, want %q", got, want)
	}
}

func TestGatewayTimeoutsRemainAmbiguous(t *testing.T) {
	for _, status := range []int{http.StatusGatewayTimeout, provider.StatusCloudflareTimeout} {
		mapped := mapAPIError(&anthropic.Error{StatusCode: status}, "anthropic-messages")
		if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetrySameOperation {
			t.Fatalf("status %d became a rejection: %#v", status, mapped)
		}
	}
	// Anthropic reports a request that timed out while processing as
	// timeout_error; that type is ambiguous whatever status carries it.
	for _, status := range []int{http.StatusInternalServerError, http.StatusGatewayTimeout} {
		apiErr := &anthropic.Error{StatusCode: status}
		if err := json.Unmarshal([]byte(`{"type":"error","error":{"type":"timeout_error","message":"Request timed out"}}`), apiErr); err != nil {
			t.Fatal(err)
		}
		mapped := mapAPIError(apiErr, "anthropic-messages")
		if mapped.Dispatch != provider.DispatchAmbiguous || mapped.SafeDetails["provider_type"] != "timeout_error" {
			t.Fatalf("timeout_error with status %d became a rejection: %#v", status, mapped)
		}
	}
}

func TestServerErrorsRemainRejected(t *testing.T) {
	for _, status := range []int{500, 502, 503, 529} {
		mapped := mapAPIError(&anthropic.Error{StatusCode: status}, "anthropic-messages")
		if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchRejected || mapped.Retry != provider.RetrySameOperation {
			t.Fatalf("status %d = %#v, want a retryable rejection", status, mapped)
		}
	}
}

// An account out of credit is an operator-side route failure: it must not
// surface as the caller's invalid_argument, which route health ignores.
func TestPaymentRequiredIsARouteLevelDenial(t *testing.T) {
	mapped := mapAPIError(&anthropic.Error{StatusCode: http.StatusPaymentRequired}, "anthropic-messages")
	if mapped.Code != provider.CodePermissionDenied || mapped.Dispatch != provider.DispatchRejected || mapped.Retry != provider.RetryNever {
		t.Fatalf("402 = %#v, want a rejected permission denial", mapped)
	}
	if got, want := mapped.SafeDetails["status"], "402"; got != want {
		t.Fatalf("safe status = %q, want %q", got, want)
	}
}

// The tier spend-cap 429 shares rate_limit_error with a rate limit but keeps
// failing until access resumes. Its documented error.details.error_code makes
// it the same route-level credit denial as a 402, while an ordinary 429 stays
// a rate limit (#1442).
func TestSpendCapRateLimitIsARouteLevelCreditDenial(t *testing.T) {
	spendCap := &anthropic.Error{StatusCode: http.StatusTooManyRequests}
	if err := spendCap.UnmarshalJSON([]byte(`{"type":"error","error":{"type":"rate_limit_error","message":"You have reached your API usage limits","details":{"error_code":"enforced_spend_limit_reached"}},"request_id":"req_1"}`)); err != nil {
		t.Fatal(err)
	}
	mapped := mapAPIError(spendCap, "anthropic-messages")
	if mapped.Code != provider.CodePermissionDenied || mapped.Dispatch != provider.DispatchRejected || mapped.Retry != provider.RetryNextRoute {
		t.Fatalf("spend-cap 429 = %#v, want a rejected permission denial", mapped)
	}
	if mapped.SafeDetails["status"] != "429" || mapped.SafeDetails["provider_code"] != "enforced_spend_limit_reached" {
		t.Fatalf("safe details = %#v", mapped.SafeDetails)
	}
	rateLimit := &anthropic.Error{StatusCode: http.StatusTooManyRequests}
	if err := rateLimit.UnmarshalJSON([]byte(`{"type":"error","error":{"type":"rate_limit_error","message":"Number of request tokens has exceeded your per-minute rate limit"}}`)); err != nil {
		t.Fatal(err)
	}
	mapped = mapAPIError(rateLimit, "anthropic-messages")
	if mapped.Code != provider.CodeProviderRateLimited || mapped.Retry != provider.RetryAfter || mapped.SafeDetails["provider_code"] != "" {
		t.Fatalf("rate limit 429 = %#v, want provider_rate_limited", mapped)
	}
}
