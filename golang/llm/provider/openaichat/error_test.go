package openaichat

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestMapErrorClassifiesEgressDenialBeforeDispatch(t *testing.T) {
	mapped := mapError(provider.ErrProviderEgressDenied, "openrouter_chat")
	if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute {
		t.Fatalf("mapped = %#v", mapped)
	}
	if !errors.Is(mapped, provider.ErrProviderEgressDenied) {
		t.Fatal("mapped error did not preserve the egress marker")
	}
}

func TestMapErrorClassifiesCertifiedPreDispatchAvailability(t *testing.T) {
	mapped := mapError(provider.ErrProviderPreDispatch, "openrouter_chat")
	if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute {
		t.Fatalf("mapped = %#v", mapped)
	}
	if !errors.Is(mapped, provider.ErrProviderPreDispatch) {
		t.Fatal("mapped error did not preserve the pre-dispatch marker")
	}
}

func TestMapAPIErrorTreatsRedirectResponseAsAmbiguous(t *testing.T) {
	mapped := mapAPIError(&openai.Error{
		StatusCode: http.StatusTemporaryRedirect,
		Response:   &http.Response{Header: http.Header{"Location": []string{"https://redirect.example/secret"}}},
	}, "chat-profile")
	if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever {
		t.Fatalf("mapped redirect = %#v, want ambiguous non-retriable provider-unavailable", mapped)
	}
}

func TestMapAPIErrorMapsRetryAfterDelay(t *testing.T) {
	mapped := mapAPIError(&openai.Error{
		StatusCode: http.StatusTooManyRequests,
		Response:   &http.Response{Header: http.Header{"Retry-After": []string{"2"}}},
	}, "chat-profile")
	if got, want := mapped.RetryAfter, 2*time.Second; got != want {
		t.Fatalf("retry after = %s, want %s", got, want)
	}
	if got, want := mapped.SafeDetails["retry_after"], "2"; got != want {
		t.Fatalf("safe retry after = %q, want %q", got, want)
	}
}

func TestGatewayTimeoutsRemainAmbiguous(t *testing.T) {
	for _, status := range []int{http.StatusGatewayTimeout, provider.StatusCloudflareTimeout} {
		mapped := mapAPIError(&openai.Error{StatusCode: status}, "openai-chat")
		if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetrySameOperation {
			t.Fatalf("status %d became a rejection: %#v", status, mapped)
		}
	}
}

func TestServerErrorsRemainRejected(t *testing.T) {
	for _, status := range []int{500, 502, 503, 529} {
		mapped := mapAPIError(&openai.Error{StatusCode: status}, "openai-chat")
		if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchRejected || mapped.Retry != provider.RetrySameOperation {
			t.Fatalf("status %d = %#v, want a retryable rejection", status, mapped)
		}
	}
}

// An account out of credit is an operator-side route failure: it must not
// surface as the caller's invalid_argument, which route health ignores.
func TestPaymentRequiredIsARouteLevelDenial(t *testing.T) {
	mapped := mapAPIError(&openai.Error{StatusCode: http.StatusPaymentRequired}, "openrouter_chat")
	if mapped.Code != provider.CodePermissionDenied || mapped.Dispatch != provider.DispatchRejected || mapped.Retry != provider.RetryNever {
		t.Fatalf("402 = %#v, want a rejected permission denial", mapped)
	}
	if got, want := mapped.SafeDetails["status"], "402"; got != want {
		t.Fatalf("safe status = %q, want %q", got, want)
	}
}

// OpenAI reports billing, spend and quota exhaustion as 429. Retrying cannot
// restore access, so it is the same route-level credit denial as a 402, while
// an ordinary 429, or the same code from another OpenAI-compatible host, stays
// a rate limit (#1442).
func TestBillingRateLimitIsARouteLevelCreditDenial(t *testing.T) {
	openAI := httptest.NewRequest(http.MethodPost, "https://api.openai.com/v1/chat/completions", nil)
	for _, apiErr := range []*openai.Error{
		{Request: openAI, StatusCode: http.StatusTooManyRequests, Code: "credit_balance_exhausted", Type: "insufficient_quota"},
		{Request: openAI, StatusCode: http.StatusTooManyRequests, Code: "organization_spend_limit_exceeded"},
		{Request: openAI, StatusCode: http.StatusTooManyRequests, Type: "insufficient_quota"},
	} {
		mapped := mapAPIError(apiErr, "openai-chat")
		if mapped.Code != provider.CodePermissionDenied || mapped.Dispatch != provider.DispatchRejected || mapped.Retry != provider.RetryNextRoute {
			t.Fatalf("429 %q/%q = %#v, want a rejected permission denial", apiErr.Code, apiErr.Type, mapped)
		}
		if got, want := mapped.SafeDetails["status"], "429"; got != want {
			t.Fatalf("safe status = %q, want %q", got, want)
		}
	}
	for _, apiErr := range []*openai.Error{
		{Request: openAI, StatusCode: http.StatusTooManyRequests, Code: "rate_limit_exceeded"},
		{Request: httptest.NewRequest(http.MethodPost, "https://example.openai.azure.com/openai/deployments/d/chat/completions", nil), StatusCode: http.StatusTooManyRequests, Code: "insufficient_quota", Type: "insufficient_quota"},
	} {
		mapped := mapAPIError(apiErr, "openai-chat")
		if mapped.Code != provider.CodeProviderRateLimited || mapped.Retry != provider.RetryAfter {
			t.Fatalf("rate limit 429 %q = %#v, want provider_rate_limited", apiErr.Code, mapped)
		}
	}
}
