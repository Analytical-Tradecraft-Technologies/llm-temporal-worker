package openairesponses

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestMapAPIErrorProducesSafeCommonFacts(t *testing.T) {
	apiErr := &openai.Error{
		Code:       "rate_limit_exceeded",
		Message:    "secret provider body",
		StatusCode: http.StatusTooManyRequests,
		Response:   &http.Response{Header: http.Header{"X-Request-Id": []string{"req-error"}, "Retry-After": []string{"2"}}},
	}
	mapped := mapError(apiErr)
	testkit.ProviderError(t, mapped, testkit.ProviderClass{Code: provider.CodeProviderRateLimited, Dispatch: provider.DispatchRejected, Retry: provider.RetryAfter})
	if mapped.Provider.RequestID != "req-error" || mapped.SafeDetails["provider_code"] != "rate_limit_exceeded" {
		t.Fatalf("provider facts = %#v", mapped)
	}
	encoded := testassert.MarshalJSON(t, mapped)
	if strings.Contains(string(encoded), "secret provider body") {
		t.Fatalf("raw provider message leaked: %s", encoded)
	}
}

func TestMapAPIErrorClassifiesCommonHTTPStatuses(t *testing.T) {
	adaptertest.HTTPStatuses(t, func(status int, response *http.Response) *provider.Error {
		return mapAPIError(&openai.Error{StatusCode: status, Response: response})
	})
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

func TestMapErrorClassifiesProviderNeutralFailures(t *testing.T) {
	adaptertest.ProviderNeutralFailures(t, mapError, provider.NewDispatchContextError)
}

func TestMapErrorRedactsTransportCause(t *testing.T) {
	transportCause := errors.New("Authorization: Bearer provider-secret; prompt=private-content; continuation=opaque-handle; body=provider-raw")
	mapped := mapError(transportCause)
	encoded := testassert.MarshalJSON(t, mapped)
	for _, raw := range []string{"provider-secret", "private-content", "opaque-handle", "provider-raw"} {
		if strings.Contains(string(encoded), raw) || strings.Contains(mapped.Error(), raw) {
			t.Fatalf("safe provider error leaked %q: %s", raw, encoded)
		}
	}
	testassert.ErrorIs(t, mapped, transportCause, "mapped error did not preserve the local diagnostic cause")
}

// OpenAI reports billing, spend and quota exhaustion as 429. Retrying cannot
// restore access, so it is the same route-level credit denial as a 402, while
// an ordinary 429, or the same code from an Azure host, stays a rate limit
// (#1442).
func TestBillingRateLimitIsARouteLevelCreditDenial(t *testing.T) {
	openAI := httptest.NewRequest(http.MethodPost, "https://api.openai.com/v1/responses", nil)
	for _, apiErr := range []*openai.Error{
		{Request: openAI, StatusCode: http.StatusTooManyRequests, Code: "credit_balance_exhausted", Type: "insufficient_quota"},
		{Request: openAI, StatusCode: http.StatusTooManyRequests, Code: "credit_balance_exhausted"},
		{Request: openAI, StatusCode: http.StatusTooManyRequests, Code: "project_spend_limit_exceeded"},
		{Request: openAI, StatusCode: http.StatusTooManyRequests, Type: "insufficient_quota"},
		{Request: httptest.NewRequest(http.MethodPost, "https://eu.api.openai.com/v1/responses", nil), StatusCode: http.StatusTooManyRequests, Code: "credit_balance_exhausted"},
	} {
		mapped := mapAPIError(apiErr)
		testkit.ProviderError(t, mapped, testkit.ProviderClass{Code: provider.CodePermissionDenied, Dispatch: provider.DispatchRejected, Retry: provider.RetryNextRoute}, fmt.Sprintf("429 %q/%q", apiErr.Code, apiErr.Type))
		if got, want := mapped.SafeDetails["status"], "429"; got != want {
			t.Fatalf("safe status = %q, want %q", got, want)
		}
	}
	for _, apiErr := range []*openai.Error{
		{Request: openAI, StatusCode: http.StatusTooManyRequests, Code: "rate_limit_exceeded", Type: "requests"},
		{Request: httptest.NewRequest(http.MethodPost, "https://example.openai.azure.com/openai/v1/responses", nil), StatusCode: http.StatusTooManyRequests, Code: "insufficient_quota", Type: "insufficient_quota"},
		{StatusCode: http.StatusTooManyRequests, Code: "credit_balance_exhausted"},
	} {
		mapped := mapAPIError(apiErr)
		testkit.ProviderError(t, mapped, testkit.ProviderClass{Code: provider.CodeProviderRateLimited, Retry: provider.RetryAfter}, fmt.Sprintf("rate limit 429 %q", apiErr.Code))
	}
}
