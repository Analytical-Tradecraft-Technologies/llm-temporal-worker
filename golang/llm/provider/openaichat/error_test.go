package openaichat

import (
	"net/http"
	"net/http/httptest"
	"testing"

	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestMapErrorClassifiesProviderNeutralFailures(t *testing.T) {
	adaptertest.ProviderNeutralFailures(t, func(err error) *provider.Error { return mapError(err, "openrouter_chat") }, provider.NewDispatchContextError)
}

func TestMapAPIErrorClassifiesCommonHTTPStatuses(t *testing.T) {
	adaptertest.HTTPStatuses(t, func(status int, response *http.Response) *provider.Error {
		return mapAPIError(&openai.Error{StatusCode: status, Response: response}, "openai-chat")
	})
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
