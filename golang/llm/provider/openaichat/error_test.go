package openaichat

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
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
		testkit.ProviderError(t, mapped, testkit.ProviderClass{Code: provider.CodePermissionDenied, Dispatch: provider.DispatchRejected, Retry: provider.RetryNextRoute}, fmt.Sprintf("429 %q/%q", apiErr.Code, apiErr.Type))
		if got, want := mapped.SafeDetails["status"], "429"; got != want {
			t.Fatalf("safe status = %q, want %q", got, want)
		}
	}
	for _, apiErr := range []*openai.Error{
		{Request: openAI, StatusCode: http.StatusTooManyRequests, Code: "rate_limit_exceeded"},
		{Request: httptest.NewRequest(http.MethodPost, "https://example.openai.azure.com/openai/deployments/d/chat/completions", nil), StatusCode: http.StatusTooManyRequests, Code: "insufficient_quota", Type: "insufficient_quota"},
	} {
		mapped := mapAPIError(apiErr, "openai-chat")
		testkit.ProviderError(t, mapped, testkit.ProviderClass{Code: provider.CodeProviderRateLimited, Retry: provider.RetryAfter}, fmt.Sprintf("rate limit 429 %q", apiErr.Code))
	}
}
