package anthropicmessages

import (
	"net/http"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/messagestest"
)

func TestMapErrorClassifiesProviderNeutralFailures(t *testing.T) {
	adaptertest.ProviderNeutralFailures(t, func(err error) *provider.Error { return mapError(err, "anthropic_messages") }, provider.NewDispatchContextError)
}

func TestMapAPIErrorClassifiesCommonHTTPStatuses(t *testing.T) {
	adaptertest.HTTPStatuses(t, func(status int, response *http.Response) *provider.Error {
		return mapAPIError(&anthropic.Error{StatusCode: status, Response: response}, "anthropic-messages")
	})
}

// Anthropic reports a request that timed out while processing as
// timeout_error; that type is ambiguous whatever status carries it.
func TestTimeoutErrorRemainsAmbiguous(t *testing.T) {
	messagestest.TimeoutErrorRemainsAmbiguous(t, func(apiErr *anthropic.Error) *provider.Error { return mapAPIError(apiErr, "anthropic-messages") })
}

// The tier spend-cap 429 shares rate_limit_error with a rate limit but keeps
// failing until access resumes. Its documented error.details.error_code makes
// it the same route-level credit denial as a 402, while an ordinary 429 stays
// a rate limit (#1442).
func TestSpendCapRateLimitIsARouteLevelCreditDenial(t *testing.T) {
	spendCap := &anthropic.Error{StatusCode: http.StatusTooManyRequests}
	testassert.NoError(t, spendCap.UnmarshalJSON([]byte(`{"type":"error","error":{"type":"rate_limit_error","message":"You have reached your API usage limits","details":{"error_code":"enforced_spend_limit_reached"}},"request_id":"req_1"}`)))
	mapped := mapAPIError(spendCap, "anthropic-messages")
	testkit.ProviderError(t, mapped, testkit.ProviderClass{Code: provider.CodePermissionDenied, Dispatch: provider.DispatchRejected, Retry: provider.RetryNextRoute}, "spend-cap 429")
	if mapped.SafeDetails["status"] != "429" || mapped.SafeDetails["provider_code"] != "enforced_spend_limit_reached" {
		t.Fatalf("safe details = %#v", mapped.SafeDetails)
	}
	rateLimit := &anthropic.Error{StatusCode: http.StatusTooManyRequests}
	testassert.NoError(t, rateLimit.UnmarshalJSON([]byte(`{"type":"error","error":{"type":"rate_limit_error","message":"Number of request tokens has exceeded your per-minute rate limit"}}`)))
	mapped = mapAPIError(rateLimit, "anthropic-messages")
	if mapped.Code != provider.CodeProviderRateLimited || mapped.Retry != provider.RetryAfter || mapped.SafeDetails["provider_code"] != "" {
		t.Fatalf("rate limit 429 = %#v, want provider_rate_limited", mapped)
	}
}
