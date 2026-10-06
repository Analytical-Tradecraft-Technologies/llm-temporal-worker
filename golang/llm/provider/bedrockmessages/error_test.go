package bedrockmessages

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestMapErrorClassifiesEgressDenialBeforeDispatch(t *testing.T) {
	mapped := mapError(provider.ErrProviderEgressDenied, "bedrock_anthropic")
	if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute {
		t.Fatalf("mapped = %#v", mapped)
	}
	if !errors.Is(mapped, provider.ErrProviderEgressDenied) {
		t.Fatal("mapped error did not preserve the egress marker")
	}
}

func TestMapErrorClassifiesCertifiedPreDispatchAvailability(t *testing.T) {
	mapped := mapError(provider.ErrProviderPreDispatch, "bedrock_anthropic")
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
	}, "bedrock-profile")
	if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever {
		t.Fatalf("mapped redirect = %#v, want ambiguous non-retriable provider-unavailable", mapped)
	}
}

func TestMapAPIErrorMapsRetryAfterDelay(t *testing.T) {
	mapped := mapAPIError(&anthropic.Error{
		StatusCode: http.StatusTooManyRequests,
		Response:   &http.Response{Header: http.Header{"Retry-After": []string{"2"}}},
	}, "bedrock-profile")
	if got, want := mapped.RetryAfter, 2*time.Second; got != want {
		t.Fatalf("retry after = %s, want %s", got, want)
	}
	if got, want := mapped.SafeDetails["retry_after"], "2"; got != want {
		t.Fatalf("safe retry after = %q, want %q", got, want)
	}
}

func TestModelProcessingFailuresRemainAmbiguous(t *testing.T) {
	for _, status := range []int{http.StatusRequestTimeout, http.StatusFailedDependency} {
		cause := &anthropic.Error{StatusCode: status, RequestID: "request-id"}
		mapped := mapError(fmt.Errorf("wrapped: %w", cause), "bedrock-messages")
		if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetrySameOperation {
			t.Fatalf("status %d became a rejection: %#v", status, mapped)
		}
		if !errors.Is(mapped, cause) || mapped.Provider.RequestID != "request-id" {
			t.Fatal("provider error identity was lost")
		}
	}
}

func TestDefiniteMessagesRejectionsRemainRejected(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 422, 429} {
		mapped := mapAPIError(&anthropic.Error{StatusCode: status}, "bedrock-messages")
		if mapped.Dispatch != provider.DispatchRejected {
			t.Fatalf("status %d became ambiguous", status)
		}
	}
}
