package provider_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestMapTransportErrorClassifiesSharedFailures(t *testing.T) {
	cases := []struct {
		err      error
		code     provider.Code
		dispatch provider.DispatchCertainty
		retry    provider.RetryDisposition
	}{
		{fmt.Errorf("dial: %w", provider.ErrProviderEgressDenied), provider.CodeProviderUnavailable, provider.DispatchNotDispatched, provider.RetryNextRoute},
		{fmt.Errorf("dial: %w", provider.ErrProviderPreDispatch), provider.CodeProviderUnavailable, provider.DispatchNotDispatched, provider.RetryNextRoute},
		{fmt.Errorf("read: %w", context.Canceled), provider.CodeCanceled, provider.DispatchAmbiguous, provider.RetryNever},
		{fmt.Errorf("read: %w", context.DeadlineExceeded), provider.CodeDeadlineExceeded, provider.DispatchAmbiguous, provider.RetryNever},
	}
	for _, tc := range cases {
		mapped := provider.MapTransportError(tc.err, "profile")
		if mapped == nil || mapped.Code != tc.code || mapped.Dispatch != tc.dispatch || mapped.Retry != tc.retry {
			t.Fatalf("MapTransportError(%v) = %#v", tc.err, mapped)
		}
		if !errors.Is(mapped, tc.err) {
			t.Fatalf("MapTransportError(%v) lost its cause", tc.err)
		}
	}
	if mapped := provider.MapTransportError(errors.New("sdk failure"), "profile"); mapped != nil {
		t.Fatalf("unrecognized error was classified: %#v", mapped)
	}
}

func TestNewUnclassifiedDispatchErrorIsAmbiguous(t *testing.T) {
	cause := errors.New("connection reset")
	mapped := provider.NewUnclassifiedDispatchError(cause, "profile")
	if mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetrySameOperation || mapped.SafeDetails["provider"] != "profile" || !errors.Is(mapped, cause) {
		t.Fatalf("unexpected fallback error: %#v", mapped)
	}
}

func TestClassifyHTTPStatusTable(t *testing.T) {
	cases := []struct {
		status   int
		code     provider.Code
		retry    provider.RetryDisposition
		dispatch provider.DispatchCertainty
	}{
		{http.StatusFound, provider.CodeProviderUnavailable, provider.RetryNever, provider.DispatchAmbiguous},
		{http.StatusGatewayTimeout, provider.CodeProviderUnavailable, provider.RetrySameOperation, provider.DispatchAmbiguous},
		{provider.StatusCloudflareTimeout, provider.CodeProviderUnavailable, provider.RetrySameOperation, provider.DispatchAmbiguous},
		{http.StatusUnauthorized, provider.CodeAuthentication, provider.RetryNever, provider.DispatchRejected},
		{http.StatusPaymentRequired, provider.CodePermissionDenied, provider.RetryNever, provider.DispatchRejected},
		{http.StatusForbidden, provider.CodePermissionDenied, provider.RetryNever, provider.DispatchRejected},
		{http.StatusBadRequest, provider.CodeInvalidArgument, provider.RetryNever, provider.DispatchRejected},
		{http.StatusUnprocessableEntity, provider.CodeInvalidArgument, provider.RetryNever, provider.DispatchRejected},
		{http.StatusNotFound, provider.CodeInvalidArgument, provider.RetryNever, provider.DispatchRejected},
		{http.StatusTooManyRequests, provider.CodeProviderRateLimited, provider.RetryAfter, provider.DispatchRejected},
		{http.StatusInternalServerError, provider.CodeProviderUnavailable, provider.RetrySameOperation, provider.DispatchRejected},
		{529, provider.CodeProviderUnavailable, provider.RetrySameOperation, provider.DispatchRejected},
	}
	for _, tc := range cases {
		code, retry, dispatch, safe := provider.ClassifyHTTPStatus(tc.status)
		if code != tc.code || retry != tc.retry || dispatch != tc.dispatch || safe == "" {
			t.Fatalf("ClassifyHTTPStatus(%d) = %s %s %s %q", tc.status, code, retry, dispatch, safe)
		}
	}
}

func TestApplyRetryAfterOnlyForRateLimits(t *testing.T) {
	header := http.Header{"Retry-After": []string{"7"}}
	limited := provider.NewError(provider.CodeProviderRateLimited, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryAfter, "limited")
	provider.ApplyRetryAfter(limited, header)
	if limited.RetryAfter != 7*time.Second || limited.SafeDetails["retry_after"] != "7" {
		t.Fatalf("retry-after not applied: %#v", limited)
	}
	unavailable := provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchRejected, provider.RetrySameOperation, "unavailable")
	provider.ApplyRetryAfter(unavailable, header)
	if unavailable.RetryAfter != 0 || unavailable.SafeDetails != nil {
		t.Fatalf("retry-after applied to a non-rate-limit error: %#v", unavailable)
	}
	malformed := provider.NewError(provider.CodeProviderRateLimited, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryAfter, "limited")
	provider.ApplyRetryAfter(malformed, http.Header{"Retry-After": []string{"soon"}})
	if malformed.RetryAfter != 0 || malformed.SafeDetails["retry_after"] != "soon" {
		t.Fatalf("malformed retry-after handled wrongly: %#v", malformed)
	}
}
