package provider

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// MapTransportError classifies the failures every adapter must recognize before
// it inspects an SDK error type: an egress denial, a guarded pre-dispatch
// failure, and caller cancellation or deadline. It returns nil when err is none
// of those, so the adapter can go on to classify its SDK error.
func MapTransportError(err error, profileName string) *Error {
	switch {
	case errors.Is(err, ErrProviderEgressDenied):
		mapped := NewEgressDeniedError(err)
		mapped.SafeDetails = map[string]string{"provider": profileName}
		return mapped
	case errors.Is(err, ErrProviderPreDispatch):
		mapped := NewPreDispatchUnavailableError(err)
		mapped.SafeDetails = map[string]string{"provider": profileName}
		return mapped
	case errors.Is(err, context.Canceled):
		mapped := NewError(CodeCanceled, PhaseDispatch, DispatchAmbiguous, RetryNever, "provider request canceled")
		mapped.Cause = err
		return mapped
	case errors.Is(err, context.DeadlineExceeded):
		mapped := NewError(CodeDeadlineExceeded, PhaseDispatch, DispatchAmbiguous, RetryNever, "provider request deadline exceeded")
		mapped.Cause = err
		return mapped
	}
	return nil
}

// NewUnclassifiedDispatchError is the fallback for a dispatch failure that
// carried no recognizable provider response. The request may have been sent,
// so dispatch is ambiguous.
func NewUnclassifiedDispatchError(cause error, profileName string) *Error {
	return &Error{
		Code:        CodeProviderUnavailable,
		Phase:       PhaseDispatch,
		Dispatch:    DispatchAmbiguous,
		Retry:       RetrySameOperation,
		SafeMessage: "provider request failed before a response was classified",
		SafeDetails: map[string]string{"provider": profileName},
		Cause:       cause,
	}
}

// ClassifyHTTPStatus is the provider status table shared by every adapter, for
// HTTP error responses and for errors found inside a successful response body.
// Adapters layer provider-specific evidence (error types, billing codes) on top.
func ClassifyHTTPStatus(status int) (Code, RetryDisposition, DispatchCertainty, string) {
	code := CodeProviderUnavailable
	retry := RetrySameOperation
	dispatch := DispatchRejected
	safe := "provider rejected the request"
	switch {
	case IsRedirectStatus(status):
		dispatch, retry, safe = DispatchAmbiguous, RetryNever, "provider redirect response is ambiguous"
	case IsGatewayTimeoutStatus(status):
		// The gateway timed out; the request may have been billed.
		dispatch, safe = DispatchAmbiguous, "provider request timed out"
	case status == http.StatusUnauthorized:
		code, retry, safe = CodeAuthentication, RetryNever, "provider authentication failed"
	case status == http.StatusPaymentRequired:
		// Anthropic, OpenRouter and Exa report an account out of credit as 402.
		// That is an operator-side account denial, not a caller error, so it
		// counts toward the route circuit and lets planning move to other routes.
		code, retry, safe = CodePermissionDenied, RetryNever, "provider account has insufficient credit"
	case status == http.StatusForbidden:
		code, retry, safe = CodePermissionDenied, RetryNever, "provider permission was denied"
	case status == http.StatusBadRequest || status == http.StatusUnprocessableEntity:
		code, retry, safe = CodeInvalidArgument, RetryNever, "provider rejected request parameters"
	case status == http.StatusTooManyRequests:
		code, retry, safe = CodeProviderRateLimited, RetryAfter, "provider rate limited the request"
	case status >= http.StatusInternalServerError:
		code, retry, safe = CodeProviderUnavailable, RetrySameOperation, "provider is unavailable"
	case status >= http.StatusBadRequest:
		code, retry, safe = CodeInvalidArgument, RetryNever, "provider rejected the request"
	}
	return code, retry, dispatch, safe
}

// ApplyRetryAfter records a response's Retry-After header on an error the
// status table marked RetryAfter. The raw value is a safe detail; a parseable
// one also becomes the retry delay.
func ApplyRetryAfter(mapped *Error, header http.Header) {
	if mapped == nil || mapped.Retry != RetryAfter {
		return
	}
	retryAfter := header.Get("retry-after")
	if retryAfter == "" {
		return
	}
	if mapped.SafeDetails == nil {
		mapped.SafeDetails = map[string]string{}
	}
	mapped.SafeDetails["retry_after"] = retryAfter
	if retryDelay, ok := ParseRetryAfter(retryAfter, time.Now()); ok {
		mapped.RetryAfter = retryDelay
	}
}
