package anthropicmessages

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func mapError(err error, profileName string) *provider.Error {
	if err == nil {
		return nil
	}
	if errors.Is(err, provider.ErrProviderEgressDenied) {
		mapped := provider.NewEgressDeniedError(err)
		mapped.SafeDetails = map[string]string{"provider": profileName}
		return mapped
	}
	if errors.Is(err, provider.ErrProviderPreDispatch) {
		mapped := provider.NewPreDispatchUnavailableError(err)
		mapped.SafeDetails = map[string]string{"provider": profileName}
		return mapped
	}
	if errors.Is(err, context.Canceled) {
		mapped := provider.NewError(provider.CodeCanceled, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "provider request canceled")
		mapped.Cause = err
		return mapped
	}
	if errors.Is(err, context.DeadlineExceeded) {
		mapped := provider.NewError(provider.CodeDeadlineExceeded, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "provider request deadline exceeded")
		mapped.Cause = err
		return mapped
	}
	var apiErr *anthropic.Error
	if errors.As(err, &apiErr) {
		return mapAPIError(apiErr, profileName)
	}
	return &provider.Error{
		Code:        provider.CodeProviderUnavailable,
		Phase:       provider.PhaseDispatch,
		Dispatch:    provider.DispatchAmbiguous,
		Retry:       provider.RetrySameOperation,
		SafeMessage: "provider request failed before a response was classified",
		SafeDetails: map[string]string{"provider": profileName},
		Cause:       err,
	}
}

func mapAPIError(apiErr *anthropic.Error, profileName string) *provider.Error {
	if apiErr == nil {
		return provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetrySameOperation, "provider request failed")
	}
	status := apiErr.StatusCode
	code := provider.CodeProviderUnavailable
	retry := provider.RetrySameOperation
	dispatch := provider.DispatchRejected
	safe := "provider rejected the request"
	switch {
	case status >= http.StatusMultipleChoices && status < http.StatusBadRequest:
		dispatch, retry, safe = provider.DispatchAmbiguous, provider.RetryNever, "provider redirect response is ambiguous"
	case provider.IsGatewayTimeoutStatus(status) || apiErr.Type() == anthropic.ErrorTypeTimeoutError:
		// The request timed out while processing; it may have been billed.
		dispatch, safe = provider.DispatchAmbiguous, "provider request timed out"
	case status == http.StatusUnauthorized:
		code, retry, safe = provider.CodeAuthentication, provider.RetryNever, "provider authentication failed"
	case status == http.StatusPaymentRequired:
		// Anthropic reports a billing problem as 402 billing_error. That is an
		// operator-side account denial, not a caller error, so it counts
		// toward the route circuit and lets planning move to other routes.
		code, retry, safe = provider.CodePermissionDenied, provider.RetryNever, "provider account has insufficient credit"
	case status == http.StatusForbidden:
		code, retry, safe = provider.CodePermissionDenied, provider.RetryNever, "provider permission was denied"
	case status == http.StatusBadRequest || status == http.StatusUnprocessableEntity:
		code, retry, safe = provider.CodeInvalidArgument, provider.RetryNever, "provider rejected request parameters"
	case status == http.StatusTooManyRequests && spendCapReached(apiErr):
		code, retry, safe = provider.CodePermissionDenied, provider.RetryNever, "provider account reached its monthly spend cap"
	case status == http.StatusTooManyRequests:
		code, retry, safe = provider.CodeProviderRateLimited, provider.RetryAfter, "provider rate limited the request"
	case status >= http.StatusInternalServerError:
		code, retry, safe = provider.CodeProviderUnavailable, provider.RetrySameOperation, "provider is unavailable"
	default:
		if status >= 400 && status < 500 {
			code, retry, safe = provider.CodeInvalidArgument, provider.RetryNever, "provider rejected the request"
		}
	}
	mapped := provider.NewError(code, provider.PhaseDispatch, dispatch, retry, safe)
	mapped.Cause = apiErr
	mapped.SafeDetails = map[string]string{"provider": profileName, "status": fmt.Sprintf("%d", status)}
	if errorType := string(apiErr.Type()); errorType != "" {
		mapped.SafeDetails["provider_type"] = errorType
	}
	if code == provider.CodePermissionDenied && status == http.StatusTooManyRequests {
		mapped.SafeDetails["provider_code"] = spendCapErrorCode
	}
	if retry == provider.RetryAfter && apiErr.Response != nil {
		if retryAfter := apiErr.Response.Header.Get("retry-after"); retryAfter != "" {
			mapped.SafeDetails["retry_after"] = retryAfter
			if retryDelay, ok := provider.ParseRetryAfter(retryAfter, time.Now()); ok {
				mapped.RetryAfter = retryDelay
			}
		}
	}
	mapped.Provider.RequestID = apiErr.RequestID
	if mapped.Provider.RequestID == "" && apiErr.Response != nil {
		mapped.Provider.RequestID = apiErr.Response.Header.Get("request-id")
		if mapped.Provider.RequestID == "" {
			mapped.Provider.RequestID = apiErr.Response.Header.Get("x-request-id")
		}
	}
	return mapped
}

// spendCapErrorCode is the error.details.error_code Anthropic documents for a
// 429 sent while the organization is over its usage tier's monthly spend cap.
const spendCapErrorCode = "enforced_spend_limit_reached"

// spendCapReached reports the tier spend-cap 429. It shares rate_limit_error
// with a rate limit but carries no retry-after and keeps failing until the
// month resets or the tier is raised, so retrying it every second would only
// burn attempts. Like a 402 it is an account-wide denial, so it is classified
// as one: it holds the route and lets the request move to another route.
func spendCapReached(apiErr *anthropic.Error) bool {
	var body struct {
		Error struct {
			Details struct {
				ErrorCode string `json:"error_code"`
			} `json:"details"`
		} `json:"error"`
	}
	raw := apiErr.RawJSON()
	return raw != "" && json.Unmarshal([]byte(raw), &body) == nil && body.Error.Details.ErrorCode == spendCapErrorCode
}
