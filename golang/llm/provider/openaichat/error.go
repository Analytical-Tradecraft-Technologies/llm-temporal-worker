package openaichat

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	openai "github.com/openai/openai-go/v3"

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
	var apiErr *openai.Error
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

func mapAPIError(apiErr *openai.Error, profileName string) *provider.Error {
	if apiErr == nil {
		return provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetrySameOperation, "provider request failed")
	}
	status := apiErr.StatusCode
	code, retry, dispatch, safe := classifyStatus(status)
	if status == http.StatusTooManyRequests && creditExhausted(apiErr) {
		code, retry, safe = provider.CodePermissionDenied, provider.RetryNever, creditExhaustedMessage
	}
	mapped := provider.NewError(code, provider.PhaseDispatch, dispatch, retry, safe)
	mapped.Cause = apiErr
	mapped.SafeDetails = map[string]string{"provider": profileName, "status": fmt.Sprintf("%d", status)}
	if apiErr.Code != "" {
		mapped.SafeDetails["provider_code"] = apiErr.Code
	}
	if retry == provider.RetryAfter && apiErr.Response != nil {
		if retryAfter := apiErr.Response.Header.Get("retry-after"); retryAfter != "" {
			mapped.SafeDetails["retry_after"] = retryAfter
			if retryDelay, ok := provider.ParseRetryAfter(retryAfter, time.Now()); ok {
				mapped.RetryAfter = retryDelay
			}
		}
	}
	if apiErr.Response != nil {
		mapped.Provider.RequestID = apiErr.Response.Header.Get("x-request-id")
	}
	return mapped
}

// creditExhaustedMessage is the safe message for a billing 429.
const creditExhaustedMessage = "provider account has exhausted its credit or spend limit"

// creditExhausted reports an OpenAI 429 that is a billing, spend or quota
// denial rather than a rate limit. OpenAI documents that retrying these will
// not restore access until credits or limits change, and that error.type can
// be insufficient_quota while error.code names the cause. Like a 402, it is an
// account-wide denial, so it is classified as one: it holds the route and lets
// the request move to another route instead of retrying a dead account.
func creditExhausted(apiErr *openai.Error) bool {
	switch apiErr.Code {
	case "credit_balance_exhausted", "insufficient_quota", "organization_spend_limit_exceeded",
		"project_spend_limit_exceeded", "organization_usage_limit_exceeded":
		return true
	}
	return apiErr.Type == "insufficient_quota"
}

// classifyStatus is the provider status table shared by HTTP error responses
// and by errors a profile finds inside a successful response body.
func classifyStatus(status int) (provider.Code, provider.RetryDisposition, provider.DispatchCertainty, string) {
	code := provider.CodeProviderUnavailable
	retry := provider.RetrySameOperation
	dispatch := provider.DispatchRejected
	safe := "provider rejected the request"
	switch {
	case status >= http.StatusMultipleChoices && status < http.StatusBadRequest:
		dispatch, retry, safe = provider.DispatchAmbiguous, provider.RetryNever, "provider redirect response is ambiguous"
	case provider.IsGatewayTimeoutStatus(status):
		// The gateway timed out; the request may have been billed.
		dispatch, safe = provider.DispatchAmbiguous, "provider request timed out"
	case status == http.StatusUnauthorized:
		code, retry, safe = provider.CodeAuthentication, provider.RetryNever, "provider authentication failed"
	case status == http.StatusPaymentRequired:
		// OpenRouter reports an account out of credit as 402. That is an
		// operator-side account denial, not a caller error, so it counts
		// toward the route circuit and lets planning move to other routes.
		code, retry, safe = provider.CodePermissionDenied, provider.RetryNever, "provider account has insufficient credit"
	case status == http.StatusForbidden:
		code, retry, safe = provider.CodePermissionDenied, provider.RetryNever, "provider permission was denied"
	case status == http.StatusBadRequest || status == http.StatusUnprocessableEntity:
		code, retry, safe = provider.CodeInvalidArgument, provider.RetryNever, "provider rejected request parameters"
	case status == http.StatusTooManyRequests:
		code, retry, safe = provider.CodeProviderRateLimited, provider.RetryAfter, "provider rate limited the request"
	case status >= http.StatusInternalServerError:
		code, retry, safe = provider.CodeProviderUnavailable, provider.RetrySameOperation, "provider is unavailable"
	default:
		if status >= 400 && status < 500 {
			code, retry, safe = provider.CodeInvalidArgument, provider.RetryNever, "provider rejected the request"
		}
	}
	return code, retry, dispatch, safe
}
