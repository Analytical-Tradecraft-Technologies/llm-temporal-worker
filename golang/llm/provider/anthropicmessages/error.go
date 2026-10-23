package anthropicmessages

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func mapError(err error, profileName string) *provider.Error {
	if err == nil {
		return nil
	}
	if mapped := provider.MapTransportError(err, profileName); mapped != nil {
		return mapped
	}
	var apiErr *anthropic.Error
	if errors.As(err, &apiErr) {
		return mapAPIError(apiErr, profileName)
	}
	return provider.NewUnclassifiedDispatchError(err, profileName)
}

func mapAPIError(apiErr *anthropic.Error, profileName string) *provider.Error {
	if apiErr == nil {
		return provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetrySameOperation, "provider request failed")
	}
	status := apiErr.StatusCode
	code, retry, dispatch, safe := provider.ClassifyHTTPStatus(status)
	switch {
	case provider.IsRedirectStatus(status):
		// A redirect stays ambiguous whatever error type the body names.
	case apiErr.Type() == anthropic.ErrorTypeTimeoutError:
		// The request timed out while processing; it may have been billed.
		code, retry, dispatch, safe = provider.CodeProviderUnavailable, provider.RetrySameOperation, provider.DispatchAmbiguous, "provider request timed out"
	case status == http.StatusTooManyRequests && spendCapReached(apiErr):
		code, retry, safe = provider.CodePermissionDenied, provider.RetryNextRoute, "provider account reached its monthly spend cap"
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
	mapped.Provider.RequestID = apiErr.RequestID
	if apiErr.Response != nil {
		provider.ApplyRetryAfter(mapped, apiErr.Response.Header)
		if mapped.Provider.RequestID == "" {
			mapped.Provider.RequestID = apiErr.Response.Header.Get("request-id")
		}
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
