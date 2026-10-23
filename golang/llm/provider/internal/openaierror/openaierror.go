// Package openaierror maps OpenAI SDK errors into the provider error contract
// for the OpenAI Responses and Chat Completions adapters.
package openaierror

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// MapError classifies an error returned by the OpenAI SDK client.
func MapError(err error, profileName string) *provider.Error {
	if err == nil {
		return nil
	}
	if mapped := provider.MapTransportError(err, profileName); mapped != nil {
		return mapped
	}
	var apiErr *openai.Error
	if errors.As(err, &apiErr) {
		return MapAPIError(apiErr, profileName)
	}
	return provider.NewUnclassifiedDispatchError(err, profileName)
}

// MapAPIError classifies an OpenAI HTTP error response.
func MapAPIError(apiErr *openai.Error, profileName string) *provider.Error {
	if apiErr == nil {
		return provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetrySameOperation, "provider request failed")
	}
	status := apiErr.StatusCode
	code, retry, dispatch, safe := provider.ClassifyHTTPStatus(status)
	if status == http.StatusTooManyRequests && creditExhausted(apiErr) {
		code, retry, safe = provider.CodePermissionDenied, provider.RetryNextRoute, creditExhaustedMessage
	}
	mapped := provider.NewError(code, provider.PhaseDispatch, dispatch, retry, safe)
	mapped.Cause = apiErr
	mapped.SafeDetails = map[string]string{"provider": profileName, "status": fmt.Sprintf("%d", status)}
	if apiErr.Code != "" {
		mapped.SafeDetails["provider_code"] = apiErr.Code
	}
	if apiErr.Response != nil {
		provider.ApplyRetryAfter(mapped, apiErr.Response.Header)
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
//
// The codes are OpenAI's, so only a response from an OpenAI API host counts.
// Azure OpenAI and other OpenAI-compatible providers share this client but
// document their 429s as throttling that clears on retry.
func creditExhausted(apiErr *openai.Error) bool {
	if apiErr.Request == nil || apiErr.Request.URL == nil {
		return false
	}
	if host := apiErr.Request.URL.Hostname(); host != "openai.com" && !strings.HasSuffix(host, ".openai.com") {
		return false
	}
	switch apiErr.Code {
	case "credit_balance_exhausted", "insufficient_quota", "organization_spend_limit_exceeded",
		"project_spend_limit_exceeded", "organization_usage_limit_exceeded":
		return true
	}
	return apiErr.Type == "insufficient_quota"
}
