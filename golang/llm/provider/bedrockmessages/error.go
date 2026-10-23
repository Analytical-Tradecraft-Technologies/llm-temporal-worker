package bedrockmessages

import (
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
	case status == http.StatusRequestTimeout || status == http.StatusFailedDependency:
		// Bedrock reports model processing failures with these 4xx statuses.
		// They do not prove rejection or zero usage: a new attempt needs budget.
		code, retry, dispatch, safe = provider.CodeProviderUnavailable, provider.RetrySameOperation, provider.DispatchAmbiguous, "provider model processing failed"
	case apiErr.Type() == anthropic.ErrorTypeTimeoutError:
		// The request timed out while processing; it may have been billed.
		code, retry, dispatch, safe = provider.CodeProviderUnavailable, provider.RetrySameOperation, provider.DispatchAmbiguous, "provider request timed out"
	}
	mapped := provider.NewError(code, provider.PhaseDispatch, dispatch, retry, safe)
	mapped.Cause = apiErr
	mapped.SafeDetails = map[string]string{"provider": profileName, "status": fmt.Sprintf("%d", status)}
	if errorType := string(apiErr.Type()); errorType != "" {
		mapped.SafeDetails["provider_type"] = errorType
	}
	mapped.Provider.RequestID = apiErr.RequestID
	if apiErr.Response != nil {
		provider.ApplyRetryAfter(mapped, apiErr.Response.Header)
		for _, header := range []string{"x-amzn-requestid", "x-amzn-request-id", "request-id", "x-request-id"} {
			if mapped.Provider.RequestID == "" {
				mapped.Provider.RequestID = apiErr.Response.Header.Get(header)
			}
		}
	}
	return mapped
}
