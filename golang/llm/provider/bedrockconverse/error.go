package bedrockconverse

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// mapError keeps AWS SDK errors out of the provider-neutral response while
// preserving retry and dispatch certainty. Smithy response errors expose an
// HTTPStatusCode method without requiring a dependency on generated service
// error types.
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
		mapped := provider.NewError(provider.CodeCanceled, provider.PhaseDispatch, provider.DispatchNotDispatched, provider.RetryNever, "provider request canceled")
		mapped.Cause = err
		return mapped
	}
	if errors.Is(err, context.DeadlineExceeded) {
		mapped := provider.NewError(provider.CodeDeadlineExceeded, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "provider request deadline exceeded")
		mapped.Cause = err
		return mapped
	}
	var statusErr interface{ HTTPStatusCode() int }
	if errors.As(err, &statusErr) {
		return mapHTTPError(statusErr.HTTPStatusCode(), err, profileName)
	}
	return &provider.Error{
		Code: provider.CodeProviderUnavailable, Phase: provider.PhaseDispatch,
		Dispatch: provider.DispatchAmbiguous, Retry: provider.RetrySameOperation,
		SafeMessage: "provider request failed before a response was classified",
		SafeDetails: map[string]string{"provider": profileName}, Cause: err,
	}
}

func mapHTTPError(status int, cause error, profileName string) *provider.Error {
	code, retry, dispatch, safe := provider.CodeProviderUnavailable, provider.RetrySameOperation, provider.DispatchRejected, "provider rejected the request"
	switch {
	case status >= http.StatusMultipleChoices && status < http.StatusBadRequest:
		dispatch, retry, safe = provider.DispatchAmbiguous, provider.RetryNever, "provider redirect response is ambiguous"
	case status == http.StatusUnauthorized:
		code, retry, safe = provider.CodeAuthentication, provider.RetryNever, "provider authentication failed"
	case status == http.StatusForbidden:
		code, retry, safe = provider.CodePermissionDenied, provider.RetryNever, "provider permission was denied"
	case status == http.StatusBadRequest || status == http.StatusUnprocessableEntity:
		code, retry, safe = provider.CodeInvalidArgument, provider.RetryNever, "provider rejected request parameters"
	case status == http.StatusTooManyRequests:
		code, retry, safe = provider.CodeProviderRateLimited, provider.RetryAfter, "provider rate limited the request"
	case status >= http.StatusInternalServerError:
		code, retry, safe = provider.CodeProviderUnavailable, provider.RetrySameOperation, "provider is unavailable"
	case status >= 400:
		code, retry, safe = provider.CodeInvalidArgument, provider.RetryNever, "provider rejected the request"
	}
	mapped := provider.NewError(code, provider.PhaseDispatch, dispatch, retry, safe)
	mapped.Cause = cause
	mapped.SafeDetails = map[string]string{"provider": profileName, "status": fmt.Sprintf("%d", status)}
	return mapped
}
