package bedrockconverse

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// mapError keeps AWS SDK errors out of the provider-neutral response while
// preserving retry and dispatch certainty. Recognize model-processing exceptions
// even without a Smithy HTTP wrapper; otherwise classify the response status.
func mapError(err error, profileName string) *provider.Error {
	if err == nil {
		return nil
	}
	if mapped := provider.MapTransportError(err, profileName); mapped != nil {
		return mapped
	}
	var modelTimeout *types.ModelTimeoutException
	if errors.As(err, &modelTimeout) {
		return mapHTTPError(http.StatusRequestTimeout, err, profileName)
	}
	var modelError *types.ModelErrorException
	if errors.As(err, &modelError) {
		return mapHTTPError(http.StatusFailedDependency, err, profileName)
	}
	var statusErr interface{ HTTPStatusCode() int }
	if errors.As(err, &statusErr) {
		mapped := mapHTTPError(statusErr.HTTPStatusCode(), err, profileName)
		var responseErr interface{ HTTPResponse() *smithyhttp.Response }
		if errors.As(err, &responseErr) {
			if response := responseErr.HTTPResponse(); response != nil && response.Response != nil {
				provider.ApplyRetryAfter(mapped, response.Header)
			}
		}
		return mapped
	}
	return provider.NewUnclassifiedDispatchError(err, profileName)
}

func mapHTTPError(status int, cause error, profileName string) *provider.Error {
	code, retry, dispatch, safe := provider.ClassifyHTTPStatus(status)
	if status == http.StatusRequestTimeout || status == http.StatusFailedDependency {
		// Bedrock reports model processing failures with these 4xx statuses.
		// They do not prove rejection or zero usage: a new attempt needs budget.
		code, retry, dispatch, safe = provider.CodeProviderUnavailable, provider.RetrySameOperation, provider.DispatchAmbiguous, "provider model processing failed"
	}
	mapped := provider.NewError(code, provider.PhaseDispatch, dispatch, retry, safe)
	mapped.Cause = cause
	mapped.SafeDetails = map[string]string{"provider": profileName, "status": fmt.Sprintf("%d", status)}
	return mapped
}
