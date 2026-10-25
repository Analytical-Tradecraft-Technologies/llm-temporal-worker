package responsesapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"go.temporal.io/sdk/temporal"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// apiError is an OpenAI-shaped error. Messages are fixed or derived from
// request validation; provider messages and storage errors never reach them.
type apiError struct {
	Status  int     `json:"-"`
	Type    string  `json:"type"`
	Code    string  `json:"code,omitempty"`
	Message string  `json:"message"`
	Param   *string `json:"param"`
}

func (err *apiError) Error() string { return err.Message }

func invalidRequest(message string) *apiError {
	return &apiError{Status: http.StatusBadRequest, Type: "invalid_request_error", Code: "invalid_request", Message: message}
}

func invalidParam(param, message string) *apiError {
	err := invalidRequest(message)
	err.Param = &param
	return err
}

func unauthenticated() *apiError {
	return &apiError{Status: http.StatusUnauthorized, Type: "invalid_request_error", Code: "invalid_api_key",
		Message: "the API key must be tenant/project/actor"}
}

func notFound(id string) *apiError {
	return &apiError{Status: http.StatusNotFound, Type: "invalid_request_error", Code: "not_found",
		Message: "no response with id " + id}
}

func serverError(message string) *apiError {
	return &apiError{Status: http.StatusInternalServerError, Type: "server_error", Code: "server_error", Message: message}
}

func unavailable(message string) *apiError {
	return &apiError{Status: http.StatusServiceUnavailable, Type: "server_error", Code: "service_unavailable", Message: message}
}

func writeError(writer http.ResponseWriter, err *apiError) {
	writeJSON(writer, err.Status, map[string]any{"error": err})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		status = http.StatusInternalServerError
		data = []byte(`{"error":{"type":"server_error","code":"server_error","message":"response encoding failed","param":null}}`)
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_, _ = writer.Write(append(data, '\n'))
}

// failureFromCode maps the worker's stable error code (provider.Code values
// and the execution failure codes) to an OpenAI error.
func failureFromCode(code string) *apiError {
	switch code {
	case "invalid_argument", "unsupported_capability", "no_route":
		return &apiError{Status: http.StatusBadRequest, Type: "invalid_request_error", Code: code, Message: "the request was rejected: " + code}
	case "permission_denied":
		return &apiError{Status: http.StatusForbidden, Type: "invalid_request_error", Code: code, Message: "the request is not permitted for this tenant, project and actor"}
	case "budget_denied":
		return &apiError{Status: http.StatusTooManyRequests, Type: "insufficient_quota", Code: "insufficient_quota", Message: "the budget for this request is exhausted"}
	case "provider_rate_limited":
		return &apiError{Status: http.StatusTooManyRequests, Type: "rate_limit_error", Code: "rate_limit_exceeded", Message: "the model provider is rate limiting requests"}
	case "operation_conflict":
		return &apiError{Status: http.StatusConflict, Type: "invalid_request_error", Code: "idempotency_conflict", Message: "this response ID was already used for a different request"}
	case "deadline_exceeded":
		return &apiError{Status: http.StatusGatewayTimeout, Type: "server_error", Code: code, Message: "the model request timed out"}
	case "provider_unavailable", "provider_invalid_response", "ambiguous_dispatch", "authentication":
		return &apiError{Status: http.StatusBadGateway, Type: "server_error", Code: code, Message: "the model provider request failed: " + code}
	case "incomplete_response":
		return &apiError{Status: http.StatusBadGateway, Type: "server_error", Code: code, Message: "the model provider returned an incomplete response"}
	default:
		return serverError("the model request failed")
	}
}

// workflowFailure maps a failed Generate workflow. Its application error
// type and safe details carry the stable code; the message is never used.
func workflowFailure(err error) *apiError {
	var application *temporal.ApplicationError
	if !errors.As(err, &application) {
		var canceled *temporal.CanceledError
		if errors.As(err, &canceled) {
			return &apiError{Status: http.StatusConflict, Type: "server_error", Code: "cancelled", Message: "the response was cancelled"}
		}
		var timeout *temporal.TimeoutError
		if errors.As(err, &timeout) {
			return failureFromCode("deadline_exceeded")
		}
		return serverError("the model request failed")
	}
	if application.HasDetails() {
		var execution struct {
			ErrorCode string `json:"error_code"`
		}
		if application.Details(&execution) == nil && execution.ErrorCode != "" {
			return failureFromCode(execution.ErrorCode)
		}
		var safe activity.SafeErrorDetails
		if application.Details(&safe) == nil && safe.Code != "" {
			return failureFromCode(safe.Code)
		}
	}
	switch application.Type() {
	case activity.ErrorTypeInvalidArgument:
		return failureFromCode("invalid_argument")
	case activity.ErrorTypeAuthentication:
		return failureFromCode("authentication")
	case activity.ErrorTypeOperationConflict:
		return failureFromCode("operation_conflict")
	case activity.ErrorTypeAmbiguous:
		return failureFromCode("ambiguous_dispatch")
	case activity.ErrorTypeBudgetWait:
		return failureFromCode("budget_denied")
	case "incomplete_response":
		return failureFromCode("incomplete_response")
	case "provider_error", "provider_rejected":
		return failureFromCode("provider_unavailable")
	default:
		return serverError("the model request failed")
	}
}

// storedFailure maps a failed operation read from the durable record.
func storedFailure(result llm.ExecutionResultV1) *apiError {
	if result.ErrorCode != "" {
		return failureFromCode(result.ErrorCode)
	}
	return failureFromCode(result.FailureCode)
}
