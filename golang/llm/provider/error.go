package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

// ErrProviderEgressDenied marks a request rejected by the configured provider
// egress transport before any provider bytes can be sent. Concrete transport
// errors must wrap this marker without exposing the rejected destination or
// request data.
var ErrProviderEgressDenied = errors.New("provider egress denied")

// ErrProviderPreDispatch marks a transport failure for which the guarded
// provider transport proved that no writable connection was acquired. It is
// deliberately distinct from ErrProviderEgressDenied: this marker describes
// endpoint availability, while the egress marker describes an explicit policy
// rejection.
var ErrProviderPreDispatch = errors.New("provider request failed before dispatch")

// ErrProviderResponseTooLarge marks a provider response body that crossed the
// centrally enforced transport limit. The marker contains no provider body,
// URL, header, or configured limit so it is safe to use for classification.
var ErrProviderResponseTooLarge = errors.New("provider response exceeded configured byte limit")

type Code string

const (
	CodeInvalidArgument         Code = "invalid_argument"
	CodeUnsupportedCapability   Code = "unsupported_capability"
	CodeNoRoute                 Code = "no_route"
	CodeAuthentication          Code = "authentication"
	CodePermissionDenied        Code = "permission_denied"
	CodeBudgetDenied            Code = "budget_denied"
	CodeOperationConflict       Code = "operation_conflict"
	CodeAmbiguousDispatch       Code = "ambiguous_dispatch"
	CodeProviderRateLimited     Code = "provider_rate_limited"
	CodeProviderUnavailable     Code = "provider_unavailable"
	CodeProviderInvalidResponse Code = "provider_invalid_response"
	CodeDeadlineExceeded        Code = "deadline_exceeded"
	CodeCanceled                Code = "canceled"
	CodeStateUnavailable        Code = "state_unavailable"
	CodeStateCorrupt            Code = "state_corrupt"
	CodeConfiguration           Code = "configuration"
	CodeInternal                Code = "internal"
)

func (code Code) Valid() bool {
	switch code {
	case CodeInvalidArgument, CodeUnsupportedCapability, CodeNoRoute,
		CodeAuthentication, CodePermissionDenied, CodeBudgetDenied,
		CodeOperationConflict, CodeAmbiguousDispatch, CodeProviderRateLimited,
		CodeProviderUnavailable, CodeProviderInvalidResponse, CodeDeadlineExceeded,
		CodeCanceled, CodeStateUnavailable, CodeStateCorrupt, CodeConfiguration,
		CodeInternal:
		return true
	default:
		return false
	}
}

type Phase string

const (
	PhaseDecode            Phase = "decode"
	PhaseNormalize         Phase = "normalize"
	PhaseStateLoad         Phase = "state_load"
	PhasePlan              Phase = "plan"
	PhasePrice             Phase = "price"
	PhaseAdmission         Phase = "admission"
	PhaseCompile           Phase = "compile"
	PhaseDispatch          Phase = "dispatch"
	PhasePoll              Phase = "poll"
	PhaseStream            Phase = "stream"
	PhaseLift              Phase = "lift"
	PhaseFinalize          Phase = "finalize"
	PhaseContinuationWrite Phase = "continuation_write"
)

func (phase Phase) Valid() bool {
	switch phase {
	case PhaseDecode, PhaseNormalize, PhaseStateLoad, PhasePlan, PhasePrice,
		PhaseAdmission, PhaseCompile, PhaseDispatch, PhasePoll, PhaseStream, PhaseLift,
		PhaseFinalize, PhaseContinuationWrite:
		return true
	default:
		return false
	}
}

type DispatchCertainty string

const (
	DispatchNotDispatched DispatchCertainty = "not_dispatched"
	DispatchRejected      DispatchCertainty = "rejected"
	DispatchAccepted      DispatchCertainty = "accepted"
	DispatchAmbiguous     DispatchCertainty = "ambiguous"
)

func (certainty DispatchCertainty) Valid() bool {
	switch certainty {
	case DispatchNotDispatched, DispatchRejected, DispatchAccepted, DispatchAmbiguous:
		return true
	default:
		return false
	}
}

type RetryDisposition string

const (
	RetryNever         RetryDisposition = "never"
	RetrySameOperation RetryDisposition = "same_operation"
	RetryNextRoute     RetryDisposition = "next_route"
	RetryAfter         RetryDisposition = "after"
)

func (retry RetryDisposition) Valid() bool {
	switch retry {
	case RetryNever, RetrySameOperation, RetryNextRoute, RetryAfter:
		return true
	default:
		return false
	}
}

type Error struct {
	Code        Code
	Phase       Phase
	Dispatch    DispatchCertainty
	Retry       RetryDisposition
	RetryAfter  time.Duration
	OperationID string
	Provider    llm.ProviderFacts
	SafeMessage string
	SafeDetails map[string]string
	Cause       error
}

func NewError(code Code, phase Phase, dispatch DispatchCertainty, retry RetryDisposition, message string) *Error {
	return &Error{Code: code, Phase: phase, Dispatch: dispatch, Retry: retry, SafeMessage: message}
}

// NewEgressDeniedError converts a provider egress preflight denial into the
// common error contract. The cause remains available for local diagnostics but
// is never serialized to callers.
func NewEgressDeniedError(cause error) *Error {
	mapped := NewError(CodeProviderUnavailable, PhaseDispatch, DispatchNotDispatched, RetryNextRoute, "provider egress policy denied request")
	mapped.Cause = cause
	return mapped
}

// NewPreDispatchUnavailableError converts a guarded transport failure that
// occurred before a writable provider connection was acquired into a safe,
// retryable next-route result. It must only be used with
// ErrProviderPreDispatch evidence.
func NewPreDispatchUnavailableError(cause error) *Error {
	mapped := NewError(CodeProviderUnavailable, PhaseDispatch, DispatchNotDispatched, RetryNextRoute, "provider connection failed before dispatch")
	mapped.Cause = withPreDispatchMarker(cause)
	return mapped
}

// NewProviderResponseTooLargeError classifies a response that was received
// but crossed the transport byte limit. The provider saw the request, so the
// dispatch is accepted and must not be retried automatically.
func NewProviderResponseTooLargeError(cause error) *Error {
	mapped := NewError(CodeProviderInvalidResponse, PhaseDispatch, DispatchAccepted, RetryNever, "provider response exceeded configured byte limit")
	mapped.Cause = cause
	return mapped
}

// NewPreDispatchContextError preserves a caller cancellation or deadline that
// the guarded transport proved happened before a writable provider connection
// was acquired. Caller intent must never become a route fallback.
func NewPreDispatchContextError(cause error) *Error {
	code := CodeCanceled
	message := "provider request canceled before dispatch"
	if errors.Is(cause, context.DeadlineExceeded) {
		code = CodeDeadlineExceeded
		message = "provider request deadline exceeded before dispatch"
	}
	mapped := NewError(code, PhaseDispatch, DispatchNotDispatched, RetryNever, message)
	mapped.Cause = withPreDispatchMarker(cause)
	return mapped
}

func withPreDispatchMarker(cause error) error {
	if cause == nil || errors.Is(cause, ErrProviderPreDispatch) {
		if cause == nil {
			return ErrProviderPreDispatch
		}
		return cause
	}
	return errors.Join(ErrProviderPreDispatch, cause)
}

// IsRedirectStatus reports whether a response status means the provider
// received a redirect response. A request may already have reached the
// provider before this status is returned, so it must never be retried.
func IsRedirectStatus(status int) bool {
	return status >= 300 && status < 400
}

// NewRedirectResponseError classifies a received redirect response
// conservatively. The status is safe to expose; response headers (including
// Location) are deliberately omitted because they may contain credentials or
// request-specific data.
func NewRedirectResponseError(status int) *Error {
	mapped := NewError(CodeProviderUnavailable, PhaseDispatch, DispatchAmbiguous, RetryNever, "provider redirect response is ambiguous")
	mapped.SafeDetails = map[string]string{"status": strconv.Itoa(status)}
	return mapped
}

func (err *Error) Error() string {
	if err == nil {
		return ""
	}
	return err.SafeMessage
}

func (err *Error) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

// WithEndpointID attaches the configured endpoint identifier to an error's
// safe details without exposing its diagnostic cause. Endpoint IDs originate
// from validated operator configuration, never a request-time provider URL.
func WithEndpointID(err *Error, endpointID string) *Error {
	if err == nil || endpointID == "" {
		return err
	}
	cloned := *err
	cloned.SafeDetails = make(map[string]string, len(err.SafeDetails)+1)
	for key, value := range err.SafeDetails {
		cloned.SafeDetails[key] = value
	}
	cloned.SafeDetails["endpoint"] = endpointID
	return &cloned
}

func (err *Error) MarshalJSON() ([]byte, error) {
	if err == nil {
		return []byte("null"), nil
	}
	if err.SafeMessage == "" {
		return nil, fmt.Errorf("safe error message must not be empty")
	}
	if !err.Code.Valid() {
		return nil, fmt.Errorf("error code %q is invalid", err.Code)
	}
	if !err.Phase.Valid() {
		return nil, fmt.Errorf("error phase %q is invalid", err.Phase)
	}
	if !err.Dispatch.Valid() {
		return nil, fmt.Errorf("dispatch certainty %q is invalid", err.Dispatch)
	}
	if !err.Retry.Valid() {
		return nil, fmt.Errorf("retry disposition %q is invalid", err.Retry)
	}
	fields := map[string]any{
		"code":     err.Code,
		"phase":    err.Phase,
		"dispatch": err.Dispatch,
		"retry":    err.Retry,
		"message":  err.SafeMessage,
	}
	if err.RetryAfter > 0 {
		fields["retry_after_ms"] = err.RetryAfter.Milliseconds()
	}
	if err.OperationID != "" {
		fields["operation_id"] = err.OperationID
	}
	provider := map[string]string{}
	if err.Provider.ResponseID != "" {
		provider["response_id"] = err.Provider.ResponseID
	}
	if err.Provider.RequestID != "" {
		provider["request_id"] = err.Provider.RequestID
	}
	if err.Provider.GenerationID != "" {
		provider["generation_id"] = err.Provider.GenerationID
	}
	if err.Provider.FinishReason != "" {
		provider["finish_reason"] = err.Provider.FinishReason
	}
	if len(provider) > 0 {
		fields["provider"] = provider
	}
	if len(err.SafeDetails) > 0 {
		fields["details"] = err.SafeDetails
	}
	return json.Marshal(fields)
}
