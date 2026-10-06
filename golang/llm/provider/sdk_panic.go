package provider

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

// CallRecovered runs one provider SDK call and reports a panic from it
// instead of letting the panic unwind the Activity. An escaped panic would
// bypass the operation ledger's terminal classification, and Temporal would
// retry the Activity as if the request had never been sent.
func CallRecovered[T any](call func() (T, error)) (result T, panicked any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			var zero T
			result, panicked, err = zero, recovered, nil
		}
	}()
	result, err = call()
	return result, nil, err
}

// DispatchProbe records how far one SDK call got, so a recovered panic can be
// classified like the call's other outcomes. Install Middleware as the
// call's last (innermost) SDK request middleware: it runs after request
// serialization and signing, immediately around the HTTP transport.
type DispatchProbe struct {
	sent   atomic.Bool
	status atomic.Int64
}

// Middleware has the shape of the OpenAI and Anthropic SDK request
// middleware.
func (probe *DispatchProbe) Middleware(request *http.Request, next func(*http.Request) (*http.Response, error)) (*http.Response, error) {
	probe.sent.Store(true)
	response, err := next(request)
	if response != nil {
		probe.status.Store(int64(response.StatusCode))
	}
	return response, err
}

// PanicError classifies a panic recovered from the probed SDK call:
//   - before the transport was entered nothing was sent, so another route may
//     be tried;
//   - after sending but without a response the outcome is ambiguous;
//   - with a success status the provider accepted (and billed) the request,
//     so the panic is an invalid response that must not be retried;
//   - with an error status the provider rejected the request, classified as
//     the HTTP error would be.
func (probe *DispatchProbe) PanicError(operationKey string, panicked any) *Error {
	status := int(probe.status.Load())
	var mapped *Error
	switch {
	case !probe.sent.Load():
		mapped = NewError(CodeInternal, PhaseDispatch, DispatchNotDispatched, RetryNextRoute, fmt.Sprintf("provider SDK panicked before the request was sent: %v", panicked))
	case status == 0:
		mapped = NewError(CodeInternal, PhaseDispatch, DispatchAmbiguous, RetryNever, fmt.Sprintf("provider SDK panicked after the request was sent: %v", panicked))
	case status >= 200 && status < 300:
		mapped = NewError(CodeProviderInvalidResponse, PhaseLift, DispatchAccepted, RetryNever, fmt.Sprintf("provider SDK panicked while decoding the response: %v", panicked))
	case status < 400:
		mapped = NewError(CodeProviderUnavailable, PhaseDispatch, DispatchAmbiguous, RetryNever, "provider redirect response is ambiguous")
	case status == http.StatusTooManyRequests:
		mapped = NewError(CodeProviderRateLimited, PhaseDispatch, DispatchRejected, RetryAfter, "provider rate limited the request")
	case status >= http.StatusInternalServerError:
		mapped = NewError(CodeProviderUnavailable, PhaseDispatch, DispatchRejected, RetrySameOperation, "provider is unavailable")
	default:
		mapped = NewError(CodeInvalidArgument, PhaseDispatch, DispatchRejected, RetryNever, "provider rejected the request")
	}
	mapped.OperationID = operationKey
	if status != 0 {
		mapped.SafeDetails = map[string]string{"status": fmt.Sprintf("%d", status)}
	}
	return mapped
}
