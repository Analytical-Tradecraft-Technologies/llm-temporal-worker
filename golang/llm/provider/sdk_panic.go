package provider

import "fmt"

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

// SDKPanicError classifies a panic recovered from a provider SDK call. Once
// the HTTP response has been received the provider has accepted (and billed)
// the request, so the panic is an invalid response that must not be retried.
// Before that the SDK may or may not have sent the request, so the outcome is
// ambiguous.
func SDKPanicError(operationKey string, panicked any, responseReceived bool) *Error {
	var mapped *Error
	if responseReceived {
		mapped = NewError(CodeProviderInvalidResponse, PhaseLift, DispatchAccepted, RetryNever, fmt.Sprintf("provider SDK panicked while decoding the response: %v", panicked))
	} else {
		mapped = NewError(CodeInternal, PhaseDispatch, DispatchAmbiguous, RetryNever, fmt.Sprintf("provider SDK panicked before a response was received: %v", panicked))
	}
	mapped.OperationID = operationKey
	return mapped
}
