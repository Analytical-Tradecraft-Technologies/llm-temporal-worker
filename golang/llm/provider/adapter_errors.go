package provider

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// The helpers in this file are the compile and dispatch building blocks every
// endpoint-family adapter shares, so the error contract they produce cannot
// drift between families.

// NewCompileError reports a request the adapter rejected before dispatch.
func NewCompileError(message string) *Error {
	return NewError(CodeInvalidArgument, PhaseCompile, DispatchNotDispatched, RetryNever, message)
}

// NewUnsupportedFeatureError reports a required feature the route's
// capability set does not support.
func NewUnsupportedFeatureError(feature Feature, message string) *Error {
	return NewUnsupportedCapabilityError(fmt.Sprintf("%s: %s", feature, message))
}

// NewUnsupportedCapabilityError reports a valid request the route cannot
// carry, such as a service class the profile has no tier for.
func NewUnsupportedCapabilityError(message string) *Error {
	return NewError(CodeUnsupportedCapability, PhaseCompile, DispatchNotDispatched, RetryNever, message)
}

// NewCompileContextError maps a caller cancellation or deadline observed
// while compiling.
func NewCompileContextError(err error) *Error {
	if err == context.Canceled {
		return NewError(CodeCanceled, PhaseCompile, DispatchNotDispatched, RetryNever, "compile canceled")
	}
	return NewError(CodeDeadlineExceeded, PhaseCompile, DispatchNotDispatched, RetryNever, "compile deadline exceeded")
}

// NewDispatchError reports a call the adapter refused to dispatch.
func NewDispatchError(message string, certainty DispatchCertainty) *Error {
	return NewError(CodeInvalidArgument, PhaseDispatch, certainty, RetryNever, message)
}

// NewDispatchContextError maps a caller cancellation or deadline observed
// before the adapter handed the call to its SDK.
func NewDispatchContextError(err error) *Error {
	if err == context.Canceled {
		return NewError(CodeCanceled, PhaseDispatch, DispatchNotDispatched, RetryNever, "dispatch canceled")
	}
	return NewError(CodeDeadlineExceeded, PhaseDispatch, DispatchNotDispatched, RetryNever, "dispatch deadline exceeded")
}

// NewDispatchObserverError reports an observer hook that rejected the call.
func NewDispatchObserverError(err error, certainty DispatchCertainty) *Error {
	mapped := NewError(CodeInternal, PhaseDispatch, certainty, RetryNever, "observer rejected provider response")
	mapped.Cause = err
	return mapped
}

// NewInvalidResponseError reports an accepted response the adapter could not
// lift.
func NewInvalidResponseError(call Call, requestID, message string) *Error {
	mapped := NewError(CodeProviderInvalidResponse, PhaseLift, DispatchAccepted, RetryNever, message)
	mapped.Provider.RequestID = requestID
	mapped.OperationID = call.OperationKey
	return mapped
}

// ValidateCapabilityQuery checks that a capability query, when it names a
// family or endpoint, names this adapter's. label prefixes the error, for
// example "openai chat".
func ValidateCapabilityQuery(query CapabilityQuery, label string, family Family, endpointID string) error {
	if query.Family != "" && query.Family != family {
		return fmt.Errorf("%s: capability family %q does not match %q", label, query.Family, family)
	}
	if query.EndpointID != "" && query.EndpointID != endpointID {
		return fmt.Errorf("%s: capability endpoint %q does not match %q", label, query.EndpointID, endpointID)
	}
	return nil
}

// UniqueFeatures drops empty and repeated features, keeping first-seen order.
func UniqueFeatures(features []Feature) []Feature {
	seen := make(map[Feature]struct{}, len(features))
	result := make([]Feature, 0, len(features))
	for _, feature := range features {
		if feature == "" {
			continue
		}
		if _, ok := seen[feature]; ok {
			continue
		}
		seen[feature] = struct{}{}
		result = append(result, feature)
	}
	return result
}

// CanonicalRequestBytes returns the canonical JSON encoding of a request, the
// size adapters record as the call's estimated bytes.
func CanonicalRequestBytes(request llm.Request) ([]byte, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	return llm.CanonicalJSON(encoded)
}
