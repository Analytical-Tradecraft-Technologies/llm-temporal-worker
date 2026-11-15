package activity

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"go.temporal.io/sdk/temporal"
)

// safeApplicationError unwraps err to a Temporal application error and decodes
// its SafeErrorDetails, failing the test when either is missing.
func safeApplicationError(t *testing.T, err error) (*temporal.ApplicationError, SafeErrorDetails) {
	t.Helper()
	var application *temporal.ApplicationError
	if !errors.As(err, &application) {
		t.Fatalf("error = %T %v, want *temporal.ApplicationError", err, err)
	}
	var details SafeErrorDetails
	if err := application.Details(&details); err != nil {
		t.Fatalf("decode SafeErrorDetails: %v", err)
	}
	return application, details
}

// assertApplicationError fails unless err is a Temporal application error of
// wantType with the given retryability.
func assertApplicationError(t *testing.T, err error, wantType string, nonRetryable bool) *temporal.ApplicationError {
	t.Helper()
	var application *temporal.ApplicationError
	if !errors.As(err, &application) {
		t.Fatalf("error = %T %v, want *temporal.ApplicationError", err, err)
	}
	assertApplicationType(t, application, wantType, nonRetryable)
	return application
}

func assertApplicationType(t *testing.T, application *temporal.ApplicationError, wantType string, nonRetryable bool) {
	t.Helper()
	if application.Type() != wantType || application.NonRetryable() != nonRetryable {
		t.Fatalf("application error type=%q non_retryable=%v, want %q non_retryable=%v", application.Type(), application.NonRetryable(), wantType, nonRetryable)
	}
}

func TestErrorMappingUsesSafeStableTypes(t *testing.T) {
	tests := []struct {
		name         string
		err          *provider.Error
		wantType     string
		nonRetryable bool
	}{
		{"invalid", provider.NewError(provider.CodeInvalidArgument, provider.PhaseNormalize, provider.DispatchNotDispatched, provider.RetryNever, "bad request secret should not leak"), ErrorTypeInvalidArgument, true},
		{"auth", provider.NewError(provider.CodeAuthentication, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNever, "auth failed"), ErrorTypeAuthentication, true},
		{"budget", provider.NewError(provider.CodeBudgetDenied, provider.PhaseAdmission, provider.DispatchNotDispatched, provider.RetryAfter, "budget wait"), ErrorTypeBudgetWait, false},
		{"ambiguous", provider.NewError(provider.CodeAmbiguousDispatch, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "ambiguous"), ErrorTypeAmbiguous, true},
		{"transient", provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNextRoute, "temporarily unavailable"), ErrorTypeProviderTransient, false},
		{"corrupt", provider.NewError(provider.CodeStateCorrupt, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever, "corrupt"), ErrorTypeStateCorrupt, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.err.OperationID = "operation-1"
			test.err.RetryAfter = time.Second
			mapped := ToTemporalError(test.err)
			application, details := safeApplicationError(t, mapped)
			assertApplicationType(t, application, test.wantType, test.nonRetryable)
			if strings.Contains(application.Error(), "secret") {
				t.Fatal("safe error leaked provider message")
			}
			if details.OperationID != "operation-1" || details.Code != string(test.err.Code) {
				t.Fatalf("safe details = %#v", details)
			}
		})
	}
}

func TestErrorMappingSetsTemporalRetryDelay(t *testing.T) {
	providerErr := provider.NewError(
		provider.CodeProviderRateLimited,
		provider.PhaseDispatch,
		provider.DispatchRejected,
		provider.RetryAfter,
		"provider rate limited the request",
	)
	providerErr.RetryAfter = 2 * time.Second

	mapped := ToTemporalError(providerErr)
	var application *temporal.ApplicationError
	if !errors.As(mapped, &application) {
		t.Fatalf("mapped error = %T %v, want *temporal.ApplicationError", mapped, mapped)
	}
	if got, want := application.NextRetryDelay(), 2*time.Second; got != want {
		t.Fatalf("next retry delay = %s, want %s", got, want)
	}
}

func TestErrorMappingPreservesCancellation(t *testing.T) {
	if got := ToTemporalError(context.Canceled); !errors.Is(got, context.Canceled) {
		t.Fatalf("mapped cancellation = %v", got)
	}
	canceled := provider.NewError(provider.CodeCanceled, provider.PhaseDispatch, provider.DispatchNotDispatched, provider.RetryNever, "canceled")
	if got := ToTemporalError(canceled); !errors.Is(got, context.Canceled) {
		t.Fatalf("provider cancellation = %v", got)
	}
}

// A certified pre-dispatch deadline is a non-retryable transient failure and
// a certified pre-dispatch caller cancellation a non-retryable invalid
// argument; neither becomes Temporal task cancellation.
func TestErrorMappingHonorsCertifiedPreDispatchContextRetryNever(t *testing.T) {
	for _, test := range []struct {
		name     string
		cause    error
		wantType string
		wantCode provider.Code
	}{
		{"deadline", context.DeadlineExceeded, ErrorTypeProviderTransient, provider.CodeDeadlineExceeded},
		{"canceled", context.Canceled, ErrorTypeInvalidArgument, provider.CodeCanceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			failure := provider.NewPreDispatchContextError(test.cause)
			failure.OperationID = "operation-pre-dispatch-" + test.name
			mapped := ToTemporalError(failure)
			application, details := safeApplicationError(t, mapped)
			assertApplicationType(t, application, test.wantType, true)
			if errors.Is(mapped, context.Canceled) {
				t.Fatal("certified pre-dispatch failure must be a non-retryable failure, not Temporal task cancellation")
			}
			if details.OperationID != failure.OperationID || details.Code != string(test.wantCode) || details.Dispatch != string(provider.DispatchNotDispatched) {
				t.Fatalf("safe details = %#v", details)
			}
		})
	}
}

func TestErrorMappingBoundsProviderControlledDetails(t *testing.T) {
	providerErr := provider.NewError(
		provider.CodeProviderUnavailable,
		provider.PhaseDispatch,
		provider.DispatchRejected,
		provider.RetryAfter,
		"provider body must never enter Temporal history",
	)
	providerErr.OperationID = strings.Repeat("o", maxSafeErrorIdentifierBytes+1)
	providerErr.Provider.RequestID = "request-id\nwith-control-data"
	providerErr.RetryAfter = 500 * time.Microsecond

	mapped := ToTemporalError(providerErr)
	_, details := safeApplicationError(t, mapped)
	if details.OperationID != "" || details.ProviderRequestID != "" {
		t.Fatalf("unsafe identifiers were copied into Temporal details: %#v", details)
	}
	if details.Code != string(provider.CodeProviderUnavailable) || details.Phase != string(provider.PhaseDispatch) || details.Dispatch != string(provider.DispatchRejected) {
		t.Fatalf("closed error facts changed during normalization: %#v", details)
	}
	if details.RetryAfterMillis != 1 {
		t.Fatalf("sub-millisecond retry hint = %d, want one millisecond minimum", details.RetryAfterMillis)
	}
	encoded, err := json.Marshal(details)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > 512 {
		t.Fatalf("safe error details are not bounded: %d bytes", len(encoded))
	}
}

func TestErrorMappingCollapsesUnknownProviderEnums(t *testing.T) {
	providerErr := provider.NewError(provider.Code("provider-controlled-code"), provider.Phase("provider-controlled-phase\nsecret"), provider.DispatchCertainty("provider-controlled-dispatch"), provider.RetryNever, "unsafe provider diagnostic")
	mapped := ToTemporalError(providerErr)
	application, details := safeApplicationError(t, mapped)
	if details.Code != string(provider.CodeInternal) || details.Phase != string(provider.PhaseFinalize) || details.Dispatch != string(provider.DispatchNotDispatched) {
		t.Fatalf("unknown provider enums were copied into Temporal details: %#v", details)
	}
	if strings.Contains(application.Error(), "provider-controlled") || strings.Contains(application.Error(), "unsafe provider diagnostic") {
		t.Fatalf("provider-controlled error text leaked: %v", application)
	}
}

func TestOperationContentionRetriesOnlyBeforeDispatch(t *testing.T) {
	options, err := validPolicy().TemporalOptions()
	if err != nil {
		t.Fatal(err)
	}
	for _, dispatch := range []provider.DispatchCertainty{provider.DispatchNotDispatched, provider.DispatchRejected, provider.DispatchAccepted, provider.DispatchAmbiguous} {
		for _, retry := range []provider.RetryDisposition{provider.RetrySameOperation, provider.RetryNever, provider.RetryNextRoute} {
			original := provider.NewError(provider.CodeOperationConflict, provider.PhaseAdmission, dispatch, retry, "sensitive operation details")
			original.RetryAfter = 3 * time.Second
			var mapped *temporal.ApplicationError
			if !errors.As(ToTemporalError(original), &mapped) {
				t.Fatal("missing application error")
			}
			wantRetry := dispatch == provider.DispatchNotDispatched && retry == provider.RetrySameOperation
			blocked := mapped.NonRetryable()
			for _, name := range options.RetryPolicy.NonRetryableErrorTypes {
				if name == mapped.Type() {
					blocked = true
				}
			}
			if blocked == wantRetry {
				t.Fatalf("dispatch=%s retry=%s: effective nonretryable=%v", dispatch, retry, blocked)
			}
			if wantRetry && mapped.NextRetryDelay() != 3*time.Second {
				t.Fatal("retry hint lost")
			}
			if !wantRetry && mapped.Type() != ErrorTypeOperationConflict {
				t.Fatalf("permanent conflict changed type: %s", mapped.Type())
			}
			if strings.Contains(mapped.Error(), "sensitive") {
				t.Fatal("operation details leaked")
			}
		}
	}
}

// A planning failure names its first closed-vocabulary cause, so a caller can
// tell a model missing from the catalog from a route that declined the
// request. Anything that is not a bounded token is dropped.
func TestErrorMappingCarriesPlanningReason(t *testing.T) {
	for reason, want := range map[string]string{
		"model_not_in_catalog":  "model_not_in_catalog",
		"capability:web_search": "capability:web_search",
		"Sensitive prompt text": "",
		strings.Repeat("a", 65): "",
		"":                      "",
	} {
		failure := provider.NewError(provider.CodeNoRoute, provider.PhasePlan, provider.DispatchNotDispatched, provider.RetryNever, "provider planning failed")
		failure.SafeDetails = map[string]string{"reason_1": reason}
		_, details := safeApplicationError(t, ToTemporalError(failure))
		if details.Reason != want || details.Code != string(provider.CodeNoRoute) {
			t.Fatalf("reason %q: details = %#v, want reason %q", reason, details, want)
		}
	}
}
