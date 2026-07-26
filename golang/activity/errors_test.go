package activity

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"go.temporal.io/sdk/temporal"
)

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
			var application *temporal.ApplicationError
			if !errors.As(mapped, &application) {
				t.Fatalf("mapped error = %T %v", mapped, mapped)
			}
			if application.Type() != test.wantType || application.NonRetryable() != test.nonRetryable {
				t.Fatalf("application error type=%q non_retryable=%v", application.Type(), application.NonRetryable())
			}
			if strings.Contains(application.Error(), "secret") {
				t.Fatal("safe error leaked provider message")
			}
			var details SafeErrorDetails
			if err := application.Details(&details); err != nil {
				t.Fatal(err)
			}
			if details.OperationID != "operation-1" || details.Code != string(test.err.Code) {
				t.Fatalf("safe details = %#v", details)
			}
		})
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

func TestErrorMappingHonorsCertifiedPreDispatchDeadlineRetryNever(t *testing.T) {
	deadline := provider.NewPreDispatchContextError(context.DeadlineExceeded)
	deadline.OperationID = "operation-pre-dispatch-deadline"

	mapped := ToTemporalError(deadline)
	var application *temporal.ApplicationError
	if !errors.As(mapped, &application) {
		t.Fatalf("mapped error = %T %v, want *temporal.ApplicationError", mapped, mapped)
	}
	if application.Type() != ErrorTypeProviderTransient || !application.NonRetryable() {
		t.Fatalf("application error type=%q non_retryable=%v, want transient non-retryable", application.Type(), application.NonRetryable())
	}
	var details SafeErrorDetails
	if err := application.Details(&details); err != nil {
		t.Fatal(err)
	}
	if details.OperationID != deadline.OperationID || details.Code != string(provider.CodeDeadlineExceeded) || details.Dispatch != string(provider.DispatchNotDispatched) {
		t.Fatalf("safe details = %#v", details)
	}
}

func TestErrorMappingHonorsCertifiedPreDispatchCancellationRetryNever(t *testing.T) {
	canceled := provider.NewPreDispatchContextError(context.Canceled)
	canceled.OperationID = "operation-pre-dispatch-canceled"

	mapped := ToTemporalError(canceled)
	var application *temporal.ApplicationError
	if !errors.As(mapped, &application) {
		t.Fatalf("mapped error = %T %v, want *temporal.ApplicationError", mapped, mapped)
	}
	if application.Type() != ErrorTypeInvalidArgument || !application.NonRetryable() {
		t.Fatalf("application error type=%q non_retryable=%v, want invalid argument non-retryable", application.Type(), application.NonRetryable())
	}
	if errors.Is(mapped, context.Canceled) {
		t.Fatal("certified pre-dispatch caller cancellation must be a non-retryable failure, not Temporal task cancellation")
	}
	var details SafeErrorDetails
	if err := application.Details(&details); err != nil {
		t.Fatal(err)
	}
	if details.OperationID != canceled.OperationID || details.Code != string(provider.CodeCanceled) || details.Dispatch != string(provider.DispatchNotDispatched) {
		t.Fatalf("safe details = %#v", details)
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
	var application *temporal.ApplicationError
	if !errors.As(mapped, &application) {
		t.Fatalf("mapped error = %T %v, want application error", mapped, mapped)
	}
	var details SafeErrorDetails
	if err := application.Details(&details); err != nil {
		t.Fatal(err)
	}
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
	var application *temporal.ApplicationError
	if !errors.As(mapped, &application) {
		t.Fatalf("mapped error = %T %v, want application error", mapped, mapped)
	}
	var details SafeErrorDetails
	if err := application.Details(&details); err != nil {
		t.Fatal(err)
	}
	if details.Code != string(provider.CodeInternal) || details.Phase != string(provider.PhaseFinalize) || details.Dispatch != string(provider.DispatchNotDispatched) {
		t.Fatalf("unknown provider enums were copied into Temporal details: %#v", details)
	}
	if strings.Contains(application.Error(), "provider-controlled") || strings.Contains(application.Error(), "unsafe provider diagnostic") {
		t.Fatalf("provider-controlled error text leaked: %v", application)
	}
}
