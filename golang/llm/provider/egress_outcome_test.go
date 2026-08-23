package provider_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestEgressOutcomeRecordsOnlyMarkedDenials(t *testing.T) {
	ctx, outcome := provider.WithEgressOutcome(context.Background())
	provider.RecordEgressDenied(ctx, errors.New("ordinary transport failure"))
	if got := outcome.Denial(); got != nil {
		t.Fatalf("outcome denial after unmarked error = %v, want nil", got)
	}

	cause := fmt.Errorf("unsafe destination: %w", provider.ErrProviderEgressDenied)
	provider.RecordEgressDenied(ctx, cause)
	if got := outcome.Denial(); !errors.Is(got, provider.ErrProviderEgressDenied) || !errors.Is(got, cause) {
		t.Fatalf("outcome denial = %v, want retained marked cause", got)
	}
}

func TestEgressDenialForContextErrorDoesNotMaskOtherErrors(t *testing.T) {
	ctx, outcome := provider.WithEgressOutcome(context.Background())
	provider.RecordEgressDenied(ctx, provider.ErrProviderEgressDenied)

	for name, err := range map[string]error{
		"canceled": context.Canceled,
		"deadline": context.DeadlineExceeded,
	} {
		t.Run(name, func(t *testing.T) {
			if got := provider.EgressDenialForContextError(outcome, err); !errors.Is(got, provider.ErrProviderEgressDenied) {
				t.Fatalf("EgressDenialForContextError(%v) = %v, want ErrProviderEgressDenied", err, got)
			}
		})
	}
	if got := provider.EgressDenialForContextError(outcome, errors.New("provider returned an HTTP error")); got != nil {
		t.Fatalf("non-context error selected egress denial %v", got)
	}
}

func TestEgressOutcomeClassifiesPolicyDenialAfterSDKContextReplacement(t *testing.T) {
	ctx, outcome := provider.WithEgressOutcome(context.Background())
	denial := fmt.Errorf("unsafe destination: %w", provider.ErrProviderEgressDenied)
	provider.RecordEgressDenied(ctx, denial)

	mapped := provider.ClassifyEgressOutcome(outcome, context.DeadlineExceeded)
	if mapped == nil {
		t.Fatal("ClassifyEgressOutcome() = nil, want policy result")
	}
	if mapped.Code != provider.CodeProviderUnavailable || mapped.Phase != provider.PhaseDispatch || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute {
		t.Fatalf("policy result = %#v, want retryable not-dispatched provider-unavailable", mapped)
	}
	if !errors.Is(mapped, provider.ErrProviderEgressDenied) || !errors.Is(mapped, denial) {
		t.Fatalf("policy result cause = %v, want retained egress denial", mapped)
	}
}

func TestEgressOutcomeClassifiesPreDispatchAvailabilityAfterSDKContextReplacement(t *testing.T) {
	ctx, outcome := provider.WithEgressOutcome(context.Background())
	failure := fmt.Errorf("dial failed: %w", provider.ErrProviderPreDispatch)
	provider.RecordPreDispatchFailure(ctx, failure)

	mapped := provider.ClassifyEgressOutcome(outcome, context.DeadlineExceeded)
	if mapped == nil {
		t.Fatal("ClassifyEgressOutcome() = nil, want pre-dispatch availability result")
	}
	if mapped.Code != provider.CodeProviderUnavailable || mapped.Phase != provider.PhaseDispatch || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute {
		t.Fatalf("pre-dispatch availability result = %#v, want retryable not-dispatched provider-unavailable", mapped)
	}
	if !errors.Is(mapped, provider.ErrProviderPreDispatch) || !errors.Is(mapped, failure) {
		t.Fatalf("pre-dispatch availability result cause = %v, want retained pre-dispatch marker", mapped)
	}
}

func TestEgressOutcomeClassifiesOversizedResponseWithoutUnsafeDiagnostics(t *testing.T) {
	_, outcome := provider.WithEgressOutcome(context.Background())
	cause := fmt.Errorf("read https://provider.example/private: raw-provider-body: %w", provider.ErrProviderResponseTooLarge)

	mapped := provider.ClassifyEgressOutcome(outcome, cause)
	if mapped == nil {
		t.Fatal("ClassifyEgressOutcome() = nil, want oversized-response result")
	}
	if mapped.Code != provider.CodeProviderInvalidResponse || mapped.Phase != provider.PhaseDispatch || mapped.Dispatch != provider.DispatchAccepted || mapped.Retry != provider.RetryNever {
		t.Fatalf("oversized-response result = %#v, want accepted non-retryable invalid response", mapped)
	}
	if !errors.Is(mapped, provider.ErrProviderResponseTooLarge) {
		t.Fatalf("oversized-response cause = %v, want ErrProviderResponseTooLarge", mapped)
	}
	encoded, err := json.Marshal(mapped)
	if err != nil {
		t.Fatal(err)
	}
	for _, unsafe := range []string{"provider.example", "raw-provider-body"} {
		if strings.Contains(mapped.Error(), unsafe) || strings.Contains(string(encoded), unsafe) {
			t.Fatalf("oversized-response diagnostics exposed %q: error=%q json=%s", unsafe, mapped.Error(), encoded)
		}
	}
}

func TestEgressOutcomeClassifiesCallerCancellationAfterSDKContextReplacement(t *testing.T) {
	ctx, outcome := provider.WithEgressOutcome(context.Background())
	provider.RecordPreDispatchContext(ctx, context.Canceled)

	mapped := provider.ClassifyEgressOutcome(outcome, context.Canceled)
	if mapped == nil {
		t.Fatal("ClassifyEgressOutcome() = nil, want pre-dispatch cancellation result")
	}
	if mapped.Code != provider.CodeCanceled || mapped.Phase != provider.PhaseDispatch || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever {
		t.Fatalf("pre-dispatch cancellation result = %#v, want non-retryable not-dispatched cancellation", mapped)
	}
	if !errors.Is(mapped, provider.ErrProviderPreDispatch) || !errors.Is(mapped, context.Canceled) {
		t.Fatalf("pre-dispatch cancellation result cause = %v, want retained pre-dispatch cancellation", mapped)
	}
}

func TestEgressOutcomeDoesNotLetLatePreflightOverwriteCallerCancellation(t *testing.T) {
	ctx, outcome := provider.WithEgressOutcome(context.Background())
	provider.RecordPreDispatchContext(ctx, context.DeadlineExceeded)
	provider.RecordEgressDenied(ctx, provider.ErrProviderEgressDenied)
	provider.RecordPreDispatchFailure(ctx, provider.ErrProviderPreDispatch)

	mapped := provider.ClassifyEgressOutcome(outcome, context.DeadlineExceeded)
	if mapped == nil {
		t.Fatal("ClassifyEgressOutcome() = nil, want preserved cancellation")
	}
	if mapped.Code != provider.CodeDeadlineExceeded || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever {
		t.Fatalf("late preflight overwrote cancellation: %#v", mapped)
	}
	if errors.Is(mapped, provider.ErrProviderEgressDenied) {
		t.Fatalf("late policy result overwrote cancellation: %v", mapped)
	}
}
