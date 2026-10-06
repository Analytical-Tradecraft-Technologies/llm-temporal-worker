package provider_test

import (
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestResumableResultValidation(t *testing.T) {
	validFailure := provider.NewError(provider.CodeProviderUnavailable, provider.PhasePoll, provider.DispatchAccepted, provider.RetryNever, "provider rejected operation")
	cases := []struct {
		name   string
		result provider.ResumableResult
		want   string
	}{
		{
			name:   "pending",
			result: provider.ResumableResult{State: provider.ResumablePending, ProviderOperationID: "provider-op", NextPollAfter: time.Second, Dispatch: provider.DispatchAccepted},
		},
		{
			name:   "completed",
			result: provider.ResumableResult{State: provider.ResumableCompleted, ProviderOperationID: "provider-op", Dispatch: provider.DispatchAccepted, Result: provider.Result{Response: llm.Response{OperationKey: "operation-key", Status: llm.ResponseStatusCompleted}}},
		},
		{
			name:   "completed may omit terminal id",
			result: provider.ResumableResult{State: provider.ResumableCompleted, Dispatch: provider.DispatchAccepted, Result: provider.Result{Response: llm.Response{OperationKey: "operation-key", Status: llm.ResponseStatusCompleted}}},
		},
		{
			name:   "failed",
			result: provider.ResumableResult{State: provider.ResumableFailed, Failure: validFailure, Dispatch: provider.DispatchAccepted},
		},
		{
			name:   "not found is ambiguous",
			result: provider.ResumableResult{State: provider.ResumableNotFound, Dispatch: provider.DispatchAmbiguous},
		},
		{
			name:   "pending requires id",
			result: provider.ResumableResult{State: provider.ResumablePending, Dispatch: provider.DispatchAccepted},
			want:   "missing provider operation id",
		},
		{
			name:   "pending cannot be rejected",
			result: provider.ResumableResult{State: provider.ResumablePending, ProviderOperationID: "provider-op", Dispatch: provider.DispatchRejected},
			want:   "dispatch must be",
		},
		{
			name:   "negative guidance is invalid",
			result: provider.ResumableResult{State: provider.ResumablePending, ProviderOperationID: "provider-op", NextPollAfter: -time.Second, Dispatch: provider.DispatchAccepted},
			want:   "must not be negative",
		},
		{
			name:   "failed requires failure",
			result: provider.ResumableResult{State: provider.ResumableFailed, Dispatch: provider.DispatchAccepted},
			want:   "missing a provider failure",
		},
		{
			name:   "not found cannot carry id",
			result: provider.ResumableResult{State: provider.ResumableNotFound, ProviderOperationID: "provider-op", Dispatch: provider.DispatchAmbiguous},
			want:   "must not contain a provider operation id",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.result.Validate()
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Validate() = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestResumableStateIsClosed(t *testing.T) {
	for _, state := range []provider.ResumableState{provider.ResumablePending, provider.ResumableCompleted, provider.ResumableFailed, provider.ResumableNotFound} {
		if !state.Valid() {
			t.Errorf("state %q is not valid", state)
		}
	}
	if provider.ResumableState("provider_running").Valid() {
		t.Fatal("unknown provider state was accepted")
	}
}

func TestResumableResultValidationBindsCompletedResponseToCall(t *testing.T) {
	result := provider.ResumableResult{
		State:    provider.ResumableCompleted,
		Dispatch: provider.DispatchAccepted,
		Result:   provider.Result{Response: llm.Response{OperationKey: "other-operation", Status: llm.ResponseStatusCompleted}},
	}
	err := result.ValidateForCall(provider.Call{OperationKey: "requested-operation"})
	if err == nil || !strings.Contains(err.Error(), "does not match call") {
		t.Fatalf("ValidateForCall() = %v, want operation-key mismatch", err)
	}
}
