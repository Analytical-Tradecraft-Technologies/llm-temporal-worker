package contracttest

import (
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func resumableCompleted(id string) provider.ResumableResult {
	return provider.ResumableResult{
		State:               provider.ResumableCompleted,
		ProviderOperationID: id,
		Dispatch:            provider.DispatchAccepted,
		Result: provider.Result{Response: llm.Response{
			OperationKey: "fixture-operation",
			Status:       llm.ResponseStatusCompleted,
		}},
	}
}

func TestValidateResumableSequenceAcceptsPendingPollsAndTerminalCompletion(t *testing.T) {
	sequence := ResumableSequence{
		Submit: provider.ResumableResult{
			State:               provider.ResumablePending,
			ProviderOperationID: "fixture-provider-operation",
			NextPollAfter:       time.Second,
			Dispatch:            provider.DispatchAccepted,
		},
		Poll: []provider.ResumableResult{
			{State: provider.ResumablePending, ProviderOperationID: "fixture-provider-operation", Dispatch: provider.DispatchAccepted},
			resumableCompleted("fixture-provider-operation"),
		},
	}
	if err := ValidateResumableSequence(sequence); err != nil {
		t.Fatalf("ValidateResumableSequence() = %v", err)
	}
}

func TestValidateResumableSequenceAllowsPendingPollPrefix(t *testing.T) {
	sequence := ResumableSequence{
		Submit: provider.ResumableResult{State: provider.ResumablePending, ProviderOperationID: "fixture-provider-operation", Dispatch: provider.DispatchAccepted},
		Poll:   []provider.ResumableResult{{State: provider.ResumablePending, ProviderOperationID: "fixture-provider-operation", Dispatch: provider.DispatchAccepted}},
	}
	if err := ValidateResumableSequence(sequence); err != nil {
		t.Fatalf("pending sequence rejected: %v", err)
	}
}

func TestValidateResumableSequenceRejectsUnsafeTransitions(t *testing.T) {
	base := ResumableSequence{
		Submit: provider.ResumableResult{State: provider.ResumablePending, ProviderOperationID: "fixture-provider-operation", Dispatch: provider.DispatchAccepted},
	}
	cases := []struct {
		name string
		make func() ResumableSequence
		want string
	}{
		{
			name: "poll after completed submit",
			make: func() ResumableSequence {
				return ResumableSequence{Submit: resumableCompleted("fixture-provider-operation"), Poll: []provider.ResumableResult{base.Submit}}
			},
			want: "terminal submit",
		},
		{
			name: "changed provider operation identity",
			make: func() ResumableSequence {
				return ResumableSequence{Submit: base.Submit, Poll: []provider.ResumableResult{{State: provider.ResumablePending, ProviderOperationID: "other-operation", Dispatch: provider.DispatchAccepted}}}
			},
			want: "changed provider operation identity",
		},
		{
			name: "poll after terminal",
			make: func() ResumableSequence {
				return ResumableSequence{Submit: base.Submit, Poll: []provider.ResumableResult{resumableCompleted("fixture-provider-operation"), base.Submit}}
			},
			want: "continues after a terminal",
		},
		{
			name: "invalid poll",
			make: func() ResumableSequence {
				return ResumableSequence{Submit: base.Submit, Poll: []provider.ResumableResult{{State: provider.ResumablePending, Dispatch: provider.DispatchAccepted}}}
			},
			want: "resumable poll is invalid",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateResumableSequence(tc.make())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateResumableSequence() = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestValidateResumableSequenceDoesNotExposeProviderIDs(t *testing.T) {
	secretID := "provider-secret-operation-id"
	sequence := ResumableSequence{
		Submit: provider.ResumableResult{State: provider.ResumablePending, ProviderOperationID: secretID, Dispatch: provider.DispatchAccepted},
		Poll:   []provider.ResumableResult{{State: provider.ResumablePending, ProviderOperationID: "different-secret-id", Dispatch: provider.DispatchAccepted}},
	}
	err := ValidateResumableSequence(sequence)
	if err == nil {
		t.Fatal("identity mismatch unexpectedly accepted")
	}
	if strings.Contains(err.Error(), secretID) || strings.Contains(err.Error(), "different-secret-id") {
		t.Fatalf("provider operation ID leaked in error: %v", err)
	}
}
