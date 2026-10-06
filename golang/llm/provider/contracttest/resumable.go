package contracttest

import (
	"fmt"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// ResumableSequence is the observable part of a resumable adapter contract.
// Submit is called at most once; Poll contains the ordered responses from
// subsequent polls of the provider-owned operation. The sequence deliberately
// carries results rather than an adapter so it can be used with checked-in
// fixtures and deterministic provider fakes without making network calls.
type ResumableSequence struct {
	Submit provider.ResumableResult
	Poll   []provider.ResumableResult
}

// ValidateResumableSequence verifies the safety properties shared by every
// provider that opts into ResumableAdapter:
//
//   - every response is a valid closed result;
//   - a pending submit has a provider operation ID;
//   - every poll retains that ID when it returns one;
//   - polling stops after a terminal response; and
//   - a terminal submit cannot be followed by a poll.
//
// It does not assert provider-specific status mappings, delay values, or
// idempotency lookup behavior. Those facts require an adapter's documented API
// contract and belong in that adapter's fixture tests.
func ValidateResumableSequence(sequence ResumableSequence) error {
	if err := sequence.Submit.Validate(); err != nil {
		return fmt.Errorf("resumable submit is invalid")
	}
	if len(sequence.Poll) == 0 {
		return nil
	}
	if sequence.Submit.State != provider.ResumablePending {
		return fmt.Errorf("resumable poll sequence follows a terminal submit")
	}
	operationID := sequence.Submit.ProviderOperationID
	terminal := false
	for _, result := range sequence.Poll {
		if terminal {
			return fmt.Errorf("resumable poll sequence continues after a terminal result")
		}
		if err := result.Validate(); err != nil {
			return fmt.Errorf("resumable poll is invalid")
		}
		if result.ProviderOperationID != "" && result.ProviderOperationID != operationID {
			return fmt.Errorf("resumable poll changed provider operation identity")
		}
		switch result.State {
		case provider.ResumablePending:
			// Validate already requires the operation ID for pending results.
		case provider.ResumableCompleted, provider.ResumableFailed, provider.ResumableNotFound:
			terminal = true
		default:
			// Validate keeps this switch exhaustive as the result state evolves.
			return fmt.Errorf("resumable poll has unsupported state")
		}
	}
	return nil
}
