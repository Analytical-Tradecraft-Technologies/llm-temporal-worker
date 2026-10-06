package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// FinishRequestExhausted closes a root after its configured execution limit.
// The active child must be a settled retryable failure or an expired unknown
// outcome. Unknown children retain their accounting and pending discovery.
// The root revision fences concurrent replacement of the active child.
func (r *Repository) FinishRequestExhausted(ctx context.Context, scope Scope, rootID, attemptID RequestID, limit int, now time.Time) (llm.ExecutionResultV1, error) {
	if limit <= 0 || !validTime(now) {
		return llm.ExecutionResultV1{}, ErrInvalid
	}
	for tries := 0; tries < 16; tries++ {
		root, err := r.Read(ctx, scope, rootID)
		if err != nil {
			return llm.ExecutionResultV1{}, err
		}
		var progress map[string]json.RawMessage
		if json.Unmarshal(root.Progress, &progress) != nil {
			return llm.ExecutionResultV1{}, ErrCorrupt
		}
		active, err := r.decodeRequestAttempt(root, progress["request_attempt"])
		if err != nil {
			return llm.ExecutionResultV1{}, err
		}
		if active == nil || active.ID != attemptID || len(active.PriorCandidates)+1 < limit {
			return llm.ExecutionResultV1{}, contracts.ErrConflict
		}
		if root.Status == StatusFailed {
			// Replay the recorded terminal failure, including the last
			// attempt's error code and dispatch when it carried them.
			var saved llm.ExecutionResultV1
			if json.Unmarshal(progress["execution_failure"], &saved) != nil || progress["attempt_limit"] == nil ||
				saved.RequestID != string(rootID) || saved.Kind != root.Request.Kind || saved.State != llm.ExecutionFailed || saved.Retryable ||
				(saved.FailureCode != "provider_error" && saved.FailureCode != "provider_rejected") {
				return llm.ExecutionResultV1{}, contracts.ErrConflict
			}
			return saved, r.advanceIndex(ctx, root)
		}
		if root.Status != StatusRunning {
			return llm.ExecutionResultV1{}, contracts.ErrConflict
		}
		if _, err := r.verifyRequestAttempt(ctx, root, *active); err != nil {
			return llm.ExecutionResultV1{}, err
		}
		execution, err := r.LoadProviderExecution(ctx, scope, attemptID)
		if err != nil {
			return llm.ExecutionResultV1{}, err
		}
		if execution.Execution.Stage != ExecutionUnknown && !(execution.Execution.Stage == ExecutionFailed && execution.Execution.Settled && execution.Execution.Failure.Retryable) {
			return llm.ExecutionResultV1{}, contracts.ErrConflict
		}
		// The terminal failure reports the last attempt's facts, so callers can
		// tell a run of rate limits from an outage once attempts are exhausted.
		failure := llm.ExecutionResultV1{RequestID: string(rootID), Kind: root.Request.Kind, State: llm.ExecutionFailed, FailureCode: "provider_error"}
		if last := execution.Execution.Failure; last != nil && last.Code.Valid() && last.Dispatch.Valid() {
			failure.ErrorCode, failure.Dispatch = string(last.Code), string(last.Dispatch)
			if last.Dispatch == provider.DispatchRejected || last.Dispatch == provider.DispatchNotDispatched {
				failure.FailureCode = "provider_rejected"
			}
		}
		if err := r.retireRequestAttempt(ctx, scope, *active, now); err != nil {
			return llm.ExecutionResultV1{}, err
		}
		progress["execution_failure"], _ = json.Marshal(failure)
		progress["attempt_limit"], _ = json.Marshal(limit)
		data, err := json.Marshal(progress)
		if err != nil {
			return llm.ExecutionResultV1{}, ErrInvalid
		}
		at := now.UTC()
		if at.Before(root.UpdatedAt) {
			at = root.UpdatedAt
		}
		_, err = r.TryUpdate(ctx, scope, rootID, Update{ExpectedRevision: root.Revision, Token: "request-attempts-exhausted", Status: StatusFailed, Progress: data, UpdatedAt: at})
		if errors.Is(err, contracts.ErrConflict) {
			continue
		}
		return failure, err
	}
	return llm.ExecutionResultV1{}, contracts.ErrConflict
}
