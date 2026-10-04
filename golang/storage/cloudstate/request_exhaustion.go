package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/llm"
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
		failure := llm.ExecutionResultV1{RequestID: string(rootID), Kind: root.Request.Kind, State: llm.ExecutionFailed, FailureCode: "provider_error"}
		if root.Status == StatusFailed {
			var saved llm.ExecutionResultV1
			if json.Unmarshal(progress["execution_failure"], &saved) != nil || !equalExecutionJSON(saved, failure) || progress["attempt_limit"] == nil {
				return llm.ExecutionResultV1{}, contracts.ErrConflict
			}
			return failure, r.advanceIndex(ctx, root)
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
