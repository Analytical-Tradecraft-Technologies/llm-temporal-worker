package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// FinishRequestFailure removes a settled failed child from pending discovery.
// Retryable failures leave the root running so explicit acquisition can create
// a new child. Permanent failures close the root as well. All progress is kept
// for diagnosis; an older child can never fail a root that has already advanced.
func (r *Repository) FinishRequestFailure(ctx context.Context, scope Scope, rootID, attemptID RequestID, failure llm.ExecutionResultV1, now time.Time) error {
	if !validTime(now) || failure.Validate() != nil || failure.State != llm.ExecutionFailed || failure.RequestID != string(rootID) {
		return ErrInvalid
	}
	for tries := 0; tries < 16; tries++ {
		root, err := r.Read(ctx, scope, rootID)
		if err != nil {
			return err
		}
		var progress map[string]json.RawMessage
		if json.Unmarshal(root.Progress, &progress) != nil {
			return ErrCorrupt
		}
		active, err := r.decodeRequestAttempt(root, progress["request_attempt"])
		if err != nil {
			return err
		}
		if active == nil || active.ID != attemptID || failure.Kind != root.Request.Kind {
			return contracts.ErrConflict
		}
		if _, err := r.verifyRequestAttempt(ctx, root, *active); err != nil {
			return err
		}
		child, err := r.Read(ctx, scope, attemptID)
		if err != nil {
			return err
		}
		_, _, execution, err := executionProgress(child)
		if err != nil {
			return err
		}
		if execution == nil || !execution.Settled {
			return contracts.ErrConflict
		}
		switch execution.Stage {
		case ExecutionFailed:
			// The failure must report the saved attempt's facts. Records written
			// before error_code/dispatch existed carry neither.
			definite := execution.Failure.Dispatch == provider.DispatchRejected || execution.Failure.Dispatch == provider.DispatchNotDispatched
			wantCode := "provider_error"
			if definite && failure.ErrorCode != "" {
				wantCode = "provider_rejected"
			}
			if failure.FailureCode != wantCode || failure.Retryable != execution.Failure.Retryable {
				return ErrInvalid
			}
			if failure.ErrorCode != "" && (failure.ErrorCode != string(execution.Failure.Code) || failure.Dispatch != string(execution.Failure.Dispatch)) {
				return ErrInvalid
			}
		case ExecutionSucceeded:
			// A paid response the worker cannot publish: an invalid compaction
			// summary, or Generate output that cannot extend the transcript.
			if failure.FailureCode != "incomplete_response" || failure.Retryable {
				return ErrInvalid
			}
		default:
			return contracts.ErrConflict
		}
		if child.Status != StatusFailed {
			at := now.UTC()
			if at.Before(child.UpdatedAt) {
				at = child.UpdatedAt
			}
			_, err = r.TryUpdate(ctx, scope, attemptID, Update{ExpectedRevision: child.Revision, Token: "attempt-failed", Status: StatusFailed, Progress: child.Progress, UpdatedAt: at})
			if errors.Is(err, contracts.ErrConflict) {
				continue
			}
			if err != nil {
				return err
			}
		} else if err := r.advanceIndex(ctx, child); err != nil {
			return err
		}
		if failure.Retryable {
			return nil
		}
		encoded, _ := json.Marshal(failure)
		if root.Status == StatusFailed {
			var saved llm.ExecutionResultV1
			if json.Unmarshal(progress["execution_failure"], &saved) != nil || !equalExecutionJSON(saved, failure) {
				return contracts.ErrConflict
			}
			return r.advanceIndex(ctx, root)
		}
		if root.Status != StatusRunning {
			return contracts.ErrConflict
		}
		progress["execution_failure"] = encoded
		data, err := json.Marshal(progress)
		if err != nil {
			return ErrInvalid
		}
		at := now.UTC()
		if at.Before(root.UpdatedAt) {
			at = root.UpdatedAt
		}
		_, err = r.TryUpdate(ctx, scope, rootID, Update{ExpectedRevision: root.Revision, Token: "request-failed", Status: StatusFailed, Progress: data, UpdatedAt: at})
		if errors.Is(err, contracts.ErrConflict) {
			continue
		}
		return err
	}
	return contracts.ErrConflict
}
