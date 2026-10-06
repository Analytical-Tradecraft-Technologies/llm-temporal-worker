package cloudstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

var ErrRequestAttemptMissing = errors.New("request attempt missing")

// RequestAttempt identifies a separate discoverable execution record. Unknown
// paid attempts retain their own pending index even when a later attempt wins.
// Neither this reference nor allocating it authorizes a provider submission.
type RequestAttempt struct {
	// PriorCandidates records one entry per retired provider execution, including
	// unknown outcomes. Unused quote renewals do not consume a provider attempt.
	PriorCandidates []string  `json:"prior_candidates,omitempty"`
	Version         int       `json:"version"`
	RootID          RequestID `json:"root_id"`
	ID              RequestID `json:"id"`
	PreviousID      RequestID `json:"previous_id,omitempty"`
	Number          uint64    `json:"number"`
	CreatedAt       time.Time `json:"created_at"`
}

func (a RequestAttempt) Validate() error {
	if a.Version != 1 || !a.RootID.valid() || !a.ID.valid() || a.RootID == a.ID || a.Number == 0 || a.Number > maxRevisions || !validTime(a.CreatedAt) ||
		(a.Number == 1 && a.PreviousID != "") || (a.Number > 1 && (!a.PreviousID.valid() || a.PreviousID == a.ID || a.PreviousID == a.RootID)) {
		return ErrInvalid
	}
	if uint64(len(a.PriorCandidates)) >= a.Number {
		return ErrInvalid
	}
	for _, candidate := range a.PriorCandidates {
		if !safeText(candidate, 512) {
			return ErrInvalid
		}
	}
	return nil
}

type attemptRetirement struct {
	Version   int       `json:"version"`
	RootID    RequestID `json:"root_id"`
	Reason    string    `json:"reason"`
	RetiredAt time.Time `json:"retired_at"`
}

// BeginRequestAttempt creates the first attempt when previous is empty, or
// replaces exactly previous after its unused quote expires or its paid outcome
// becomes unknown, or after a settled retryable failure. Retrying an uncertain write returns the same winning child.
// The original request and materialized preparation never change. Child
// discovery is committed before the root references it and before admission.
func (r *Repository) BeginRequestAttempt(ctx context.Context, scope Scope, rootID, previous RequestID, now time.Time) (RequestAttempt, error) {
	if err := validContext(ctx); err != nil {
		return RequestAttempt{}, err
	}
	if !validTime(now) || (previous != "" && !previous.valid()) {
		return RequestAttempt{}, ErrInvalid
	}
	for tries := 0; tries < 16; tries++ {
		root, err := r.Read(ctx, scope, rootID)
		if err != nil {
			return RequestAttempt{}, err
		}
		progress, preparation, _, err := requestPreparationProgress(root)
		if err != nil {
			return RequestAttempt{}, err
		}
		if root.Status != StatusRunning || preparation == nil ||
			progress["attempt_parent"] != nil || preparationAlreadyStarted(progress) {
			return RequestAttempt{}, contracts.ErrConflict
		}
		active, err := r.decodeRequestAttempt(root, progress["request_attempt"])
		if err != nil {
			return RequestAttempt{}, err
		}
		if active != nil && active.PreviousID == previous {
			if err := r.repairBudgetPlanIndex(ctx, root); err != nil {
				return RequestAttempt{}, err
			}
			return r.verifyRequestAttempt(ctx, root, *active)
		}
		// A worker whose clock lags the one that last wrote the root is late,
		// not conflicting: its child begins no earlier than the record it extends.
		if now.Before(root.UpdatedAt) {
			now = root.UpdatedAt
		}
		number := uint64(1)
		var priorCandidates []string
		if previous == "" {
			if active != nil {
				return RequestAttempt{}, contracts.ErrConflict
			}
		} else {
			if active == nil || active.ID != previous {
				return RequestAttempt{}, contracts.ErrConflict
			}
			if _, err := r.verifyRequestAttempt(ctx, root, *active); err != nil {
				return RequestAttempt{}, err
			}
			if err := r.retireRequestAttempt(ctx, scope, *active, now); err != nil {
				return RequestAttempt{}, err
			}
			priorCandidates = append([]string(nil), active.PriorCandidates...)
			previousExecution, loadErr := r.LoadProviderExecution(ctx, scope, active.ID)
			if loadErr == nil {
				priorCandidates = append(priorCandidates, previousExecution.Plan.Estimate.CandidateID)
			} else if !errors.Is(loadErr, ErrProviderExecutionMissing) && !errors.Is(loadErr, ErrBudgetPlanMissing) {
				return RequestAttempt{}, loadErr
			}
			number = active.Number + 1
		}
		if number > maxRevisions {
			return RequestAttempt{}, ErrInvalid
		}
		proposal := RequestAttempt{Version: 1, RootID: rootID, ID: r.requestAttemptID(root, number), PreviousID: previous, Number: number, PriorCandidates: priorCandidates}
		child, err := r.createRequestAttempt(ctx, root, *preparation, proposal, now)
		if errors.Is(err, contracts.ErrConflict) {
			continue
		}
		if err != nil {
			return RequestAttempt{}, err
		}
		proposal.CreatedAt = child.Request.CreatedAt
		encoded, _ := json.Marshal(proposal)
		progress["request_attempt"] = encoded
		data, err := json.Marshal(progress)
		if err != nil || len(data) > maxPayloadBytes {
			return RequestAttempt{}, ErrInvalid
		}
		updatedAt := now.UTC()
		if updatedAt.Before(child.UpdatedAt) {
			updatedAt = child.UpdatedAt
		}
		_, err = r.TryUpdate(ctx, scope, rootID, Update{ExpectedRevision: root.Revision, Token: "request-attempt-" + string(proposal.ID), Status: StatusRunning, Progress: data, UpdatedAt: updatedAt})
		if errors.Is(err, contracts.ErrConflict) {
			continue
		}
		if err != nil {
			return RequestAttempt{}, err
		}
		return proposal, nil
	}
	return RequestAttempt{}, contracts.ErrConflict
}

// LoadRequestAttempt reads only the active child. Earlier unknown children
// remain independently enumerable through the normal pending discovery API.
func (r *Repository) LoadRequestAttempt(ctx context.Context, scope Scope, rootID RequestID) (RequestAttempt, error) {
	root, err := r.Read(ctx, scope, rootID)
	if err != nil {
		return RequestAttempt{}, err
	}
	var progress map[string]json.RawMessage
	if json.Unmarshal(root.Progress, &progress) != nil || progress == nil {
		return RequestAttempt{}, ErrCorrupt
	}
	active, err := r.decodeRequestAttempt(root, progress["request_attempt"])
	if err != nil {
		return RequestAttempt{}, err
	}
	if active == nil {
		return RequestAttempt{}, ErrRequestAttemptMissing
	}
	if err := r.repairBudgetPlanIndex(ctx, root); err != nil {
		return RequestAttempt{}, err
	}
	return r.verifyRequestAttempt(ctx, root, *active)
}

func (r *Repository) requestAttemptID(root Record, number uint64) RequestID {
	data, _ := json.Marshal(struct {
		Namespace string
		Root      RequestID
		Number    uint64
	}{r.namespace, root.Request.ID, number})
	raw := derive(r.secret, "request-attempt-id", data)
	raw[6], raw[8] = raw[6]&0x0f|0x80, raw[8]&0x3f|0x80
	id, _ := uuid.FromBytes(raw[:16])
	return RequestID(RequestIDPrefix + id.String())
}

func (r *Repository) decodeRequestAttempt(root Record, data json.RawMessage) (*RequestAttempt, error) {
	if data == nil {
		return nil, nil
	}
	var value RequestAttempt
	if json.Unmarshal(data, &value) != nil || value.Validate() != nil || value.RootID != root.Request.ID ||
		value.ID != r.requestAttemptID(root, value.Number) || value.CreatedAt.Before(root.Request.CreatedAt) ||
		(value.Number == 1 && value.PreviousID != "") || (value.Number > 1 && value.PreviousID != r.requestAttemptID(root, value.Number-1)) {
		return nil, ErrCorrupt
	}
	return &value, nil
}

func (r *Repository) verifyRequestAttempt(ctx context.Context, root Record, active RequestAttempt) (RequestAttempt, error) {
	child, err := r.Read(ctx, root.Request.Scope, active.ID)
	if err != nil {
		return RequestAttempt{}, err
	}
	var progress map[string]json.RawMessage
	var link RequestAttempt
	if child.Request.Kind != root.Request.Kind || child.Request.RequestIndex != root.Request.RequestIndex ||
		!bytes.Equal(child.Request.Manifest, root.Request.Manifest) || !child.Request.CreatedAt.Equal(active.CreatedAt) ||
		json.Unmarshal(child.Progress, &progress) != nil || json.Unmarshal(progress["attempt_parent"], &link) != nil || !equalExecutionJSON(link, active) {
		return RequestAttempt{}, ErrCorrupt
	}
	if err := r.repairBudgetPlanIndex(ctx, child); err != nil {
		return RequestAttempt{}, err
	}
	return active, nil
}

func (r *Repository) createRequestAttempt(ctx context.Context, root Record, preparation RequestPreparation, link RequestAttempt, now time.Time) (Record, error) {
	request := root.Request
	request.ID, request.CreatedAt = link.ID, now.UTC()
	// An interrupted initializer may have only its discovery row. Reuse its
	// timestamp so different workers can finish the same immutable child.
	indexRow, err := r.table.Get(ctx, r.indexKey(link.ID))
	if err == nil {
		index, decodeErr := r.decodeIndex(indexRow)
		if decodeErr != nil {
			return Record{}, decodeErr
		}
		if index.ScopeTag != r.scopeTag(request.Scope) {
			return Record{}, ErrCorrupt
		}
		existing, readErr := r.Read(ctx, request.Scope, request.ID)
		if readErr == nil {
			request.CreatedAt = existing.Request.CreatedAt
		} else if index.Revision == 0 && errors.Is(readErr, contracts.ErrNotFound) {
			request.CreatedAt = index.UpdatedAt
		} else {
			return Record{}, readErr
		}
	} else if !errors.Is(err, contracts.ErrNotFound) {
		return Record{}, err
	}
	existing, err := r.Read(ctx, request.Scope, request.ID)
	if errors.Is(err, contracts.ErrNotFound) {
		existing, err = r.Create(ctx, request)
	}
	if err != nil {
		return Record{}, err
	}
	if existing.Request.Scope != request.Scope || existing.Request.Kind != request.Kind || existing.Request.RequestIndex != request.RequestIndex ||
		!bytes.Equal(existing.Request.Manifest, request.Manifest) {
		return Record{}, ErrCorrupt
	}
	link.CreatedAt = existing.Request.CreatedAt
	if existing.Status != StatusPending {
		_, err := r.verifyRequestAttempt(ctx, root, link)
		return existing, err
	}
	preparation.PreparedAt = link.CreatedAt
	progress, _ := json.Marshal(struct {
		Version     int                `json:"version"`
		Preparation RequestPreparation `json:"request_preparation"`
		Parent      RequestAttempt     `json:"attempt_parent"`
	}{1, preparation, link})
	return r.TryUpdate(ctx, request.Scope, request.ID, Update{ExpectedRevision: existing.Revision, Token: "attempt-initialize", Status: StatusRunning, Progress: progress, UpdatedAt: link.CreatedAt})
}

func (r *Repository) retireRequestAttempt(ctx context.Context, scope Scope, active RequestAttempt, now time.Time) error {
	for tries := 0; tries < 16; tries++ {
		record, err := r.Read(ctx, scope, active.ID)
		if err != nil {
			return err
		}
		progress, plan, execution, err := executionProgress(record)
		if errors.Is(err, ErrBudgetPlanMissing) {
			progress, plan, err = budgetPlanProgress(record.Progress)
			if progress["provider_execution"] != nil {
				return ErrCorrupt
			}
		}
		if err != nil {
			return err
		}
		if data := progress["attempt_retired"]; data != nil {
			var retired attemptRetirement
			if json.Unmarshal(data, &retired) != nil || retired.Version != 1 || retired.RootID != active.RootID || !validTime(retired.RetiredAt) ||
				(retired.Reason != "unused_quote_expired" && retired.Reason != "outcome_unknown") {
				return ErrCorrupt
			}
			return r.repairBudgetPlanIndex(ctx, record)
		}
		// Failed children are immutable. Settlement and retry classification must
		// already have been saved before allocating a replacement reservation.
		if execution != nil && execution.Stage == ExecutionFailed && execution.Settled && execution.Failure.Retryable && !now.Before(execution.Failure.RetryNotBefore) && record.Status == StatusFailed {
			return r.repairBudgetPlanIndex(ctx, record)
		}
		if executionFinalizing(progress) {
			return contracts.ErrConflict
		}
		if now.Before(record.UpdatedAt) {
			now = record.UpdatedAt
		}
		reason, status := "", record.Status
		expires := active.CreatedAt.Add(durable.BudgetStartLease)
		if plan != nil {
			expires = plan.Reservation.ExpiresAt
		}
		if execution == nil && record.Status == StatusRunning && !now.Before(expires) {
			reason, status = "unused_quote_expired", StatusFailed
		} else if execution != nil && execution.Stage == ExecutionUnknown && record.Status == StatusOutcomeUnknown && !now.Before(execution.RecoverAfter) {
			reason = "outcome_unknown"
		} else {
			return contracts.ErrConflict
		}
		progress["attempt_retired"], _ = json.Marshal(attemptRetirement{1, active.RootID, reason, now.UTC()})
		data, err := json.Marshal(progress)
		if err != nil || len(data) > maxPayloadBytes {
			return ErrInvalid
		}
		_, err = r.TryUpdate(ctx, scope, active.ID, Update{ExpectedRevision: record.Revision, Token: fmt.Sprintf("retire-attempt-%d", active.Number), Status: status, Progress: data, UpdatedAt: now.UTC()})
		if errors.Is(err, contracts.ErrConflict) {
			continue
		}
		return err
	}
	return contracts.ErrConflict
}

// completeRequestAttempt removes the current finished child from recovery before
// the root drops its execution metadata. The committed handoff makes this
// recoverable: retries repair the child index before completing the root.
// Older unknown paid children remain discoverable.
func (r *Repository) completeRequestAttempt(ctx context.Context, root Record, now time.Time) error {
	progress, handoff, err := handoffProgress(root.Progress)
	if err != nil {
		return err
	}
	if handoff == nil {
		return nil
	}
	if err := r.verifyHandoffCheckpoint(ctx, root, *handoff); err != nil {
		return err
	}
	active, err := r.decodeRequestAttempt(root, progress["request_attempt"])
	if err != nil || active == nil {
		return err
	}
	if _, err := r.verifyRequestAttempt(ctx, root, *active); err != nil {
		return err
	}
	for tries := 0; tries < 16; tries++ {
		child, err := r.Read(ctx, root.Request.Scope, active.ID)
		if err != nil {
			return err
		}
		var childProgress map[string]json.RawMessage
		if json.Unmarshal(child.Progress, &childProgress) != nil || childProgress == nil {
			return ErrCorrupt
		}
		if handoff.Mode == "provider" {
			_, _, execution, err := executionProgress(child)
			if err != nil {
				return err
			}
			if execution == nil || execution.Stage != ExecutionSucceeded || !execution.Settled {
				return contracts.ErrConflict
			}
		} else if childProgress["provider_execution"] != nil {
			// A cache hit or no-work checkpoint cannot erase a paid attempt.
			return contracts.ErrConflict
		}
		var completedRoot RequestID
		if data := childProgress["attempt_completed"]; data != nil {
			if json.Unmarshal(data, &completedRoot) != nil || completedRoot != root.Request.ID || child.Status != StatusCompleted {
				return ErrCorrupt
			}
			return r.advanceIndex(ctx, child)
		}
		if child.Status != StatusRunning || executionFinalizing(childProgress) {
			return contracts.ErrConflict
		}
		childProgress["attempt_completed"], _ = json.Marshal(root.Request.ID)
		data, err := json.Marshal(childProgress)
		if err != nil || len(data) > maxPayloadBytes {
			return ErrInvalid
		}
		if now.Before(child.UpdatedAt) {
			now = child.UpdatedAt
		}
		_, err = r.TryUpdate(ctx, root.Request.Scope, active.ID, Update{ExpectedRevision: child.Revision, Token: "attempt-complete", Status: StatusCompleted, Progress: data, UpdatedAt: now})
		if errors.Is(err, contracts.ErrConflict) {
			continue
		}
		return err
	}
	return contracts.ErrConflict
}
