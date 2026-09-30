package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

// ErrCheckpointFinalizationMissing means no publication plan has been saved.
// Missing referenced blobs/checkpoints are different errors and must stop replay.
var ErrCheckpointFinalizationMissing = fmt.Errorf("checkpoint finalization missing: %w", contracts.ErrNotFound)

// CheckpointFinalization is the immutable publication plan saved before any
// checkpoint metadata is published. Referenced content blobs must already have
// been written. It contains no permission to dispatch or acquire fresh budget.
type CheckpointFinalization struct {
	Checkpoint state.DurableCheckpoint `json:"checkpoint"`
	Handoff    FinalizationHandoff     `json:"handoff"`
}

// SaveCheckpointFinalization makes checkpoint publication and subsequent
// settlement recoverable. Identical retries repair the discovery index; a
// competing plan conflicts. No checkpoint, cache or budget mutation occurs here.
func (r *Repository) SaveCheckpointFinalization(ctx context.Context, scope Scope, id RequestID, plan CheckpointFinalization, now time.Time) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if !validTime(now) {
		return ErrInvalid
	}
	plan, err := normalizeCheckpointFinalization(plan)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 16; attempt++ {
		record, err := r.Read(ctx, scope, id)
		if err != nil {
			return err
		}
		if record.Status != StatusRunning {
			return contracts.ErrConflict
		}
		if err := r.verifyHandoffIdentity(record, plan.Checkpoint, plan.Handoff); err != nil {
			return err
		}
		progress, existingHandoff, err := handoffProgress(record.Progress)
		if err != nil {
			return err
		}
		if existingHandoff != nil {
			if !equalHandoff(*existingHandoff, plan.Handoff) {
				return contracts.ErrConflict
			}
			committed, err := r.Checkpoints().Get(ctx, plan.Checkpoint.ScopeID, plan.Checkpoint.ID)
			if err != nil {
				return err
			}
			actual, _ := committed.CanonicalDigest()
			expected, _ := plan.Checkpoint.CanonicalDigest()
			if actual != expected {
				return contracts.ErrConflict
			}
			return r.advanceIndex(ctx, record)
		}
		existing, err := checkpointFinalizationProgress(progress)
		if err != nil {
			return err
		}
		if existing != nil {
			if !equalCheckpointFinalization(*existing, plan) {
				return contracts.ErrConflict
			}
			return r.advanceIndex(ctx, record)
		}
		progress["checkpoint_finalization"], err = json.Marshal(plan)
		if err != nil {
			return ErrInvalid
		}
		data, err := json.Marshal(progress)
		if err != nil || len(data) > maxPayloadBytes {
			return ErrInvalid
		}
		if now.Before(record.UpdatedAt) {
			now = record.UpdatedAt
		}
		_, err = r.TryUpdate(ctx, scope, id, Update{ExpectedRevision: record.Revision, Token: "checkpoint-finalization", Status: StatusRunning, Progress: data, UpdatedAt: now})
		if errors.Is(err, contracts.ErrConflict) {
			continue
		}
		return err
	}
	return contracts.ErrConflict
}

func (r *Repository) LoadCheckpointFinalization(ctx context.Context, scope Scope, id RequestID) (CheckpointFinalization, error) {
	record, err := r.Read(ctx, scope, id)
	if err != nil {
		return CheckpointFinalization{}, err
	}
	if record.Status != StatusRunning {
		return CheckpointFinalization{}, contracts.ErrConflict
	}
	progress, _, err := handoffProgress(record.Progress)
	if err != nil {
		return CheckpointFinalization{}, err
	}
	plan, err := checkpointFinalizationProgress(progress)
	if err != nil {
		return CheckpointFinalization{}, err
	}
	if plan == nil {
		return CheckpointFinalization{}, ErrCheckpointFinalizationMissing
	}
	if err := r.verifyHandoffIdentity(record, plan.Checkpoint, plan.Handoff); err != nil {
		return CheckpointFinalization{}, err
	}
	return *plan, nil
}

// ResumeCheckpointFinalization publishes the saved checkpoint, then attaches
// its readable-checkpoint handoff. Retry identical publication after uncertain
// writes. This method never settles Redis or returns a terminal operation.
func (r *Repository) ResumeCheckpointFinalization(ctx context.Context, scope Scope, id RequestID, now time.Time) (FinalizationHandoff, error) {
	if !validTime(now) {
		return FinalizationHandoff{}, ErrInvalid
	}
	plan, err := r.LoadCheckpointFinalization(ctx, scope, id)
	if errors.Is(err, ErrCheckpointFinalizationMissing) {
		return r.LoadFinalizationHandoff(ctx, scope, id)
	}
	if err != nil {
		return FinalizationHandoff{}, err
	}
	err = state.WithCheckpointUnitOfWork(ctx, r.Checkpoints(), func(ctx context.Context, unit state.CheckpointUnitOfWork) error {
		return unit.PutCheckpoint(ctx, state.CheckpointWrite{Checkpoint: plan.Checkpoint})
	})
	if err != nil {
		return FinalizationHandoff{}, err
	}
	if err := r.SaveFinalizationHandoff(ctx, scope, id, plan.Handoff, now); err != nil {
		return FinalizationHandoff{}, err
	}
	return plan.Handoff, nil
}

func normalizeCheckpointFinalization(plan CheckpointFinalization) (CheckpointFinalization, error) {
	if validateCloudCheckpoint(plan.Checkpoint) != nil {
		return CheckpointFinalization{}, ErrInvalid
	}
	handoff, err := normalizeHandoff(plan.Handoff)
	if err != nil {
		return CheckpointFinalization{}, err
	}
	if plan.Checkpoint.ID != handoff.CheckpointID || plan.Checkpoint.ScopeID != handoff.CheckpointScope || plan.Checkpoint.OriginOperationID != handoff.OperationID {
		return CheckpointFinalization{}, ErrInvalid
	}
	plan.Handoff = handoff
	return plan, nil
}

func checkpointFinalizationProgress(progress map[string]json.RawMessage) (*CheckpointFinalization, error) {
	data, ok := progress["checkpoint_finalization"]
	if !ok {
		return nil, nil
	}
	var plan CheckpointFinalization
	if json.Unmarshal(data, &plan) != nil {
		return nil, ErrCorrupt
	}
	normalized, err := normalizeCheckpointFinalization(plan)
	if err != nil {
		return nil, ErrCorrupt
	}
	return &normalized, nil
}

func equalCheckpointFinalization(left, right CheckpointFinalization) bool {
	l, le := left.Checkpoint.CanonicalDigest()
	r, re := right.Checkpoint.CanonicalDigest()
	return le == nil && re == nil && l == r && equalHandoff(left.Handoff, right.Handoff)
}
