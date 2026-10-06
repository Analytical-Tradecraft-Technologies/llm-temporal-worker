package cloudstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

// ErrFinalizationHandoffMissing distinguishes an absent instruction from a
// missing request/checkpoint. Both are storage not-found errors, but only this
// sentinel permits the runtime to consult its earlier-phase recovery path.
var ErrFinalizationHandoffMissing = fmt.Errorf("finalization handoff missing: %w", contracts.ErrNotFound)

// FinalizationHandoff is an encrypted, operation-scoped replay instruction.
// Payload is a versioned application object containing the exact response and
// receipt inputs needed to finish either provider or cache-hit reconciliation.
// It is not an authorization to submit paid work again.
// OperationID is the internal checkpoint owner, distinct from the caller key.
type FinalizationHandoff struct {
	OperationID     state.OperationID  `json:"operation_id"`
	Mode            string             `json:"mode"`
	CheckpointScope string             `json:"checkpoint_scope"`
	CheckpointID    state.CheckpointID `json:"checkpoint_id"`
	Payload         json.RawMessage    `json:"payload"`
}

// SaveFinalizationHandoff attaches the immutable handoff to a running request
// only after its checkpoint is readable. The same value may be retried after
// an uncertain write; a different value for the operation is a conflict.
func (r *Repository) SaveFinalizationHandoff(ctx context.Context, scope Scope, id RequestID, handoff FinalizationHandoff, now time.Time) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if !validTime(now) {
		return ErrInvalid
	}
	var err error
	if handoff, err = normalizeHandoff(handoff); err != nil {
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
		if err := r.verifyHandoffCheckpoint(ctx, record, handoff); err != nil {
			return err
		}
		progress, existing, err := handoffProgress(record.Progress)
		if err != nil {
			return err
		}
		plan, err := checkpointFinalizationProgress(progress)
		if err != nil {
			return err
		}
		if plan != nil {
			if !equalHandoff(plan.Handoff, handoff) {
				return contracts.ErrConflict
			}
			committed, err := r.Checkpoints().Get(ctx, plan.Checkpoint.ScopeID, plan.Checkpoint.ID)
			if err != nil {
				return err
			}
			actual, actualErr := committed.CanonicalDigest()
			expected, expectedErr := plan.Checkpoint.CanonicalDigest()
			if actualErr != nil || expectedErr != nil || actual != expected {
				return contracts.ErrConflict
			}
		}
		if existing != nil {
			if !equalHandoff(*existing, handoff) {
				return contracts.ErrConflict
			}
			return r.advanceIndex(ctx, record)
		}
		encoded, _ := json.Marshal(handoff)
		// The handoff replaces its now-committed publication plan, avoiding
		// duplicate large response payloads in the current operation view.
		delete(progress, "checkpoint_finalization")
		progress["finalization_handoff"] = encoded
		data, err := json.Marshal(progress)
		if err != nil || len(data) > maxPayloadBytes {
			return ErrInvalid
		}
		if now.Before(record.UpdatedAt) {
			now = record.UpdatedAt
		}
		_, err = r.TryUpdate(ctx, scope, id, Update{ExpectedRevision: record.Revision, Token: "finalization-handoff", Status: StatusRunning, Progress: data, UpdatedAt: now})
		if errors.Is(err, contracts.ErrConflict) {
			continue
		}
		return err
	}
	return contracts.ErrConflict
}

// LoadFinalizationHandoff returns an already committed handoff without
// dispatching or acquiring budget. A missing handoff is not a cache miss:
// callers must recover earlier phases before they can resume finalization.
func (r *Repository) LoadFinalizationHandoff(ctx context.Context, scope Scope, id RequestID) (FinalizationHandoff, error) {
	record, err := r.Read(ctx, scope, id)
	if err != nil {
		return FinalizationHandoff{}, err
	}
	if record.Status != StatusRunning {
		return FinalizationHandoff{}, contracts.ErrConflict
	}
	_, handoff, err := handoffProgress(record.Progress)
	if err != nil {
		return FinalizationHandoff{}, err
	}
	if handoff == nil {
		return FinalizationHandoff{}, ErrFinalizationHandoffMissing
	}
	if err := r.verifyHandoffCheckpoint(ctx, record, *handoff); err != nil {
		return FinalizationHandoff{}, err
	}
	return *handoff, nil
}

func normalizeHandoff(handoff FinalizationHandoff) (FinalizationHandoff, error) {
	if !safeText(string(handoff.OperationID), 128) || handoff.Mode != "provider" && handoff.Mode != "cache" && handoff.Mode != "no_work" || !safeText(handoff.CheckpointScope, 512) || !safeText(string(handoff.CheckpointID), 256) {
		return FinalizationHandoff{}, ErrInvalid
	}
	data, err := objectJSON(handoff.Payload)
	if err != nil {
		return FinalizationHandoff{}, err
	}
	var payload struct {
		Version int `json:"version"`
	}
	if json.Unmarshal(data, &payload) != nil || payload.Version != 1 {
		return FinalizationHandoff{}, ErrInvalid
	}
	handoff.Payload = data
	return handoff, nil
}

func handoffProgress(data json.RawMessage) (map[string]json.RawMessage, *FinalizationHandoff, error) {
	var progress map[string]json.RawMessage
	if json.Unmarshal(data, &progress) != nil || progress == nil || string(progress["version"]) != "1" {
		return nil, nil, ErrCorrupt
	}
	encoded, exists := progress["finalization_handoff"]
	if !exists {
		return progress, nil, nil
	}
	var handoff FinalizationHandoff
	if json.Unmarshal(encoded, &handoff) != nil {
		return nil, nil, ErrCorrupt
	}
	handoff, err := normalizeHandoff(handoff)
	if err != nil {
		return nil, nil, ErrCorrupt
	}
	return progress, &handoff, nil
}

func equalHandoff(left, right FinalizationHandoff) bool {
	return left.OperationID == right.OperationID && left.Mode == right.Mode && left.CheckpointScope == right.CheckpointScope && left.CheckpointID == right.CheckpointID && bytes.Equal(left.Payload, right.Payload)
}

func (r *Repository) verifyHandoffCheckpoint(ctx context.Context, record Record, handoff FinalizationHandoff) error {
	checkpoint, err := r.Checkpoints().Get(ctx, handoff.CheckpointScope, handoff.CheckpointID)
	if err != nil {
		return err
	}
	return r.verifyHandoffIdentity(record, checkpoint, handoff)
}

// verifyHandoffIdentity also validates plans whose checkpoint is not yet published.
func (r *Repository) verifyHandoffIdentity(record Record, checkpoint state.DurableCheckpoint, handoff FinalizationHandoff) error {
	var manifest struct {
		OperationKey string `json:"operation_key"`
	}
	if json.Unmarshal(record.Request.Manifest, &manifest) != nil || manifest.OperationKey == "" ||
		r.operationID(record.Request.Scope, record.Request.Kind, manifest.OperationKey) != record.Request.ID ||
		checkpoint.OriginOperationID != handoff.OperationID {
		return contracts.ErrConflict
	}
	expected := state.CheckpointCacheReplay
	if handoff.Mode == "no_work" {
		if record.Request.Kind != "compact" || checkpoint.OriginCacheEntryID != nil {
			return contracts.ErrConflict
		}
		expected = state.CheckpointCompaction
	}
	if handoff.Mode == "provider" {
		expected = state.CheckpointGeneration
	}
	// Compaction cache consumers retain the compaction kind. OriginCacheEntryID
	// records cache provenance; the response-cache receipt verifies that binding.
	if record.Request.Kind == "compact" {
		expected = state.CheckpointCompaction
	}
	if checkpoint.Kind != expected {
		return contracts.ErrConflict
	}
	return nil
}
