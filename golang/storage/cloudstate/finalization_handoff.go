package cloudstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

// FinalizationHandoff is an encrypted, operation-scoped replay instruction.
// Payload is a versioned application object containing the exact response and
// receipt inputs needed to finish either provider or cache-hit reconciliation.
// It is not an authorization to submit paid work again.
type FinalizationHandoff struct {
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
		if existing != nil {
			if !equalHandoff(*existing, handoff) {
				return contracts.ErrConflict
			}
			return r.advanceIndex(ctx, record)
		}
		encoded, _ := json.Marshal(handoff)
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
		return FinalizationHandoff{}, contracts.ErrNotFound
	}
	if err := r.verifyHandoffCheckpoint(ctx, record, *handoff); err != nil {
		return FinalizationHandoff{}, err
	}
	return *handoff, nil
}

func normalizeHandoff(handoff FinalizationHandoff) (FinalizationHandoff, error) {
	if handoff.Mode != "provider" && handoff.Mode != "cache" || !safeText(handoff.CheckpointScope, 512) || !safeText(string(handoff.CheckpointID), 256) {
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
	return left.Mode == right.Mode && left.CheckpointScope == right.CheckpointScope && left.CheckpointID == right.CheckpointID && bytes.Equal(left.Payload, right.Payload)
}

func (r *Repository) verifyHandoffCheckpoint(ctx context.Context, record Record, handoff FinalizationHandoff) error {
	checkpoint, err := r.Checkpoints().Get(ctx, handoff.CheckpointScope, handoff.CheckpointID)
	if err != nil {
		return err
	}
	var manifest struct {
		OperationKey string `json:"operation_key"`
	}
	if json.Unmarshal(record.Request.Manifest, &manifest) != nil || manifest.OperationKey == "" ||
		r.operationID(record.Request.Scope, record.Request.Kind, manifest.OperationKey) != record.Request.ID ||
		string(checkpoint.OriginOperationID) != manifest.OperationKey {
		return contracts.ErrConflict
	}
	expected := state.CheckpointCacheReplay
	if handoff.Mode == "provider" {
		expected = state.CheckpointGeneration
		if record.Request.Kind == "compact" {
			expected = state.CheckpointCompaction
		}
	}
	if checkpoint.Kind != expected {
		return contracts.ErrConflict
	}
	return nil
}
