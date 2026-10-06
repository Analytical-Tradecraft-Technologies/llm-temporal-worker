package cloudstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

// ErrRequestPreparationMissing permits materialization only before admission.
// Missing/corrupt referenced storage is never a preparation cache miss.
var ErrRequestPreparationMissing = fmt.Errorf("request preparation missing: %w", contracts.ErrNotFound)

const MaxPreparedParentBytes = 4 << 20

// MaxExtendableParentBytes bounds a Generate's parent plus its appended input,
// measured as the encoded snapshot of that transcript. The rest of
// MaxPreparedParentBytes is headroom for the turn's output, so the child it
// publishes can still be prepared as a parent, at least for compaction.
const MaxExtendableParentBytes = MaxPreparedParentBytes - MaxPreparedParentBytes/4

// RequestPreparation retains the authorized parent independently of checkpoint
// expiry. The original public input remains in the immutable request manifest.
// Snapshot is a versioned CheckpointBlobCodec snapshot, not JSON encoding of
// interface-valued Go structs. All fields are encrypted by the request store.
type RequestPreparation struct {
	Version         int             `json:"version"`
	ConfigDigest    [32]byte        `json:"config_digest"`
	CheckpointScope string          `json:"checkpoint_scope"`
	ParentSnapshot  json.RawMessage `json:"parent_snapshot,omitempty"`
	PreparedAt      time.Time       `json:"prepared_at"`
}

func (p RequestPreparation) Validate() error {
	_, err := p.ValidateParent()
	return err
}

// ValidateParent validates p exactly as Validate does and also returns the
// parent snapshot that validation decoded, or nil when p has no parent. A
// caller that needs the snapshot uses it instead of decoding the parent again
// (#1112). Each call decodes afresh, so the caller owns the result.
func (p RequestPreparation) ValidateParent() (*state.CheckpointSnapshot, error) {
	if p.Version != 1 || p.ConfigDigest == ([32]byte{}) || !safeText(p.CheckpointScope, 512) || !validTime(p.PreparedAt) {
		return nil, ErrInvalid
	}
	if len(p.ParentSnapshot) == 0 {
		return nil, nil
	}
	codec := state.CheckpointBlobCodec{MaxBytes: MaxPreparedParentBytes}
	snapshot, err := codec.DecodeSnapshot(p.ParentSnapshot)
	if err != nil {
		return nil, ErrInvalid
	}
	return &snapshot, nil
}

// SaveRequestPreparation has one immutable winner and preserves other progress.
// It must precede budget planning/acceptance and provider submission. An identical
// retry after an uncertain write repairs discovery without rematerializing input.
func (r *Repository) SaveRequestPreparation(ctx context.Context, scope Scope, id RequestID, preparation RequestPreparation) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if preparation.Validate() != nil {
		return ErrInvalid
	}
	preparation.PreparedAt = preparation.PreparedAt.UTC()
	encoded, _ := json.Marshal(preparation)
	encoded, err := objectJSON(encoded)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 16; attempt++ {
		record, err := r.Read(ctx, scope, id)
		if err != nil {
			return err
		}
		progress, existing, _, err := requestPreparationProgress(record)
		if err != nil {
			return err
		}
		if existing != nil {
			previous, _ := json.Marshal(existing)
			previous, _ = objectJSON(previous)
			if !bytes.Equal(previous, encoded) {
				return contracts.ErrConflict
			}
			return r.repairBudgetPlanIndex(ctx, record)
		}
		if record.Status != StatusRunning || preparation.PreparedAt.Before(record.Request.CreatedAt) || preparationAlreadyStarted(progress) {
			return contracts.ErrConflict
		}
		progress["request_preparation"] = encoded
		data, err := json.Marshal(progress)
		if err != nil || len(data) > maxPayloadBytes {
			return ErrInvalid
		}
		now := preparation.PreparedAt
		if now.Before(record.UpdatedAt) {
			now = record.UpdatedAt
		}
		_, err = r.TryUpdate(ctx, scope, id, Update{ExpectedRevision: record.Revision, Token: "request-preparation", Status: StatusRunning, Progress: data, UpdatedAt: now})
		if errors.Is(err, contracts.ErrConflict) {
			continue
		}
		return err
	}
	return contracts.ErrConflict
}

// LoadRequestPreparation never reopens or revalidates expiry of the parent
// checkpoint. Callers must authorize the request scope before using this method.
func (r *Repository) LoadRequestPreparation(ctx context.Context, scope Scope, id RequestID) (RequestPreparation, error) {
	preparation, _, err := r.LoadRequestPreparationParent(ctx, scope, id)
	return preparation, err
}

// LoadRequestPreparationParent is LoadRequestPreparation that also returns the
// parent snapshot decoded while validating the loaded preparation (nil when it
// has no parent), so a caller restoring the parent does not decode it again.
// The snapshot is decoded for this call only and belongs to the caller.
func (r *Repository) LoadRequestPreparationParent(ctx context.Context, scope Scope, id RequestID) (RequestPreparation, *state.CheckpointSnapshot, error) {
	record, err := r.Read(ctx, scope, id)
	if err != nil {
		return RequestPreparation{}, nil, err
	}
	progress, preparation, parent, err := requestPreparationProgress(record)
	if err != nil {
		return RequestPreparation{}, nil, err
	}
	if preparation == nil {
		if record.Status != StatusRunning || preparationAlreadyStarted(progress) {
			return RequestPreparation{}, nil, ErrCorrupt
		}
		return RequestPreparation{}, nil, ErrRequestPreparationMissing
	}
	if err := r.repairBudgetPlanIndex(ctx, record); err != nil {
		return RequestPreparation{}, nil, err
	}
	return *preparation, parent, nil
}

func preparationAlreadyStarted(progress map[string]json.RawMessage) bool {
	for _, key := range []string{"budget_plan", "provider_execution", "checkpoint_finalization", "finalization_handoff"} {
		if _, exists := progress[key]; exists {
			return true
		}
	}
	return false
}

// requestPreparationProgress also returns the parent snapshot that validating
// the preparation decoded (nil when there is no preparation or no parent).
func requestPreparationProgress(record Record) (map[string]json.RawMessage, *RequestPreparation, *state.CheckpointSnapshot, error) {
	var progress map[string]json.RawMessage
	if json.Unmarshal(record.Progress, &progress) != nil || progress == nil || string(progress["version"]) != "1" {
		return nil, nil, nil, ErrCorrupt
	}
	data, present := progress["request_preparation"]
	if !present {
		return progress, nil, nil, nil
	}
	var preparation RequestPreparation
	if json.Unmarshal(data, &preparation) != nil {
		return nil, nil, nil, ErrCorrupt
	}
	parent, err := preparation.ValidateParent()
	if err != nil || preparation.PreparedAt.Before(record.Request.CreatedAt) {
		return nil, nil, nil, ErrCorrupt
	}
	return progress, &preparation, parent, nil
}
