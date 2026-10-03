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
	if p.Version != 1 || p.ConfigDigest == ([32]byte{}) || !safeText(p.CheckpointScope, 512) || !validTime(p.PreparedAt) {
		return ErrInvalid
	}
	if len(p.ParentSnapshot) != 0 {
		codec := state.CheckpointBlobCodec{MaxBytes: MaxPreparedParentBytes}
		if _, err := codec.DecodeSnapshot(p.ParentSnapshot); err != nil {
			return ErrInvalid
		}
	}
	return nil
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
		progress, existing, err := requestPreparationProgress(record)
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
	record, err := r.Read(ctx, scope, id)
	if err != nil {
		return RequestPreparation{}, err
	}
	progress, preparation, err := requestPreparationProgress(record)
	if err != nil {
		return RequestPreparation{}, err
	}
	if preparation == nil {
		if record.Status != StatusRunning || preparationAlreadyStarted(progress) {
			return RequestPreparation{}, ErrCorrupt
		}
		return RequestPreparation{}, ErrRequestPreparationMissing
	}
	if err := r.repairBudgetPlanIndex(ctx, record); err != nil {
		return RequestPreparation{}, err
	}
	return *preparation, nil
}

func preparationAlreadyStarted(progress map[string]json.RawMessage) bool {
	for _, key := range []string{"budget_plan", "provider_execution", "checkpoint_finalization", "finalization_handoff"} {
		if _, exists := progress[key]; exists {
			return true
		}
	}
	return false
}

func requestPreparationProgress(record Record) (map[string]json.RawMessage, *RequestPreparation, error) {
	var progress map[string]json.RawMessage
	if json.Unmarshal(record.Progress, &progress) != nil || progress == nil || string(progress["version"]) != "1" {
		return nil, nil, ErrCorrupt
	}
	data, present := progress["request_preparation"]
	if !present {
		return progress, nil, nil
	}
	var preparation RequestPreparation
	if json.Unmarshal(data, &preparation) != nil || preparation.Validate() != nil || preparation.PreparedAt.Before(record.Request.CreatedAt) {
		return nil, nil, ErrCorrupt
	}
	return progress, &preparation, nil
}
