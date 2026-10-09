package cloudstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

// GenerationPlan retains the first authorized public Generate decision. It is
// independent of request progress so failed planning never creates a running
// request, and finalization cannot erase the original public input binding.
type GenerationPlan struct {
	CompactBeforeGenerate bool `json:"compact_before_generate"`
}

type generationPlanEnvelope struct {
	Version      int             `json:"version"`
	Manifest     json.RawMessage `json:"manifest"`
	RequestIndex int64           `json:"request_index"`
	Plan         GenerationPlan  `json:"plan"`
}

type generationPlanPointer struct {
	Version int    `json:"version"`
	Blob    string `json:"blob"`
}

type generationBindingEnvelope struct {
	Version      int             `json:"version"`
	Manifest     json.RawMessage `json:"manifest"`
	RequestIndex int64           `json:"request_index"`
	Effective    json.RawMessage `json:"effective"`
}

// LoadGenerationBinding returns the immutable effective Generate input. Its
// retention does not depend on the automatic Compact checkpoint or result.
func (r *Repository) LoadGenerationBinding(ctx context.Context, op Operation) (json.RawMessage, error) {
	if _, err := r.LoadGenerationPlan(ctx, op); err != nil {
		return nil, err
	}
	op, err := r.normalizeGenerationPlanOperation(op)
	if err != nil {
		return nil, err
	}
	key := r.generationPlanKey(op)
	key.SortKey = "effective-v1"
	row, err := r.table.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	data, ok := row.Item.Fields["generation_binding"].(kv.KeyValueBytes)
	var pointer generationPlanPointer
	if !ok || len(data) > 2048 || row.Item.Key() != key || row.Version == "" || json.Unmarshal(data, &pointer) != nil || pointer.Version != 1 || !r.validBlobKey(pointer.Blob) {
		return nil, ErrCorrupt
	}
	data, err = r.readReferencedBlob(ctx, key.PartitionKey+"/effective", pointer.Blob)
	if err != nil {
		return nil, err
	}
	var envelope generationBindingEnvelope
	if json.Unmarshal(data, &envelope) != nil || envelope.Version != 1 {
		return nil, ErrCorrupt
	}
	manifest, err := objectJSON(envelope.Manifest)
	if err != nil {
		return nil, ErrCorrupt
	}
	if !bytes.Equal(manifest, op.Manifest) || envelope.RequestIndex != op.RequestIndex {
		return nil, contracts.ErrConflict
	}
	effective, err := objectJSON(envelope.Effective)
	if err != nil {
		return nil, ErrCorrupt
	}
	return effective, nil
}

// SaveGenerationBinding is called only after the runtime verifies that the
// effective parent is the authorized automatic Compact result. The repository
// preserves that proof across completion and rejects any later substitution.
func (r *Repository) SaveGenerationBinding(ctx context.Context, op Operation, effective json.RawMessage) error {
	if _, err := r.LoadGenerationPlan(ctx, op); err != nil {
		return err
	}
	op, err := r.normalizeGenerationPlanOperation(op)
	if err != nil {
		return err
	}
	effective, err = objectJSON(effective)
	if err != nil {
		return err
	}
	stored, err := r.LoadGenerationBinding(ctx, op)
	if err == nil {
		if !bytes.Equal(stored, effective) {
			return contracts.ErrConflict
		}
		return nil
	}
	if !errors.Is(err, contracts.ErrNotFound) || errors.Is(err, contracts.ErrOutcomeUnknown) {
		return err
	}
	data, err := json.Marshal(generationBindingEnvelope{Version: 1, Manifest: op.Manifest, RequestIndex: op.RequestIndex, Effective: effective})
	if err != nil || len(data) > maxPayloadBytes {
		return ErrInvalid
	}
	key := r.generationPlanKey(op)
	key.SortKey = "effective-v1"
	blob, err := r.writeBlob(ctx, key.PartitionKey+"/effective", data)
	if err != nil {
		return err
	}
	pointer, _ := json.Marshal(generationPlanPointer{Version: 1, Blob: blob})
	_, err = r.table.Create(ctx, kv.KeyValueItem{PartitionKey: key.PartitionKey, SortKey: key.SortKey, Fields: kv.KeyValueDocument{"generation_binding": kv.Bytes(pointer)}})
	if errors.Is(err, contracts.ErrAlreadyExists) && !errors.Is(err, contracts.ErrOutcomeUnknown) {
		stored, err = r.LoadGenerationBinding(ctx, op)
		if err == nil && !bytes.Equal(stored, effective) {
			return contracts.ErrConflict
		}
	}
	return err
}

func (r *Repository) generationPlanKey(op Operation) kv.KeyValueKey {
	identity, _ := json.Marshal([]string{op.Scope.Tenant, op.Scope.Project, op.Key})
	return kv.KeyValueKey{PartitionKey: r.namespace + "/generation-plan/" + r.digest("generation-plan", identity), SortKey: "v1"}
}

func (r *Repository) normalizeGenerationPlanOperation(op Operation) (Operation, error) {
	if op.Kind != "generate" || !safeText(op.Key, 4096) {
		return Operation{}, ErrInvalid
	}
	request, err := normalizeRequest(CreateRequest{ID: r.operationID(op.Scope, op.Kind, op.Key), Scope: op.Scope, Kind: op.Kind, RequestIndex: op.RequestIndex, Manifest: op.Manifest, CreatedAt: op.Now})
	if err != nil {
		return Operation{}, err
	}
	op.Manifest = request.Manifest
	return op, nil
}

// LoadGenerationPlan only treats an absent pointer as a missing decision. A
// pointer whose encrypted blob is missing is corrupt, never fresh planning.
// Callers must authorize the supplied scope before accessing the repository.
func (r *Repository) LoadGenerationPlan(ctx context.Context, op Operation) (GenerationPlan, error) {
	if err := validContext(ctx); err != nil {
		return GenerationPlan{}, err
	}
	op, err := r.normalizeGenerationPlanOperation(op)
	if err != nil {
		return GenerationPlan{}, err
	}
	key := r.generationPlanKey(op)
	row, err := r.table.Get(ctx, key)
	if err != nil {
		return GenerationPlan{}, err
	}
	data, ok := row.Item.Fields["generation_plan"].(kv.KeyValueBytes)
	var pointer generationPlanPointer
	if !ok || len(data) > 2048 || row.Item.Key() != key || row.Version == "" || json.Unmarshal(data, &pointer) != nil || pointer.Version != 1 || !r.validBlobKey(pointer.Blob) {
		return GenerationPlan{}, ErrCorrupt
	}
	data, err = r.readReferencedBlob(ctx, key.PartitionKey, pointer.Blob)
	if err != nil {
		return GenerationPlan{}, err
	}
	var envelope generationPlanEnvelope
	if json.Unmarshal(data, &envelope) != nil || envelope.Version != 1 {
		return GenerationPlan{}, ErrCorrupt
	}
	stored := op
	stored.Manifest, stored.RequestIndex = envelope.Manifest, envelope.RequestIndex
	stored, err = r.normalizeGenerationPlanOperation(stored)
	if err != nil {
		return GenerationPlan{}, ErrCorrupt
	}
	if !bytes.Equal(stored.Manifest, op.Manifest) || stored.RequestIndex != op.RequestIndex {
		return GenerationPlan{}, contracts.ErrConflict
	}
	return envelope.Plan, nil
}

// SaveGenerationPlan elects one decision across concurrent planners. Identical
// public inputs reuse the winner even when their current routing snapshots give
// different decisions. Unknown write outcomes are returned for same-input retry.
func (r *Repository) SaveGenerationPlan(ctx context.Context, op Operation, plan GenerationPlan) (GenerationPlan, error) {
	existing, err := r.LoadGenerationPlan(ctx, op)
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, contracts.ErrNotFound) || errors.Is(err, contracts.ErrOutcomeUnknown) {
		return GenerationPlan{}, err
	}
	op, err = r.normalizeGenerationPlanOperation(op)
	if err != nil {
		return GenerationPlan{}, err
	}
	data, err := json.Marshal(generationPlanEnvelope{Version: 1, Manifest: op.Manifest, RequestIndex: op.RequestIndex, Plan: plan})
	if err != nil || len(data) > maxPayloadBytes {
		return GenerationPlan{}, ErrInvalid
	}
	key := r.generationPlanKey(op)
	blob, err := r.writeBlob(ctx, key.PartitionKey, data)
	if err != nil {
		return GenerationPlan{}, err
	}
	pointer, _ := json.Marshal(generationPlanPointer{Version: 1, Blob: blob})
	_, err = r.table.Create(ctx, kv.KeyValueItem{PartitionKey: key.PartitionKey, SortKey: key.SortKey, Fields: kv.KeyValueDocument{"generation_plan": kv.Bytes(pointer)}})
	if errors.Is(err, contracts.ErrAlreadyExists) && !errors.Is(err, contracts.ErrOutcomeUnknown) {
		return r.LoadGenerationPlan(ctx, op)
	}
	if err != nil {
		return GenerationPlan{}, err
	}
	return plan, nil
}
