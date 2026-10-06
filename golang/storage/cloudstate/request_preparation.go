package cloudstate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
//
// ParentSnapshot is the in-memory form: callers pass and receive the snapshot
// bytes. The repository stores the snapshot once, as its own immutable
// encrypted blob, and keeps only a reference to it in the request record, so
// rewriting progress does not rewrite the transcript (#1112). Preparations
// saved before that keep the snapshot inline and still load.
type RequestPreparation struct {
	Version         int             `json:"version"`
	ConfigDigest    [32]byte        `json:"config_digest"`
	CheckpointScope string          `json:"checkpoint_scope"`
	ParentSnapshot  json.RawMessage `json:"parent_snapshot,omitempty"`
	// ParentProvenance pins provider state in the parent snapshot's items.
	// It is additive: a preparation saved without it restores with none.
	ParentProvenance []state.ProviderStateProvenance `json:"parent_provenance,omitempty"`
	PreparedAt       time.Time                       `json:"prepared_at"`
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
	if err := p.validateFields(len(p.ParentSnapshot) != 0); err != nil {
		return nil, err
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

// validateFields checks everything except the parent snapshot's content.
func (p RequestPreparation) validateFields(hasParent bool) error {
	if p.Version != 1 || p.ConfigDigest == ([32]byte{}) || !safeText(p.CheckpointScope, 512) || !validTime(p.PreparedAt) {
		return ErrInvalid
	}
	if (len(p.ParentProvenance) != 0 && !hasParent) || state.ValidateProviderStateProvenance(p.ParentProvenance) != nil {
		return ErrInvalid
	}
	return nil
}

// parentSnapshotRef names a parent snapshot stored as its own immutable blob.
// Digest is the hex SHA-256 of the plaintext snapshot. Blob is its
// content-addressed key in the scope's parent-snapshot stream, so every
// initializer of one preparation derives the same reference.
type parentSnapshotRef struct {
	Digest     string `json:"digest"`
	ByteLength int64  `json:"byte_length"`
	Blob       string `json:"blob"`
}

// storedRequestPreparation is the durable form of a RequestPreparation. New
// writes set ParentSnapshotRef and omit the inline parent_snapshot. Legacy
// preparations carry the inline snapshot and no reference. Never both.
type storedRequestPreparation struct {
	RequestPreparation
	ParentSnapshotRef *parentSnapshotRef `json:"parent_snapshot_ref,omitempty"`
}

// parentSnapshotStream separates parent snapshots from request records and
// checkpoint blobs, and scopes them: a reference copied into another scope's
// record names a key that does not verify there. A request's attempt children
// share its scope, so they reuse the root's reference.
func (r *Repository) parentSnapshotStream(scope Scope) string {
	return "request-parent/" + r.scopeTag(scope)
}

func (r *Repository) newParentSnapshotRef(scope Scope, snapshot []byte) parentSnapshotRef {
	sum := sha256.Sum256(snapshot)
	return parentSnapshotRef{Digest: hex.EncodeToString(sum[:]), ByteLength: int64(len(snapshot)), Blob: r.blobKey(r.parentSnapshotStream(scope), snapshot)}
}

// validStored checks a stored preparation without reading a parent blob. A
// legacy inline parent is decoded and returned, exactly as before. A
// referenced parent is checked only for a well-formed reference here; resolve
// verifies and decodes it.
func (r *Repository) validStored(stored storedRequestPreparation) (*state.CheckpointSnapshot, error) {
	ref := stored.ParentSnapshotRef
	if ref == nil {
		return stored.ValidateParent()
	}
	if len(stored.ParentSnapshot) != 0 || !hexDigest(ref.Digest) || ref.ByteLength <= 0 || ref.ByteLength > MaxPreparedParentBytes || !r.validBlobKey(ref.Blob) {
		return nil, ErrInvalid
	}
	return nil, stored.validateFields(true)
}

// resolve returns the preparation with its parent snapshot inline, reading a
// referenced parent from its blob. It verifies the blob against the reference
// and fails closed: a missing blob or a digest or length mismatch is
// ErrCorrupt, never a cache miss. legacyParent is the snapshot that validStored
// already decoded from an inline parent.
func (r *Repository) resolve(ctx context.Context, scope Scope, stored storedRequestPreparation, legacyParent *state.CheckpointSnapshot) (RequestPreparation, *state.CheckpointSnapshot, error) {
	preparation, ref := stored.RequestPreparation, stored.ParentSnapshotRef
	if ref == nil {
		return preparation, legacyParent, nil
	}
	data, err := r.readReferencedBlob(ctx, r.parentSnapshotStream(scope), ref.Blob)
	if err != nil {
		return RequestPreparation{}, nil, err
	}
	sum := sha256.Sum256(data)
	if int64(len(data)) != ref.ByteLength || hex.EncodeToString(sum[:]) != ref.Digest {
		return RequestPreparation{}, nil, ErrCorrupt
	}
	preparation.ParentSnapshot = data
	parent, err := preparation.ValidateParent()
	if err != nil {
		return RequestPreparation{}, nil, ErrCorrupt
	}
	return preparation, parent, nil
}

// SaveRequestPreparation has one immutable winner and preserves other progress.
// It must precede budget planning/acceptance and provider submission. An identical
// retry after an uncertain write repairs discovery without rematerializing input.
//
// A parent snapshot is written to its own encrypted, content-addressed blob,
// then read back and compared, before the record that references it. The
// stored reference depends only on the snapshot bytes and scope, so concurrent
// initializers of one preparation store identical values.
func (r *Repository) SaveRequestPreparation(ctx context.Context, scope Scope, id RequestID, preparation RequestPreparation) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if !scope.valid() || preparation.Validate() != nil {
		return ErrInvalid
	}
	preparation.PreparedAt = preparation.PreparedAt.UTC()
	stored := storedRequestPreparation{RequestPreparation: preparation}
	snapshot := preparation.ParentSnapshot
	if len(snapshot) != 0 {
		ref := r.newParentSnapshotRef(scope, snapshot)
		stored.ParentSnapshot, stored.ParentSnapshotRef = nil, &ref
	}
	encoded, err := encodePreparation(stored)
	if err != nil {
		return err
	}
	written := len(snapshot) == 0
	for attempt := 0; attempt < 16; attempt++ {
		record, err := r.Read(ctx, scope, id)
		if err != nil {
			return err
		}
		progress, existing, _, err := r.storedPreparationProgress(record)
		if err != nil {
			return err
		}
		if existing != nil {
			previous, _ := encodePreparation(*existing)
			same := bytes.Equal(previous, encoded)
			if !same && existing.ParentSnapshotRef == nil && len(snapshot) != 0 {
				// A preparation saved inline before #1112 is the same logical
				// preparation in its legacy form.
				legacy, _ := encodePreparation(storedRequestPreparation{RequestPreparation: preparation})
				same = bytes.Equal(previous, legacy)
			}
			if !same {
				return contracts.ErrConflict
			}
			return r.repairBudgetPlanIndex(ctx, record)
		}
		if record.Status != StatusRunning || preparation.PreparedAt.Before(record.Request.CreatedAt) || preparationAlreadyStarted(progress) {
			return contracts.ErrConflict
		}
		if !written {
			if err := r.writeParentSnapshot(ctx, scope, snapshot, *stored.ParentSnapshotRef); err != nil {
				return err
			}
			written = true
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

// writeParentSnapshot publishes the parent blob and verifies it by reading it
// back, before any record references it.
func (r *Repository) writeParentSnapshot(ctx context.Context, scope Scope, snapshot []byte, ref parentSnapshotRef) error {
	stream := r.parentSnapshotStream(scope)
	key, err := r.writeBlob(ctx, stream, snapshot)
	if err != nil {
		return err
	}
	if key != ref.Blob {
		return ErrCorrupt
	}
	stored, err := r.readReferencedBlob(ctx, stream, key)
	if err != nil {
		return err
	}
	if !bytes.Equal(stored, snapshot) {
		return ErrCorrupt
	}
	return nil
}

func encodePreparation(stored storedRequestPreparation) (json.RawMessage, error) {
	encoded, err := json.Marshal(stored)
	if err != nil {
		return nil, ErrInvalid
	}
	return objectJSON(encoded)
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
	progress, stored, legacyParent, err := r.storedPreparationProgress(record)
	if err != nil {
		return RequestPreparation{}, nil, err
	}
	if stored == nil {
		if record.Status != StatusRunning || preparationAlreadyStarted(progress) {
			return RequestPreparation{}, nil, ErrCorrupt
		}
		return RequestPreparation{}, nil, ErrRequestPreparationMissing
	}
	if err := r.repairBudgetPlanIndex(ctx, record); err != nil {
		return RequestPreparation{}, nil, err
	}
	return r.resolve(ctx, record.Request.Scope, *stored, legacyParent)
}

func preparationAlreadyStarted(progress map[string]json.RawMessage) bool {
	for _, key := range []string{"budget_plan", "provider_execution", "checkpoint_finalization", "finalization_handoff"} {
		if _, exists := progress[key]; exists {
			return true
		}
	}
	return false
}

// storedPreparationProgress decodes and validates the stored preparation
// without reading a referenced parent blob. legacyParent is the inline parent
// snapshot that validating a legacy preparation decoded (nil otherwise).
func (r *Repository) storedPreparationProgress(record Record) (progress map[string]json.RawMessage, stored *storedRequestPreparation, legacyParent *state.CheckpointSnapshot, err error) {
	if json.Unmarshal(record.Progress, &progress) != nil || progress == nil || string(progress["version"]) != "1" {
		return nil, nil, nil, ErrCorrupt
	}
	data, present := progress["request_preparation"]
	if !present {
		return progress, nil, nil, nil
	}
	var value storedRequestPreparation
	if json.Unmarshal(data, &value) != nil {
		return nil, nil, nil, ErrCorrupt
	}
	legacyParent, err = r.validStored(value)
	if err != nil || value.PreparedAt.Before(record.Request.CreatedAt) {
		return nil, nil, nil, ErrCorrupt
	}
	return progress, &value, legacyParent, nil
}
