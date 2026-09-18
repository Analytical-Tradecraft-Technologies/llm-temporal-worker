package state

// This file defines the storage-neutral contract between checkpoint
// materialization and a future durable repository.  It deliberately contains
// no SQL, blob-store client, Temporal payload, or provider implementation.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

// CheckpointKind is persisted metadata, not an Activity or provider enum.
type CheckpointKind string

const (
	CheckpointGeneration  CheckpointKind = "generation"
	CheckpointCompaction  CheckpointKind = "compaction"
	CheckpointCacheReplay CheckpointKind = "cache_replay"
)

// The named identifiers keep repository signatures from collapsing distinct
// durable relationships into untyped strings.
type CheckpointID string
type OperationID string
type CacheEntryID string
type BlobID string

// CheckpointBlobReference points at an immutable blob that was written before
// checkpoint publication. The locator itself is owned by the blob repository
// and is intentionally absent from this DTO.
type CheckpointBlobReference struct {
	ID         BlobID
	Digest     [32]byte
	ByteLength int64
	MediaType  string
}

func (reference CheckpointBlobReference) validate(name string) error {
	if reference.ID == "" {
		return fmt.Errorf("checkpoint %s blob ID is required", name)
	}
	if reference.Digest == ([32]byte{}) {
		return fmt.Errorf("checkpoint %s blob digest is required", name)
	}
	if reference.ByteLength < 0 {
		return fmt.Errorf("checkpoint %s blob byte length must not be negative", name)
	}
	if strings.TrimSpace(reference.MediaType) == "" {
		return fmt.Errorf("checkpoint %s blob media type is required", name)
	}
	return nil
}

// CheckpointProviderState is the non-secret durable provider-state metadata
// attached to one checkpoint. The opaque state bytes remain in StateBlob.
type CheckpointProviderState struct {
	Ordinal             int
	Provider            string
	EndpointID          string
	EndpointAccountHMAC [32]byte
	Region              string
	EndpointFamily      string
	ModelLineage        string
	StateKind           string
	StateBlob           CheckpointBlobReference
	StateDigest         [32]byte
	Required            bool
	ImmutableForkSafe   bool
	CreatedAt           time.Time
	ExpiresAt           *time.Time
}

func (state CheckpointProviderState) validate() error {
	if state.Ordinal < 0 {
		return fmt.Errorf("checkpoint provider state ordinal must not be negative")
	}
	for name, value := range map[string]string{
		"provider": state.Provider, "endpoint ID": state.EndpointID,
		"region": state.Region, "endpoint family": state.EndpointFamily,
		"model lineage": state.ModelLineage, "state kind": state.StateKind,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("checkpoint provider state %s is required", name)
		}
	}
	if state.EndpointAccountHMAC == ([32]byte{}) {
		return fmt.Errorf("checkpoint provider state endpoint account HMAC is required")
	}
	if err := state.StateBlob.validate("provider state"); err != nil {
		return err
	}
	if state.StateDigest == ([32]byte{}) {
		return fmt.Errorf("checkpoint provider state digest is required")
	}
	if state.CreatedAt.IsZero() {
		return fmt.Errorf("checkpoint provider state created time is required")
	}
	if state.ExpiresAt != nil && !state.ExpiresAt.After(state.CreatedAt) {
		return fmt.Errorf("checkpoint provider state expiry must be after created time")
	}
	return nil
}

// DurableCheckpoint is the immutable row-shaped DTO shared by future
// PostgreSQL and in-memory adapters. ScopeID is an opaque repository scope
// identity; raw tenant/project strings are never persisted in this record.
type DurableCheckpoint struct {
	ID                         CheckpointID
	ScopeID                    string
	PublicIDHMAC               [32]byte
	HandleKeyID                string
	ParentID                   *CheckpointID
	Kind                       CheckpointKind
	Depth                      int32
	OriginOperationID          OperationID
	OriginCacheEntryID         *CacheEntryID
	DeltaBlob                  CheckpointBlobReference
	ResponseBlob               CheckpointBlobReference
	SettingsPatchBlob          CheckpointBlobReference
	MaterializedSnapshotBlob   *CheckpointBlobReference
	CanonicalLineageDigest     [32]byte
	MaterializedSettingsDigest [32]byte
	ToolFrontierDigest         [32]byte
	SchemaVersion              int
	CompilerEpoch              string
	CompactionPolicyVersion    *string
	CompactionPromptVersion    *string
	CompactedThroughID         *CheckpointID
	ProviderState              []CheckpointProviderState
	Affinities                 ProviderCacheAffinitySet
	CreatedAt                  time.Time
	ExpiresAt                  time.Time
}

// Validate checks invariants that every adapter must enforce before exposing
// a row or accepting it for publication. Expired rows remain valid immutable
// history; callers decide whether a read may use them at a supplied time.
func (checkpoint DurableCheckpoint) Validate(_ time.Time) error {
	if checkpoint.ID == "" || strings.TrimSpace(checkpoint.ScopeID) == "" {
		return fmt.Errorf("checkpoint ID and scope ID are required")
	}
	if checkpoint.PublicIDHMAC == ([32]byte{}) {
		return fmt.Errorf("checkpoint public ID HMAC is required")
	}
	if strings.TrimSpace(checkpoint.HandleKeyID) == "" {
		return fmt.Errorf("checkpoint handle key ID is required")
	}
	switch checkpoint.Kind {
	case CheckpointGeneration, CheckpointCompaction, CheckpointCacheReplay:
	default:
		return fmt.Errorf("checkpoint kind %q is invalid", checkpoint.Kind)
	}
	if checkpoint.Depth < 0 || (checkpoint.ParentID == nil) != (checkpoint.Depth == 0) {
		return fmt.Errorf("checkpoint parent and depth are inconsistent")
	}
	if checkpoint.ParentID != nil && *checkpoint.ParentID == "" {
		return fmt.Errorf("checkpoint parent ID is empty")
	}
	if checkpoint.ParentID != nil && *checkpoint.ParentID == checkpoint.ID {
		return fmt.Errorf("checkpoint parent ID must differ from checkpoint ID")
	}
	if checkpoint.OriginOperationID == "" {
		return fmt.Errorf("checkpoint origin operation ID is required")
	}
	if checkpoint.OriginCacheEntryID != nil && *checkpoint.OriginCacheEntryID == "" {
		return fmt.Errorf("checkpoint origin cache entry ID is empty")
	}
	if checkpoint.Kind == CheckpointCacheReplay && checkpoint.OriginCacheEntryID == nil {
		return fmt.Errorf("checkpoint cache replay origin cache entry ID is required")
	}
	for name, reference := range map[string]CheckpointBlobReference{
		"delta": checkpoint.DeltaBlob, "response": checkpoint.ResponseBlob,
		"settings patch": checkpoint.SettingsPatchBlob,
	} {
		if err := reference.validate(name); err != nil {
			return err
		}
	}
	if checkpoint.MaterializedSnapshotBlob != nil {
		if err := checkpoint.MaterializedSnapshotBlob.validate("materialized snapshot"); err != nil {
			return err
		}
	}
	for name, digest := range map[string][32]byte{
		"canonical lineage":     checkpoint.CanonicalLineageDigest,
		"materialized settings": checkpoint.MaterializedSettingsDigest,
		"tool frontier":         checkpoint.ToolFrontierDigest,
	} {
		if digest == ([32]byte{}) {
			return fmt.Errorf("checkpoint %s digest is required", name)
		}
	}
	if checkpoint.SchemaVersion <= 0 || strings.TrimSpace(checkpoint.CompilerEpoch) == "" {
		return fmt.Errorf("checkpoint schema version and compiler epoch are required")
	}
	if checkpoint.CreatedAt.IsZero() || checkpoint.ExpiresAt.IsZero() || !checkpoint.ExpiresAt.After(checkpoint.CreatedAt) {
		return fmt.Errorf("checkpoint created and expiry times are invalid")
	}
	seenOrdinals := make(map[int]struct{}, len(checkpoint.ProviderState))
	for _, providerState := range checkpoint.ProviderState {
		if err := providerState.validate(); err != nil {
			return err
		}
		if _, exists := seenOrdinals[providerState.Ordinal]; exists {
			return fmt.Errorf("checkpoint provider state ordinal %d is duplicated", providerState.Ordinal)
		}
		seenOrdinals[providerState.Ordinal] = struct{}{}
	}
	if err := checkpoint.Affinities.Validate(checkpoint.CreatedAt); err != nil {
		return fmt.Errorf("checkpoint affinities: %w", err)
	}
	return nil
}

// CanonicalDigest is the stable content digest used for idempotency and
// conflict detection. It includes immutable metadata and references, but not
// database-generated timestamps outside this record or any blob bytes.
func (checkpoint DurableCheckpoint) CanonicalDigest() ([32]byte, error) {
	if err := checkpoint.Validate(checkpoint.CreatedAt); err != nil {
		return [32]byte{}, err
	}
	type blobDigest struct {
		ID         BlobID `json:"id"`
		Digest     string `json:"digest"`
		ByteLength int64  `json:"byte_length"`
		MediaType  string `json:"media_type"`
	}
	encodeBlob := func(reference CheckpointBlobReference) blobDigest {
		return blobDigest{ID: reference.ID, Digest: hex.EncodeToString(reference.Digest[:]), ByteLength: reference.ByteLength, MediaType: reference.MediaType}
	}
	// Repositories may hydrate zero child rows as either nil or an allocated
	// empty slice. Normalize both representations before hashing so the same
	// immutable checkpoint has one canonical digest.
	providerState := checkpoint.ProviderState
	if providerState == nil {
		providerState = []CheckpointProviderState{}
	}
	affinities := checkpoint.Affinities
	if affinities == nil {
		affinities = ProviderCacheAffinitySet{}
	}
	payload := struct {
		SchemaVersion              int
		ID                         CheckpointID
		ScopeID                    string
		PublicIDHMAC               string
		HandleKeyID                string
		ParentID                   *CheckpointID
		Kind                       CheckpointKind
		Depth                      int32
		OriginOperationID          OperationID
		OriginCacheEntryID         *CacheEntryID
		DeltaBlob                  blobDigest
		ResponseBlob               blobDigest
		SettingsPatchBlob          blobDigest
		MaterializedSnapshotBlob   *blobDigest
		CanonicalLineageDigest     string
		MaterializedSettingsDigest string
		ToolFrontierDigest         string
		CompilerEpoch              string
		CompactionPolicyVersion    *string
		CompactionPromptVersion    *string
		CompactedThroughID         *CheckpointID
		ProviderState              []CheckpointProviderState
		Affinities                 ProviderCacheAffinitySet
		CreatedAt                  time.Time
		ExpiresAt                  time.Time
	}{
		checkpoint.SchemaVersion, checkpoint.ID, checkpoint.ScopeID, hex.EncodeToString(checkpoint.PublicIDHMAC[:]), checkpoint.HandleKeyID,
		checkpoint.ParentID, checkpoint.Kind, checkpoint.Depth, checkpoint.OriginOperationID, checkpoint.OriginCacheEntryID,
		encodeBlob(checkpoint.DeltaBlob), encodeBlob(checkpoint.ResponseBlob), encodeBlob(checkpoint.SettingsPatchBlob), nil,
		hex.EncodeToString(checkpoint.CanonicalLineageDigest[:]), hex.EncodeToString(checkpoint.MaterializedSettingsDigest[:]), hex.EncodeToString(checkpoint.ToolFrontierDigest[:]),
		checkpoint.CompilerEpoch, checkpoint.CompactionPolicyVersion, checkpoint.CompactionPromptVersion, checkpoint.CompactedThroughID,
		providerState, affinities, checkpoint.CreatedAt.UTC(), checkpoint.ExpiresAt.UTC(),
	}
	if checkpoint.MaterializedSnapshotBlob != nil {
		encoded := encodeBlob(*checkpoint.MaterializedSnapshotBlob)
		payload.MaterializedSnapshotBlob = &encoded
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return [32]byte{}, fmt.Errorf("marshal checkpoint digest: %w", err)
	}
	canonical, err := llm.CanonicalJSON(data)
	if err != nil {
		return [32]byte{}, fmt.Errorf("canonicalize checkpoint digest: %w", err)
	}
	return sha256.Sum256(canonical), nil
}

// CheckpointWrite is the repository's immutable publication unit. Blobs must
// already be durable when this value enters a unit of work.
type CheckpointWrite struct {
	Checkpoint DurableCheckpoint
}

func (write CheckpointWrite) Validate(now time.Time) error { return write.Checkpoint.Validate(now) }

// CheckpointRepository is the storage-neutral durable port. Implementations
// must scope reads by ScopeID and never return a row from another scope.
type CheckpointRepository interface {
	Get(context.Context, string, CheckpointID) (DurableCheckpoint, error)
	BeginCheckpoint(context.Context) (CheckpointUnitOfWork, error)
}

// CheckpointLineageRepository is an optional read-optimized extension. It
// returns leaf-to-root scoped metadata in one bounded call and may stop after
// the nearest snapshot-bearing row. Materializers always retain Get fallback
// so an invalid snapshot cannot hide authoritative ancestors.
type CheckpointLineageRepository interface {
	GetLineage(context.Context, string, CheckpointID, int) ([]DurableCheckpoint, error)
}

// CheckpointMaterializer resolves a checkpoint graph through a repository.
// It returns the same state.MaterializedState contract as CheckpointGraph;
// materialization is intentionally not wired into Generate or Compact here.
type CheckpointMaterializer interface {
	Materialize(context.Context, string, CheckpointID, MaterializeLimits) (MaterializedState, error)
}

// CheckpointUnitOfWork owns one short durable publication transaction. The
// interface does not expose a SQL transaction, preventing blob/provider I/O
// from being performed while a future implementation holds database locks.
type CheckpointUnitOfWork interface {
	PutCheckpoint(context.Context, CheckpointWrite) error
	Commit(context.Context) error
	Rollback(context.Context) error
}

// WithCheckpointUnitOfWork provides the common commit/rollback lifecycle for
// repository adapters while preserving the original callback error if rollback
// itself fails.
func WithCheckpointUnitOfWork(ctx context.Context, repository CheckpointRepository, fn func(context.Context, CheckpointUnitOfWork) error) error {
	if ctx == nil {
		return errors.New("checkpoint transaction context is nil")
	}
	if repository == nil {
		return errors.New("checkpoint repository is nil")
	}
	if fn == nil {
		return errors.New("checkpoint transaction callback is nil")
	}
	unit, err := repository.BeginCheckpoint(ctx)
	if err != nil {
		return err
	}
	if unit == nil {
		return errors.New("checkpoint repository returned a nil unit of work")
	}
	committed := false
	defer func() {
		if !committed {
			_ = unit.Rollback(context.Background())
		}
	}()
	if err := fn(ctx, unit); err != nil {
		return err
	}
	if err := unit.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}
