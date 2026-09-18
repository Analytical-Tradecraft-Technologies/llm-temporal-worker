package postgres

// PostgreSQL persistence for the storage-neutral durable checkpoint port.
// Blobs are uploaded before publication; this repository publishes references
// and metadata in one short transaction and never performs provider I/O.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

var (
	ErrCheckpointNotFound       = errors.New("checkpoint not found")
	ErrCheckpointParentNotFound = errors.New("checkpoint parent does not exist")
	ErrCheckpointConflict       = errors.New("checkpoint already exists with different immutable metadata")
)

// DurableCheckpointRepository implements state.CheckpointRepository. Every
// read is scoped by scope_id and every publication verifies referenced blobs
// belong to that scope.
type DurableCheckpointRepository struct {
	Pool      *pgxpool.Pool
	Namespace Namespace
	Now       func() time.Time
}

func (repository DurableCheckpointRepository) validate() error {
	if repository.Pool == nil {
		return errors.New("checkpoint repository pool is nil")
	}
	return repository.Namespace.Validate()
}

func (repository DurableCheckpointRepository) clock() time.Time {
	if repository.Now != nil {
		return repository.Now().UTC()
	}
	return time.Now().UTC()
}

func parseCheckpointUUID(id state.CheckpointID, label string) (uuid.UUID, error) {
	parsed, err := uuid.Parse(string(id))
	if err != nil || parsed == uuid.Nil {
		return uuid.Nil, fmt.Errorf("checkpoint %s must be a UUID", label)
	}
	return parsed, nil
}

func parseBlobUUID(id state.BlobID, label string) (uuid.UUID, error) {
	parsed, err := uuid.Parse(string(id))
	if err != nil || parsed == uuid.Nil {
		return uuid.Nil, fmt.Errorf("checkpoint %s blob ID must be a UUID", label)
	}
	return parsed, nil
}

func parseScopeUUID(value string) (uuid.UUID, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(value))
	if err != nil || parsed == uuid.Nil {
		return uuid.Nil, errors.New("checkpoint scope ID must be a UUID")
	}
	return parsed, nil
}

// parseOperationUUID enforces the PostgreSQL checkpoint boundary's canonical
// identifier representation. The established operations schema stores UUIDs
// only, while OperationRepository still accepts legacy string IDs by hashing
// them. A checkpoint cannot safely use that legacy derivation: the schema has
// no column to retain the original string, so reading it back would change
// OriginOperationID and invalidate the checkpoint's canonical identity.
func parseOperationUUID(id state.OperationID) (uuid.UUID, error) {
	raw := string(id)
	parsed, err := uuid.Parse(raw)
	if err != nil || parsed == uuid.Nil || parsed.String() != raw {
		return uuid.Nil, errors.New("checkpoint origin operation ID must be a UUID")
	}
	return parsed, nil
}

func (repository DurableCheckpointRepository) relation(name string) (string, error) {
	return repository.Namespace.Render(name)
}

// Get reads one immutable checkpoint and all provider-state and affinity child
// rows. Expired rows remain readable for audit and materialization.
func (repository DurableCheckpointRepository) Get(ctx context.Context, scopeID string, id state.CheckpointID) (state.DurableCheckpoint, error) {
	var checkpoint state.DurableCheckpoint
	if err := repository.validate(); err != nil {
		return checkpoint, err
	}
	scope, err := parseScopeUUID(scopeID)
	if err != nil {
		return checkpoint, err
	}
	checkpointID, err := parseCheckpointUUID(id, "ID")
	if err != nil {
		return checkpoint, err
	}
	relation, err := repository.relation("conversation_checkpoints")
	if err != nil {
		return checkpoint, err
	}
	query := "SELECT checkpoint_id, scope_id, public_id_hmac, handle_key_id, parent_checkpoint_id, checkpoint_kind, depth, origin_operation_id, origin_cache_entry_id, delta_blob_id, response_blob_id, settings_patch_blob_id, materialized_snapshot_blob_id, canonical_lineage_digest, materialized_settings_digest, tool_frontier_digest, schema_version, compiler_epoch, compaction_policy_version, compaction_prompt_version, compacted_through_checkpoint_id, created_at, expires_at FROM " + relation + " WHERE checkpoint_id=$1 AND scope_id=$2"
	var publicHMAC, lineageDigest, settingsDigest, frontierDigest []byte
	var parentID, cacheID, snapshotID, compactedID *uuid.UUID
	var originOperation uuid.UUID
	var deltaID, responseID, settingsID uuid.UUID
	if err := repository.Pool.QueryRow(ctx, query, checkpointID, scope).Scan(
		&checkpointID, &scope, &publicHMAC, &checkpoint.HandleKeyID, &parentID,
		&checkpoint.Kind, &checkpoint.Depth, &originOperation, &cacheID, &deltaID,
		&responseID, &settingsID, &snapshotID, &lineageDigest, &settingsDigest,
		&frontierDigest, &checkpoint.SchemaVersion, &checkpoint.CompilerEpoch,
		&checkpoint.CompactionPolicyVersion, &checkpoint.CompactionPromptVersion,
		&compactedID, &checkpoint.CreatedAt, &checkpoint.ExpiresAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return state.DurableCheckpoint{}, errors.Join(ErrCheckpointNotFound, state.ErrNotFound)
		}
		return state.DurableCheckpoint{}, redactPostgresError(fmt.Errorf("get PostgreSQL checkpoint: %w", err))
	}
	if len(publicHMAC) != keyDigestBytes || len(lineageDigest) != keyDigestBytes || len(settingsDigest) != keyDigestBytes || len(frontierDigest) != keyDigestBytes {
		return state.DurableCheckpoint{}, errors.New("PostgreSQL checkpoint digest has invalid length")
	}
	checkpoint.ID = state.CheckpointID(checkpointID.String())
	checkpoint.ScopeID = scope.String()
	copy(checkpoint.PublicIDHMAC[:], publicHMAC)
	copy(checkpoint.CanonicalLineageDigest[:], lineageDigest)
	copy(checkpoint.MaterializedSettingsDigest[:], settingsDigest)
	copy(checkpoint.ToolFrontierDigest[:], frontierDigest)
	checkpoint.OriginOperationID = state.OperationID(originOperation.String())
	if parentID != nil {
		value := state.CheckpointID(parentID.String())
		checkpoint.ParentID = &value
	}
	if cacheID != nil {
		value := state.CacheEntryID(cacheID.String())
		checkpoint.OriginCacheEntryID = &value
	}
	if compactedID != nil {
		value := state.CheckpointID(compactedID.String())
		checkpoint.CompactedThroughID = &value
	}
	if parentID != nil {
		if err := repository.ensureCheckpointScope(ctx, scope, *parentID, "parent checkpoint"); err != nil {
			return state.DurableCheckpoint{}, err
		}
	}
	if compactedID != nil {
		if err := repository.ensureCheckpointScope(ctx, scope, *compactedID, "compacted-through checkpoint"); err != nil {
			return state.DurableCheckpoint{}, err
		}
	}
	checkpoint.DeltaBlob, err = repository.readBlob(ctx, scope, deltaID, "delta")
	if err != nil {
		return state.DurableCheckpoint{}, err
	}
	checkpoint.ResponseBlob, err = repository.readBlob(ctx, scope, responseID, "response")
	if err != nil {
		return state.DurableCheckpoint{}, err
	}
	checkpoint.SettingsPatchBlob, err = repository.readBlob(ctx, scope, settingsID, "settings patch")
	if err != nil {
		return state.DurableCheckpoint{}, err
	}
	if snapshotID != nil {
		value, readErr := repository.readBlob(ctx, scope, *snapshotID, "materialized snapshot")
		if readErr != nil {
			return state.DurableCheckpoint{}, readErr
		}
		checkpoint.MaterializedSnapshotBlob = &value
	}
	checkpoint.ProviderState, err = repository.readProviderState(ctx, scope, checkpoint.ID)
	if err != nil {
		return state.DurableCheckpoint{}, err
	}
	checkpoint.Affinities, err = repository.readAffinities(ctx, scope, checkpoint.ID)
	if err != nil {
		return state.DurableCheckpoint{}, err
	}
	if err := checkpoint.Validate(repository.clock()); err != nil {
		return state.DurableCheckpoint{}, fmt.Errorf("validate PostgreSQL checkpoint: %w", err)
	}
	return checkpoint, nil
}

// GetLineage loads the bounded leaf-to-root materialization metadata with one
// tenant-scoped recursive query. Recursion stops at the nearest snapshot row;
// DurableCheckpointMaterializer falls back through Get if that optimization
// later fails cryptographic/content validation.
func (repository DurableCheckpointRepository) GetLineage(ctx context.Context, scopeID string, id state.CheckpointID, maxRows int) ([]state.DurableCheckpoint, error) {
	if err := repository.validate(); err != nil {
		return nil, err
	}
	if maxRows <= 0 {
		return nil, errors.New("checkpoint lineage row bound must be positive")
	}
	scope, err := parseScopeUUID(scopeID)
	if err != nil {
		return nil, err
	}
	checkpointID, err := parseCheckpointUUID(id, "ID")
	if err != nil {
		return nil, err
	}
	checkpoints, err := repository.relation("conversation_checkpoints")
	if err != nil {
		return nil, err
	}
	blobs, err := repository.relation("blobs")
	if err != nil {
		return nil, err
	}
	query := `WITH RECURSIVE lineage AS (
	SELECT c.*, 0 AS hops
	FROM ` + checkpoints + ` c
	WHERE c.checkpoint_id=$1 AND c.scope_id=$2
	UNION ALL
	SELECT parent.*, lineage.hops + 1
	FROM ` + checkpoints + ` parent
	JOIN lineage ON parent.checkpoint_id=lineage.parent_checkpoint_id
	WHERE parent.scope_id=$2
	  AND lineage.materialized_snapshot_blob_id IS NULL
	  AND lineage.hops + 1 < $3
)
SELECT lineage.checkpoint_id, lineage.scope_id, lineage.public_id_hmac,
	lineage.handle_key_id, lineage.parent_checkpoint_id, lineage.checkpoint_kind,
	lineage.depth, lineage.origin_operation_id, lineage.origin_cache_entry_id,
	lineage.delta_blob_id, lineage.response_blob_id, lineage.settings_patch_blob_id,
	lineage.materialized_snapshot_blob_id, lineage.canonical_lineage_digest,
	lineage.materialized_settings_digest, lineage.tool_frontier_digest,
	lineage.schema_version, lineage.compiler_epoch, lineage.compaction_policy_version,
	lineage.compaction_prompt_version, lineage.compacted_through_checkpoint_id,
	lineage.created_at, lineage.expires_at,
	delta.sha256, delta.byte_length, delta.media_type,
	response.sha256, response.byte_length, response.media_type,
	settings.sha256, settings.byte_length, settings.media_type,
	COALESCE(snapshot.sha256, ''::bytea),
	COALESCE(snapshot.byte_length, 0),
	COALESCE(snapshot.media_type, '')
FROM lineage
JOIN ` + blobs + ` delta ON delta.blob_id=lineage.delta_blob_id AND delta.scope_id=lineage.scope_id
JOIN ` + blobs + ` response ON response.blob_id=lineage.response_blob_id AND response.scope_id=lineage.scope_id
JOIN ` + blobs + ` settings ON settings.blob_id=lineage.settings_patch_blob_id AND settings.scope_id=lineage.scope_id
LEFT JOIN ` + blobs + ` snapshot ON snapshot.blob_id=lineage.materialized_snapshot_blob_id AND snapshot.scope_id=lineage.scope_id
ORDER BY lineage.hops`
	rows, err := repository.Pool.Query(ctx, query, checkpointID, scope, maxRows)
	if err != nil {
		return nil, redactPostgresError(fmt.Errorf("read PostgreSQL checkpoint lineage: %w", err))
	}
	defer rows.Close()
	result := make([]state.DurableCheckpoint, 0, min(maxRows, 16))
	for rows.Next() {
		var checkpoint state.DurableCheckpoint
		var rowID, rowScope, originOperation, deltaID, responseID, settingsID uuid.UUID
		var parentID, cacheID, snapshotID, compactedID *uuid.UUID
		var publicHMAC, lineageDigest, settingsDigest, frontierDigest []byte
		var deltaDigest, responseDigest, patchDigest, snapshotDigest []byte
		var deltaLength, responseLength, patchLength, snapshotLength int64
		var deltaMedia, responseMedia, patchMedia, snapshotMedia string
		if err := rows.Scan(
			&rowID, &rowScope, &publicHMAC, &checkpoint.HandleKeyID, &parentID,
			&checkpoint.Kind, &checkpoint.Depth, &originOperation, &cacheID,
			&deltaID, &responseID, &settingsID, &snapshotID, &lineageDigest,
			&settingsDigest, &frontierDigest, &checkpoint.SchemaVersion,
			&checkpoint.CompilerEpoch, &checkpoint.CompactionPolicyVersion,
			&checkpoint.CompactionPromptVersion, &compactedID,
			&checkpoint.CreatedAt, &checkpoint.ExpiresAt,
			&deltaDigest, &deltaLength, &deltaMedia,
			&responseDigest, &responseLength, &responseMedia,
			&patchDigest, &patchLength, &patchMedia,
			&snapshotDigest, &snapshotLength, &snapshotMedia,
		); err != nil {
			return nil, redactPostgresError(fmt.Errorf("scan PostgreSQL checkpoint lineage: %w", err))
		}
		if rowScope != scope {
			return nil, state.ErrTenantMismatch
		}
		if len(publicHMAC) != keyDigestBytes || len(lineageDigest) != keyDigestBytes ||
			len(settingsDigest) != keyDigestBytes || len(frontierDigest) != keyDigestBytes ||
			len(deltaDigest) != keyDigestBytes || len(responseDigest) != keyDigestBytes ||
			len(patchDigest) != keyDigestBytes {
			return nil, errors.New("PostgreSQL checkpoint lineage digest has invalid length")
		}
		checkpoint.ID = state.CheckpointID(rowID.String())
		checkpoint.ScopeID = rowScope.String()
		checkpoint.OriginOperationID = state.OperationID(originOperation.String())
		copy(checkpoint.PublicIDHMAC[:], publicHMAC)
		copy(checkpoint.CanonicalLineageDigest[:], lineageDigest)
		copy(checkpoint.MaterializedSettingsDigest[:], settingsDigest)
		copy(checkpoint.ToolFrontierDigest[:], frontierDigest)
		checkpoint.DeltaBlob = checkpointBlobReference(deltaID, deltaDigest, deltaLength, deltaMedia)
		checkpoint.ResponseBlob = checkpointBlobReference(responseID, responseDigest, responseLength, responseMedia)
		checkpoint.SettingsPatchBlob = checkpointBlobReference(settingsID, patchDigest, patchLength, patchMedia)
		if parentID != nil {
			value := state.CheckpointID(parentID.String())
			checkpoint.ParentID = &value
		}
		if cacheID != nil {
			value := state.CacheEntryID(cacheID.String())
			checkpoint.OriginCacheEntryID = &value
		}
		if compactedID != nil {
			value := state.CheckpointID(compactedID.String())
			checkpoint.CompactedThroughID = &value
		}
		if snapshotID != nil {
			if len(snapshotDigest) != keyDigestBytes {
				return nil, errors.New("PostgreSQL checkpoint snapshot digest has invalid length")
			}
			value := checkpointBlobReference(*snapshotID, snapshotDigest, snapshotLength, snapshotMedia)
			checkpoint.MaterializedSnapshotBlob = &value
		}
		if err := checkpoint.Validate(repository.clock()); err != nil {
			return nil, fmt.Errorf("validate PostgreSQL checkpoint lineage: %w", err)
		}
		result = append(result, checkpoint)
	}
	if err := rows.Err(); err != nil {
		return nil, redactPostgresError(fmt.Errorf("iterate PostgreSQL checkpoint lineage: %w", err))
	}
	if len(result) == 0 {
		return nil, errors.Join(ErrCheckpointNotFound, state.ErrNotFound)
	}
	return result, nil
}

func checkpointBlobReference(id uuid.UUID, digest []byte, length int64, media string) state.CheckpointBlobReference {
	reference := state.CheckpointBlobReference{
		ID: state.BlobID(id.String()), ByteLength: length, MediaType: media,
	}
	copy(reference.Digest[:], digest)
	return reference
}

func (repository DurableCheckpointRepository) readBlob(ctx context.Context, scope, id uuid.UUID, label string) (state.CheckpointBlobReference, error) {
	relation, err := repository.relation("blobs")
	if err != nil {
		return state.CheckpointBlobReference{}, err
	}
	var digest []byte
	var reference state.CheckpointBlobReference
	if err := repository.Pool.QueryRow(ctx, "SELECT blob_id, sha256, byte_length, media_type FROM "+relation+" WHERE blob_id=$1 AND scope_id=$2", id, scope).Scan(&id, &digest, &reference.ByteLength, &reference.MediaType); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return state.CheckpointBlobReference{}, fmt.Errorf("checkpoint %s blob is missing", label)
		}
		return state.CheckpointBlobReference{}, redactPostgresError(fmt.Errorf("read PostgreSQL checkpoint %s blob: %w", label, err))
	}
	if len(digest) != keyDigestBytes {
		return state.CheckpointBlobReference{}, fmt.Errorf("checkpoint %s blob digest has invalid length", label)
	}
	reference.ID = state.BlobID(id.String())
	copy(reference.Digest[:], digest)
	return reference, nil
}

// ensureCheckpointScope protects lineage references on reads as well as
// publication. The foreign key proves existence but, because scope_id is not
// duplicated in that key, it cannot prove tenant ownership.
func (repository DurableCheckpointRepository) ensureCheckpointScope(ctx context.Context, scope, checkpoint uuid.UUID, label string) error {
	relation, err := repository.relation("conversation_checkpoints")
	if err != nil {
		return err
	}
	var checkpointScope uuid.UUID
	if err := repository.Pool.QueryRow(ctx, "SELECT scope_id FROM "+relation+" WHERE checkpoint_id=$1", checkpoint).Scan(&checkpointScope); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("checkpoint %s does not exist", label)
		}
		return redactPostgresError(fmt.Errorf("check PostgreSQL %s scope: %w", label, err))
	}
	if checkpointScope != scope {
		return fmt.Errorf("checkpoint %s belongs to a different scope", label)
	}
	return nil
}

func (repository DurableCheckpointRepository) readProviderState(ctx context.Context, scope uuid.UUID, checkpointID state.CheckpointID) ([]state.CheckpointProviderState, error) {
	relation, err := repository.relation("checkpoint_provider_state")
	if err != nil {
		return nil, err
	}
	id, err := parseCheckpointUUID(checkpointID, "ID")
	if err != nil {
		return nil, err
	}
	rows, err := repository.Pool.Query(ctx, "SELECT ordinal, provider, endpoint_id, endpoint_account_hmac, region, endpoint_family, model_lineage, state_kind, state_blob_id, state_digest, required, immutable_fork_safe, created_at, expires_at FROM "+relation+" WHERE checkpoint_id=$1 ORDER BY ordinal", id)
	if err != nil {
		return nil, redactPostgresError(fmt.Errorf("read PostgreSQL checkpoint provider state: %w", err))
	}
	defer rows.Close()
	var result []state.CheckpointProviderState
	for rows.Next() {
		var value state.CheckpointProviderState
		var endpointHMAC, digest []byte
		var blobID uuid.UUID
		if err := rows.Scan(&value.Ordinal, &value.Provider, &value.EndpointID, &endpointHMAC, &value.Region, &value.EndpointFamily, &value.ModelLineage, &value.StateKind, &blobID, &digest, &value.Required, &value.ImmutableForkSafe, &value.CreatedAt, &value.ExpiresAt); err != nil {
			return nil, redactPostgresError(fmt.Errorf("scan PostgreSQL checkpoint provider state: %w", err))
		}
		if len(endpointHMAC) != keyDigestBytes || len(digest) != keyDigestBytes {
			return nil, errors.New("PostgreSQL checkpoint provider state digest has invalid length")
		}
		copy(value.EndpointAccountHMAC[:], endpointHMAC)
		copy(value.StateDigest[:], digest)
		value.StateBlob, err = repository.readBlob(ctx, scope, blobID, "provider state")
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, redactPostgresError(fmt.Errorf("iterate PostgreSQL checkpoint provider state: %w", err))
	}
	return result, nil
}

func (repository DurableCheckpointRepository) readAffinities(ctx context.Context, scope uuid.UUID, checkpointID state.CheckpointID) (state.ProviderCacheAffinitySet, error) {
	relation, err := repository.relation("checkpoint_provider_affinities")
	if err != nil {
		return nil, err
	}
	id, err := parseCheckpointUUID(checkpointID, "ID")
	if err != nil {
		return nil, err
	}
	rows, err := repository.Pool.Query(ctx, "SELECT affinity_rank, provider, route_id, endpoint_id, endpoint_account_hmac, region, endpoint_family, model_lineage, route_model_revision, provider_cache_key_hmac, cache_epoch, hard_pinned, observed_cache_read_tokens, observed_cache_write_tokens, last_success_at, expires_at FROM "+relation+" WHERE checkpoint_id=$1 ORDER BY affinity_rank", id)
	if err != nil {
		return nil, redactPostgresError(fmt.Errorf("read PostgreSQL checkpoint affinities: %w", err))
	}
	defer rows.Close()
	var result state.ProviderCacheAffinitySet
	for rows.Next() {
		var value state.ProviderCacheAffinity
		var endpointHMAC, cacheHMAC []byte
		if err := rows.Scan(&value.Rank, &value.Provider, &value.RouteID, &value.EndpointID, &endpointHMAC, &value.Region, &value.EndpointFamily, &value.ModelLineage, &value.RouteModelRevision, &cacheHMAC, &value.CacheEpoch, &value.HardPinned, &value.ObservedCacheReadTokens, &value.ObservedCacheWriteTokens, &value.LastSuccessAt, &value.ExpiresAt); err != nil {
			return nil, redactPostgresError(fmt.Errorf("scan PostgreSQL checkpoint affinity: %w", err))
		}
		if len(endpointHMAC) != keyDigestBytes || (len(cacheHMAC) != 0 && len(cacheHMAC) != keyDigestBytes) {
			return nil, errors.New("PostgreSQL checkpoint affinity HMAC has invalid length")
		}
		copy(value.EndpointAccountHMAC[:], endpointHMAC)
		if len(cacheHMAC) == keyDigestBytes {
			copy(value.ProviderCacheKeyHMAC[:], cacheHMAC)
			value.HasProviderCacheKey = true
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, redactPostgresError(fmt.Errorf("iterate PostgreSQL checkpoint affinities: %w", err))
	}
	return result, nil
}

// BeginCheckpoint opens a transaction with UTC timestamps and synchronous
// commit. The returned unit owns the transaction until Commit or Rollback.
func (repository DurableCheckpointRepository) BeginCheckpoint(ctx context.Context) (state.CheckpointUnitOfWork, error) {
	if err := repository.validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, errors.New("checkpoint transaction context is nil")
	}
	tx, err := repository.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite, DeferrableMode: pgx.NotDeferrable})
	if err != nil {
		return nil, redactPostgresError(fmt.Errorf("begin PostgreSQL checkpoint transaction: %w", err))
	}
	if _, err := tx.Exec(ctx, "SET LOCAL TIME ZONE 'UTC'"); err != nil {
		_ = tx.Rollback(context.Background())
		return nil, redactPostgresError(fmt.Errorf("set PostgreSQL checkpoint timezone: %w", err))
	}
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = 'on'"); err != nil {
		_ = tx.Rollback(context.Background())
		return nil, redactPostgresError(fmt.Errorf("set PostgreSQL checkpoint durability: %w", err))
	}
	return &checkpointUnitOfWork{tx: tx, namespace: repository.Namespace, now: repository.clock}, nil
}

type checkpointUnitOfWork struct {
	tx        pgx.Tx
	namespace Namespace
	now       func() time.Time
	closed    bool
}

func (unit *checkpointUnitOfWork) PutCheckpoint(ctx context.Context, write state.CheckpointWrite) error {
	if unit == nil || unit.tx == nil || unit.closed {
		return errors.New("checkpoint unit of work is closed")
	}
	if ctx == nil {
		return errors.New("checkpoint publication context is nil")
	}
	if err := write.Validate(unit.now()); err != nil {
		return err
	}
	checkpoint := write.Checkpoint
	id, err := parseCheckpointUUID(checkpoint.ID, "ID")
	if err != nil {
		return err
	}
	scope, err := parseScopeUUID(checkpoint.ScopeID)
	if err != nil {
		return err
	}
	var parentID *uuid.UUID
	if checkpoint.ParentID != nil {
		parsed, parseErr := parseCheckpointUUID(*checkpoint.ParentID, "parent ID")
		if parseErr != nil {
			return parseErr
		}
		parentID = &parsed
	}
	var cacheID *uuid.UUID
	if checkpoint.OriginCacheEntryID != nil {
		parsed, parseErr := uuid.Parse(string(*checkpoint.OriginCacheEntryID))
		if parseErr != nil || parsed == uuid.Nil {
			return errors.New("checkpoint origin cache entry ID must be a UUID")
		}
		cacheID = &parsed
	}
	var compactedID *uuid.UUID
	if checkpoint.CompactedThroughID != nil {
		parsed, parseErr := parseCheckpointUUID(*checkpoint.CompactedThroughID, "compacted-through ID")
		if parseErr != nil {
			return parseErr
		}
		compactedID = &parsed
	}
	deltaID, err := parseBlobUUID(checkpoint.DeltaBlob.ID, "delta")
	if err != nil {
		return err
	}
	responseID, err := parseBlobUUID(checkpoint.ResponseBlob.ID, "response")
	if err != nil {
		return err
	}
	settingsID, err := parseBlobUUID(checkpoint.SettingsPatchBlob.ID, "settings patch")
	if err != nil {
		return err
	}
	var snapshotID *uuid.UUID
	if checkpoint.MaterializedSnapshotBlob != nil {
		parsed, parseErr := parseBlobUUID(checkpoint.MaterializedSnapshotBlob.ID, "materialized snapshot")
		if parseErr != nil {
			return parseErr
		}
		snapshotID = &parsed
	}
	// Foreign keys constrain blob identity but not scope. Check every blob so
	// a checkpoint cannot reference a different tenant's content.
	for name, reference := range map[string]state.CheckpointBlobReference{
		"delta": checkpoint.DeltaBlob, "response": checkpoint.ResponseBlob, "settings patch": checkpoint.SettingsPatchBlob,
	} {
		if err := unit.ensureBlob(ctx, scope, reference, name); err != nil {
			return err
		}
	}
	if checkpoint.MaterializedSnapshotBlob != nil {
		if err := unit.ensureBlob(ctx, scope, *checkpoint.MaterializedSnapshotBlob, "materialized snapshot"); err != nil {
			return err
		}
	}
	if parentID != nil {
		if err := unit.ensureParent(ctx, scope, *parentID, checkpoint.Depth); err != nil {
			return err
		}
	}
	if compactedID != nil {
		if err := unit.ensureCheckpointScope(ctx, scope, *compactedID, "compacted-through checkpoint"); err != nil {
			return err
		}
	}
	originOperationID, err := parseOperationUUID(checkpoint.OriginOperationID)
	if err != nil {
		return err
	}
	if err := unit.ensureOperation(ctx, scope, originOperationID); err != nil {
		return err
	}
	if cacheID != nil {
		if err := unit.ensureCacheEntry(ctx, scope, *cacheID); err != nil {
			return err
		}
	}
	relation, err := unit.namespace.Render("conversation_checkpoints")
	if err != nil {
		return err
	}
	query := "INSERT INTO " + relation + " (checkpoint_id, scope_id, public_id_hmac, handle_key_id, parent_checkpoint_id, checkpoint_kind, depth, origin_operation_id, origin_cache_entry_id, delta_blob_id, response_blob_id, settings_patch_blob_id, materialized_snapshot_blob_id, canonical_lineage_digest, materialized_settings_digest, tool_frontier_digest, schema_version, compiler_epoch, compaction_policy_version, compaction_prompt_version, compacted_through_checkpoint_id, created_at, expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23) ON CONFLICT (checkpoint_id) DO NOTHING"
	command, err := unit.tx.Exec(ctx, query, id, scope, checkpoint.PublicIDHMAC[:], checkpoint.HandleKeyID, parentID, checkpoint.Kind, checkpoint.Depth, originOperationID, cacheID, deltaID, responseID, settingsID, snapshotID, checkpoint.CanonicalLineageDigest[:], checkpoint.MaterializedSettingsDigest[:], checkpoint.ToolFrontierDigest[:], checkpoint.SchemaVersion, checkpoint.CompilerEpoch, checkpoint.CompactionPolicyVersion, checkpoint.CompactionPromptVersion, compactedID, checkpoint.CreatedAt.UTC(), checkpoint.ExpiresAt.UTC())
	if err != nil {
		return redactPostgresError(fmt.Errorf("insert PostgreSQL checkpoint: %w", err))
	}
	if command.RowsAffected() == 0 {
		matches, compareErr := unit.sameCheckpointRow(ctx, scope, checkpoint)
		if compareErr != nil {
			return compareErr
		}
		if matches {
			childrenMatch, childErr := unit.sameCheckpointChildren(ctx, id, checkpoint)
			if childErr != nil {
				return childErr
			}
			if childrenMatch {
				return nil
			}
		}
		return ErrCheckpointConflict
	}
	for _, provider := range checkpoint.ProviderState {
		if err := unit.putProviderState(ctx, scope, id, provider); err != nil {
			return err
		}
	}
	for _, affinity := range checkpoint.Affinities {
		if err := unit.putAffinity(ctx, id, affinity); err != nil {
			return err
		}
	}
	return nil
}

func (unit *checkpointUnitOfWork) ensureParent(ctx context.Context, scope, parent uuid.UUID, depth int32) error {
	relation, err := unit.namespace.Render("conversation_checkpoints")
	if err != nil {
		return err
	}
	var parentScope uuid.UUID
	var parentDepth int32
	if err := unit.tx.QueryRow(ctx, "SELECT scope_id, depth FROM "+relation+" WHERE checkpoint_id=$1", parent).Scan(&parentScope, &parentDepth); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.Join(ErrCheckpointParentNotFound, state.ErrNotFound)
		}
		return redactPostgresError(fmt.Errorf("check PostgreSQL checkpoint parent: %w", err))
	}
	if parentScope != scope {
		return errors.New("checkpoint parent belongs to a different scope")
	}
	if depth != parentDepth+1 {
		return errors.New("checkpoint depth must be parent depth plus one")
	}
	return nil
}

// ensureCheckpointScope closes the schema's intentionally simple UUID foreign
// key by checking that a lineage reference belongs to the publishing scope.
// PostgreSQL can enforce that the referenced checkpoint exists, but the
// checkpoint schema does not duplicate scope_id in the foreign key. Without
// this check, compacted_through_checkpoint_id could point across tenants.
func (unit *checkpointUnitOfWork) ensureCheckpointScope(ctx context.Context, scope, checkpoint uuid.UUID, label string) error {
	relation, err := unit.namespace.Render("conversation_checkpoints")
	if err != nil {
		return err
	}
	var checkpointScope uuid.UUID
	if err := unit.tx.QueryRow(ctx, "SELECT scope_id FROM "+relation+" WHERE checkpoint_id=$1", checkpoint).Scan(&checkpointScope); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("checkpoint %s does not exist", label)
		}
		return redactPostgresError(fmt.Errorf("check PostgreSQL %s scope: %w", label, err))
	}
	if checkpointScope != scope {
		return fmt.Errorf("checkpoint %s belongs to a different scope", label)
	}
	return nil
}

func (unit *checkpointUnitOfWork) ensureOperation(ctx context.Context, scope, operation uuid.UUID) error {
	relation, err := unit.namespace.Render("operations")
	if err != nil {
		return err
	}
	var operationScope uuid.UUID
	if err := unit.tx.QueryRow(ctx, "SELECT scope_id FROM "+relation+" WHERE operation_id=$1", operation).Scan(&operationScope); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.New("checkpoint origin operation does not exist")
		}
		return redactPostgresError(fmt.Errorf("check PostgreSQL checkpoint operation: %w", err))
	}
	if operationScope != scope {
		return errors.New("checkpoint origin operation belongs to a different scope")
	}
	return nil
}

func (unit *checkpointUnitOfWork) ensureCacheEntry(ctx context.Context, scope, cache uuid.UUID) error {
	relation, err := unit.namespace.Render("response_cache_entries")
	if err != nil {
		return err
	}
	var cacheScope uuid.UUID
	if err := unit.tx.QueryRow(ctx, "SELECT scope_id FROM "+relation+" WHERE cache_entry_id=$1", cache).Scan(&cacheScope); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.New("checkpoint origin cache entry does not exist")
		}
		return redactPostgresError(fmt.Errorf("check PostgreSQL checkpoint cache entry: %w", err))
	}
	if cacheScope != scope {
		return errors.New("checkpoint origin cache entry belongs to a different scope")
	}
	return nil
}

// sameCheckpointRow makes retries idempotent without adding a second mutable
// digest column to the established schema. Child rows are inserted only after
// this comparison, so a matching retry leaves the immutable graph untouched.
func (unit *checkpointUnitOfWork) sameCheckpointRow(ctx context.Context, scope uuid.UUID, checkpoint state.DurableCheckpoint) (bool, error) {
	relation, err := unit.namespace.Render("conversation_checkpoints")
	if err != nil {
		return false, err
	}
	id, err := parseCheckpointUUID(checkpoint.ID, "ID")
	if err != nil {
		return false, err
	}
	originOperationID, err := parseOperationUUID(checkpoint.OriginOperationID)
	if err != nil {
		return false, err
	}
	var publicHMAC, lineage, settings, frontier []byte
	var storedScope uuid.UUID
	var handleKey, kind, compiler string
	var parent, originCache, delta, response, patch, snapshot, compacted *uuid.UUID
	var depth, schema int32
	var operation uuid.UUID
	var policy, prompt *string
	var created, expires time.Time
	err = unit.tx.QueryRow(ctx, "SELECT scope_id, public_id_hmac, handle_key_id, parent_checkpoint_id, checkpoint_kind, depth, origin_operation_id, origin_cache_entry_id, delta_blob_id, response_blob_id, settings_patch_blob_id, materialized_snapshot_blob_id, canonical_lineage_digest, materialized_settings_digest, tool_frontier_digest, schema_version, compiler_epoch, compaction_policy_version, compaction_prompt_version, compacted_through_checkpoint_id, created_at, expires_at FROM "+relation+" WHERE checkpoint_id=$1", id).Scan(&storedScope, &publicHMAC, &handleKey, &parent, &kind, &depth, &operation, &originCache, &delta, &response, &patch, &snapshot, &lineage, &settings, &frontier, &schema, &compiler, &policy, &prompt, &compacted, &created, &expires)
	if err != nil {
		return false, redactPostgresError(fmt.Errorf("compare PostgreSQL checkpoint retry: %w", err))
	}
	checks := []struct {
		ok bool
	}{
		{storedScope == scope}, {string(checkpoint.Kind) == kind}, {checkpoint.Depth == depth},
		{checkpoint.HandleKeyID == handleKey}, {checkpoint.SchemaVersion == int(schema)}, {checkpoint.CompilerEpoch == compiler},
		{sameDatabaseInstant(created, checkpoint.CreatedAt)}, {sameDatabaseInstant(expires, checkpoint.ExpiresAt)},
		{bytesEqual(publicHMAC, checkpoint.PublicIDHMAC[:])}, {bytesEqual(lineage, checkpoint.CanonicalLineageDigest[:])},
		{bytesEqual(settings, checkpoint.MaterializedSettingsDigest[:])}, {bytesEqual(frontier, checkpoint.ToolFrontierDigest[:])},
		{operation == originOperationID}, {optionalUUIDEqual(parent, checkpoint.ParentID)},
		{optionalUUIDEqual(originCache, checkpoint.OriginCacheEntryID)}, {optionalUUIDEqual(delta, checkpoint.DeltaBlob.ID)},
		{optionalUUIDEqual(response, checkpoint.ResponseBlob.ID)}, {optionalUUIDEqual(patch, checkpoint.SettingsPatchBlob.ID)},
		{optionalUUIDEqual(snapshot, optionalBlobID(checkpoint.MaterializedSnapshotBlob))}, {optionalUUIDEqual(compacted, checkpoint.CompactedThroughID)},
		{optionalStringEqual(policy, checkpoint.CompactionPolicyVersion)}, {optionalStringEqual(prompt, checkpoint.CompactionPromptVersion)},
	}
	for _, check := range checks {
		if !check.ok {
			return false, nil
		}
	}
	return true, nil
}

func sameDatabaseInstant(left, right time.Time) bool {
	return left.UTC().Truncate(time.Microsecond).Equal(right.UTC().Truncate(time.Microsecond))
}

func optionalStringEqual(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func optionalUUIDEqual(left *uuid.UUID, right interface{}) bool {
	if left == nil {
		switch typed := right.(type) {
		case nil:
			return true
		case *state.CheckpointID:
			return typed == nil
		case *state.CacheEntryID:
			return typed == nil
		case *state.BlobID:
			return typed == nil
		default:
			return false
		}
	}
	var value string
	switch typed := right.(type) {
	case *state.CheckpointID:
		if typed == nil {
			return false
		}
		value = string(*typed)
	case *state.CacheEntryID:
		if typed == nil {
			return false
		}
		value = string(*typed)
	case state.BlobID:
		value = string(typed)
	case *state.BlobID:
		if typed == nil {
			return false
		}
		value = string(*typed)
	default:
		return false
	}
	parsed, err := uuid.Parse(value)
	return err == nil && parsed == *left
}

func optionalBlobID(reference *state.CheckpointBlobReference) interface{} {
	if reference == nil {
		return nil
	}
	return reference.ID
}

func (unit *checkpointUnitOfWork) sameCheckpointChildren(ctx context.Context, checkpointID uuid.UUID, checkpoint state.DurableCheckpoint) (bool, error) {
	providerRelation, err := unit.namespace.Render("checkpoint_provider_state")
	if err != nil {
		return false, err
	}
	rows, err := unit.tx.Query(ctx, "SELECT ordinal, provider, endpoint_id, endpoint_account_hmac, region, endpoint_family, model_lineage, state_kind, state_blob_id, state_digest, required, immutable_fork_safe, created_at, expires_at FROM "+providerRelation+" WHERE checkpoint_id=$1 ORDER BY ordinal", checkpointID)
	if err != nil {
		return false, redactPostgresError(fmt.Errorf("compare PostgreSQL checkpoint provider state retry: %w", err))
	}
	for _, expected := range checkpoint.ProviderState {
		if !rows.Next() {
			rows.Close()
			return false, rows.Err()
		}
		var ordinal int
		var provider, endpointID, region, endpointFamily, modelLineage, stateKind string
		var endpointHMAC, digest []byte
		var blobID uuid.UUID
		var required, forkSafe bool
		var created time.Time
		var expires *time.Time
		if err := rows.Scan(&ordinal, &provider, &endpointID, &endpointHMAC, &region, &endpointFamily, &modelLineage, &stateKind, &blobID, &digest, &required, &forkSafe, &created, &expires); err != nil {
			rows.Close()
			return false, redactPostgresError(fmt.Errorf("scan PostgreSQL checkpoint provider state retry: %w", err))
		}
		if ordinal != expected.Ordinal || provider != expected.Provider || endpointID != expected.EndpointID || !bytesEqual(endpointHMAC, expected.EndpointAccountHMAC[:]) || region != expected.Region || endpointFamily != expected.EndpointFamily || modelLineage != expected.ModelLineage || stateKind != expected.StateKind || blobID.String() != string(expected.StateBlob.ID) || !bytesEqual(digest, expected.StateDigest[:]) || required != expected.Required || forkSafe != expected.ImmutableForkSafe || !sameDatabaseInstant(created, expected.CreatedAt) || !optionalTimeEqual(expires, expected.ExpiresAt) {
			rows.Close()
			return false, nil
		}
	}
	if rows.Next() {
		rows.Close()
		return false, nil
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, redactPostgresError(fmt.Errorf("iterate PostgreSQL checkpoint provider state retry: %w", err))
	}
	rows.Close()
	affinityRelation, err := unit.namespace.Render("checkpoint_provider_affinities")
	if err != nil {
		return false, err
	}
	rows, err = unit.tx.Query(ctx, "SELECT affinity_rank, provider, route_id, endpoint_id, endpoint_account_hmac, region, endpoint_family, model_lineage, route_model_revision, provider_cache_key_hmac, cache_epoch, hard_pinned, observed_cache_read_tokens, observed_cache_write_tokens, last_success_at, expires_at FROM "+affinityRelation+" WHERE checkpoint_id=$1 ORDER BY affinity_rank", checkpointID)
	if err != nil {
		return false, redactPostgresError(fmt.Errorf("compare PostgreSQL checkpoint affinity retry: %w", err))
	}
	for _, expected := range checkpoint.Affinities {
		if !rows.Next() {
			rows.Close()
			return false, rows.Err()
		}
		var rank int
		var provider, routeID, endpointID, region, endpointFamily, modelLineage, routeRevision, cacheEpoch string
		var endpointHMAC, cacheHMAC []byte
		var hardPinned bool
		var readTokens, writeTokens int64
		var lastSuccess time.Time
		var expires *time.Time
		if err := rows.Scan(&rank, &provider, &routeID, &endpointID, &endpointHMAC, &region, &endpointFamily, &modelLineage, &routeRevision, &cacheHMAC, &cacheEpoch, &hardPinned, &readTokens, &writeTokens, &lastSuccess, &expires); err != nil {
			rows.Close()
			return false, redactPostgresError(fmt.Errorf("scan PostgreSQL checkpoint affinity retry: %w", err))
		}
		cacheMatches := (!expected.HasProviderCacheKey && len(cacheHMAC) == 0) || (expected.HasProviderCacheKey && bytesEqual(cacheHMAC, expected.ProviderCacheKeyHMAC[:]))
		if rank != expected.Rank || provider != expected.Provider || routeID != expected.RouteID || endpointID != expected.EndpointID || !bytesEqual(endpointHMAC, expected.EndpointAccountHMAC[:]) || region != expected.Region || endpointFamily != expected.EndpointFamily || modelLineage != expected.ModelLineage || routeRevision != expected.RouteModelRevision || !cacheMatches || cacheEpoch != expected.CacheEpoch || hardPinned != expected.HardPinned || readTokens != expected.ObservedCacheReadTokens || writeTokens != expected.ObservedCacheWriteTokens || !sameDatabaseInstant(lastSuccess, expected.LastSuccessAt) || !optionalTimeEqual(expires, expected.ExpiresAt) {
			rows.Close()
			return false, nil
		}
	}
	if rows.Next() {
		rows.Close()
		return false, nil
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, redactPostgresError(fmt.Errorf("iterate PostgreSQL checkpoint affinities retry: %w", err))
	}
	rows.Close()
	return true, nil
}

func optionalTimeEqual(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return sameDatabaseInstant(*left, *right)
}

func (unit *checkpointUnitOfWork) ensureBlob(ctx context.Context, scope uuid.UUID, reference state.CheckpointBlobReference, label string) error {
	id, err := parseBlobUUID(reference.ID, label)
	if err != nil {
		return err
	}
	relation, err := unit.namespace.Render("blobs")
	if err != nil {
		return err
	}
	var digest []byte
	var length int64
	var mediaType string
	if err := unit.tx.QueryRow(ctx, "SELECT sha256, byte_length, media_type FROM "+relation+" WHERE blob_id=$1 AND scope_id=$2", id, scope).Scan(&digest, &length, &mediaType); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("checkpoint %s blob is not durable in scope", label)
		}
		return redactPostgresError(fmt.Errorf("check PostgreSQL checkpoint %s blob: %w", label, err))
	}
	if len(digest) != keyDigestBytes || !bytesEqual(digest, reference.Digest[:]) || length != reference.ByteLength || mediaType != reference.MediaType {
		return fmt.Errorf("checkpoint %s blob metadata does not match publication", label)
	}
	return nil
}

func bytesEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (unit *checkpointUnitOfWork) putProviderState(ctx context.Context, scope, checkpointID uuid.UUID, value state.CheckpointProviderState) error {
	if err := unit.ensureBlob(ctx, scope, value.StateBlob, "provider state"); err != nil {
		return err
	}
	blobID, err := parseBlobUUID(value.StateBlob.ID, "provider state")
	if err != nil {
		return err
	}
	relation, err := unit.namespace.Render("checkpoint_provider_state")
	if err != nil {
		return err
	}
	_, err = unit.tx.Exec(ctx, "INSERT INTO "+relation+" (checkpoint_id, ordinal, provider, endpoint_id, endpoint_account_hmac, region, endpoint_family, model_lineage, state_kind, state_blob_id, state_digest, required, immutable_fork_safe, created_at, expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)", checkpointID, value.Ordinal, value.Provider, value.EndpointID, value.EndpointAccountHMAC[:], value.Region, value.EndpointFamily, value.ModelLineage, value.StateKind, blobID, value.StateDigest[:], value.Required, value.ImmutableForkSafe, value.CreatedAt.UTC(), value.ExpiresAt)
	if err != nil {
		return redactPostgresError(fmt.Errorf("insert PostgreSQL checkpoint provider state: %w", err))
	}
	return nil
}

func (unit *checkpointUnitOfWork) putAffinity(ctx context.Context, checkpointID uuid.UUID, value state.ProviderCacheAffinity) error {
	var cacheHMAC []byte
	if value.HasProviderCacheKey {
		cacheHMAC = value.ProviderCacheKeyHMAC[:]
	}
	relation, err := unit.namespace.Render("checkpoint_provider_affinities")
	if err != nil {
		return err
	}
	_, err = unit.tx.Exec(ctx, "INSERT INTO "+relation+" (checkpoint_id, affinity_rank, provider, route_id, endpoint_id, endpoint_account_hmac, region, endpoint_family, model_lineage, route_model_revision, provider_cache_key_hmac, cache_epoch, hard_pinned, observed_cache_read_tokens, observed_cache_write_tokens, last_success_at, expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)", checkpointID, value.Rank, value.Provider, value.RouteID, value.EndpointID, value.EndpointAccountHMAC[:], value.Region, value.EndpointFamily, value.ModelLineage, value.RouteModelRevision, cacheHMAC, value.CacheEpoch, value.HardPinned, value.ObservedCacheReadTokens, value.ObservedCacheWriteTokens, value.LastSuccessAt.UTC(), value.ExpiresAt)
	if err != nil {
		return redactPostgresError(fmt.Errorf("insert PostgreSQL checkpoint affinity: %w", err))
	}
	return nil
}

func (unit *checkpointUnitOfWork) Commit(ctx context.Context) error {
	if unit == nil || unit.tx == nil || unit.closed {
		return errors.New("checkpoint unit of work is closed")
	}
	if ctx == nil {
		return errors.New("checkpoint commit context is nil")
	}
	if err := unit.tx.Commit(ctx); err != nil {
		return redactPostgresError(fmt.Errorf("commit PostgreSQL checkpoint transaction: %w", err))
	}
	unit.closed = true
	return nil
}

func (unit *checkpointUnitOfWork) Rollback(ctx context.Context) error {
	if unit == nil || unit.tx == nil || unit.closed {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	err := unit.tx.Rollback(ctx)
	unit.closed = true
	if err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return redactPostgresError(fmt.Errorf("rollback PostgreSQL checkpoint transaction: %w", err))
	}
	return nil
}
