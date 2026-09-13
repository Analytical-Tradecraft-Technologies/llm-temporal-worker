package postgres

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
	blobstore "github.com/mfow/llm-temporal-worker/golang/storage/blob"
)

// AtomicFinalizationRepository owns the one PostgreSQL commit that publishes
// checkpoint object locators, the immutable checkpoint graph, the result
// reference, and the terminal operation transition.
type AtomicFinalizationRepository struct {
	Blobs       BlobRepository
	Checkpoints DurableCheckpointRepository
	Operations  OperationRepository
	Now         func() time.Time
}

func (repository AtomicFinalizationRepository) validate() error {
	if err := repository.Blobs.validate(); err != nil {
		return err
	}
	if err := repository.Checkpoints.validate(); err != nil {
		return err
	}
	if err := repository.Operations.validate(); err != nil {
		return err
	}
	if repository.Blobs.Pool != repository.Checkpoints.Pool || repository.Blobs.Pool != repository.Operations.Pool {
		return errors.New("atomic finalization repositories do not share one PostgreSQL pool")
	}
	if repository.Blobs.Namespace != repository.Checkpoints.Namespace || repository.Blobs.Namespace != repository.Operations.Namespace {
		return errors.New("atomic finalization repositories do not share one PostgreSQL namespace")
	}
	return nil
}

func (repository AtomicFinalizationRepository) clock() time.Time {
	if repository.Now != nil {
		return repository.Now().UTC()
	}
	return time.Now().UTC()
}

func (repository AtomicFinalizationRepository) Finalize(ctx context.Context, request admission.AtomicFinalization) (state.DurableCheckpoint, error) {
	if err := repository.validate(); err != nil {
		return state.DurableCheckpoint{}, err
	}
	if ctx == nil {
		return state.DurableCheckpoint{}, errors.New("atomic finalization context is nil")
	}
	scopeID, err := parseCheckpointScope(request.ScopeID)
	if err != nil {
		return state.DurableCheckpoint{}, err
	}
	objectCount := 1
	if request.Checkpoint.Depth%16 == 0 || request.Checkpoint.Kind == state.CheckpointCompaction {
		objectCount = 2
	}
	if len(request.CheckpointObjects) != objectCount {
		return state.DurableCheckpoint{}, fmt.Errorf("checkpoint object count is %d, want %d", len(request.CheckpointObjects), objectCount)
	}
	checkpoint := request.Checkpoint
	err = WithTransaction(ctx, repository.Blobs.Pool, func(ctx context.Context, tx pgx.Tx) error {
		references := make([]state.CheckpointBlobReference, 0, len(request.CheckpointObjects))
		for _, ref := range request.CheckpointObjects {
			persisted, err := repository.putCheckpointLocator(ctx, tx, scopeID, ref)
			if err != nil {
				return err
			}
			references = append(references, persisted)
		}
		// Schema v1 keeps three reference columns. A bundle is deliberately
		// bound into all three so old rows remain readable while new rows need
		// one immutable object fetch; equality is the bundle discriminator.
		checkpoint.DeltaBlob = references[0]
		checkpoint.ResponseBlob = references[0]
		checkpoint.SettingsPatchBlob = references[0]
		if len(references) == 2 {
			checkpoint.MaterializedSnapshotBlob = &references[1]
		}
		unit := &checkpointUnitOfWork{tx: tx, namespace: repository.Checkpoints.Namespace, now: repository.Checkpoints.clock}
		if err := unit.PutCheckpoint(ctx, state.CheckpointWrite{Checkpoint: checkpoint}); err != nil {
			return err
		}
		return repository.Operations.completeTx(ctx, tx, request.Complete)
	})
	if err != nil {
		return state.DurableCheckpoint{}, err
	}
	return checkpoint, nil
}

func (repository AtomicFinalizationRepository) putCheckpointLocator(ctx context.Context, tx pgx.Tx, scopeID uuid.UUID, ref blobstore.Ref) (state.CheckpointBlobReference, error) {
	if err := ref.Validate(repository.clock()); err != nil {
		return state.CheckpointBlobReference{}, errors.New("checkpoint blob locator is invalid")
	}
	if !ref.ExpiresAt.IsZero() {
		ref.ExpiresAt = ref.ExpiresAt.UTC().Truncate(time.Microsecond)
	}
	digestBytes, err := hex.DecodeString(ref.Digest)
	if err != nil || len(digestBytes) != keyDigestBytes {
		return state.CheckpointBlobReference{}, errors.New("checkpoint blob digest is invalid")
	}
	var digest [keyDigestBytes]byte
	copy(digest[:], digestBytes)
	encoded, err := json.Marshal(ref)
	if err != nil {
		return state.CheckpointBlobReference{}, errors.New("checkpoint blob locator is invalid")
	}
	canonical, err := llm.CanonicalJSON(encoded)
	if err != nil {
		return state.CheckpointBlobReference{}, errors.New("checkpoint blob locator is invalid")
	}
	var expiresAt *time.Time
	if !ref.ExpiresAt.IsZero() {
		expires := ref.ExpiresAt.UTC()
		expiresAt = &expires
	}
	record, err := repository.Blobs.putLocator(ctx, tx, scopeID, "checkpoint", BlobMetadata{
		StoreID: ref.Store, Digest: digest, ByteLength: ref.ByteLength,
		MediaType: ref.MediaType, ExpiresAt: expiresAt,
	}, canonical)
	if err != nil {
		return state.CheckpointBlobReference{}, errors.New("checkpoint blob locator is unavailable")
	}
	return state.CheckpointBlobReference{
		ID: state.BlobID(record.BlobID.String()), Digest: digest,
		ByteLength: record.ByteLength, MediaType: record.MediaType,
	}, nil
}
