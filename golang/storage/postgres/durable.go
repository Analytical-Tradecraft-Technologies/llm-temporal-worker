package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/blob"
)

// DurableRepositories is the immutable PostgreSQL repository bundle owned by
// one worker configuration snapshot. It exposes repositories rather than a
// pool so runtime composition cannot silently select a different namespace or
// keyring for one phase.
type DurableRepositories struct {
	Scopes      ScopeRepository
	Operations  OperationRepository
	Blobs       BlobRepository
	Checkpoints DurableCheckpointRepository
	Journal     BudgetJournalRepository
}

// DurableRepositoryOptions binds the PostgreSQL namespace, cryptographic
// identities, retention, and clock used by a single runtime snapshot.
type DurableRepositoryOptions struct {
	Pool               *pgxpool.Pool
	Namespace          Namespace
	EnvelopeKeys       Keyring
	ScopeKeys          ScopeKeyring
	OperationRetention time.Duration
	Clock              func() time.Time
}

func NewDurableRepositories(options DurableRepositoryOptions) (DurableRepositories, error) {
	if options.Pool == nil {
		return DurableRepositories{}, errors.New("durable PostgreSQL pool is required")
	}
	if err := options.Namespace.Validate(); err != nil {
		return DurableRepositories{}, err
	}
	if options.Clock == nil {
		return DurableRepositories{}, errors.New("durable PostgreSQL clock is required")
	}
	if options.OperationRetention <= 0 {
		return DurableRepositories{}, errors.New("durable PostgreSQL operation retention must be positive")
	}
	scopes := DefaultScopeRepository(options.Pool, options.Namespace, options.ScopeKeys)
	operations := DefaultOperationRepository(options.Pool, options.Namespace, options.EnvelopeKeys, scopes)
	operations.Retention = options.OperationRetention
	operations.Now = options.Clock
	blobs := BlobRepository{Pool: options.Pool, Namespace: options.Namespace, Keys: options.EnvelopeKeys, NewID: UUIDv7}
	checkpoints := DurableCheckpointRepository{Pool: options.Pool, Namespace: options.Namespace, Now: options.Clock}
	journal := BudgetJournalRepository{Pool: options.Pool, Namespace: options.Namespace}
	if err := scopes.validate(); err != nil {
		return DurableRepositories{}, fmt.Errorf("validate durable scope repository: %w", err)
	}
	if err := operations.validate(); err != nil {
		return DurableRepositories{}, fmt.Errorf("validate durable operation repository: %w", err)
	}
	if err := blobs.validate(); err != nil {
		return DurableRepositories{}, fmt.Errorf("validate durable blob repository: %w", err)
	}
	return DurableRepositories{Scopes: scopes, Operations: operations, Blobs: blobs, Checkpoints: checkpoints, Journal: journal}, nil
}

// CheckpointBlobLocator resolves only metadata belonging to the supplied
// PostgreSQL scope and decrypts the opaque object-store reference with the
// snapshot-owned envelope keyring. It never derives a locator from Activity
// input and rejects malformed or copied metadata.
func CheckpointBlobLocator(repository BlobRepository) state.BlobLocator {
	return func(ctx context.Context, scopeID string, blobID state.BlobID) (blob.Ref, error) {
		if ctx == nil {
			return blob.Ref{}, errors.New("checkpoint blob context is nil")
		}
		if err := ctx.Err(); err != nil {
			return blob.Ref{}, err
		}
		scope, err := uuid.Parse(scopeID)
		if err != nil || scope == uuid.Nil || scope.String() != scopeID {
			return blob.Ref{}, errors.New("checkpoint blob scope is invalid")
		}
		id, err := uuid.Parse(string(blobID))
		if err != nil || id == uuid.Nil || id.String() != string(blobID) {
			return blob.Ref{}, errors.New("checkpoint blob identity is invalid")
		}
		record, err := repository.Get(ctx, scope, id)
		if err != nil {
			return blob.Ref{}, errors.New("checkpoint blob metadata is unavailable")
		}
		locator, err := repository.OpenLocator(ctx, scope, "checkpoint", record)
		if err != nil {
			return blob.Ref{}, errors.New("checkpoint blob locator is unavailable")
		}
		var ref blob.Ref
		if err := json.Unmarshal(locator, &ref); err != nil {
			return blob.Ref{}, errors.New("checkpoint blob locator is invalid")
		}
		if err := ref.Validate(time.Now().UTC()); err != nil {
			return blob.Ref{}, errors.New("checkpoint blob locator is invalid")
		}
		return ref, nil
	}
}
