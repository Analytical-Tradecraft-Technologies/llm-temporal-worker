package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type BlobMetadata struct {
	BlobID     uuid.UUID
	ScopeID    uuid.UUID
	StoreID    string
	Digest     [keyDigestBytes]byte
	ByteLength int64
	MediaType  string
	ExpiresAt  *time.Time
}

type BlobRecord struct {
	BlobMetadata
	LocatorKeyID  string
	Locator       SealedValue
	CreatedAt     time.Time
	DeletionState string
}

type BlobRepository struct {
	Pool      *pgxpool.Pool
	Namespace Namespace
	Keys      Keyring
	NewID     func() (uuid.UUID, error)
}

func (repository BlobRepository) validate() error {
	if repository.Pool == nil {
		return errors.New("blob repository pool is nil")
	}
	if err := repository.Namespace.Validate(); err != nil {
		return err
	}
	if _, _, err := repository.Keys.activeKey(); err != nil {
		return err
	}
	if repository.NewID == nil {
		return errors.New("blob repository UUID generator is nil")
	}
	return nil
}

func (repository BlobRepository) PutLocator(ctx context.Context, scopeID uuid.UUID, payloadKind string, metadata BlobMetadata, locator []byte) (record BlobRecord, err error) {
	if err := repository.validate(); err != nil {
		return BlobRecord{}, err
	}
	err = WithTransaction(ctx, repository.Pool, func(ctx context.Context, tx pgx.Tx) error {
		record, err = repository.putLocator(ctx, tx, scopeID, payloadKind, metadata, locator)
		return err
	})
	if err != nil {
		return BlobRecord{}, err
	}
	return record, nil
}

type blobQueryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (repository BlobRepository) putLocator(ctx context.Context, queryer blobQueryRower, scopeID uuid.UUID, payloadKind string, metadata BlobMetadata, locator []byte) (BlobRecord, error) {
	var record BlobRecord
	if scopeID == uuid.Nil {
		return record, errors.New("blob scope id is required")
	}
	if metadata.StoreID == "" || strings.ContainsAny(metadata.StoreID, "\x00\r\n") {
		return record, errors.New("blob store id is empty or contains control characters")
	}
	if metadata.MediaType == "" || strings.ContainsAny(metadata.MediaType, "\x00\r\n") {
		return record, errors.New("blob media type is empty or contains control characters")
	}
	if metadata.ByteLength < 0 {
		return record, errors.New("blob byte length must not be negative")
	}
	if metadata.BlobID == uuid.Nil {
		var err error
		metadata.BlobID, err = repository.NewID()
		if err != nil {
			return record, fmt.Errorf("generate blob id: %w", err)
		}
	}
	if metadata.Digest == [keyDigestBytes]byte{} {
		return record, errors.New("blob content digest is required")
	}
	envelopeContext := EnvelopeContext{ScopeID: scopeID, OperationID: metadata.BlobID, PayloadKind: payloadKind, Digest: metadata.Digest}
	sealed, err := repository.Keys.Seal(envelopeContext, locator)
	if err != nil {
		return record, err
	}
	relation, err := repository.Namespace.Render("blobs")
	if err != nil {
		return record, err
	}
	query := "INSERT INTO " + relation + " (blob_id, scope_id, store_id, locator_ciphertext, locator_key_id, sha256, byte_length, media_type, encryption_context_digest, expires_at) " +
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) " +
		"ON CONFLICT (scope_id, store_id, sha256, byte_length, media_type) DO UPDATE SET deletion_state = 'retained', " +
		"expires_at = CASE WHEN " + relation + ".expires_at IS NULL OR EXCLUDED.expires_at IS NULL THEN NULL ELSE GREATEST(" + relation + ".expires_at, EXCLUDED.expires_at) END " +
		"WHERE " + relation + ".deletion_state IN ('retained', 'eligible') " +
		"RETURNING blob_id, created_at, deletion_state, locator_key_id, locator_ciphertext, encryption_context_digest, expires_at"
	var returnedContextHash []byte
	if err := queryer.QueryRow(ctx, query,
		metadata.BlobID, scopeID, metadata.StoreID, sealed.Ciphertext, sealed.KeyID,
		metadata.Digest[:], metadata.ByteLength, metadata.MediaType, sealed.ContextHash[:], metadata.ExpiresAt,
	).Scan(&record.BlobID, &record.CreatedAt, &record.DeletionState, &record.LocatorKeyID, &record.Locator.Ciphertext, &returnedContextHash, &record.ExpiresAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return BlobRecord{}, fmt.Errorf("blob content-addressed row is not writable: %w", ErrBlobNotWritable)
		}
		return BlobRecord{}, redactPostgresError(fmt.Errorf("insert PostgreSQL blob metadata: %w", err))
	}
	if len(returnedContextHash) != keyDigestBytes {
		return BlobRecord{}, errors.New("PostgreSQL blob context digest has invalid length")
	}
	copy(record.Locator.ContextHash[:], returnedContextHash)
	record.ScopeID = scopeID
	record.StoreID = metadata.StoreID
	record.Digest = metadata.Digest
	record.ByteLength = metadata.ByteLength
	record.MediaType = metadata.MediaType
	record.Locator.KeyID = record.LocatorKeyID

	// A content-addressed checkpoint reuse keeps the retained row's blob ID.
	// Its locator must therefore be resealed under that authoritative ID when
	// the conflict update changes the locator's embedded expiry. The upsert
	// holds the row lock until the surrounding transaction commits, so expiry
	// and its authenticated locator cannot become visible independently.
	if payloadKind == "checkpoint" && sameBlobExpiry(record.ExpiresAt, metadata.ExpiresAt) &&
		!bytes.Equal(record.Locator.Ciphertext, sealed.Ciphertext) {
		retainedContext := EnvelopeContext{ScopeID: scopeID, OperationID: record.BlobID, PayloadKind: payloadKind, Digest: metadata.Digest}
		retainedSealed, err := repository.Keys.Seal(retainedContext, locator)
		if err != nil {
			return BlobRecord{}, err
		}
		update := "UPDATE " + relation + " SET locator_ciphertext = $1, locator_key_id = $2, encryption_context_digest = $3 " +
			"WHERE blob_id = $4 AND scope_id = $5 AND expires_at IS NOT DISTINCT FROM $6 " +
			"RETURNING locator_key_id, locator_ciphertext, encryption_context_digest"
		returnedContextHash = nil
		if err := queryer.QueryRow(ctx, update,
			retainedSealed.Ciphertext, retainedSealed.KeyID, retainedSealed.ContextHash[:],
			record.BlobID, scopeID, record.ExpiresAt,
		).Scan(&record.LocatorKeyID, &record.Locator.Ciphertext, &returnedContextHash); err != nil {
			return BlobRecord{}, redactPostgresError(fmt.Errorf("refresh PostgreSQL checkpoint blob locator: %w", err))
		}
		if len(returnedContextHash) != keyDigestBytes {
			return BlobRecord{}, errors.New("PostgreSQL blob context digest has invalid length")
		}
		copy(record.Locator.ContextHash[:], returnedContextHash)
		record.Locator.KeyID = record.LocatorKeyID
	}
	return record, nil
}

func sameBlobExpiry(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.UTC().Truncate(time.Microsecond).Equal(right.UTC().Truncate(time.Microsecond))
}

func (repository BlobRepository) OpenLocator(ctx context.Context, scopeID uuid.UUID, payloadKind string, record BlobRecord) ([]byte, error) {
	if err := repository.validate(); err != nil {
		return nil, err
	}
	if record.ScopeID != scopeID || record.BlobID == uuid.Nil {
		return nil, errors.New("blob scope does not match requested scope")
	}
	context := EnvelopeContext{ScopeID: scopeID, OperationID: record.BlobID, PayloadKind: payloadKind, Digest: record.Digest}
	return repository.Keys.Open(context, record.Locator)
}

// Get returns metadata only; opening a locator is an explicit operation so a
// caller cannot accidentally decrypt a blob while holding a SQL transaction.
func (repository BlobRepository) Get(ctx context.Context, scopeID, blobID uuid.UUID) (BlobRecord, error) {
	var record BlobRecord
	if err := repository.validate(); err != nil {
		return record, err
	}
	if scopeID == uuid.Nil || blobID == uuid.Nil {
		return record, errors.New("blob scope and id are required")
	}
	relation, err := repository.Namespace.Render("blobs")
	if err != nil {
		return record, err
	}
	query := "SELECT blob_id, scope_id, store_id, sha256, byte_length, media_type, expires_at, created_at, deletion_state, locator_key_id, locator_ciphertext, encryption_context_digest FROM " + relation + " WHERE blob_id = $1 AND scope_id = $2"
	var digest, ciphertext, contextHash []byte
	if err := repository.Pool.QueryRow(ctx, query, blobID, scopeID).Scan(
		&record.BlobID, &record.ScopeID, &record.StoreID, &digest, &record.ByteLength, &record.MediaType,
		&record.ExpiresAt, &record.CreatedAt, &record.DeletionState, &record.LocatorKeyID, &ciphertext, &contextHash,
	); err != nil {
		return BlobRecord{}, redactPostgresError(fmt.Errorf("get PostgreSQL blob metadata: %w", err))
	}
	if len(digest) != keyDigestBytes || len(contextHash) != keyDigestBytes {
		return BlobRecord{}, errors.New("PostgreSQL blob digest has invalid length")
	}
	copy(record.Digest[:], digest)
	copy(record.Locator.ContextHash[:], contextHash)
	record.Locator.KeyID = record.LocatorKeyID
	record.Locator.Ciphertext = append([]byte(nil), ciphertext...)
	return record, nil
}
