package postgres

// Response-cache persistence is deliberately separate from operation replay.
// A cache entry may serve many distinct operations, but every hit is recorded
// against the consuming operation exactly once.  This package currently
// supports bounded envelope-encrypted inline responses; blob-backed entries
// remain an explicit follow-up so callers cannot accidentally persist an
// unbounded response in PostgreSQL.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// DefaultResponseCacheMaxInlineBytes keeps cache rows bounded while still
	// allowing ordinary normalized responses to avoid an external blob.
	DefaultResponseCacheMaxInlineBytes = 256 << 10
	DefaultResponseCacheMaxAge         = 180 * 24 * time.Hour
	maxInt32                           = int32(^uint32(0) >> 1)
)

var (
	ErrCacheEntryNotFound  = errors.New("response cache entry not found")
	ErrCacheFillBusy       = errors.New("response cache fill lease is held")
	ErrCacheFillNotOwned   = errors.New("response cache fill is not owned by operation")
	ErrCacheFillExpired    = errors.New("response cache fill lease has expired")
	ErrCacheOperationReuse = errors.New("operation already consumed another cache entry")
)

// CacheKey is the indexed, route-isolated identity of a response template.
// Variant is part of the key exactly once and is never sent to a provider.
type CacheKey struct {
	ScopeID                 uuid.UUID
	FingerprintVersion      int32
	SemanticFingerprintHMAC [keyDigestBytes]byte
	RouteIdentityHMAC       [keyDigestBytes]byte
	Variant                 int32
}

func (key CacheKey) validate() error {
	if key.ScopeID == uuid.Nil {
		return errors.New("response cache scope id is required")
	}
	if key.FingerprintVersion <= 0 {
		return errors.New("response cache fingerprint version must be positive")
	}
	if key.SemanticFingerprintHMAC == [keyDigestBytes]byte{} {
		return errors.New("response cache semantic fingerprint is required")
	}
	if key.RouteIdentityHMAC == [keyDigestBytes]byte{} {
		return errors.New("response cache route identity is required")
	}
	if key.Variant < 0 {
		return errors.New("response cache variant must be non-negative")
	}
	return nil
}

// CacheEntry contains metadata only.  Response bytes are returned by Lookup
// after the envelope is authenticated; callers cannot accidentally treat the
// database ciphertext as a provider response.
type CacheEntry struct {
	ID                     uuid.UUID
	Key                    CacheKey
	CanonicalRequestDigest [keyDigestBytes]byte
	CanonicalRequestJSON   []byte
	RouteIdentityHMAC      [keyDigestBytes]byte
	SemanticProfileVersion string
	CacheEpoch             string
	Response               SealedValue
	ResponseDigest         [keyDigestBytes]byte
	OriginOperationID      uuid.UUID
	OriginCheckpointID     uuid.UUID
	OriginProvider         string
	OriginEndpointID       string
	OriginResolvedModel    string
	CreatedAt              time.Time
	CompletedAt            time.Time
	LastUsedAt             time.Time
	UseCount               int32
}

// CacheLookupRequest identifies one opted-in operation consuming a cache
// entry. OperationID must already exist in the operation ledger so the
// response_cache_uses foreign key binds the hit to durable operation state.
type CacheLookupRequest struct {
	Key         CacheKey
	OperationID string
	MaxAge      time.Duration
}

type CacheLookupResult struct {
	Hit      bool
	Entry    CacheEntry
	Response []byte
}

// CacheFillRequest identifies the owner of a fill lease. A lease is acquired
// before the provider call and must be published or failed by that operation.
type CacheFillRequest struct {
	Key         CacheKey
	OperationID string
	Lease       time.Duration
}

type CacheFillStatus string

const (
	CacheFillAcquired CacheFillStatus = "acquired"
	CacheFillExisting CacheFillStatus = "existing"
	CacheFillBusy     CacheFillStatus = "busy"
)

type CacheFillResult struct {
	Status     CacheFillStatus
	EntryID    uuid.UUID
	LeaseUntil time.Time
}

// CachePublishRequest supplies one successful normalized response. The
// response is sealed with the entry identity and its SHA-256 digest, so a
// copied ciphertext cannot be opened under another cache row.
type CachePublishRequest struct {
	Fill                   CacheFillRequest
	CanonicalRequestJSON   []byte
	CanonicalRequestDigest [keyDigestBytes]byte
	SemanticProfileVersion string
	CacheEpoch             string
	OriginOperationID      string
	OriginCheckpointID     uuid.UUID
	OriginProvider         string
	OriginEndpointID       string
	OriginResolvedModel    string
	Response               []byte
	CompletedAt            time.Time
}

type ResponseCacheRepository struct {
	Pool      *pgxpool.Pool
	Namespace Namespace
	Keys      Keyring
	// Observer is optional. It receives bounded cache and PostgreSQL query
	// latency signals after each cache boundary.
	Observer       CacheObserver
	MaxInlineBytes int
	MaxLookupAge   time.Duration
	NewID          func() (uuid.UUID, error)
	Now            func() time.Time
}

func DefaultResponseCacheRepository(pool *pgxpool.Pool, namespace Namespace, keys Keyring) ResponseCacheRepository {
	return ResponseCacheRepository{
		Pool:           pool,
		Namespace:      namespace,
		Keys:           keys,
		MaxInlineBytes: DefaultResponseCacheMaxInlineBytes,
		MaxLookupAge:   DefaultResponseCacheMaxAge,
		NewID:          UUIDv7,
		Now:            time.Now,
	}
}

func (repository ResponseCacheRepository) validate() error {
	if repository.Pool == nil {
		return errors.New("response cache repository pool is nil")
	}
	if err := repository.Namespace.Validate(); err != nil {
		return err
	}
	if _, _, err := repository.Keys.activeKey(); err != nil {
		return err
	}
	if repository.MaxInlineBytes <= 0 || repository.MaxInlineBytes > maxEnvelope {
		return fmt.Errorf("response cache inline limit must be between 1 and %d bytes", maxEnvelope)
	}
	if repository.MaxLookupAge <= 0 {
		return errors.New("response cache maximum age must be positive")
	}
	if repository.NewID == nil {
		return errors.New("response cache UUID generator is nil")
	}
	return nil
}

func (repository ResponseCacheRepository) clock() time.Time {
	if repository.Now != nil {
		return repository.Now().UTC()
	}
	return time.Now().UTC()
}

func operationID(value string) (uuid.UUID, error) {
	if strings.TrimSpace(value) == "" {
		return uuid.Nil, errors.New("operation id is required")
	}
	return operationUUID(value), nil
}

func normalizeCacheManifest(raw []byte) ([]byte, error) {
	if len(raw) == 0 {
		return []byte(`{}`), nil
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("cache request manifest is invalid JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, errors.New("cache request manifest must contain one JSON value")
		}
		return nil, fmt.Errorf("cache request manifest is invalid JSON: %w", err)
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, errors.New("cache request manifest must be a JSON object")
	}
	return json.Marshal(value)
}

type cacheEntryScanner interface {
	Scan(...any) error
}

func scanCacheEntry(row cacheEntryScanner) (CacheEntry, error) {
	var entry CacheEntry
	var semantic, canonical, route, responseDigest []byte
	var keyID string
	var ciphertextBytes []byte
	if err := row.Scan(
		&entry.ID, &entry.Key.ScopeID, &entry.Key.FingerprintVersion, &semantic, &entry.Key.Variant,
		&canonical, &entry.CanonicalRequestJSON, &route, &entry.SemanticProfileVersion, &entry.CacheEpoch,
		&ciphertextBytes, &keyID, &responseDigest, &entry.OriginOperationID, &entry.OriginCheckpointID,
		&entry.OriginProvider, &entry.OriginEndpointID, &entry.OriginResolvedModel, &entry.CreatedAt,
		&entry.CompletedAt, &entry.LastUsedAt, &entry.UseCount,
	); err != nil {
		return CacheEntry{}, err
	}
	if len(semantic) != keyDigestBytes || len(canonical) != keyDigestBytes || len(route) != keyDigestBytes || len(responseDigest) != keyDigestBytes || len(ciphertextBytes) == 0 || keyID == "" {
		return CacheEntry{}, errors.New("PostgreSQL response cache entry has invalid digest or ciphertext length")
	}
	copy(entry.Key.SemanticFingerprintHMAC[:], semantic)
	copy(entry.CanonicalRequestDigest[:], canonical)
	copy(entry.RouteIdentityHMAC[:], route)
	copy(entry.Key.RouteIdentityHMAC[:], route)
	copy(entry.ResponseDigest[:], responseDigest)
	entry.Response = SealedValue{KeyID: keyID, Ciphertext: append([]byte(nil), ciphertextBytes...)}
	contextHash, err := contextDigest(EnvelopeContext{ScopeID: entry.Key.ScopeID, OperationID: entry.ID, PayloadKind: "response-cache", Digest: entry.ResponseDigest})
	if err != nil {
		return CacheEntry{}, err
	}
	entry.Response.ContextHash = contextHash
	return entry, nil
}

const cacheEntrySelect = "cache_entry_id, scope_id, fingerprint_version, semantic_fingerprint_hmac, variant, canonical_request_digest, canonical_request_jsonb, cache_route_identity_hmac, semantic_profile_version, cache_epoch, response_inline_ciphertext, response_key_id, response_digest, origin_operation_id, origin_checkpoint_id, origin_provider, origin_endpoint_id, origin_resolved_model, created_at, completed_at, last_used_at, use_count"

func (repository ResponseCacheRepository) relations() (entries, uses, fills string, err error) {
	if err = repository.validate(); err != nil {
		return "", "", "", err
	}
	if entries, err = repository.Namespace.Render("response_cache_entries"); err != nil {
		return "", "", "", err
	}
	if uses, err = repository.Namespace.Render("response_cache_uses"); err != nil {
		return "", "", "", err
	}
	if fills, err = repository.Namespace.Render("response_cache_fills"); err != nil {
		return "", "", "", err
	}
	return entries, uses, fills, nil
}

// Lookup performs eligibility, decryption, and use accounting in one
// transaction. A retry with the same operation ID inserts no second use and
// does not increment use_count. Staleness is based on completed_at, never on
// last_used_at.
func (repository ResponseCacheRepository) Lookup(ctx context.Context, request CacheLookupRequest) (result CacheLookupResult, returnErr error) {
	started := time.Now()
	useRecorded := false
	defer func() {
		event := "miss"
		if result.Hit {
			event = "hit"
			if returnErr == nil && useRecorded && repository.Observer != nil {
				repository.Observer.RecordCache("use")
			}
		}
		repository.observeCache(event, started, returnErr)
	}()
	if ctx == nil {
		return result, errors.New("response cache lookup context is nil")
	}
	if err := request.Key.validate(); err != nil {
		return result, err
	}
	if request.MaxAge <= 0 || request.MaxAge > repository.MaxLookupAge {
		return result, fmt.Errorf("response cache max age must be between 1ns and %s", repository.MaxLookupAge)
	}
	opID, err := operationID(request.OperationID)
	if err != nil {
		return result, err
	}
	entries, uses, _, err := repository.relations()
	if err != nil {
		return result, err
	}
	return result, WithTransaction(ctx, repository.Pool, func(ctx context.Context, tx pgx.Tx) error {
		var now time.Time
		if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
			return redactPostgresError(fmt.Errorf("read PostgreSQL cache time: %w", err))
		}
		cutoff := now.Add(-request.MaxAge)
		query := "SELECT " + cacheEntrySelect + " FROM " + entries + " WHERE scope_id=$1 AND fingerprint_version=$2 AND semantic_fingerprint_hmac=$3 AND variant=$4 AND cache_route_identity_hmac=$5 AND state='ready' AND completed_at >= $6"
		var entry CacheEntry
		row := tx.QueryRow(ctx, query, request.Key.ScopeID, request.Key.FingerprintVersion, request.Key.SemanticFingerprintHMAC[:], request.Key.Variant, request.Key.RouteIdentityHMAC[:], cutoff)
		if entry, err = scanCacheEntry(row); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			return redactPostgresError(fmt.Errorf("lookup PostgreSQL response cache: %w", err))
		}
		entry.Key = request.Key
		// Authenticate before recording the use. A corrupt or tampered row must
		// fail closed and must not make a broken entry look popular.
		response, err := repository.Keys.Open(EnvelopeContext{ScopeID: request.Key.ScopeID, OperationID: entry.ID, PayloadKind: "response-cache", Digest: entry.ResponseDigest}, entry.Response)
		if err != nil {
			return err
		}
		if len(response) > repository.MaxInlineBytes {
			return errors.New("PostgreSQL response cache entry exceeds configured inline response limit")
		}
		inserted, err := tx.Exec(ctx, "INSERT INTO "+uses+" (cache_entry_id, operation_id, first_used_at) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING", entry.ID, opID, now)
		if err != nil {
			return redactPostgresError(fmt.Errorf("record PostgreSQL response cache use: %w", err))
		}
		useRecorded = inserted.RowsAffected() > 0
		if !useRecorded {
			var usedEntry uuid.UUID
			if err := tx.QueryRow(ctx, "SELECT cache_entry_id FROM "+uses+" WHERE operation_id=$1", opID).Scan(&usedEntry); err != nil {
				return redactPostgresError(fmt.Errorf("check PostgreSQL response cache use: %w", err))
			}
			if usedEntry != entry.ID {
				return ErrCacheOperationReuse
			}
		} else {
			if err := tx.QueryRow(ctx, "UPDATE "+entries+" SET use_count=CASE WHEN use_count >= $2 THEN $2 ELSE use_count + 1 END, last_used_at=$3 WHERE cache_entry_id=$1 AND state='ready' RETURNING use_count, last_used_at", entry.ID, maxInt32, now).Scan(&entry.UseCount, &entry.LastUsedAt); err != nil {
				return redactPostgresError(fmt.Errorf("update PostgreSQL response cache use count: %w", err))
			}
		}
		if inserted.RowsAffected() == 0 {
			if err := tx.QueryRow(ctx, "SELECT use_count, last_used_at FROM "+entries+" WHERE cache_entry_id=$1", entry.ID).Scan(&entry.UseCount, &entry.LastUsedAt); err != nil {
				return redactPostgresError(fmt.Errorf("read PostgreSQL response cache use count: %w", err))
			}
		}
		result.Hit = true
		result.Entry = entry
		result.Response = append([]byte(nil), response...)
		return nil
	})
}

// BeginFill acquires an idempotent, expiring lease. A ready entry wins over a
// stale fill, while an active lease is reported as busy without waiting.
func (repository ResponseCacheRepository) BeginFill(ctx context.Context, request CacheFillRequest) (result CacheFillResult, returnErr error) {
	started := time.Now()
	defer func() {
		event := "error"
		if returnErr == nil {
			switch result.Status {
			case CacheFillAcquired:
				event = "fill"
			case CacheFillExisting:
				event = "fill_existing"
			case CacheFillBusy:
				event = "fill_busy"
			}
		}
		repository.observeCache(event, started, returnErr)
	}()
	if ctx == nil {
		return result, errors.New("response cache fill context is nil")
	}
	if err := request.Key.validate(); err != nil {
		return result, err
	}
	if request.Lease <= 0 || request.Lease > repository.MaxLookupAge {
		return result, fmt.Errorf("response cache fill lease must be between 1ns and %s", repository.MaxLookupAge)
	}
	owner, err := operationID(request.OperationID)
	if err != nil {
		return result, err
	}
	entries, _, fills, err := repository.relations()
	if err != nil {
		return result, err
	}
	return result, WithTransaction(ctx, repository.Pool, func(ctx context.Context, tx pgx.Tx) error {
		var now time.Time
		if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
			return redactPostgresError(fmt.Errorf("read PostgreSQL fill time: %w", err))
		}
		var ready uuid.UUID
		if err := tx.QueryRow(ctx, "SELECT cache_entry_id FROM "+entries+" WHERE scope_id=$1 AND fingerprint_version=$2 AND semantic_fingerprint_hmac=$3 AND variant=$4 AND cache_route_identity_hmac=$5 AND state='ready'", request.Key.ScopeID, request.Key.FingerprintVersion, request.Key.SemanticFingerprintHMAC[:], request.Key.Variant, request.Key.RouteIdentityHMAC[:]).Scan(&ready); err == nil {
			result.Status, result.EntryID = CacheFillExisting, ready
			return nil
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return redactPostgresError(fmt.Errorf("check PostgreSQL response cache entry: %w", err))
		}
		leaseUntil := now.Add(request.Lease)
		var existingOwner uuid.UUID
		var state string
		var existingLease time.Time
		var existingEntry *uuid.UUID
		fillQuery := "SELECT owner_operation_id, state, lease_expires_at, cache_entry_id FROM " + fills + " WHERE scope_id=$1 AND fingerprint_version=$2 AND semantic_fingerprint_hmac=$3 AND variant=$4 AND cache_route_identity_hmac=$5 FOR UPDATE"
		err := tx.QueryRow(ctx, fillQuery, request.Key.ScopeID, request.Key.FingerprintVersion, request.Key.SemanticFingerprintHMAC[:], request.Key.Variant, request.Key.RouteIdentityHMAC[:]).Scan(&existingOwner, &state, &existingLease, &existingEntry)
		if errors.Is(err, pgx.ErrNoRows) {
			inserted, insertErr := tx.Exec(ctx, "INSERT INTO "+fills+" (scope_id, fingerprint_version, semantic_fingerprint_hmac, variant, cache_route_identity_hmac, owner_operation_id, state, lease_expires_at) VALUES ($1,$2,$3,$4,$5,$6,'filling',$7) ON CONFLICT DO NOTHING", request.Key.ScopeID, request.Key.FingerprintVersion, request.Key.SemanticFingerprintHMAC[:], request.Key.Variant, request.Key.RouteIdentityHMAC[:], owner, leaseUntil)
			if insertErr != nil {
				return redactPostgresError(fmt.Errorf("create PostgreSQL response cache fill: %w", insertErr))
			}
			if inserted.RowsAffected() > 0 {
				result.Status, result.LeaseUntil = CacheFillAcquired, leaseUntil
				return nil
			}
			if err := tx.QueryRow(ctx, fillQuery, request.Key.ScopeID, request.Key.FingerprintVersion, request.Key.SemanticFingerprintHMAC[:], request.Key.Variant, request.Key.RouteIdentityHMAC[:]).Scan(&existingOwner, &state, &existingLease, &existingEntry); err != nil {
				return redactPostgresError(fmt.Errorf("reread PostgreSQL response cache fill: %w", err))
			}
			// The initial SELECT legitimately observed no row. Once the
			// conflict-safe INSERT has completed, the reread supplies the
			// lock result; clear the outer lookup error before evaluating it
			// below. Keep the INSERT error in a separate variable so this
			// assignment cannot be shadowed by a short declaration.
			err = nil
		}
		if err != nil {
			return redactPostgresError(fmt.Errorf("lock PostgreSQL response cache fill: %w", err))
		}
		if state == "completed" && existingEntry != nil {
			var entryState string
			if err := tx.QueryRow(ctx, "SELECT state FROM "+entries+" WHERE cache_entry_id=$1 FOR UPDATE", *existingEntry).Scan(&entryState); err != nil {
				return redactPostgresError(fmt.Errorf("check PostgreSQL completed response cache entry: %w", err))
			}
			if entryState == "ready" {
				result.Status, result.EntryID = CacheFillExisting, *existingEntry
				return nil
			}
		}
		if state == "filling" && existingLease.After(now) && existingOwner != owner {
			result.Status, result.LeaseUntil = CacheFillBusy, existingLease
			return nil
		}
		if _, err := tx.Exec(ctx, "UPDATE "+fills+" SET owner_operation_id=$6, state='filling', lease_expires_at=$7, cache_entry_id=NULL, updated_at=$8 WHERE scope_id=$1 AND fingerprint_version=$2 AND semantic_fingerprint_hmac=$3 AND variant=$4 AND cache_route_identity_hmac=$5", request.Key.ScopeID, request.Key.FingerprintVersion, request.Key.SemanticFingerprintHMAC[:], request.Key.Variant, request.Key.RouteIdentityHMAC[:], owner, leaseUntil, now); err != nil {
			return redactPostgresError(fmt.Errorf("renew PostgreSQL response cache fill: %w", err))
		}
		result.Status, result.LeaseUntil = CacheFillAcquired, leaseUntil
		return nil
	})
}

// Publish completes a fill and inserts a ready entry in the same transaction.
// It is safe to call again after a committed response: the completed fill
// returns the existing entry rather than inserting a duplicate template.
func (repository ResponseCacheRepository) Publish(ctx context.Context, request CachePublishRequest) (entry CacheEntry, returnErr error) {
	started := time.Now()
	defer func() {
		event := "fill"
		if returnErr != nil {
			event = "error"
		}
		repository.observeCache(event, started, returnErr)
	}()
	if ctx == nil {
		return entry, errors.New("response cache publish context is nil")
	}
	if err := request.Fill.Key.validate(); err != nil {
		return entry, err
	}
	if len(request.Response) == 0 || len(request.Response) > repository.MaxInlineBytes {
		return entry, fmt.Errorf("response cache inline response must be between 1 and %d bytes", repository.MaxInlineBytes)
	}
	manifest, err := normalizeCacheManifest(request.CanonicalRequestJSON)
	if err != nil {
		return entry, err
	}
	canonicalDigest := request.CanonicalRequestDigest
	if canonicalDigest == [keyDigestBytes]byte{} {
		canonicalDigest = sha256.Sum256(manifest)
	}
	if expected := sha256.Sum256(manifest); canonicalDigest != expected {
		return entry, errors.New("response cache canonical request digest does not match manifest")
	}
	if request.SemanticProfileVersion == "" || request.CacheEpoch == "" {
		return entry, errors.New("response cache profile version and epoch are required")
	}
	if request.OriginCheckpointID == uuid.Nil {
		return entry, errors.New("response cache origin checkpoint is required")
	}
	originOperation, err := operationID(request.OriginOperationID)
	if err != nil {
		return entry, err
	}
	fillOwner, err := operationID(request.Fill.OperationID)
	if err != nil {
		return entry, err
	}
	if fillOwner != originOperation {
		return entry, ErrCacheFillNotOwned
	}
	for name, value := range map[string]string{"provider": request.OriginProvider, "endpoint": request.OriginEndpointID, "model": request.OriginResolvedModel} {
		if strings.TrimSpace(value) == "" || strings.ContainsAny(value, "\x00\r\n") {
			return entry, fmt.Errorf("response cache origin %s is invalid", name)
		}
	}
	entryID, err := repository.NewID()
	if err != nil || entryID == uuid.Nil {
		return entry, fmt.Errorf("generate response cache entry id: %w", err)
	}
	responseDigest := sha256.Sum256(request.Response)
	sealed, err := repository.Keys.Seal(EnvelopeContext{ScopeID: request.Fill.Key.ScopeID, OperationID: entryID, PayloadKind: "response-cache", Digest: responseDigest}, request.Response)
	if err != nil {
		return entry, err
	}
	entries, uses, fills, err := repository.relations()
	if err != nil {
		return entry, err
	}
	completed := request.CompletedAt.UTC()
	if request.CompletedAt.IsZero() {
		completed = repository.clock()
	}
	return entry, WithTransaction(ctx, repository.Pool, func(ctx context.Context, tx pgx.Tx) error {
		var now time.Time
		if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
			return redactPostgresError(fmt.Errorf("read PostgreSQL publish time: %w", err))
		}
		var owner uuid.UUID
		var state string
		var lease time.Time
		var existing *uuid.UUID
		if err := tx.QueryRow(ctx, "SELECT owner_operation_id, state, lease_expires_at, cache_entry_id FROM "+fills+" WHERE scope_id=$1 AND fingerprint_version=$2 AND semantic_fingerprint_hmac=$3 AND variant=$4 AND cache_route_identity_hmac=$5 FOR UPDATE", request.Fill.Key.ScopeID, request.Fill.Key.FingerprintVersion, request.Fill.Key.SemanticFingerprintHMAC[:], request.Fill.Key.Variant, request.Fill.Key.RouteIdentityHMAC[:]).Scan(&owner, &state, &lease, &existing); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrCacheFillNotOwned
			}
			return redactPostgresError(fmt.Errorf("lock PostgreSQL response cache fill: %w", err))
		}
		if owner != originOperation {
			return ErrCacheFillNotOwned
		}
		if state == "completed" && existing != nil {
			entry, err = scanCacheEntry(tx.QueryRow(ctx, "SELECT "+cacheEntrySelect+" FROM "+entries+" WHERE cache_entry_id=$1", *existing))
			return err
		}
		if state != "filling" {
			return ErrCacheFillNotOwned
		}
		if !lease.After(now) {
			return ErrCacheFillExpired
		}
		created := now
		if completed.Before(created) {
			created = completed
		}
		insert := "INSERT INTO " + entries + " (cache_entry_id, scope_id, fingerprint_version, semantic_fingerprint_hmac, canonical_request_digest, canonical_request_jsonb, variant, cache_route_identity_hmac, semantic_profile_version, cache_epoch, response_inline_ciphertext, response_key_id, response_digest, origin_operation_id, origin_checkpoint_id, origin_provider, origin_endpoint_id, origin_resolved_model, created_at, completed_at, last_used_at, use_count, state) VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$20,1,'ready')"
		if _, err := tx.Exec(ctx, insert, entryID, request.Fill.Key.ScopeID, request.Fill.Key.FingerprintVersion, request.Fill.Key.SemanticFingerprintHMAC[:], canonicalDigest[:], manifest, request.Fill.Key.Variant, request.Fill.Key.RouteIdentityHMAC[:], request.SemanticProfileVersion, request.CacheEpoch, sealed.Ciphertext, sealed.KeyID, responseDigest[:], originOperation, request.OriginCheckpointID, request.OriginProvider, request.OriginEndpointID, request.OriginResolvedModel, created, completed); err != nil {
			return redactPostgresError(fmt.Errorf("insert PostgreSQL response cache entry: %w", err))
		}
		used, err := tx.Exec(ctx, "INSERT INTO "+uses+" (cache_entry_id, operation_id, first_used_at) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING", entryID, originOperation, completed)
		if err != nil {
			return redactPostgresError(fmt.Errorf("record PostgreSQL response cache origin use: %w", err))
		}
		if used.RowsAffected() == 0 {
			return ErrCacheOperationReuse
		}
		if _, err := tx.Exec(ctx, "UPDATE "+fills+" SET state='completed', cache_entry_id=$6, updated_at=$7 WHERE scope_id=$1 AND fingerprint_version=$2 AND semantic_fingerprint_hmac=$3 AND variant=$4 AND cache_route_identity_hmac=$5 AND owner_operation_id=$8 AND state='filling'", request.Fill.Key.ScopeID, request.Fill.Key.FingerprintVersion, request.Fill.Key.SemanticFingerprintHMAC[:], request.Fill.Key.Variant, request.Fill.Key.RouteIdentityHMAC[:], entryID, now, originOperation); err != nil {
			return redactPostgresError(fmt.Errorf("complete PostgreSQL response cache fill: %w", err))
		}
		entry = CacheEntry{ID: entryID, Key: request.Fill.Key, CanonicalRequestDigest: canonicalDigest, CanonicalRequestJSON: append([]byte(nil), manifest...), RouteIdentityHMAC: request.Fill.Key.RouteIdentityHMAC, SemanticProfileVersion: request.SemanticProfileVersion, CacheEpoch: request.CacheEpoch, Response: sealed, ResponseDigest: responseDigest, OriginOperationID: originOperation, OriginCheckpointID: request.OriginCheckpointID, OriginProvider: request.OriginProvider, OriginEndpointID: request.OriginEndpointID, OriginResolvedModel: request.OriginResolvedModel, CreatedAt: created, CompletedAt: completed, LastUsedAt: completed, UseCount: 1}
		return nil
	})
}

// FailFill lets an owner release a failed provider attempt without deleting
// the row. The failed marker allows a later operation to take over safely.
func (repository ResponseCacheRepository) FailFill(ctx context.Context, request CacheFillRequest) (returnErr error) {
	started := time.Now()
	defer func() { repository.observeCache("fill_failed", started, returnErr) }()
	if ctx == nil {
		return errors.New("response cache fill context is nil")
	}
	if err := request.Key.validate(); err != nil {
		return err
	}
	owner, err := operationID(request.OperationID)
	if err != nil {
		return err
	}
	_, _, fills, err := repository.relations()
	if err != nil {
		return err
	}
	return WithTransaction(ctx, repository.Pool, func(ctx context.Context, tx pgx.Tx) error {
		updated, err := tx.Exec(ctx, "UPDATE "+fills+" SET state='failed', updated_at=clock_timestamp() WHERE scope_id=$1 AND fingerprint_version=$2 AND semantic_fingerprint_hmac=$3 AND variant=$4 AND cache_route_identity_hmac=$5 AND owner_operation_id=$6 AND state='filling'", request.Key.ScopeID, request.Key.FingerprintVersion, request.Key.SemanticFingerprintHMAC[:], request.Key.Variant, request.Key.RouteIdentityHMAC[:], owner)
		if err != nil {
			return redactPostgresError(fmt.Errorf("fail PostgreSQL response cache fill: %w", err))
		}
		if updated.RowsAffected() == 0 {
			return ErrCacheFillNotOwned
		}
		return nil
	})
}
