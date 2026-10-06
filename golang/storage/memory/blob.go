package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/blob"
)

// BlobStore is an immutable, process-local content-addressed blob store.
//
// It is intentionally suitable only for the development memory composition:
// all bytes disappear when the process exits and no external persistence or
// coordination is attempted. Entries are expiry-bound and can be removed with
// Sweep so callers can keep the process-local footprint bounded.
type BlobStore struct {
	mu       sync.RWMutex
	clock    func() time.Time
	maxBytes int64
	records  map[string]blobRecord
}

type blobRecord struct {
	ref  blob.Ref
	data []byte
}

type BlobOptions struct {
	MaxBytes int64
	Clock    func() time.Time
}

func NewBlobStore(options BlobOptions) (*BlobStore, error) {
	if options.MaxBytes <= 0 {
		return nil, fmt.Errorf("memory blob max bytes must be positive")
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	return &BlobStore{
		clock:    options.Clock,
		maxBytes: options.MaxBytes,
		records:  make(map[string]blobRecord),
	}, nil
}

// ProbeBucket implements the common runtime readiness seam. Memory has no
// external dependency, so this only checks local availability and context.
func (store *BlobStore) ProbeBucket(ctx context.Context) error {
	if store == nil {
		return fmt.Errorf("memory blob store is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return ctx.Err()
}

func (store *BlobStore) Put(ctx context.Context, request blob.PutRequest) (blob.Ref, error) {
	if store == nil {
		return blob.Ref{}, fmt.Errorf("memory blob store is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return blob.Ref{}, err
	}
	if request.Tenant == "" || request.MediaType == "" {
		return blob.Ref{}, fmt.Errorf("blob tenant and media type are required")
	}
	if int64(len(request.Data)) > store.maxBytes {
		return blob.Ref{}, fmt.Errorf("blob exceeds the configured size limit")
	}
	now := store.clock()
	if request.ExpiresAt.IsZero() || !now.Before(request.ExpiresAt) {
		return blob.Ref{}, blob.ErrExpired
	}
	tenantPrefix, err := blob.TenantPrefix(request.Tenant)
	if err != nil {
		return blob.Ref{}, err
	}
	digest := blob.Digest(request.Data)
	ref := blob.Ref{
		Store:      "memory",
		Locator:    tenantPrefix + "/" + digest,
		Digest:     digest,
		ByteLength: int64(len(request.Data)),
		MediaType:  request.MediaType,
		ExpiresAt:  request.ExpiresAt,
	}
	if err := ref.Validate(now); err != nil {
		return blob.Ref{}, err
	}
	key := ref.Locator
	store.mu.Lock()
	defer store.mu.Unlock()
	store.sweepLocked(now)
	if existing, ok := store.records[key]; ok {
		if existing.ref.ByteLength != ref.ByteLength || existing.ref.MediaType != ref.MediaType || blob.Digest(existing.data) != digest {
			return blob.Ref{}, blob.ErrConflict
		}
		// The same content may be referenced by operations with different
		// retention windows. Keep the record until the longest requested
		// expiry while returning the caller's operation-specific reference.
		if ref.ExpiresAt.After(existing.ref.ExpiresAt) {
			existing.ref.ExpiresAt = ref.ExpiresAt
			store.records[key] = existing
		}
		return ref, nil
	}
	store.records[key] = blobRecord{ref: ref, data: append([]byte(nil), request.Data...)}
	return ref, nil
}

func (store *BlobStore) Get(ctx context.Context, tenant string, ref blob.Ref) ([]byte, error) {
	if store == nil {
		return nil, fmt.Errorf("memory blob store is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := store.clock()
	if err := ref.Validate(now); err != nil {
		return nil, err
	}
	prefix, err := blob.TenantPrefix(tenant)
	if err != nil {
		return nil, err
	}
	if ref.Store != "memory" || ref.Locator != prefix+"/"+ref.Digest {
		return nil, blob.ErrTenantMismatch
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.sweepLocked(now)
	record, ok := store.records[ref.Locator]
	if !ok {
		return nil, blob.ErrNotFound
	}
	if record.ref.Store != ref.Store || record.ref.Locator != ref.Locator || record.ref.ByteLength != ref.ByteLength || record.ref.MediaType != ref.MediaType || blob.Digest(record.data) != ref.Digest {
		return nil, blob.ErrDigestMismatch
	}
	return append([]byte(nil), record.data...), nil
}

// Sweep removes expired blobs and returns the number removed. Get and Put also
// perform opportunistic expiry cleanup, while a lifecycle owner can call Sweep
// periodically when no traffic is present.
func (store *BlobStore) Sweep(now time.Time) int {
	if store == nil {
		return 0
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.sweepLocked(now)
}

func (store *BlobStore) sweepLocked(now time.Time) int {
	removed := 0
	for key, record := range store.records {
		if !now.Before(record.ref.ExpiresAt) {
			delete(store.records, key)
			removed++
		}
	}
	return removed
}

var _ blob.Store = (*BlobStore)(nil)
