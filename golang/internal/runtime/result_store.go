package runtime

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/blob"
)

// BlobRefResolver reconstructs the opaque object-store locator from the
// tenant and the content-addressed state reference. The locator is not stored
// in Redis, so this callback is what makes result replay work after a worker
// restart. Implementations must validate the store name, tenant binding, and
// digest before returning a reference.
type BlobRefResolver func(context.Context, string, state.BlobRef, time.Time) (blob.Ref, error)

// BlobResultStore persists normalized responses in the configured immutable
// blob store and keeps only the small digest/size/media tuple in the shared
// admission ledger. The in-memory map is an optimization for the process that
// wrote a result; RefResolver is required for restart-safe reads.
//
// The map holds only live operations: entries leave it when their operation
// expires or is no longer found, and expired entries are swept on Put. With a
// resolver the map is also capped, because an uncached reference can be
// reconstructed; without one (development memory mode) it is the only way back
// to a result, so it is bounded by operation retention alone.
type BlobResultStore struct {
	store      blob.Store
	admission  admission.AdmissionStore
	resolveRef BlobRefResolver
	clock      func() time.Time

	mu        sync.RWMutex
	refs      map[string]cachedResultRef
	nextSweep time.Time
}

type cachedResultRef struct {
	ref       blob.Ref
	expiresAt time.Time
}

const (
	// maxCachedResultRefs caps the reference cache when a resolver can
	// rebuild evicted references.
	maxCachedResultRefs = 4096
	// resultRefSweepInterval spaces the expired-entry sweeps done by Put.
	resultRefSweepInterval = time.Minute
)

// NewBlobResultStore validates the durable result-store composition. A nil
// resolver is allowed only for tests that never read after the process loses
// its in-memory reference cache.
func NewBlobResultStore(store blob.Store, admissions admission.AdmissionStore, resolver BlobRefResolver, clock func() time.Time) (*BlobResultStore, error) {
	if store == nil {
		return nil, fmt.Errorf("result blob store is required")
	}
	if admissions == nil {
		return nil, fmt.Errorf("result admission store is required")
	}
	if clock == nil {
		clock = time.Now
	}
	return &BlobResultStore{store: store, admission: admissions, resolveRef: resolver, clock: clock, refs: make(map[string]cachedResultRef)}, nil
}

var _ engine.ResultStore = (*BlobResultStore)(nil)

func (results *BlobResultStore) Put(ctx context.Context, operationID string, response llm.Response) (state.BlobRef, error) {
	if results == nil || results.store == nil || results.admission == nil {
		return state.BlobRef{}, fmt.Errorf("result store is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return state.BlobRef{}, err
	}
	if strings.TrimSpace(operationID) == "" {
		return state.BlobRef{}, fmt.Errorf("result operation ID is required")
	}
	operation, err := results.admission.Get(ctx, operationID)
	if err != nil {
		return state.BlobRef{}, fmt.Errorf("load result operation: %w", err)
	}
	tenant, err := tenantFromScope(operation.ScopeKey)
	if err != nil {
		return state.BlobRef{}, err
	}
	if operation.ExpiresAt.IsZero() {
		return state.BlobRef{}, fmt.Errorf("result operation has no expiration")
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return state.BlobRef{}, fmt.Errorf("encode result: %w", err)
	}
	canonical, err := llm.CanonicalJSON(encoded)
	if err != nil {
		return state.BlobRef{}, fmt.Errorf("canonicalize result: %w", err)
	}
	ref, err := results.store.Put(ctx, blob.PutRequest{Tenant: tenant, MediaType: "application/json", Data: canonical, ExpiresAt: operation.ExpiresAt})
	if err != nil {
		return state.BlobRef{}, fmt.Errorf("persist result: %w", err)
	}
	stateRef, err := stateBlobRef(ref)
	if err != nil {
		return state.BlobRef{}, err
	}
	results.remember(operationID, ref, operation.ExpiresAt)
	return stateRef, nil
}

func (results *BlobResultStore) remember(operationID string, ref blob.Ref, expiresAt time.Time) {
	now := results.clock()
	results.mu.Lock()
	defer results.mu.Unlock()
	if !now.Before(results.nextSweep) {
		for id, cached := range results.refs {
			if !now.Before(cached.expiresAt) {
				delete(results.refs, id)
			}
		}
		results.nextSweep = now.Add(resultRefSweepInterval)
	}
	if _, cached := results.refs[operationID]; !cached && results.resolveRef != nil && len(results.refs) >= maxCachedResultRefs {
		return
	}
	results.refs[operationID] = cachedResultRef{ref: ref, expiresAt: expiresAt}
}

func (results *BlobResultStore) cached(operationID string) (blob.Ref, bool) {
	results.mu.RLock()
	cached, ok := results.refs[operationID]
	results.mu.RUnlock()
	if !ok {
		return blob.Ref{}, false
	}
	if !results.clock().Before(cached.expiresAt) {
		results.forget(operationID)
		return blob.Ref{}, false
	}
	return cached.ref, true
}

func (results *BlobResultStore) forget(operationID string) {
	results.mu.Lock()
	delete(results.refs, operationID)
	results.mu.Unlock()
}

func (results *BlobResultStore) Get(ctx context.Context, operationID string) (llm.Response, error) {
	if results == nil || results.store == nil || results.admission == nil {
		return llm.Response{}, engine.ErrResultNotFound
	}
	if err := ctx.Err(); err != nil {
		return llm.Response{}, err
	}
	if strings.TrimSpace(operationID) == "" {
		return llm.Response{}, engine.ErrResultNotFound
	}
	operation, err := results.admission.Get(ctx, operationID)
	if err != nil {
		if errors.Is(err, admission.ErrOperationNotFound) {
			results.forget(operationID)
			return llm.Response{}, engine.ErrResultNotFound
		}
		return llm.Response{}, fmt.Errorf("load result operation: %w", err)
	}
	tenant, err := tenantFromScope(operation.ScopeKey)
	if err != nil {
		return llm.Response{}, err
	}
	ref, ok := results.cached(operationID)
	if operation.ResultRef == nil || !operation.ResultRef.Valid() {
		// The ledger records the reference only when the operation completes.
		// A dispatching operation whose result this process already stored
		// (completion failed after Put) is still readable through the cached
		// reference, so the engine can finish the completion without calling
		// the provider again.
		if !ok || operation.State != admission.StateDispatching {
			return llm.Response{}, engine.ErrResultNotFound
		}
	} else if !ok {
		if results.resolveRef == nil {
			return llm.Response{}, engine.ErrResultNotFound
		}
		ref, err = results.resolveRef(ctx, tenant, *operation.ResultRef, operation.ExpiresAt)
		if err != nil {
			return llm.Response{}, fmt.Errorf("resolve result reference: %w", err)
		}
	}
	data, err := results.store.Get(ctx, tenant, ref)
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) || errors.Is(err, blob.ErrExpired) {
			results.forget(operationID)
			return llm.Response{}, engine.ErrResultNotFound
		}
		return llm.Response{}, fmt.Errorf("load result blob: %w", err)
	}
	var response llm.Response
	if err := json.Unmarshal(data, &response); err != nil {
		return llm.Response{}, fmt.Errorf("decode result: %w", err)
	}
	return response, nil
}

func tenantFromScope(scope string) (string, error) {
	parts := strings.SplitN(scope, "\x00", 2)
	if len(parts) == 1 {
		parts = strings.SplitN(scope, "/", 2)
	}
	if len(parts) < 2 || strings.TrimSpace(parts[0]) == "" {
		return "", fmt.Errorf("result operation scope has no tenant")
	}
	return parts[0], nil
}

func stateBlobRef(ref blob.Ref) (state.BlobRef, error) {
	if err := ref.Validate(time.Now()); err != nil {
		return state.BlobRef{}, fmt.Errorf("invalid result blob reference: %w", err)
	}
	digest, err := hex.DecodeString(ref.Digest)
	if err != nil || len(digest) != 32 {
		return state.BlobRef{}, fmt.Errorf("invalid result blob digest")
	}
	var value [32]byte
	copy(value[:], digest)
	return state.BlobRef{Digest: value, Size: ref.ByteLength, Media: ref.MediaType}, nil
}

// NewContentAddressedBlobRefResolver returns a resolver for stores whose
// locator is <prefix>/<sha256(tenant)>/<sha256(content)>, including the S3
// implementation in storage/s3blob. The development-only file store is the
// one exception: it uses a tenant-relative locator and therefore permits an
// empty prefix only when storeName is file. Prefix is normalized once and never
// accepts path traversal.
func NewContentAddressedBlobRefResolver(storeName, prefix string) (BlobRefResolver, error) {
	storeName = strings.TrimSpace(storeName)
	prefix = strings.Trim(prefix, "/")
	if storeName == "" || strings.ContainsAny(prefix, "\\\r\n") || strings.Contains(prefix, "..") || (prefix == "" && storeName != "file") {
		return nil, fmt.Errorf("content-addressed blob store and safe prefix are required")
	}
	return func(ctx context.Context, tenant string, value state.BlobRef, expiresAt time.Time) (blob.Ref, error) {
		if err := ctx.Err(); err != nil {
			return blob.Ref{}, err
		}
		if !value.Valid() {
			return blob.Ref{}, fmt.Errorf("invalid result state reference")
		}
		tenantPrefix, err := blob.TenantPrefix(tenant)
		if err != nil {
			return blob.Ref{}, err
		}
		locator := tenantPrefix + "/" + value.DigestHex()
		if prefix != "" {
			locator = prefix + "/" + locator
		}
		ref := blob.Ref{Store: storeName, Locator: locator, Digest: value.DigestHex(), ByteLength: value.Size, MediaType: value.Media, ExpiresAt: expiresAt}
		if err := ref.Validate(time.Now()); err != nil && !errors.Is(err, blob.ErrExpired) {
			return blob.Ref{}, err
		}
		return ref, nil
	}, nil
}
