package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/state"
)

type continuationRecord struct {
	handle state.Handle
	value  state.Continuation
}

// continuationOperationKey is the identity of one child write. Operation
// keys are caller-provided and are only idempotent within a tenant's parent
// branch; the same key may be used independently by another branch.
type continuationOperationKey struct {
	tenant    string
	parent    state.Handle
	operation string
}

// ContinuationStore is a bounded, immutable in-process continuation store.
// It is suitable for tests and explicitly single-process development only.
type ContinuationStore struct {
	mu       sync.RWMutex
	keyring  *state.Keyring
	clock    func() time.Time
	maxDepth int
	records  map[state.Handle]continuationRecord
	byOp     map[continuationOperationKey]state.Handle
}

type ContinuationOptions struct {
	Keyring  *state.Keyring
	Clock    func() time.Time
	MaxDepth int
}

func NewContinuationStore(options ContinuationOptions) (*ContinuationStore, error) {
	if options.Keyring == nil {
		return nil, fmt.Errorf("continuation keyring is required")
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.MaxDepth <= 0 {
		options.MaxDepth = 64
	}
	return &ContinuationStore{keyring: options.Keyring, clock: options.Clock, maxDepth: options.MaxDepth, records: make(map[state.Handle]continuationRecord), byOp: make(map[continuationOperationKey]state.Handle)}, nil
}

func (store *ContinuationStore) CreateRoot(ctx context.Context, continuation state.Continuation) (state.Handle, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if continuation.Tenant == "" {
		return "", fmt.Errorf("continuation tenant is required")
	}
	handle, err := store.keyring.Issue(continuation.Tenant)
	if err != nil {
		return "", err
	}
	continuation.ID = handle
	continuation.Depth = 0
	return store.put(state.Handle(handle), continuation)
}

func (store *ContinuationStore) Get(ctx context.Context, handle state.Handle) (state.Continuation, error) {
	if err := ctx.Err(); err != nil {
		return state.Continuation{}, err
	}
	store.mu.RLock()
	record, ok := store.records[handle]
	store.mu.RUnlock()
	if !ok {
		return state.Continuation{}, state.ErrNotFound
	}
	if _, err := store.keyring.Verify(record.value.Tenant, handle.String()); err != nil {
		return state.Continuation{}, state.ErrInvalidHandle
	}
	if !store.clock().Before(record.value.ExpiresAt) {
		return state.Continuation{}, state.ErrExpired
	}
	return record.value.Clone(), nil
}

func (store *ContinuationStore) GetForTenant(ctx context.Context, tenant string, handle state.Handle) (state.Continuation, error) {
	if tenant == "" {
		return state.Continuation{}, state.ErrInvalidHandle
	}
	if _, err := store.keyring.Verify(tenant, handle.String()); err != nil {
		return state.Continuation{}, state.ErrInvalidHandle
	}
	value, err := store.Get(ctx, handle)
	if err != nil {
		return state.Continuation{}, err
	}
	if value.Tenant != tenant {
		return state.Continuation{}, state.ErrInvalidHandle
	}
	return value, nil
}

func (store *ContinuationStore) PutChild(ctx context.Context, request state.PutChildRequest) (state.Handle, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	parent, err := store.Get(ctx, request.Parent)
	if err != nil {
		return "", err
	}
	child := request.Child.Clone()
	if child.Tenant == "" {
		child.Tenant = parent.Tenant
	}
	if child.Tenant != parent.Tenant || child.Depth != parent.Depth+1 || child.ParentID != request.Parent.String() {
		return "", state.ErrConflict
	}
	if child.Depth > store.maxDepth {
		return "", fmt.Errorf("continuation depth exceeds limit")
	}
	// Keep the operation-key lookup and publication in one critical section.
	// A retry storm can call PutChild concurrently with the same parent and
	// operation key; checking under RLock and publishing later allowed every
	// caller to mint a different child before any caller populated byOp.
	store.mu.Lock()
	defer store.mu.Unlock()
	if request.OperationKey != "" {
		operationKey := continuationOperationKey{tenant: parent.Tenant, parent: request.Parent, operation: request.OperationKey}
		if handle, exists := store.byOp[operationKey]; exists {
			return handle, nil
		}
	}
	handle, err := store.keyring.Issue(child.Tenant)
	if err != nil {
		return "", err
	}
	child.ID = handle
	if err := child.Validate(store.clock()); err != nil {
		return "", err
	}
	if _, err := store.putLocked(state.Handle(handle), child); err != nil {
		return "", err
	}
	if request.OperationKey != "" {
		operationKey := continuationOperationKey{tenant: child.Tenant, parent: request.Parent, operation: request.OperationKey}
		store.byOp[operationKey] = state.Handle(handle)
	}
	return state.Handle(handle), nil
}

func (store *ContinuationStore) put(handle state.Handle, continuation state.Continuation) (state.Handle, error) {
	if err := continuation.Validate(store.clock()); err != nil {
		return "", err
	}
	continuation.ID = handle.String()
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.putLocked(handle, continuation)
}

func (store *ContinuationStore) putLocked(handle state.Handle, continuation state.Continuation) (state.Handle, error) {
	if existing, ok := store.records[handle]; ok {
		if existing.value.TranscriptDigest == continuation.TranscriptDigest && existing.value.LastOperationID == continuation.LastOperationID {
			return handle, nil
		}
		return "", state.ErrConflict
	}
	store.records[handle] = continuationRecord{handle: handle, value: continuation.Clone()}
	return handle, nil
}

func (store *ContinuationStore) Sweep(now time.Time) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	removed := 0
	for handle, record := range store.records {
		if !now.Before(record.value.ExpiresAt) {
			delete(store.records, handle)
			removed++
		}
	}
	return removed
}
