package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/redis/go-redis/v9"
)

// ContinuationOptions configures immutable Redis continuation records. A
// Keyring signs handles and binds them to tenants; records are stored under a
// tenant-hashed key and an opaque handle index so Get can resolve the tenant
// without exposing it to Redis key scans.
type ContinuationOptions struct {
	Client   redis.Scripter
	Reader   StringReader
	Invoker  ContinuationInvoker
	Keys     KeyOptions
	Keyring  *state.Keyring
	Clock    func() time.Time
	MaxBytes int
	MaxDepth int
}

// ContinuationInvoker is the narrow atomic SET-if-absent seam used by the
// continuation store. Production uses the embedded immutable Lua script;
// offline tests can supply a fake command/function harness.
type ContinuationInvoker interface {
	Put(context.Context, []string, string, string, string) (string, error)
}

type continuationInvoker struct{ client redis.Scripter }

func (invoker continuationInvoker) Put(ctx context.Context, keys []string, value, handle, ttl string) (string, error) {
	result, err := continuationPutScript.Run(ctx, invoker.client, keys, value, ttl, handle).Result()
	if err != nil {
		return "", err
	}
	array, ok := result.([]interface{})
	if !ok || len(array) < 1 {
		return "", ErrUnavailable
	}
	status, ok := resultString(array[0])
	if !ok {
		return "", ErrUnavailable
	}
	if len(array) > 1 {
		payload, _ := resultString(array[1])
		return status + "\x00" + payload, nil
	}
	return status, nil
}

type ContinuationStore struct {
	space    keySpace
	keyring  *state.Keyring
	clock    func() time.Time
	maxBytes int
	maxDepth int
	invoke   ContinuationInvoker
	reader   StringReader
}

var _ state.ContinuationStore = (*ContinuationStore)(nil)

func NewContinuationStore(options ContinuationOptions) (*ContinuationStore, error) {
	space, err := newKeySpace(options.Keys)
	if err != nil {
		return nil, err
	}
	if options.Keyring == nil {
		return nil, fmt.Errorf("continuation keyring is required")
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.MaxBytes <= 0 {
		options.MaxBytes = 512 << 10
	}
	if options.MaxDepth <= 0 {
		options.MaxDepth = 64
	}
	invoke := options.Invoker
	if invoke == nil {
		client, ok := options.Client.(redis.Scripter)
		if !ok || client == nil {
			return nil, fmt.Errorf("Redis continuation client is required")
		}
		invoke = continuationInvoker{client: client}
	}
	reader := options.Reader
	if reader == nil {
		client, ok := options.Client.(interface {
			Get(context.Context, string) *redis.StringCmd
		})
		if !ok || client == nil {
			return nil, fmt.Errorf("Redis continuation reader is required")
		}
		reader = redisReader{client: client}
	}
	return &ContinuationStore{space: space, keyring: options.Keyring, clock: options.Clock, maxBytes: options.MaxBytes, maxDepth: options.MaxDepth, invoke: invoke, reader: reader}, nil
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
	return store.put(ctx, state.Handle(handle), continuation, "")
}

func (store *ContinuationStore) Get(ctx context.Context, handle state.Handle) (state.Continuation, error) {
	if err := ctx.Err(); err != nil {
		return state.Continuation{}, err
	}
	if handle == "" || len(handle) > 512 {
		return state.Continuation{}, state.ErrInvalidHandle
	}
	indexKey := store.space.continuationIndexKey(handle.String())
	index, err := store.reader.Get(ctx, indexKey)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return state.Continuation{}, state.ErrNotFound
		}
		return state.Continuation{}, resolveStateError(ctx, err)
	}
	if index == "" || len(index) > 512 {
		return state.Continuation{}, ErrUnavailable
	}
	data, err := store.reader.Get(ctx, index)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// Redis can expire the record after the first index read. Recheck the
			// index before failing closed: if it also disappeared, this was a
			// normal expiration race; if it remains, persisted state is dangling.
			_, recheckErr := store.reader.Get(ctx, indexKey)
			if errors.Is(recheckErr, redis.Nil) {
				return state.Continuation{}, state.ErrNotFound
			}
			if recheckErr != nil {
				return state.Continuation{}, resolveStateError(ctx, recheckErr)
			}
			return state.Continuation{}, ErrUnavailable
		}
		return state.Continuation{}, resolveStateError(ctx, err)
	}
	return store.decode(handle, data, "")
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
		if errors.Is(err, state.ErrNotFound) || errors.Is(err, state.ErrExpired) {
			return state.Continuation{}, state.ErrInvalidHandle
		}
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
	operationIndexKey := ""
	if request.OperationKey != "" {
		operationIndexKey = store.space.continuationOperationKey(parent.Tenant, request.Parent.String(), request.OperationKey)
		if value, err := store.reader.Get(ctx, operationIndexKey); err == nil && value != "" {
			existing, getErr := store.Get(ctx, state.Handle(value))
			if getErr == nil && !sameChildFacts(existing, child) {
				return "", state.ErrConflict
			}
			return state.Handle(value), nil
		} else if err != nil && !errors.Is(err, redis.Nil) {
			return "", resolveStateError(ctx, err)
		}
	}
	handle, err := store.keyring.Issue(child.Tenant)
	if err != nil {
		return "", err
	}
	child.ID = handle
	return store.put(ctx, state.Handle(handle), child, operationIndexKey)
}

// Sweep is retained for parity with the memory implementation. Redis expires
// continuation/index records through server-side TTLs, so no unbounded key
// scan is required here; the timestamp is intentionally unused.
func (store *ContinuationStore) Sweep(_ time.Time) int { return 0 }

func (store *ContinuationStore) put(ctx context.Context, handle state.Handle, continuation state.Continuation, operationIndexKey string) (state.Handle, error) {
	if err := continuation.Validate(store.clock()); err != nil {
		return "", err
	}
	data, err := encodeContinuation(continuation)
	if err != nil {
		return "", err
	}
	if err := validJSONSize(data, store.maxBytes); err != nil {
		return "", err
	}
	recordKey := store.space.continuationKey(continuation.Tenant, handle.String())
	handleIndex := store.space.continuationIndexKey(handle.String())
	keys := []string{handleIndex, recordKey}
	if operationIndexKey != "" {
		keys = append(keys, operationIndexKey)
	}
	ttl := ttlSeconds(store.clock(), continuation.ExpiresAt)
	result, err := store.invoke.Put(ctx, keys, string(data), handle.String(), strconv.FormatInt(ttl, 10))
	if err != nil {
		return "", resolveStateError(ctx, err)
	}
	parts := splitResult(result)
	switch parts[0] {
	case "created":
		return handle, nil
	case "existing":
		if len(parts) < 2 || parts[1] == "" {
			return "", ErrUnavailable
		}
		// A concurrent retry can lose the operation-index race after missing
		// the preliminary read. The winning child is immutable, so comparing
		// it here closes that window without trusting a stale read.
		existing := state.Handle(parts[1])
		if stored, getErr := store.Get(ctx, existing); getErr == nil && !sameChildFacts(stored, continuation) {
			return "", state.ErrConflict
		}
		return existing, nil
	case "conflict":
		return "", state.ErrConflict
	case "invalid":
		return "", state.ErrInvalidHandle
	default:
		return "", resolveStateError(ctx, fmt.Errorf("continuation mutation failed"))
	}
}

func (store *ContinuationStore) decode(handle state.Handle, data, tenant string) (state.Continuation, error) {
	if len(data) == 0 || len(data) > store.maxBytes {
		return state.Continuation{}, ErrUnavailable
	}
	value, err := decodeContinuation([]byte(data))
	if err != nil {
		return state.Continuation{}, ErrUnavailable
	}
	if value.ID != handle.String() || value.Tenant == "" {
		return state.Continuation{}, state.ErrInvalidHandle
	}
	if tenant != "" && value.Tenant != tenant {
		return state.Continuation{}, state.ErrInvalidHandle
	}
	if _, err := store.keyring.Verify(value.Tenant, handle.String()); err != nil {
		return state.Continuation{}, state.ErrInvalidHandle
	}
	if err := value.Validate(store.clock()); err != nil {
		return state.Continuation{}, err
	}
	return value.Clone(), nil
}

func splitResult(value string) []string {
	for index := range value {
		if value[index] == 0 {
			return []string{value[:index], value[index+1:]}
		}
	}
	return []string{value}
}

func resolveStateError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, redis.Nil) {
		return state.ErrNotFound
	}
	return fmt.Errorf("Redis state mutation outcome is unresolved: %w", ErrUnavailable)
}

// sameChildFacts compares the immutable facts of an idempotent child retry.
// A retry with the same parent and operation key but a different history is
// a conflict, not a replay of the stored child.
func sameChildFacts(stored, incoming state.Continuation) bool {
	return stored.TranscriptDigest == incoming.TranscriptDigest && stored.TranscriptComplete == incoming.TranscriptComplete
}
