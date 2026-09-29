package cache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/state"
)

// ResponseKey is the exact-response disclosure and compatibility boundary.
// ScopeID is the authenticated, opaque checkpoint scope. Fingerprint covers
// normalized content, effective settings, source context and policy/compiler
// versions (not operation IDs, freshness or timestamps). Route must be resolved
// before lookup. RequestIndex is solely a sample discriminator; zero is usual.
// Compaction fingerprints must include source content and compaction policies.
type ResponseKey struct {
	ScopeID      string
	Operation    OperationKind
	Route        RouteIdentity
	Fingerprint  Fingerprint
	RequestIndex int64
}

// ResponseEntry is an immutable successful origin response, not a response
// ready to return to another caller. The consuming finalizer must create its
// own operation/checkpoint and zero-cost response with cache provenance.
// Response contains the validated Generate or Compact v1 JSON envelope.
type ResponseEntry struct {
	ID                 state.CacheEntryID
	Key                ResponseKey
	OriginOperationID  state.OperationID
	OriginCheckpointID state.CheckpointID
	CompletedAt        time.Time
	Response           json.RawMessage
}

type ResponseLookup struct {
	Key ResponseKey
	Now time.Time
	// Nil means no age restriction. A supplied age must be positive and is
	// measured from successful completion, never from insertion or last use.
	MaxAge *time.Duration
}

// ResponseUse is the immutable receipt for a completed cache replay. It is
// unique per scope/consuming operation, so retries cannot count another use.
type ResponseUse struct {
	ScopeID      string
	OperationID  state.OperationID
	EntryID      state.CacheEntryID
	CheckpointID state.CheckpointID
	CompletedAt  time.Time
}

// ResponseRepository persists successful entries and finalizer receipts. A
// nil Lookup result is a miss. Errors are not misses. Publish and RecordUse
// require already committed, same-scope checkpoints; retry unknown writes
// with identical values. No method authorizes provider dispatch, owns a fill
// lease, settles budget, deletes data or records failed/incomplete results.
type ResponseRepository interface {
	Publish(context.Context, ResponseEntry) error
	Lookup(context.Context, ResponseLookup) (*ResponseEntry, error)
	RecordUse(context.Context, ResponseUse) error
	ReadUse(context.Context, string, state.OperationID) (ResponseUse, error)
}
