package cloudstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"
	"unicode/utf8"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/google/uuid"
)

// Operation binds an idempotency key to exactly one input in one scope. Now is
// used only for the first discovery row; retries reuse that row's timestamp.
// Manifest must include every execution-relevant input. This is operation
// replay, not cross-operation response caching.
type Operation struct {
	Scope        Scope
	Kind         string
	Key          string
	RequestIndex int64
	Manifest     json.RawMessage
	Now          time.Time
}

// BeginOperation makes an operation discoverable before its runtime is called.
// The keyed UUID is stable across workers/restarts without exposing the caller's
// operation key. The discovery row is still the first write. Running is an
// observation, NOT a lock or authorization to dispatch: the inner runtime must
// enforce idempotency and Redis's single-use budget claim on every invocation.
func (r *Repository) BeginOperation(ctx context.Context, operation Operation) (Record, error) {
	if err := validContext(ctx); err != nil {
		return Record{}, err
	}
	if operation.Key == "" || len(operation.Key) > 4096 || !utf8.ValidString(operation.Key) {
		return Record{}, ErrInvalid
	}
	id := r.operationID(operation.Scope, operation.Kind, operation.Key)
	request, err := normalizeRequest(CreateRequest{ID: id, Scope: operation.Scope, Kind: operation.Kind, RequestIndex: operation.RequestIndex, Manifest: operation.Manifest, CreatedAt: operation.Now})
	if err != nil {
		return Record{}, err
	}
	// A competing initializer chooses the timestamp. Reuse it even if it died
	// between writing the discovery row and writing its first event.
	for attempt := 0; attempt < 3; attempt++ {
		var existing *Record
		row, readErr := r.table.Get(ctx, r.indexKey(request.ID))
		if readErr == nil {
			index, err := r.decodeIndex(row)
			if err != nil {
				return Record{}, err
			}
			request.CreatedAt = index.UpdatedAt
			record, err := r.Read(ctx, request.Scope, request.ID)
			if err == nil {
				request.CreatedAt = record.Request.CreatedAt
				existing = &record
			} else if index.Revision != 0 || !errors.Is(err, contracts.ErrNotFound) {
				return Record{}, err
			}
			if index.ScopeTag != r.scopeTag(request.Scope) || index.Binding != r.binding(request) {
				return Record{}, contracts.ErrConflict
			}
		} else if !errors.Is(readErr, contracts.ErrNotFound) {
			return Record{}, readErr
		}
		if existing == nil {
			_, err = r.Create(ctx, request)
			if errors.Is(err, contracts.ErrConflict) {
				continue
			}
			if err != nil {
				return Record{}, err
			}
			record, err := r.Read(ctx, request.Scope, request.ID)
			if err != nil {
				return Record{}, err
			}
			existing = &record
		}
		// Replays never attempt another immutable blob/event write. The only
		// possible mutation is repairing a lagging discovery index.
		record := *existing
		if record.Status != StatusPending {
			return record, r.advanceIndex(ctx, record)
		}
		record, err = r.TryUpdate(ctx, request.Scope, request.ID, Update{ExpectedRevision: record.Revision, Token: "runtime-start", Status: StatusRunning, Progress: json.RawMessage(`{"version":1}`), UpdatedAt: request.CreatedAt})
		if errors.Is(err, contracts.ErrConflict) {
			continue
		}
		return record, err
	}
	return Record{}, contracts.ErrConflict
}

func (r *Repository) operationID(scope Scope, kind, key string) RequestID {
	identity, _ := json.Marshal(struct {
		Namespace string
		Scope     Scope
		Kind, Key string
	}{r.namespace, scope, kind, key})
	data := derive(r.secret, "operation-id", identity)
	// UUIDv8 carries application-defined, HMAC-derived bits.
	data[6] = (data[6] & 0x0f) | 0x80
	data[8] = (data[8] & 0x3f) | 0x80
	id, _ := uuid.FromBytes(data[:16])
	return RequestID(RequestIDPrefix + id.String())
}

// CompleteOperation publishes the result only after the inner runtime has
// finalized its checkpoint and settled Redis. On an uncertain acknowledgement,
// retrying repairs the index or verifies the already committed result. It never
// replaces a terminal result with a different response.
func (r *Repository) CompleteOperation(ctx context.Context, scope Scope, id RequestID, progress json.RawMessage, now time.Time) (Record, error) {
	progress, err := objectJSON(progress)
	if err != nil || !validTime(now) {
		return Record{}, ErrInvalid
	}
	for attempt := 0; attempt < 3; attempt++ {
		record, err := r.Read(ctx, scope, id)
		if err != nil {
			return Record{}, err
		}
		if record.Status == StatusCompleted {
			if !bytes.Equal(record.Progress, progress) {
				return Record{}, contracts.ErrConflict
			}
			return record, r.advanceIndex(ctx, record)
		}
		if record.Status != StatusRunning {
			return Record{}, contracts.ErrConflict
		}
		if now.Before(record.UpdatedAt) {
			now = record.UpdatedAt
		}
		updated, err := r.TryUpdate(ctx, scope, id, Update{ExpectedRevision: record.Revision, Token: "runtime-complete", Status: StatusCompleted, Progress: progress, UpdatedAt: now})
		if errors.Is(err, contracts.ErrConflict) {
			continue
		}
		return updated, err
	}
	return Record{}, contracts.ErrConflict
}
