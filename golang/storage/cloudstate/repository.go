package cloudstate

import (
	"context"
	"crypto/cipher"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"

	events "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/eventsourcing"
	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

type Options struct {
	Table     kv.KeyValueStore
	Blobs     blob.BlobStore
	Namespace string
	// Secret is a stable, independently provisioned 32-byte key. It must not be
	// derived from AWS credentials, Redis passwords, or changing configuration.
	// Keep it with backups. Key rotation/re-encryption is a separate operation.
	Secret []byte
}

// Repository owns no network clients. The caller supplies opened stores and
// retains their lifecycle. No automatic write retries, expiry, or cleanup occur.
type Repository struct {
	table     kv.KeyValueStore
	blobs     blob.BlobStore
	events    *events.EventStreamStore[recordPointer]
	namespace string
	secret    []byte
	cipher    cipher.AEAD
}

var namespacePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func NewRepository(options Options) (*Repository, error) {
	if nilInterface(options.Table) || nilInterface(options.Blobs) || !namespacePattern.MatchString(options.Namespace) || len(options.Secret) != 32 {
		return nil, ErrInvalid
	}
	cipher, err := newCipher(options.Secret)
	if err != nil {
		return nil, err
	}
	r := &Repository{table: options.Table, blobs: options.Blobs, namespace: options.Namespace, secret: append([]byte(nil), options.Secret...), cipher: cipher}
	r.events, err = events.NewEventStreamStore(options.Table, func() recordPointer { return recordPointer{} }, r.buildState)
	return r, err
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}

type recordPointer struct {
	ID       RequestID `json:"id"`
	ScopeTag string    `json:"scope"`
	Binding  string    `json:"binding"`
	Revision uint64    `json:"revision"`
	Status   Status    `json:"status"`
	Blob     string    `json:"blob"`
}

func (r *Repository) stream(id RequestID) string { return r.namespace + "/request/" + string(id) }

func (r *Repository) scopeTag(scope Scope) string {
	encoded, _ := json.Marshal(scope)
	return r.digest("scope", encoded)
}

func (r *Repository) binding(request CreateRequest) string {
	encoded, _ := json.Marshal(request)
	return r.digest("request-binding", encoded)
}

func (r *Repository) fingerprint(request CreateRequest) string {
	// Independent request IDs and timestamps do not affect cache identity.
	// The sample index does, without coercing any JSON numbers to float64.
	encoded, _ := json.Marshal(struct {
		Scope    Scope           `json:"scope"`
		Kind     string          `json:"kind"`
		Index    int64           `json:"index"`
		Manifest json.RawMessage `json:"manifest"`
	}{request.Scope, request.Kind, request.RequestIndex, request.Manifest})
	return r.digest("request-fingerprint", encoded)
}

// Create writes the recovery index FIRST, then encrypted payload bytes, then
// the authoritative immutable event, and finally advances index status. No
// successful return is possible before all four steps. Repeat the same request
// after any uncertain result; never create a new ID merely to retry storage.
func (r *Repository) Create(ctx context.Context, request CreateRequest) (Record, error) {
	if err := validContext(ctx); err != nil {
		return Record{}, err
	}
	request, err := normalizeRequest(request)
	if err != nil {
		return Record{}, err
	}
	record := Record{Request: request, Fingerprint: r.fingerprint(request), Revision: 1, Status: StatusPending, Progress: json.RawMessage(`{}`), UpdatedAt: request.CreatedAt}
	data, err := encodeRecord(record)
	if err != nil {
		return Record{}, err
	}
	if err := r.ensureIndex(ctx, request); err != nil {
		return Record{}, err
	}
	return r.publish(ctx, record, data, "create")
}

// Read checks scope before opening payload bytes. A mismatched scope returns
// the same not-found kind as a missing request.
func (r *Repository) Read(ctx context.Context, scope Scope, id RequestID) (Record, error) {
	if !scope.valid() {
		return Record{}, ErrInvalid
	}
	return r.read(ctx, id, r.scopeTag(scope))
}

// ReadForRecovery is privileged, service-internal access for enumerated pending
// IDs. Never expose it directly to an untrusted request or Temporal caller.
func (r *Repository) ReadForRecovery(ctx context.Context, id RequestID) (Record, error) {
	return r.read(ctx, id, "")
}

func (r *Repository) read(ctx context.Context, id RequestID, scopeTag string) (Record, error) {
	if err := validContext(ctx); err != nil {
		return Record{}, err
	}
	if !id.valid() {
		return Record{}, ErrInvalid
	}
	state, err := r.events.ReadState(ctx, r.stream(id))
	if err != nil {
		return Record{}, err
	}
	if state.Revision == 0 || (scopeTag != "" && state.Value.ScopeTag != scopeTag) {
		return Record{}, contracts.ErrNotFound
	}
	if uint64(state.Revision) != state.Value.Revision || state.Value.ID != id {
		return Record{}, ErrCorrupt
	}
	return r.loadRecord(ctx, state.Value)
}

// TryUpdate appends exactly at ExpectedRevision+1. Competing updates return
// ErrConflict; identical retries return the original committed revision even
// when newer revisions already exist. No provider dispatch occurs here.
func (r *Repository) TryUpdate(ctx context.Context, scope Scope, id RequestID, update Update) (Record, error) {
	if err := validContext(ctx); err != nil {
		return Record{}, err
	}
	if !scope.valid() || !id.valid() || update.ExpectedRevision == 0 || update.ExpectedRevision >= maxRevisions || !safeText(update.Token, 128) || !update.Status.valid() || !validTime(update.UpdatedAt) {
		return Record{}, ErrInvalid
	}
	progress, err := objectJSON(update.Progress)
	if err != nil {
		return Record{}, err
	}
	previous, err := r.readRevision(ctx, id, update.ExpectedRevision)
	if err != nil {
		return Record{}, err
	}
	if previous.ScopeTag != r.scopeTag(scope) {
		return Record{}, contracts.ErrNotFound
	}
	record, err := r.loadRecord(ctx, previous)
	if err != nil {
		return Record{}, err
	}
	if !validTransition(record.Status, update.Status) || update.UpdatedAt.Before(record.UpdatedAt) {
		return Record{}, ErrInvalid
	}
	record.Revision++
	record.Status, record.Progress, record.UpdatedAt = update.Status, progress, update.UpdatedAt.UTC()
	data, err := encodeRecord(record)
	if err != nil {
		return Record{}, err
	}
	return r.publish(ctx, record, data, update.Token)
}

func (r *Repository) publish(ctx context.Context, record Record, data []byte, token string) (Record, error) {
	key, err := r.writeBlob(ctx, r.stream(record.Request.ID), data)
	if err != nil {
		return Record{}, err
	}
	pointer := recordPointer{ID: record.Request.ID, ScopeTag: r.scopeTag(record.Request.Scope), Binding: r.binding(record.Request), Revision: record.Revision, Status: record.Status, Blob: key}
	change, _ := json.Marshal(pointer)
	_, err = r.events.TryAppend(ctx, r.stream(pointer.ID), events.EventStreamRevision(record.Revision-1), events.EventAppend{
		AppendToken: r.digest("append-token", []byte(token)), ApplicationVersion: 1, Changes: []json.RawMessage{change},
	})
	if err != nil {
		return Record{}, err
	}
	if err := r.advanceIndex(ctx, record); err != nil {
		return Record{}, fmt.Errorf("%w: %w", ErrIndexPending, err)
	}
	return record, nil
}

func (r *Repository) buildState(ctx context.Context, previous recordPointer, changes []events.EventChange) (recordPointer, error) {
	for _, change := range changes {
		if err := ctx.Err(); err != nil {
			return recordPointer{}, err
		}
		var next recordPointer
		if change.Version != 1 || len(change.Data) > 4096 || json.Unmarshal(change.Data, &next) != nil || !next.ID.valid() || !hexDigest(next.ScopeTag) || !hexDigest(next.Binding) || !r.validBlobKey(next.Blob) || next.Revision != previous.Revision+1 || next.Revision > maxRevisions || !next.Status.valid() {
			return recordPointer{}, ErrCorrupt
		}
		if previous.Revision == 0 {
			if next.Status != StatusPending {
				return recordPointer{}, ErrCorrupt
			}
		} else if next.ID != previous.ID || next.ScopeTag != previous.ScopeTag || next.Binding != previous.Binding || !validTransition(previous.Status, next.Status) {
			return recordPointer{}, ErrCorrupt
		}
		previous = next
	}
	return previous, nil
}

func (r *Repository) readRevision(ctx context.Context, id RequestID, revision uint64) (recordPointer, error) {
	query := events.EventHistoryQuery{}
	var current recordPointer
	seen := map[string]bool{}
	for {
		page, err := r.events.ReadHistory(ctx, r.stream(id), query)
		if err != nil {
			return recordPointer{}, err
		}
		for _, entry := range page.Entries {
			if len(entry.Changes) != 1 {
				return recordPointer{}, ErrCorrupt
			}
			current, err = r.buildState(ctx, current, []events.EventChange{{Version: entry.ApplicationVersion, Data: entry.Changes[0]}})
			if err != nil {
				return recordPointer{}, err
			}
			if current.ID != id || current.Revision != uint64(entry.Revision) {
				return recordPointer{}, ErrCorrupt
			}
			if current.Revision == revision {
				return current, nil
			}
		}
		if page.NextPageToken == "" {
			return recordPointer{}, contracts.ErrConflict
		}
		if seen[page.NextPageToken] {
			return recordPointer{}, ErrCorrupt
		}
		seen[page.NextPageToken], query.PageToken = true, page.NextPageToken
	}
}

func encodeRecord(record Record) ([]byte, error) {
	data, err := json.Marshal(record)
	if err != nil || len(data) > maxPayloadBytes {
		return nil, ErrInvalid
	}
	return data, nil
}

func (r *Repository) loadRecord(ctx context.Context, pointer recordPointer) (Record, error) {
	data, err := r.readBlob(ctx, r.stream(pointer.ID), pointer.Blob)
	if err != nil {
		return Record{}, err
	}
	var record Record
	if json.Unmarshal(data, &record) != nil {
		return Record{}, ErrCorrupt
	}
	request, err := normalizeRequest(record.Request)
	if err != nil || record.Request.ID != pointer.ID || record.Revision != pointer.Revision || record.Status != pointer.Status || r.scopeTag(request.Scope) != pointer.ScopeTag || r.binding(request) != pointer.Binding || record.Fingerprint != r.fingerprint(request) || !validTime(record.UpdatedAt) || record.UpdatedAt.Before(request.CreatedAt) {
		return Record{}, ErrCorrupt
	}
	if _, err := objectJSON(record.Progress); err != nil {
		return Record{}, ErrCorrupt
	}
	return record, nil
}

func validContext(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalid
	}
	return ctx.Err()
}
