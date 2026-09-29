package cloudstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

// Responses shares the configured KV/blob stores and encryption secret. Cache
// records have their own key domains and do not enter pending-request shards.
func (r *Repository) Responses() cache.ResponseRepository { return &responseCache{repository: r} }

type responseCache struct{ repository *Repository }

var _ cache.ResponseRepository = (*responseCache)(nil)

type cachePointer struct {
	Version int    `json:"version"`
	Blob    string `json:"blob"`
	Stream  string `json:"stream"`
}

type cacheEntryEnvelope struct {
	Version int                 `json:"version"`
	Entry   cache.ResponseEntry `json:"entry"`
}

type cacheUseEnvelope struct {
	Version int               `json:"version"`
	Use     cache.ResponseUse `json:"use"`
}

func (s *responseCache) key(kind string, value any) kv.KeyValueKey {
	data, _ := json.Marshal(value)
	return kv.KeyValueKey{PartitionKey: s.repository.namespace + "/cache/" + kind + "/" + s.repository.digest("cache/"+kind, data), SortKey: "v1"}
}

func (s *responseCache) entryKey(scope string, id state.CacheEntryID) kv.KeyValueKey {
	return s.key("entry", []string{scope, string(id)})
}

func cacheItem(key kv.KeyValueKey, pointer cachePointer) kv.KeyValueItem {
	data, _ := json.Marshal(pointer)
	return kv.KeyValueItem{PartitionKey: key.PartitionKey, SortKey: key.SortKey, Fields: kv.KeyValueDocument{"cache": kv.Bytes(data)}}
}

func (s *responseCache) readPointer(ctx context.Context, key kv.KeyValueKey) (cachePointer, kv.KeyValueVersion, error) {
	row, err := s.repository.table.Get(ctx, key)
	if err != nil {
		return cachePointer{}, "", err
	}
	data, ok := row.Item.Fields["cache"].(kv.KeyValueBytes)
	var pointer cachePointer
	if !ok || len(data) > 2048 || row.Item.Key() != key || row.Version == "" || json.Unmarshal(data, &pointer) != nil || pointer.Version != 1 || !s.repository.validBlobKey(pointer.Blob) || !s.validStream(pointer.Stream) {
		return cachePointer{}, "", ErrCorrupt
	}
	return pointer, row.Version, nil
}

// immutable stores a bounded encrypted document then conditionally binds its
// ID. Replays read first to avoid writes once the ID has been committed.
func (s *responseCache) immutable(ctx context.Context, key kv.KeyValueKey, stream string, data []byte) (cachePointer, error) {
	want := cachePointer{Version: 1, Blob: s.repository.blobKey(stream, data), Stream: stream}
	existing, _, err := s.readPointer(ctx, key)
	if err == nil {
		if existing != want {
			return cachePointer{}, contracts.ErrConflict
		}
		stored, err := s.repository.readBlob(ctx, stream, existing.Blob)
		if err != nil {
			return cachePointer{}, err
		}
		if !bytes.Equal(stored, data) {
			return cachePointer{}, ErrCorrupt
		}
		return existing, nil
	}
	if !errors.Is(err, contracts.ErrNotFound) {
		return cachePointer{}, err
	}
	if _, err := s.repository.writeBlob(ctx, stream, data); err != nil {
		return cachePointer{}, err
	}
	_, err = s.repository.table.Create(ctx, cacheItem(key, want))
	if errors.Is(err, contracts.ErrAlreadyExists) && !errors.Is(err, contracts.ErrOutcomeUnknown) {
		existing, _, err = s.readPointer(ctx, key)
		if err == nil && existing != want {
			err = contracts.ErrConflict
		}
	}
	return want, err
}

func (s *responseCache) loadEntry(ctx context.Context, scope string, pointer cachePointer) (cache.ResponseEntry, error) {
	data, err := s.repository.readBlob(ctx, pointer.Stream, pointer.Blob)
	if err != nil {
		return cache.ResponseEntry{}, err
	}
	var envelope cacheEntryEnvelope
	if json.Unmarshal(data, &envelope) != nil || envelope.Version != 1 || envelope.Entry.Key.ScopeID != scope || pointer.Stream != s.entryKey(scope, envelope.Entry.ID).PartitionKey {
		return cache.ResponseEntry{}, ErrCorrupt
	}
	entry, err := normalizeCacheEntry(envelope.Entry)
	if err != nil {
		return cache.ResponseEntry{}, ErrCorrupt
	}
	bound, _, err := s.readPointer(ctx, s.entryKey(scope, entry.ID))
	if err != nil {
		return cache.ResponseEntry{}, err
	}
	if bound != pointer {
		return cache.ResponseEntry{}, ErrCorrupt
	}
	return entry, nil
}

// Publish preserves an immutable entry and advances the latest-success pointer
// only by completion time (entry ID breaks ties). Failures cannot replace a
// success. An old retry cannot roll the head back. Only definite CAS conflicts
// are retried here; an unknown write outcome is returned to the finalizer.
func (s *responseCache) Publish(ctx context.Context, entry cache.ResponseEntry) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	entry, err := normalizeCacheEntry(entry)
	if err != nil {
		return err
	}
	if err := s.validateOrigin(ctx, entry); err != nil {
		return err
	}
	data, err := json.Marshal(cacheEntryEnvelope{Version: 1, Entry: entry})
	if err != nil || len(data) > maxPayloadBytes {
		return ErrInvalid
	}
	head := s.key("success", entry.Key)
	entryKey := s.entryKey(entry.Key.ScopeID, entry.ID)
	pointer, err := s.immutable(ctx, entryKey, entryKey.PartitionKey, data)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 16; attempt++ {
		previous, version, err := s.readPointer(ctx, head)
		switch {
		case errors.Is(err, contracts.ErrNotFound):
			_, err = s.repository.table.Create(ctx, cacheItem(head, pointer))
		case err != nil:
			return err
		default:
			current, loadErr := s.loadEntry(ctx, entry.Key.ScopeID, previous)
			if loadErr != nil {
				return loadErr
			}
			if current.Key != entry.Key {
				return ErrCorrupt
			}
			if current.CompletedAt.After(entry.CompletedAt) || (current.CompletedAt.Equal(entry.CompletedAt) && current.ID >= entry.ID) {
				return nil
			}
			_, err = s.repository.table.Replace(ctx, cacheItem(head, pointer), version)
		}
		if errors.Is(err, contracts.ErrOutcomeUnknown) || (!errors.Is(err, contracts.ErrConflict) && !errors.Is(err, contracts.ErrAlreadyExists)) {
			return err
		}
	}
	return contracts.ErrConflict
}

func (s *responseCache) Lookup(ctx context.Context, lookup cache.ResponseLookup) (*cache.ResponseEntry, error) {
	if err := validContext(ctx); err != nil {
		return nil, err
	}
	if !validCacheKey(lookup.Key) || !validTime(lookup.Now) || (lookup.MaxAge != nil && *lookup.MaxAge <= 0) {
		return nil, ErrInvalid
	}
	pointer, _, err := s.readPointer(ctx, s.key("success", lookup.Key))
	if errors.Is(err, contracts.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	entry, err := s.loadEntry(ctx, lookup.Key.ScopeID, pointer)
	if err != nil {
		return nil, err
	}
	if entry.Key != lookup.Key {
		return nil, ErrCorrupt
	}
	if entry.CompletedAt.After(lookup.Now) || (lookup.MaxAge != nil && entry.CompletedAt.Before(lookup.Now.Add(-*lookup.MaxAge))) {
		return nil, nil
	}
	return &entry, nil
}

// RecordUse is the finalizer's durable, idempotent cache-consumption receipt.
// Checkpoint publication comes first; retry only this step after a lost write.
func (s *responseCache) RecordUse(ctx context.Context, use cache.ResponseUse) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if !validCacheUse(use) {
		return ErrInvalid
	}
	use.CompletedAt = use.CompletedAt.UTC()
	pointer, _, err := s.readPointer(ctx, s.entryKey(use.ScopeID, use.EntryID))
	if err != nil {
		return err
	}
	entry, err := s.loadEntry(ctx, use.ScopeID, pointer)
	if err != nil {
		return err
	}
	if entry.ID != use.EntryID || use.CompletedAt.Before(entry.CompletedAt) {
		return ErrInvalid
	}
	if err := s.validateUse(ctx, use, entry); err != nil {
		return err
	}
	data, _ := json.Marshal(cacheUseEnvelope{Version: 1, Use: use})
	key := s.key("use", []string{use.ScopeID, string(use.OperationID)})
	_, err = s.immutable(ctx, key, key.PartitionKey, data)
	return err
}

func (s *responseCache) ReadUse(ctx context.Context, scope string, operation state.OperationID) (cache.ResponseUse, error) {
	if err := validContext(ctx); err != nil {
		return cache.ResponseUse{}, err
	}
	if !safeText(scope, 512) || !safeText(string(operation), 4096) {
		return cache.ResponseUse{}, ErrInvalid
	}
	key := s.key("use", []string{scope, string(operation)})
	pointer, _, err := s.readPointer(ctx, key)
	if err != nil {
		return cache.ResponseUse{}, err
	}
	if pointer.Stream != key.PartitionKey {
		return cache.ResponseUse{}, ErrCorrupt
	}
	data, err := s.repository.readBlob(ctx, key.PartitionKey, pointer.Blob)
	if err != nil {
		return cache.ResponseUse{}, err
	}
	var envelope cacheUseEnvelope
	if json.Unmarshal(data, &envelope) != nil || envelope.Version != 1 || !validCacheUse(envelope.Use) || envelope.Use.ScopeID != scope || envelope.Use.OperationID != operation {
		return cache.ResponseUse{}, ErrCorrupt
	}
	return envelope.Use, nil
}

func (s *responseCache) validStream(stream string) bool {
	for _, kind := range []string{"entry", "use", "fill"} {
		prefix := s.repository.namespace + "/cache/" + kind + "/"
		if strings.HasPrefix(stream, prefix) && hexDigest(strings.TrimPrefix(stream, prefix)) {
			return true
		}
	}
	return false
}
