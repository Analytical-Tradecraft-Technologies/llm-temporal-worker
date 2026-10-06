package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
)

func (s *cacheFills) terminalKey(lease cache.FillLease) kv.KeyValueKey {
	return s.responses.key("fill-receipt", struct {
		Key     cache.ResponseKey
		Attempt string
	}{lease.Key, lease.Attempt})
}

// sealTerminal must only be called after observing a committed terminal head.
// It binds the existing encrypted blob; no extra blob write is needed. Acquire
// also seals before replacing terminal heads, closing the lost-ack/takeover gap.
func (s *cacheFills) sealTerminal(ctx context.Context, record cache.FillRecord) error {
	key := s.terminalKey(record.Lease)
	stream := s.responses.key("fill", record.Lease.Key).PartitionKey
	data, err := json.Marshal(fillEnvelope{Version: 1, Record: record})
	if err != nil {
		return ErrInvalid
	}
	want := cachePointer{Version: 1, Stream: stream, Blob: s.responses.repository.blobKey(stream, data)}
	existing, _, err := s.responses.readPointer(ctx, key)
	if err == nil {
		if existing != want {
			return contracts.ErrConflict
		}
		return nil
	}
	if !errors.Is(err, contracts.ErrNotFound) || errors.Is(err, contracts.ErrOutcomeUnknown) {
		return err
	}
	_, err = s.responses.repository.table.Create(ctx, cacheItem(key, want))
	if errors.Is(err, contracts.ErrAlreadyExists) && !errors.Is(err, contracts.ErrOutcomeUnknown) {
		existing, _, err = s.responses.readPointer(ctx, key)
		if err == nil && existing != want {
			return contracts.ErrConflict
		}
	}
	return err
}

func (s *cacheFills) readTerminal(ctx context.Context, lease cache.FillLease) (cache.FillRecord, bool, error) {
	pointer, _, err := s.responses.readPointer(ctx, s.terminalKey(lease))
	if errors.Is(err, contracts.ErrNotFound) && !errors.Is(err, contracts.ErrOutcomeUnknown) {
		return cache.FillRecord{}, false, nil
	}
	if err != nil {
		return cache.FillRecord{}, false, err
	}
	stream := s.responses.key("fill", lease.Key).PartitionKey
	if pointer.Stream != stream {
		return cache.FillRecord{}, false, ErrCorrupt
	}
	data, err := s.responses.repository.readBlob(ctx, stream, pointer.Blob)
	if err != nil {
		return cache.FillRecord{}, false, fillBlobError(err)
	}
	var envelope fillEnvelope
	if json.Unmarshal(data, &envelope) != nil || envelope.Version != 1 || !validFillRecord(envelope.Record) ||
		(envelope.Record.State != cache.FillReleased && envelope.Record.State != cache.FillFinished) || envelope.Record.Lease.Key != lease.Key || envelope.Record.Lease.Attempt != lease.Attempt {
		return cache.FillRecord{}, false, ErrCorrupt
	}
	if envelope.Record.Lease != lease {
		return cache.FillRecord{}, false, contracts.ErrConflict
	}
	return envelope.Record, true, nil
}

func matchTerminal(record cache.FillRecord, state cache.FillState, completion cache.FillCompletion, now time.Time) error {
	if record.State != state || record.Completion != completion || !record.UpdatedAt.Equal(now) {
		return contracts.ErrConflict
	}
	return nil
}
