package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/mfow/llm-temporal-worker/golang/cache"
)

// ResponseFills shares the snapshot's cloud stores and encryption key. Fill
// metadata is encrypted; only a keyed pointer participates in conditional writes.
func (r *Repository) ResponseFills() cache.FillRepository {
	return &cacheFills{responses: &responseCache{repository: r}}
}

type cacheFills struct{ responses *responseCache }

var _ cache.FillRepository = (*cacheFills)(nil)

type fillEnvelope struct {
	Version int              `json:"version"`
	Record  cache.FillRecord `json:"record"`
}

func (s *cacheFills) read(ctx context.Context, key cache.ResponseKey) (cache.FillRecord, kv.KeyValueVersion, error) {
	rowKey := s.responses.key("fill", key)
	pointer, version, err := s.responses.readPointer(ctx, rowKey)
	if err != nil {
		return cache.FillRecord{}, "", err
	}
	if pointer.Stream != rowKey.PartitionKey {
		return cache.FillRecord{}, "", ErrCorrupt
	}
	data, err := s.responses.repository.readBlob(ctx, pointer.Stream, pointer.Blob)
	if err != nil {
		// A dangling reference must never look like an absent fill to Acquire.
		return cache.FillRecord{}, "", errors.Join(ErrCorrupt, err)
	}
	var envelope fillEnvelope
	if json.Unmarshal(data, &envelope) != nil || envelope.Version != 1 || envelope.Record.Lease.Key != key || !validFillRecord(envelope.Record) {
		return cache.FillRecord{}, "", ErrCorrupt
	}
	return envelope.Record, version, nil
}

func (s *cacheFills) write(ctx context.Context, record cache.FillRecord, version kv.KeyValueVersion) error {
	key := s.responses.key("fill", record.Lease.Key)
	data, err := json.Marshal(fillEnvelope{Version: 1, Record: record})
	if err != nil {
		return ErrInvalid
	}
	blob, err := s.responses.repository.writeBlob(ctx, key.PartitionKey, data)
	if err != nil {
		return err
	}
	item := cacheItem(key, cachePointer{Version: 1, Blob: blob, Stream: key.PartitionKey})
	if version == "" {
		_, err = s.responses.repository.table.Create(ctx, item)
	} else {
		_, err = s.responses.repository.table.Replace(ctx, item, version)
	}
	return err
}

func (s *cacheFills) Acquire(ctx context.Context, lease cache.FillLease) (cache.FillDecision, error) {
	if err := validContext(ctx); err != nil {
		return cache.FillDecision{}, err
	}
	lease, err := normalizeFillLease(lease)
	if err != nil {
		return cache.FillDecision{}, err
	}
	if terminal, found, err := s.readTerminal(ctx, lease); err != nil || found {
		return cache.FillDecision{Disposition: cache.FillAttemptFinished, Record: terminal}, err
	}
	for range 16 {
		current, version, err := s.read(ctx, lease.Key)
		if err != nil && (!errors.Is(err, contracts.ErrNotFound) || errors.Is(err, ErrCorrupt) || errors.Is(err, contracts.ErrOutcomeUnknown)) {
			return cache.FillDecision{}, err
		}
		if err == nil {
			decision, replace, err := decideFill(current, lease)
			if err != nil || !replace {
				return decision, err
			}
			if current.State == cache.FillReleased || current.State == cache.FillFinished {
				// Preserve a completion witness before replacing the head, so a
				// finalizer whose acknowledgement was lost can still reconcile.
				if err := s.sealTerminal(ctx, current); err != nil {
					return cache.FillDecision{}, err
				}
			}
		}
		next := cache.FillRecord{Lease: lease, State: cache.FillHeld, UpdatedAt: lease.AcquiredAt}
		err = s.write(ctx, next, version)
		if err == nil {
			return cache.FillDecision{Disposition: cache.FillOwned, Record: next}, nil
		}
		if !retryFillConflict(err) {
			return cache.FillDecision{}, err
		}
	}
	return cache.FillDecision{}, contracts.ErrConflict
}

func decideFill(current cache.FillRecord, lease cache.FillLease) (cache.FillDecision, bool, error) {
	decision := cache.FillDecision{Record: current}
	if current.Lease.Attempt == lease.Attempt {
		if current.Lease != lease {
			return decision, false, contracts.ErrConflict
		}
		switch current.State {
		case cache.FillHeld:
			decision.Disposition = cache.FillOwned
		case cache.FillStarted:
			decision.Disposition = cache.FillRecoveryNeeded
		default:
			decision.Disposition = cache.FillAttemptFinished
		}
		return decision, false, nil
	}
	if current.State == cache.FillStarted {
		decision.Disposition = cache.FillRecoveryNeeded
		return decision, false, nil
	}
	if current.State == cache.FillHeld && lease.AcquiredAt.Before(current.Lease.ExpiresAt) {
		decision.Disposition = cache.FillWait
		return decision, false, nil
	}
	// Tombstones and strictly increasing acquisition times fence delayed old
	// acquisitions. Never delete/TTL this row independently of its operations.
	if !lease.AcquiredAt.After(current.UpdatedAt) {
		return decision, false, contracts.ErrConflict
	}
	return decision, true, nil
}

func retryFillConflict(err error) bool {
	return !errors.Is(err, contracts.ErrOutcomeUnknown) && (errors.Is(err, contracts.ErrConflict) || errors.Is(err, contracts.ErrAlreadyExists))
}

// Start deliberately does not return a reusable dispatch token. Repeated calls
// for the same started lease return false, including after a lost acknowledgement.
func (s *cacheFills) Start(ctx context.Context, lease cache.FillLease, now time.Time) (bool, error) {
	if err := validContext(ctx); err != nil {
		return false, err
	}
	lease, err := normalizeFillLease(lease)
	if err != nil || !validTime(now) || now.Before(lease.AcquiredAt) {
		return false, ErrInvalid
	}
	for range 16 {
		current, version, err := s.readOwner(ctx, lease)
		if err != nil {
			return false, err
		}
		if current.State == cache.FillStarted {
			return false, nil
		}
		if current.State != cache.FillHeld || !now.Before(lease.ExpiresAt) {
			return false, contracts.ErrConflict
		}
		current.State, current.UpdatedAt = cache.FillStarted, now.UTC()
		err = s.write(ctx, current, version)
		if err == nil {
			return true, nil
		}
		if !retryFillConflict(err) {
			return false, err
		}
	}
	return false, contracts.ErrConflict
}

func (s *cacheFills) readOwner(ctx context.Context, lease cache.FillLease) (cache.FillRecord, kv.KeyValueVersion, error) {
	record, version, err := s.read(ctx, lease.Key)
	if err == nil && record.Lease != lease {
		err = contracts.ErrConflict
	}
	return record, version, err
}

func (s *cacheFills) Release(ctx context.Context, lease cache.FillLease, now time.Time) error {
	return s.finish(ctx, lease, cache.FillReleased, cache.FillCompletion{}, now)
}

func (s *cacheFills) Complete(ctx context.Context, lease cache.FillLease, completion cache.FillCompletion) error {
	completion.CompletedAt = completion.CompletedAt.UTC()
	if !validFillCompletion(completion) {
		return ErrInvalid
	}
	return s.finish(ctx, lease, cache.FillFinished, completion, completion.CompletedAt)
}

func (s *cacheFills) finish(ctx context.Context, lease cache.FillLease, target cache.FillState, completion cache.FillCompletion, now time.Time) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	lease, err := normalizeFillLease(lease)
	if err != nil || !validTime(now) || now.Before(lease.AcquiredAt) {
		return ErrInvalid
	}
	if terminal, found, err := s.readTerminal(ctx, lease); err != nil || found {
		if err != nil {
			return err
		}
		return matchTerminal(terminal, target, completion, now)
	}
	for range 16 {
		current, version, err := s.readOwner(ctx, lease)
		if err != nil {
			if errors.Is(err, contracts.ErrConflict) {
				if terminal, found, readErr := s.readTerminal(ctx, lease); readErr != nil || found {
					if readErr != nil {
						return readErr
					}
					return matchTerminal(terminal, target, completion, now)
				}
			}
			return err
		}
		if current.State == target && current.UpdatedAt.Equal(now) && current.Completion == completion {
			return s.sealTerminal(ctx, current)
		}
		if now.Before(current.UpdatedAt) || (target == cache.FillReleased && current.State != cache.FillHeld) || (target == cache.FillFinished && current.State != cache.FillStarted) {
			return contracts.ErrConflict
		}
		if completion.Outcome == cache.FillPublished {
			if err := s.validatePublished(ctx, lease, current.UpdatedAt, completion); err != nil {
				return err
			}
		}
		current.State, current.UpdatedAt, current.Completion = target, now.UTC(), completion
		err = s.write(ctx, current, version)
		if err == nil {
			return s.sealTerminal(ctx, current)
		}
		if !retryFillConflict(err) {
			return err
		}
	}
	return contracts.ErrConflict
}

func (s *cacheFills) validatePublished(ctx context.Context, lease cache.FillLease, startedAt time.Time, completion cache.FillCompletion) error {
	pointer, _, err := s.responses.readPointer(ctx, s.responses.entryKey(lease.Key.ScopeID, completion.EntryID))
	if err != nil {
		return err
	}
	entry, err := s.responses.loadEntry(ctx, lease.Key.ScopeID, pointer)
	if err != nil {
		return err
	}
	if entry.ID != completion.EntryID || entry.Key != lease.Key || entry.OriginOperationID != lease.OperationID || entry.CompletedAt.Before(startedAt) || entry.CompletedAt.After(completion.CompletedAt) {
		return ErrInvalid
	}
	// An immutable entry can exist before its success pointer. Finish only
	// after publication is visible (or superseded by a newer success).
	// Do not apply the caller's completion-time freshness filter here: a newer
	// success may have arrived while this older finalization was being retried.
	headPointer, _, err := s.responses.readPointer(ctx, s.responses.key("success", lease.Key))
	if errors.Is(err, contracts.ErrNotFound) && !errors.Is(err, contracts.ErrOutcomeUnknown) {
		return contracts.ErrConflict
	}
	if err != nil {
		return err
	}
	head, err := s.responses.loadEntry(ctx, lease.Key.ScopeID, headPointer)
	if err != nil {
		return err
	}
	if head.Key != lease.Key {
		return ErrCorrupt
	}
	if head.CompletedAt.Before(entry.CompletedAt) || (head.CompletedAt.Equal(entry.CompletedAt) && head.ID < entry.ID) {
		return contracts.ErrConflict
	}
	return nil
}
