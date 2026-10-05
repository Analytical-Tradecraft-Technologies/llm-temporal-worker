package cloudstate

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

const checkpointFormat = 1

// Checkpoints shares the opened table, blob store and independent encryption
// key. Checkpoints have their own key domains and never enter the pending-request
// index. Only an authenticated internal scope identity may be supplied here.
func (r *Repository) Checkpoints() state.CheckpointStore { return &checkpointStore{repository: r} }

type checkpointStore struct{ repository *Repository }

var _ state.CheckpointStore = (*checkpointStore)(nil)

type checkpointEnvelope struct {
	Version    int                     `json:"version"`
	Checkpoint state.DurableCheckpoint `json:"checkpoint"`
}

// All fields other than the format are keyed digests or encrypted-blob locators.
// The operation row is the publication point. ID and handle rows reserve unique
// names first; a reservation alone is never a readable checkpoint.
type checkpointPointer struct {
	Version   int    `json:"version"`
	Blob      string `json:"blob"`
	Operation string `json:"operation"`
	Handle    string `json:"handle"`
}

func (s *checkpointStore) identity(scope, kind, value string) string {
	data, _ := json.Marshal([]string{scope, value})
	return s.repository.digest("checkpoint/"+kind, data)
}

func (s *checkpointStore) stream(scope string, id state.CheckpointID) string {
	return "checkpoint/" + s.identity(scope, "id", string(id))
}

func (s *checkpointStore) key(kind, tag string) kv.KeyValueKey {
	return kv.KeyValueKey{PartitionKey: s.repository.namespace + "/checkpoint/" + kind + "/" + tag, SortKey: "v1"}
}

func (s *checkpointStore) Get(ctx context.Context, scope string, id state.CheckpointID) (state.DurableCheckpoint, error) {
	if err := validContext(ctx); err != nil {
		return state.DurableCheckpoint{}, err
	}
	if !safeText(scope, 512) || !safeText(string(id), 256) {
		return state.DurableCheckpoint{}, ErrInvalid
	}
	pointer, err := s.readPointer(ctx, s.key("id", s.identity(scope, "id", string(id))))
	if err != nil {
		return state.DurableCheckpoint{}, err
	}
	committed, err := s.readPointer(ctx, s.key("operation", pointer.Operation))
	if err != nil {
		return state.DurableCheckpoint{}, err
	}
	if committed != pointer {
		// Another checkpoint won this operation. This ID is only an abandoned
		// reservation, never a partially visible or competing publication.
		return state.DurableCheckpoint{}, contracts.ErrNotFound
	}
	data, err := s.repository.readReferencedBlob(ctx, s.stream(scope, id), pointer.Blob)
	if err != nil {
		return state.DurableCheckpoint{}, err
	}
	var envelope checkpointEnvelope
	if json.Unmarshal(data, &envelope) != nil || envelope.Version != checkpointFormat {
		return state.DurableCheckpoint{}, ErrCorrupt
	}
	checkpoint := envelope.Checkpoint
	if checkpoint.ScopeID != scope || checkpoint.ID != id || validateCloudCheckpoint(checkpoint) != nil || s.pointer(checkpoint, pointer.Blob) != pointer {
		return state.DurableCheckpoint{}, ErrCorrupt
	}
	return checkpoint, nil
}

func validateCloudCheckpoint(checkpoint state.DurableCheckpoint) error {
	if _, err := checkpoint.CanonicalDigest(); err != nil {
		return ErrInvalid
	}
	if !safeText(checkpoint.ScopeID, 512) || !safeText(string(checkpoint.ID), 256) || !safeText(string(checkpoint.OriginOperationID), 4096) {
		return ErrInvalid
	}
	return nil
}

func (s *checkpointStore) pointer(checkpoint state.DurableCheckpoint, blob string) checkpointPointer {
	return checkpointPointer{Version: checkpointFormat, Blob: blob,
		Operation: s.identity(checkpoint.ScopeID, "operation", string(checkpoint.OriginOperationID)),
		Handle:    s.identity(checkpoint.ScopeID, "handle", hex.EncodeToString(checkpoint.PublicIDHMAC[:]))}
}

func (s *checkpointStore) readPointer(ctx context.Context, key kv.KeyValueKey) (checkpointPointer, error) {
	row, err := s.repository.table.Get(ctx, key)
	if err != nil {
		return checkpointPointer{}, err
	}
	data, ok := row.Item.Fields["checkpoint"].(kv.KeyValueBytes)
	var pointer checkpointPointer
	if !ok || len(data) > 2048 || row.Item.Key() != key || row.Version == "" || json.Unmarshal(data, &pointer) != nil || pointer.Version != checkpointFormat || !s.repository.validBlobKey(pointer.Blob) || !hexDigest(pointer.Operation) || !hexDigest(pointer.Handle) {
		return checkpointPointer{}, ErrCorrupt
	}
	return pointer, nil
}

func (s *checkpointStore) createPointer(ctx context.Context, key kv.KeyValueKey, pointer checkpointPointer) error {
	data, _ := json.Marshal(pointer)
	_, err := s.repository.table.Create(ctx, kv.KeyValueItem{PartitionKey: key.PartitionKey, SortKey: key.SortKey, Fields: kv.KeyValueDocument{"checkpoint": kv.Bytes(data)}})
	if err == nil {
		return nil
	}
	// An uncertain mutation stays uncertain. A later identical retry can
	// reconcile it, but must not generate new checkpoint IDs or timestamps.
	if errors.Is(err, contracts.ErrOutcomeUnknown) || !errors.Is(err, contracts.ErrAlreadyExists) {
		return err
	}
	existing, err := s.readPointer(ctx, key)
	if err != nil {
		return err
	}
	if existing != pointer {
		return contracts.ErrConflict
	}
	return nil
}

func (s *checkpointStore) publish(ctx context.Context, checkpoint state.DurableCheckpoint, data []byte) error {
	// Avoid all writes on completed retries, and detect conflicting immutable
	// content before creating unreferenced encrypted objects.
	existing, err := s.Get(ctx, checkpoint.ScopeID, checkpoint.ID)
	if err == nil {
		want, _ := checkpoint.CanonicalDigest()
		got, err := existing.CanonicalDigest()
		if err != nil || got != want {
			return contracts.ErrConflict
		}
		return nil
	}
	if !errors.Is(err, contracts.ErrNotFound) {
		return err
	}
	if err := s.validateReferences(ctx, checkpoint); err != nil {
		return err
	}
	blob, err := s.repository.writeBlob(ctx, s.stream(checkpoint.ScopeID, checkpoint.ID), data)
	if err != nil {
		return err
	}
	pointer := s.pointer(checkpoint, blob)
	for _, key := range []kv.KeyValueKey{
		s.key("id", s.identity(checkpoint.ScopeID, "id", string(checkpoint.ID))),
		s.key("handle", pointer.Handle),
		s.key("operation", pointer.Operation),
	} {
		if err := s.createPointer(ctx, key, pointer); err != nil {
			return err
		}
	}
	return nil
}

func (s *checkpointStore) validateReferences(ctx context.Context, checkpoint state.DurableCheckpoint) error {
	if checkpoint.ParentID != nil {
		parent, err := s.Get(ctx, checkpoint.ScopeID, *checkpoint.ParentID)
		if err != nil {
			return err
		}
		if int64(checkpoint.Depth) != int64(parent.Depth)+1 {
			return ErrInvalid
		}
	}
	if checkpoint.CompactedThroughID != nil {
		if _, err := s.Get(ctx, checkpoint.ScopeID, *checkpoint.CompactedThroughID); err != nil {
			return err
		}
	}
	references := []state.CheckpointBlobReference{checkpoint.DeltaBlob, checkpoint.ResponseBlob, checkpoint.SettingsPatchBlob}
	if checkpoint.MaterializedSnapshotBlob != nil {
		references = append(references, *checkpoint.MaterializedSnapshotBlob)
	}
	for _, provider := range checkpoint.ProviderState {
		references = append(references, provider.StateBlob)
	}
	for _, reference := range references {
		if _, err := s.Read(ctx, checkpoint.ScopeID, reference); err != nil {
			return err
		}
	}
	// Operation/cache IDs are provenance supplied by the finalizer. Their
	// existence and request authorization belong to that composition, not to
	// this adapter; they do not independently authorize paid work.
	return nil
}

func (s *checkpointStore) BeginCheckpoint(ctx context.Context) (state.CheckpointUnitOfWork, error) {
	if err := validContext(ctx); err != nil {
		return nil, err
	}
	return &checkpointUnit{store: s}, nil
}

// This is a local staging buffer, not a distributed transaction or lock. No
// storage mutation occurs until Commit. One unit cannot publish multiple rows.
type checkpointUnit struct {
	mu     sync.Mutex
	store  *checkpointStore
	value  state.DurableCheckpoint
	data   []byte
	closed bool
}

func (u *checkpointUnit) PutCheckpoint(ctx context.Context, write state.CheckpointWrite) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := validContext(ctx); err != nil {
		return err
	}
	if u.closed || validateCloudCheckpoint(write.Checkpoint) != nil {
		return ErrInvalid
	}
	// Encoding and decoding detaches all caller-owned slices and pointers.
	data, err := json.Marshal(checkpointEnvelope{Version: checkpointFormat, Checkpoint: write.Checkpoint})
	if err != nil {
		return ErrInvalid
	}
	data, err = objectJSON(data)
	if err != nil {
		return err
	}
	var envelope checkpointEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return ErrInvalid
	}
	value := envelope.Checkpoint
	value.CreatedAt, value.ExpiresAt = value.CreatedAt.UTC(), value.ExpiresAt.UTC()
	if value.ProviderState == nil {
		value.ProviderState = []state.CheckpointProviderState{}
	}
	if value.Affinities == nil {
		value.Affinities = state.ProviderCacheAffinitySet{}
	}
	for i := range value.ProviderState {
		value.ProviderState[i].CreatedAt = value.ProviderState[i].CreatedAt.UTC()
		if expiry := value.ProviderState[i].ExpiresAt; expiry != nil {
			utc := expiry.UTC()
			value.ProviderState[i].ExpiresAt = &utc
		}
	}
	for i := range value.Affinities {
		value.Affinities[i].LastSuccessAt = value.Affinities[i].LastSuccessAt.UTC()
		if expiry := value.Affinities[i].ExpiresAt; expiry != nil {
			utc := expiry.UTC()
			value.Affinities[i].ExpiresAt = &utc
		}
	}
	if u.data != nil {
		previous, _ := u.value.CanonicalDigest()
		next, err := value.CanonicalDigest()
		if err != nil || next != previous {
			return contracts.ErrConflict
		}
		return nil
	}
	encoded, err := json.Marshal(checkpointEnvelope{Version: checkpointFormat, Checkpoint: value})
	if err != nil {
		return ErrInvalid
	}
	data, err = objectJSON(encoded)
	if err != nil {
		return err
	}
	u.value, u.data = value, data
	return nil
}

func (u *checkpointUnit) Commit(ctx context.Context) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := validContext(ctx); err != nil {
		return err
	}
	if u.closed || u.data == nil {
		return ErrInvalid
	}
	u.closed = true
	return u.store.publish(ctx, u.value, u.data)
}

func (u *checkpointUnit) Rollback(ctx context.Context) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := validContext(ctx); err != nil {
		return err
	}
	u.closed, u.data = true, nil
	u.value = state.DurableCheckpoint{}
	return nil
}
