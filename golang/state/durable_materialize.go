package state

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

// DurableCheckpointMaterializer is the storage-neutral adapter that turns the
// metadata-only CheckpointRepository port into the same MaterializedState
// contract implemented by CheckpointGraph. It deliberately performs all blob
// reads before building the in-memory graph and does not open a SQL
// transaction, publish rows, or invoke Generate/Compact.
type DurableCheckpointMaterializer struct {
	Repository     CheckpointRepository
	Blobs          CheckpointBlobReader
	Codec          CheckpointBlobCodec
	HandleVerifier CheckpointHandleVerifier
	Now            func() time.Time
}

var _ CheckpointMaterializer = (*DurableCheckpointMaterializer)(nil)
var _ CheckpointHandleMaterializer = (*DurableCheckpointMaterializer)(nil)

func (materializer *DurableCheckpointMaterializer) Materialize(ctx context.Context, scopeID string, checkpointID CheckpointID, limits MaterializeLimits) (MaterializedState, error) {
	if materializer == nil || materializer.Repository == nil {
		return MaterializedState{}, errors.New("durable checkpoint materializer repository is not configured")
	}
	if materializer.Blobs == nil {
		return MaterializedState{}, errors.New("durable checkpoint materializer blob reader is not configured")
	}
	if ctx == nil {
		return MaterializedState{}, errors.New("durable checkpoint materializer context is nil")
	}
	if strings.TrimSpace(scopeID) == "" || checkpointID == "" {
		return MaterializedState{}, ErrNotFound
	}
	if err := ctx.Err(); err != nil {
		return MaterializedState{}, err
	}
	limits = limits.withDefaults()
	now := materializer.clock()
	codec := materializer.Codec.withDefaults()
	blobCache := make(map[CheckpointBlobReference][]byte)
	blobCacheErrors := make(map[CheckpointBlobReference]error)
	var blobCacheMu sync.Mutex
	readBlob := func(reference CheckpointBlobReference) ([]byte, error) {
		blobCacheMu.Lock()
		cached, ok := blobCache[reference]
		cachedErr, failed := blobCacheErrors[reference]
		blobCacheMu.Unlock()
		if ok || failed {
			return cached, cachedErr
		}
		value, err := materializer.Blobs.Read(ctx, scopeID, reference)
		blobCacheMu.Lock()
		if err != nil {
			blobCacheErrors[reference] = err
		} else {
			blobCache[reference] = value
		}
		blobCacheMu.Unlock()
		return value, err
	}

	type loaded struct {
		row      DurableCheckpoint
		delta    []llm.Item
		response []llm.Item
		patch    SettingsPatch
		snapshot *CheckpointSnapshot
	}
	path := make([]loaded, 0, 16)
	batched := make(map[CheckpointID]DurableCheckpoint)
	var batchedRows []DurableCheckpoint
	if repository, ok := materializer.Repository.(CheckpointLineageRepository); ok {
		rows, err := repository.GetLineage(ctx, scopeID, checkpointID, limits.MaxRows)
		if err != nil {
			return MaterializedState{}, err
		}
		for _, row := range rows {
			if _, duplicate := batched[row.ID]; duplicate {
				return MaterializedState{}, fmt.Errorf("%w: durable checkpoint lineage batch contains a cycle", ErrInvalidCheckpoint)
			}
			batched[row.ID] = row
			batchedRows = append(batchedRows, row)
		}
	}
	if len(batchedRows) != 0 {
		seenBatch := make(map[CheckpointID]struct{}, len(batchedRows))
		references := make(map[CheckpointBlobReference]struct{}, len(batchedRows)+1)
		for index, row := range batchedRows {
			if row.ScopeID != scopeID {
				return MaterializedState{}, ErrTenantMismatch
			}
			if err := row.Validate(now); err != nil {
				return MaterializedState{}, invalidCheckpoint(fmt.Errorf("validate durable checkpoint %s: %w", row.ID, err))
			}
			if !now.Before(row.ExpiresAt) {
				return MaterializedState{}, ErrExpired
			}
			if row.Depth > limits.MaxDepth {
				return MaterializedState{}, fmt.Errorf("%w: checkpoint depth/row count", ErrMaterializeLimit)
			}
			if _, duplicate := seenBatch[row.ID]; duplicate {
				return MaterializedState{}, fmt.Errorf("%w: durable checkpoint lineage batch contains a cycle", ErrInvalidCheckpoint)
			}
			seenBatch[row.ID] = struct{}{}
			if index > 0 {
				child := batchedRows[index-1]
				if child.ParentID == nil || *child.ParentID != row.ID || child.Depth != row.Depth+1 {
					return MaterializedState{}, fmt.Errorf("%w: durable checkpoint %s depth does not match lineage", ErrInvalidCheckpoint, row.ID)
				}
			}
			if row.MaterializedSnapshotBlob != nil {
				references[*row.MaterializedSnapshotBlob] = struct{}{}
				continue
			}
			references[row.DeltaBlob] = struct{}{}
			references[row.ResponseBlob] = struct{}{}
			references[row.SettingsPatchBlob] = struct{}{}
		}
		// Object fetch latency is bounded by a small worker pool. Results are
		// placed in the immutable per-materialization cache and replay order is
		// still determined solely by validated lineage metadata.
		jobs := make(chan CheckpointBlobReference)
		workers := min(8, len(references))
		var wait sync.WaitGroup
		wait.Add(workers)
		for range workers {
			go func() {
				defer wait.Done()
				for reference := range jobs {
					_, _ = readBlob(reference)
				}
			}()
		}
		for reference := range references {
			jobs <- reference
		}
		close(jobs)
		wait.Wait()
	}
	getCheckpoint := func(id CheckpointID) (DurableCheckpoint, error) {
		if row, ok := batched[id]; ok {
			return row, nil
		}
		return materializer.Repository.Get(ctx, scopeID, id)
	}
	seen := make(map[CheckpointID]struct{})
	current := checkpointID
	for current != "" {
		if _, exists := seen[current]; exists {
			return MaterializedState{}, fmt.Errorf("%w: durable checkpoint graph contains a cycle", ErrInvalidCheckpoint)
		}
		seen[current] = struct{}{}
		row, err := getCheckpoint(current)
		if err != nil {
			return MaterializedState{}, err
		}
		if row.ScopeID != scopeID {
			return MaterializedState{}, ErrTenantMismatch
		}
		if err := row.Validate(now); err != nil {
			return MaterializedState{}, invalidCheckpoint(fmt.Errorf("validate durable checkpoint %s: %w", current, err))
		}
		if !now.Before(row.ExpiresAt) {
			return MaterializedState{}, ErrExpired
		}
		if row.Depth > limits.MaxDepth || len(path)+1 > limits.MaxRows {
			return MaterializedState{}, fmt.Errorf("%w: checkpoint depth/row count", ErrMaterializeLimit)
		}
		if len(path) > 0 && path[len(path)-1].row.Depth != row.Depth+1 {
			return MaterializedState{}, fmt.Errorf("%w: durable checkpoint %s depth does not match lineage", ErrInvalidCheckpoint, current)
		}
		if row.ParentID == nil && row.Depth != 0 {
			return MaterializedState{}, fmt.Errorf("%w: durable checkpoint %s root depth is not zero", ErrInvalidCheckpoint, current)
		}

		// A snapshot is an optimization only. Any unavailable, malformed, or
		// incorrectly bound snapshot falls back to authoritative parent deltas.
		if row.MaterializedSnapshotBlob != nil {
			if value, readErr := readBlob(*row.MaterializedSnapshotBlob); readErr == nil {
				if decoded, decodeErr := codec.DecodeSnapshot(value); decodeErr == nil && validSnapshotBase(row, decoded, limits) {
					path = append(path, loaded{row: row, snapshot: &decoded})
					break
				}
			}
		}

		entry := loaded{row: row}
		if sameCheckpointBlobReference(row.DeltaBlob, row.ResponseBlob) &&
			sameCheckpointBlobReference(row.DeltaBlob, row.SettingsPatchBlob) {
			value, readErr := readBlob(row.DeltaBlob)
			if readErr != nil {
				return MaterializedState{}, fmt.Errorf("checkpoint %s bundle: %w", current, readErr)
			}
			bundle, decodeErr := codec.DecodeBundle(value)
			if decodeErr != nil {
				return MaterializedState{}, fmt.Errorf("checkpoint %s bundle: %w", current, invalidCheckpoint(decodeErr))
			}
			entry.delta, entry.response, entry.patch = bundle.Delta, bundle.Response, bundle.SettingsPatch
		} else {
			entry.delta, entry.response, entry.patch, err = materializer.readLegacyParts(row, codec, readBlob)
			if err != nil {
				return MaterializedState{}, fmt.Errorf("checkpoint %s replay parts: %w", current, err)
			}
		}
		path = append(path, entry)
		if row.ParentID == nil {
			break
		}
		current = *row.ParentID
	}
	if len(path) == 0 {
		return MaterializedState{}, fmt.Errorf("%w: durable checkpoint graph has no root", ErrInvalidCheckpoint)
	}
	base := len(path) - 1
	result := MaterializedState{Handle: Handle(checkpointID), Tenant: scopeID, Settings: RootModelState("")}
	if path[base].snapshot != nil {
		snapshot := path[base].snapshot
		result.Items = cloneItems(snapshot.Items)
		result.Settings = snapshot.Settings.Clone()
		result.Depth = snapshot.Depth
		result.Lineage = append([]Handle(nil), snapshot.Lineage...)
		base--
	} else if path[base].row.ParentID != nil {
		return MaterializedState{}, fmt.Errorf("%w: durable checkpoint graph has no root", ErrInvalidCheckpoint)
	}
	itemCapacity := len(result.Items)
	for index := base; index >= 0; index-- {
		count := len(path[index].delta) + len(path[index].response)
		if count > limits.MaxItems-itemCapacity {
			return MaterializedState{}, fmt.Errorf("%w: checkpoint item count", ErrMaterializeLimit)
		}
		itemCapacity += count
	}
	if itemCapacity > len(result.Items) {
		items := make([]llm.Item, len(result.Items), itemCapacity)
		copy(items, result.Items)
		result.Items = items
	}
	for index := base; index >= 0; index-- {
		entry := path[index]
		var err error
		result.Settings, err = ApplySettingsPatch(result.Settings, entry.patch)
		if err != nil {
			return MaterializedState{}, fmt.Errorf("checkpoint %s settings: %w", entry.row.ID, invalidCheckpoint(err))
		}
		result.Items = append(result.Items, entry.delta...)
		result.Items = append(result.Items, entry.response...)
		result.Depth = entry.row.Depth
		result.Lineage = append(result.Lineage, Handle(entry.row.ID))
	}
	graph := NewCheckpointGraph(limits)
	if err := graph.validateMaterializedLimits(result.Items); err != nil {
		return MaterializedState{}, err
	}
	if result.Settings.Model == "" {
		return MaterializedState{}, fmt.Errorf("%w: materialized root model is required", ErrInvalidCheckpoint)
	}
	pending, err := validateItems(result.Items)
	if err != nil {
		return MaterializedState{}, invalidCheckpoint(err)
	}
	if sameCheckpointBlobReference(path[0].row.DeltaBlob, path[0].row.ResponseBlob) &&
		sameCheckpointBlobReference(path[0].row.DeltaBlob, path[0].row.SettingsPatchBlob) {
		if err := verifyMaterializedCheckpointDigests(path[0].row, result, codec); err != nil {
			return MaterializedState{}, invalidCheckpoint(err)
		}
	}
	result.Items = cloneItems(result.Items)
	result.PendingToolCalls = append([]string(nil), pending...)
	result.Settings = result.Settings.Clone()
	return result, nil
}

func sameCheckpointBlobReference(left, right CheckpointBlobReference) bool {
	return left.ID == right.ID && left.Digest == right.Digest &&
		left.ByteLength == right.ByteLength && left.MediaType == right.MediaType
}
func verifyMaterializedCheckpointDigests(row DurableCheckpoint, materialized MaterializedState, codec CheckpointBlobCodec) error {
	lineage, err := json.Marshal(materialized.Lineage)
	if err != nil || sha256.Sum256(lineage) != row.CanonicalLineageDigest {
		return fmt.Errorf("materialized checkpoint lineage digest mismatch")
	}
	settingsDigest, err := codec.DigestMaterializedSettings(materialized.Settings)
	if err != nil {
		return err
	}
	if settingsDigest != row.MaterializedSettingsDigest {
		return fmt.Errorf("materialized checkpoint settings digest mismatch")
	}
	frontier, err := ValidateTranscript(materialized.Items)
	if err != nil {
		return err
	}
	encodedFrontier, err := json.Marshal(frontier)
	if err != nil || sha256.Sum256(encodedFrontier) != row.ToolFrontierDigest {
		return fmt.Errorf("materialized checkpoint tool frontier digest mismatch")
	}
	return nil
}

func validSnapshotBase(row DurableCheckpoint, snapshot CheckpointSnapshot, limits MaterializeLimits) bool {
	if snapshot.validate() != nil || snapshot.Depth != row.Depth ||
		snapshot.Depth > limits.MaxDepth || len(snapshot.Lineage) != int(snapshot.Depth)+1 ||
		len(snapshot.Lineage) > limits.MaxRows || len(snapshot.Lineage) == 0 ||
		snapshot.Lineage[len(snapshot.Lineage)-1] != Handle(row.ID) {
		return false
	}
	lineage, err := json.Marshal(snapshot.Lineage)
	if err != nil {
		return false
	}
	return sha256.Sum256(lineage) == row.CanonicalLineageDigest
}

func (materializer *DurableCheckpointMaterializer) MaterializeHandle(ctx context.Context, scopeID, handle string, limits MaterializeLimits) (MaterializedState, error) {
	if materializer == nil || materializer.HandleVerifier == nil {
		return MaterializedState{}, errors.New("durable checkpoint materializer handle verifier is not configured")
	}
	checkpointID, err := materializer.HandleVerifier.VerifyCheckpointHandle(ctx, scopeID, handle)
	if err != nil {
		return MaterializedState{}, err
	}
	result, err := materializer.Materialize(ctx, scopeID, checkpointID, limits)
	if err != nil {
		return MaterializedState{}, err
	}
	// Preserve the caller-facing opaque token. The durable UUID is an internal
	// repository key and must not be substituted into an Activity payload.
	result.Handle = Handle(handle)
	return result, nil
}

func invalidCheckpoint(err error) error {
	if err == nil || errors.Is(err, ErrMaterializeLimit) || errors.Is(err, ErrInvalidCheckpoint) {
		return err
	}
	return fmt.Errorf("%w: %v", ErrInvalidCheckpoint, err)
}

func (materializer *DurableCheckpointMaterializer) readLegacyParts(row DurableCheckpoint, codec CheckpointBlobCodec, readBlob func(CheckpointBlobReference) ([]byte, error)) ([]llm.Item, []llm.Item, SettingsPatch, error) {
	references := []CheckpointBlobReference{row.DeltaBlob, row.ResponseBlob, row.SettingsPatchBlob}
	unique := make(map[CheckpointBlobReference]struct{}, len(references))
	for _, reference := range references {
		unique[reference] = struct{}{}
	}
	type result struct {
		reference CheckpointBlobReference
		value     []byte
		err       error
	}
	results := make(chan result, len(unique))
	for reference := range unique {
		reference := reference
		go func() {
			value, err := readBlob(reference)
			results <- result{reference: reference, value: value, err: err}
		}()
	}
	values := make(map[CheckpointBlobReference][]byte, len(unique))
	for range unique {
		read := <-results
		if read.err != nil {
			return nil, nil, SettingsPatch{}, read.err
		}
		values[read.reference] = read.value
	}
	delta, err := codec.DecodeDelta(values[row.DeltaBlob])
	if err != nil {
		return nil, nil, SettingsPatch{}, invalidCheckpoint(err)
	}
	response, err := codec.DecodeResponse(values[row.ResponseBlob])
	if err != nil {
		return nil, nil, SettingsPatch{}, invalidCheckpoint(err)
	}
	patch, err := codec.DecodeSettingsPatch(values[row.SettingsPatchBlob])
	if err != nil {
		return nil, nil, SettingsPatch{}, invalidCheckpoint(err)
	}
	return delta, response, patch, nil
}

func (materializer *DurableCheckpointMaterializer) readItems(ctx context.Context, scopeID string, reference CheckpointBlobReference, codec CheckpointBlobCodec, kind CheckpointBlobKind) ([]llm.Item, error) {
	if reference.MediaType != "application/json" {
		return nil, fmt.Errorf("unsupported media type %q", reference.MediaType)
	}
	data, err := materializer.Blobs.Read(ctx, scopeID, reference)
	if err != nil {
		return nil, err
	}
	if kind == CheckpointDeltaBlob {
		return codec.DecodeDelta(data)
	}
	return codec.DecodeResponse(data)
}

func (materializer *DurableCheckpointMaterializer) readPatch(ctx context.Context, scopeID string, reference CheckpointBlobReference, codec CheckpointBlobCodec) (SettingsPatch, error) {
	if reference.MediaType != "application/json" {
		return SettingsPatch{}, fmt.Errorf("unsupported media type %q", reference.MediaType)
	}
	data, err := materializer.Blobs.Read(ctx, scopeID, reference)
	if err != nil {
		return SettingsPatch{}, err
	}
	return codec.DecodeSettingsPatch(data)
}

func (materializer *DurableCheckpointMaterializer) clock() time.Time {
	if materializer.Now != nil {
		return materializer.Now().UTC()
	}
	return time.Now().UTC()
}
