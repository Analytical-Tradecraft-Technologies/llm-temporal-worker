package state

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

	// Collect leaf-to-root rows first. The repository's scoped Get is the only
	// database capability used here; a cycle, a missing parent, or a depth gap
	// is rejected before any graph node is exposed to callers.
	type loaded struct {
		row      DurableCheckpoint
		delta    []llm.Item
		response []llm.Item
		patch    SettingsPatch
		snapshot *CheckpointSnapshot
	}
	path := make([]loaded, 0, 8)
	seen := make(map[CheckpointID]struct{})
	current := checkpointID
	for current != "" {
		if _, exists := seen[current]; exists {
			return MaterializedState{}, fmt.Errorf("durable checkpoint graph contains a cycle")
		}
		seen[current] = struct{}{}
		row, err := materializer.Repository.Get(ctx, scopeID, current)
		if err != nil {
			return MaterializedState{}, err
		}
		if row.ScopeID != scopeID {
			return MaterializedState{}, ErrTenantMismatch
		}
		if err := row.Validate(now); err != nil {
			return MaterializedState{}, fmt.Errorf("validate durable checkpoint %s: %w", current, err)
		}
		if len(path) > 0 && path[len(path)-1].row.Depth != row.Depth+1 {
			return MaterializedState{}, fmt.Errorf("durable checkpoint %s depth does not match lineage", current)
		}
		if row.ParentID == nil && row.Depth != 0 {
			return MaterializedState{}, fmt.Errorf("durable checkpoint %s root depth is not zero", current)
		}
		if int32(len(path)) > limits.MaxDepth || len(path)+1 > limits.MaxRows {
			return MaterializedState{}, fmt.Errorf("checkpoint materialization exceeds depth/row limit")
		}
		delta, err := materializer.readItems(ctx, scopeID, row.DeltaBlob, codec, CheckpointDeltaBlob)
		if err != nil {
			return MaterializedState{}, fmt.Errorf("checkpoint %s delta: %w", current, err)
		}
		response, err := materializer.readItems(ctx, scopeID, row.ResponseBlob, codec, CheckpointResponseBlob)
		if err != nil {
			return MaterializedState{}, fmt.Errorf("checkpoint %s response: %w", current, err)
		}
		patch, err := materializer.readPatch(ctx, scopeID, row.SettingsPatchBlob, codec)
		if err != nil {
			return MaterializedState{}, fmt.Errorf("checkpoint %s settings patch: %w", current, err)
		}
		var snapshot *CheckpointSnapshot
		if row.MaterializedSnapshotBlob != nil {
			value, readErr := materializer.Blobs.Read(ctx, scopeID, *row.MaterializedSnapshotBlob)
			if readErr != nil {
				return MaterializedState{}, fmt.Errorf("checkpoint %s snapshot: %w", current, readErr)
			}
			if row.MaterializedSnapshotBlob.MediaType != "application/json" {
				return MaterializedState{}, fmt.Errorf("checkpoint %s snapshot has unsupported media type", current)
			}
			decoded, decodeErr := codec.DecodeSnapshot(value)
			if decodeErr != nil {
				return MaterializedState{}, fmt.Errorf("checkpoint %s snapshot: %w", current, decodeErr)
			}
			snapshot = &decoded
		}
		path = append(path, loaded{row: row, delta: delta, response: response, patch: patch, snapshot: snapshot})
		if row.ParentID == nil {
			break
		}
		current = *row.ParentID
	}
	if len(path) == 0 || path[len(path)-1].row.ParentID != nil {
		return MaterializedState{}, fmt.Errorf("durable checkpoint graph has no root")
	}

	graph := NewCheckpointGraph(limits)
	// Repository validation and graph replay must use one clock. Without this,
	// a test or caller-provided materializer clock can disagree with the graph's
	// wall clock and incorrectly reject an otherwise live checkpoint.
	graph.Now = func() time.Time { return now }
	for index := len(path) - 1; index >= 0; index-- {
		value := path[index]
		checkpoint := Checkpoint{
			Handle:        Handle(value.row.ID),
			Tenant:        scopeID,
			OperationKey:  string(value.row.OriginOperationID),
			Delta:         value.delta,
			Output:        value.response,
			SettingsPatch: value.patch,
			Depth:         value.row.Depth,
			ExpiresAt:     value.row.ExpiresAt,
			Snapshot:      value.snapshot,
		}
		if value.row.ParentID != nil {
			parent := Handle(*value.row.ParentID)
			checkpoint.Parent = &parent
		}
		if checkpoint.Parent == nil {
			if err := graph.PutRoot(checkpoint); err != nil {
				return MaterializedState{}, fmt.Errorf("publish durable root %s: %w", value.row.ID, err)
			}
		} else if err := graph.PutChild(checkpoint); err != nil {
			return MaterializedState{}, fmt.Errorf("publish durable child %s: %w", value.row.ID, err)
		}
	}
	result, err := graph.Materialize(scopeID, Handle(checkpointID))
	if err != nil {
		return MaterializedState{}, err
	}
	return result, nil
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
