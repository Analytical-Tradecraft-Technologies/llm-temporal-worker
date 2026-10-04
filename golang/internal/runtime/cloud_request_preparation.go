package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

type CloudRequestPreparationStore interface {
	CloudRequestRepository
	Read(context.Context, cloudstate.Scope, cloudstate.RequestID) (cloudstate.Record, error)
	SaveRequestPreparation(context.Context, cloudstate.Scope, cloudstate.RequestID, cloudstate.RequestPreparation) error
	LoadRequestPreparation(context.Context, cloudstate.Scope, cloudstate.RequestID) (cloudstate.RequestPreparation, error)
}

// CloudRequestPreparation authorizes before every storage access and retains
// materialized input before any budget or provider effect. A restart reads that
// input instead of reopening a parent whose retention deadline may have passed.
type CloudRequestPreparation struct {
	store  CloudRequestPreparationStore
	replay *CheckpointReplay
	digest [32]byte
	clock  func() time.Time
}

// PreparedCloudRequest is invocation-local, not a Temporal result or authority
// to dispatch. Completed operations contain no preparation: replay their saved
// final response instead of attempting to materialize their parent again.
type PreparedCloudRequest struct {
	Record         cloudstate.Record
	Preparation    cloudstate.RequestPreparation
	Generate       *llm.GenerateRequestV1
	Compact        *llm.CompactRequestV1
	GenerateReplay durable.GenerateReplay
	CompactReplay  durable.CompactReplay
}

func (capabilities V1RuntimeCapabilities) NewCloudRequestPreparation(resolve CheckpointScopeResolver, limits state.MaterializeLimits) (*CloudRequestPreparation, error) {
	store, ok := capabilities.Requests.(CloudRequestPreparationStore)
	if !ok || isNilCapability(store) || capabilities.ConfigDigest == ([32]byte{}) || capabilities.Clock == nil {
		return nil, checkpointReplayError(provider.CodeConfiguration)
	}
	replay, err := capabilities.NewCheckpointReplay(resolve, limits)
	if err != nil {
		return nil, err
	}
	return &CloudRequestPreparation{store: store, replay: replay, digest: capabilities.ConfigDigest, clock: capabilities.Clock}, nil
}

func (p *CloudRequestPreparation) Prepare(ctx context.Context, input llm.PrepareExecutionV1) (PreparedCloudRequest, error) {
	if _, err := input.MarshalJSON(); err != nil {
		return PreparedCloudRequest{}, checkpointReplayError(provider.CodeInvalidArgument)
	}
	var caller llm.RequestContext
	var parent, kind, key string
	var index int64
	var manifest []byte
	if input.Generate != nil {
		request := input.Generate
		caller, kind, key = request.Context, "generate", request.OperationKey
		if request.Parent != nil {
			parent = string(*request.Parent)
		}
		if request.Cache != nil {
			index = int64(request.Cache.Variant)
		}
		manifest, _ = json.Marshal(request)
	} else {
		request := input.Compact
		caller, kind, key, parent = request.Context, "compact", request.OperationKey, string(request.Parent)
		index = request.Cache.SampleIndex()
		manifest, _ = json.Marshal(request)
	}
	checkpointScope, err := p.authorize(ctx, caller)
	if err != nil {
		return PreparedCloudRequest{}, err
	}
	scope := cloudstate.Scope{Tenant: caller.Tenant, Project: caller.Project}
	record, err := p.store.BeginOperation(ctx, cloudstate.Operation{Scope: scope, Kind: kind, Key: key, RequestIndex: index, Manifest: manifest, Now: p.clock()})
	if err != nil {
		return PreparedCloudRequest{}, cloudRuntimeError(err, false)
	}
	if record.Request.Scope != scope || record.Request.Kind != kind || record.Request.RequestIndex != index {
		return PreparedCloudRequest{}, cloudRuntimeError(cloudstate.ErrCorrupt, false)
	}
	if record.Status == cloudstate.StatusCompleted || record.Status == cloudstate.StatusFailed {
		return p.restore(ctx, record, cloudstate.RequestPreparation{}, checkpointScope)
	}
	prepared, err := p.prepareActive(ctx, record, caller, parent, checkpointScope)
	if err != nil {
		return p.completedAfterError(ctx, record, checkpointScope, err)
	}
	return prepared, nil
}

func (p *CloudRequestPreparation) prepareActive(ctx context.Context, record cloudstate.Record, caller llm.RequestContext, parent, checkpointScope string) (PreparedCloudRequest, error) {
	scope := record.Request.Scope
	preparation, err := p.store.LoadRequestPreparation(ctx, scope, record.Request.ID)
	if errors.Is(err, cloudstate.ErrRequestPreparationMissing) {
		materialized, loadErr := p.replay.materializeAuthorized(ctx, caller, parent, checkpointScope)
		if loadErr != nil {
			return PreparedCloudRequest{}, loadErr
		}
		preparation = cloudstate.RequestPreparation{Version: 1, ConfigDigest: p.digest, CheckpointScope: checkpointScope, PreparedAt: p.clock().UTC()}
		if parent != "" {
			codec := state.CheckpointBlobCodec{MaxBytes: cloudstate.MaxPreparedParentBytes}
			preparation.ParentSnapshot, err = codec.EncodeSnapshot(*state.NewCheckpointSnapshot(materialized))
			if err != nil {
				return PreparedCloudRequest{}, checkpointReplayError(provider.CodeInvalidArgument)
			}
		}
		// Validate the input against the materialized parent before writing it.
		if _, err := p.restore(ctx, record, preparation, checkpointScope); err != nil {
			return PreparedCloudRequest{}, err
		}
		if err := ctx.Err(); err != nil {
			return PreparedCloudRequest{}, err
		}
		err = p.store.SaveRequestPreparation(ctx, scope, record.Request.ID, preparation)
		if err != nil && !errors.Is(err, contracts.ErrConflict) {
			return PreparedCloudRequest{}, cloudRuntimeError(err, false)
		}
		// A competing initializer may have won. Only the durable winner is used.
		preparation, err = p.store.LoadRequestPreparation(ctx, scope, record.Request.ID)
	}
	if err != nil {
		return PreparedCloudRequest{}, cloudRuntimeError(err, false)
	}
	return p.restore(ctx, record, preparation, checkpointScope)
}

// Load authenticates the current caller but uses the original immutable input.
// A request ID is only a locator, never permission to read its transcript.
func (p *CloudRequestPreparation) Load(ctx context.Context, reference llm.ExecutionReferenceV1) (PreparedCloudRequest, error) {
	if _, err := reference.MarshalJSON(); err != nil {
		return PreparedCloudRequest{}, checkpointReplayError(provider.CodeInvalidArgument)
	}
	checkpointScope, err := p.authorize(ctx, reference.Context)
	if err != nil {
		return PreparedCloudRequest{}, err
	}
	scope := cloudstate.Scope{Tenant: reference.Context.Tenant, Project: reference.Context.Project}
	record, err := p.store.Read(ctx, scope, cloudstate.RequestID(reference.RequestID))
	if err != nil {
		return PreparedCloudRequest{}, cloudRuntimeError(err, false)
	}
	if record.Request.ID != cloudstate.RequestID(reference.RequestID) || record.Request.Scope != scope {
		return PreparedCloudRequest{}, cloudRuntimeError(cloudstate.ErrCorrupt, false)
	}
	if record.Status == cloudstate.StatusCompleted || record.Status == cloudstate.StatusFailed {
		return p.restore(ctx, record, cloudstate.RequestPreparation{}, checkpointScope)
	}
	preparation, err := p.store.LoadRequestPreparation(ctx, scope, record.Request.ID)
	if err != nil {
		return p.completedAfterError(ctx, record, checkpointScope, cloudRuntimeError(err, false))
	}
	prepared, err := p.restore(ctx, record, preparation, checkpointScope)
	if err != nil {
		return p.completedAfterError(ctx, record, checkpointScope, err)
	}
	return prepared, nil
}

// Completion replaces intermediate progress with the immutable final response.
// An already-authorized caller can therefore lose a preparation/finalization
// read to a successful concurrent caller. Replay only that completed operation;
// preserve the original error if no matching durable success is available.
func (p *CloudRequestPreparation) completedAfterError(ctx context.Context, original cloudstate.Record, checkpointScope string, cause error) (PreparedCloudRequest, error) {
	current, err := p.store.Read(ctx, original.Request.Scope, original.Request.ID)
	if err != nil || current.Status != cloudstate.StatusCompleted {
		return PreparedCloudRequest{}, cause
	}
	if !equalFinalizationValue(current.Request, original.Request) {
		return PreparedCloudRequest{}, cloudRuntimeError(cloudstate.ErrCorrupt, false)
	}
	return p.restore(ctx, current, cloudstate.RequestPreparation{}, checkpointScope)
}

func (p *CloudRequestPreparation) authorize(ctx context.Context, caller llm.RequestContext) (string, error) {
	if p == nil || p.replay == nil || isNilCapability(p.store) || p.clock == nil {
		return "", checkpointReplayError(provider.CodeConfiguration)
	}
	return p.replay.authorize(ctx, caller)
}

func (p *CloudRequestPreparation) restore(ctx context.Context, record cloudstate.Record, preparation cloudstate.RequestPreparation, checkpointScope string) (PreparedCloudRequest, error) {
	result := PreparedCloudRequest{Record: record, Preparation: preparation}
	var caller llm.RequestContext
	var parent string
	var index int64
	switch record.Request.Kind {
	case "generate":
		var request llm.GenerateRequestV1
		if json.Unmarshal(record.Request.Manifest, &request) != nil {
			return PreparedCloudRequest{}, cloudRuntimeError(cloudstate.ErrCorrupt, false)
		}
		result.Generate, caller = &request, request.Context
		if request.Cache != nil {
			index = int64(request.Cache.Variant)
		}
		if request.Parent != nil {
			parent = string(*request.Parent)
		}
	case "compact":
		var request llm.CompactRequestV1
		if json.Unmarshal(record.Request.Manifest, &request) != nil {
			return PreparedCloudRequest{}, cloudRuntimeError(cloudstate.ErrCorrupt, false)
		}
		result.Compact, caller, parent = &request, request.Context, string(request.Parent)
		index = request.Cache.SampleIndex()
	default:
		return PreparedCloudRequest{}, cloudRuntimeError(cloudstate.ErrCorrupt, false)
	}
	if record.Request.Scope != (cloudstate.Scope{Tenant: caller.Tenant, Project: caller.Project}) || record.Request.RequestIndex != index {
		return PreparedCloudRequest{}, cloudRuntimeError(cloudstate.ErrCorrupt, false)
	}
	if record.Status == cloudstate.StatusCompleted || record.Status == cloudstate.StatusFailed {
		return result, nil
	}
	if record.Status != cloudstate.StatusRunning && record.Status != cloudstate.StatusProviderPending && record.Status != cloudstate.StatusOutcomeUnknown {
		return PreparedCloudRequest{}, cloudRuntimeError(contracts.ErrConflict, false)
	}
	if preparation.Validate() != nil || preparation.CheckpointScope != checkpointScope || preparation.PreparedAt.Before(record.Request.CreatedAt) ||
		(parent == "") != (len(preparation.ParentSnapshot) == 0) {
		return PreparedCloudRequest{}, cloudRuntimeError(cloudstate.ErrCorrupt, false)
	}
	if preparation.ConfigDigest != p.digest {
		return PreparedCloudRequest{}, checkpointReplayError(provider.CodeConfiguration)
	}
	var materialized state.MaterializedState
	if parent != "" {
		codec := state.CheckpointBlobCodec{MaxBytes: cloudstate.MaxPreparedParentBytes}
		snapshot, err := codec.DecodeSnapshot(preparation.ParentSnapshot)
		if err != nil || len(snapshot.Lineage) == 0 {
			return PreparedCloudRequest{}, cloudRuntimeError(cloudstate.ErrCorrupt, false)
		}
		pending, err := state.ValidateTranscript(snapshot.Items)
		if err != nil {
			return PreparedCloudRequest{}, cloudRuntimeError(cloudstate.ErrCorrupt, false)
		}
		materialized = state.MaterializedState{Handle: state.Handle(parent), Tenant: caller.Tenant, Project: caller.Project, Depth: snapshot.Depth, Items: snapshot.Items, Settings: snapshot.Settings, Lineage: snapshot.Lineage, PendingToolCalls: pending}
	}
	result.GenerateReplay.State, result.CompactReplay.State = materialized, materialized
	var err error
	if result.Generate != nil {
		_, err = PrepareGenerateInput(ctx, *result.Generate, result.GenerateReplay)
	} else {
		_, err = PrepareCompactInput(ctx, *result.Compact, result.CompactReplay)
	}
	if err != nil {
		return PreparedCloudRequest{}, err
	}
	return result, nil
}
