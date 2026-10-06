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
	LookupOperation(context.Context, cloudstate.Operation) (cloudstate.Record, error)
	SaveRequestPreparation(context.Context, cloudstate.Scope, cloudstate.RequestID, cloudstate.RequestPreparation) error
	LoadRequestPreparation(context.Context, cloudstate.Scope, cloudstate.RequestID) (cloudstate.RequestPreparation, error)
}

// CloudRequestPreparation authorizes before every storage access and retains
// materialized input before any budget or provider effect. A restart reads that
// input instead of reopening a parent whose retention deadline may have passed.
type CloudRequestPreparation struct {
	store         CloudRequestPreparationStore
	replay        *CheckpointReplay
	digest        [32]byte
	clock         func() time.Time
	requestLimits CloudRequestLimits
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
	operation := cloudstate.Operation{Scope: scope, Kind: kind, Key: key, RequestIndex: index, Manifest: manifest, Now: p.clock()}
	// BeginOperation makes a request discoverable as running, and only a settled
	// attempt can later fail it. Validate a new operation first so rejected input
	// never leaves a permanent pending record. An existing operation skips this:
	// it replays its saved result or preparation without reopening the parent.
	var initial *cloudstate.RequestPreparation
	if _, err := p.store.LookupOperation(ctx, operation); errors.Is(err, contracts.ErrNotFound) {
		// The configured request limits bound new input only. An operation
		// admitted under an earlier snapshot keeps replaying its saved result.
		if err := p.requestLimits.validate(input); err != nil {
			return PreparedCloudRequest{}, err
		}
		candidate := cloudstate.Record{Status: cloudstate.StatusRunning, Request: cloudstate.CreateRequest{Scope: scope, Kind: kind, RequestIndex: index, Manifest: manifest, CreatedAt: operation.Now}}
		preparation, err := p.materialize(ctx, candidate, caller, parent, checkpointScope, operation.Now.UTC())
		if err != nil {
			return PreparedCloudRequest{}, err
		}
		initial = &preparation
	} else if err != nil {
		return PreparedCloudRequest{}, cloudRuntimeError(err, false)
	}
	record, err := p.store.BeginOperation(ctx, operation)
	if err != nil {
		return PreparedCloudRequest{}, cloudRuntimeError(err, false)
	}
	if record.Request.Scope != scope || record.Request.Kind != kind || record.Request.RequestIndex != index {
		return PreparedCloudRequest{}, cloudRuntimeError(cloudstate.ErrCorrupt, false)
	}
	if record.Status == cloudstate.StatusCompleted || record.Status == cloudstate.StatusFailed {
		return p.restore(ctx, record, cloudstate.RequestPreparation{}, checkpointScope)
	}
	prepared, err := p.prepareActive(ctx, record, caller, parent, checkpointScope, initial)
	if err != nil {
		return p.completedAfterError(ctx, record, checkpointScope, err)
	}
	return prepared, nil
}

func (p *CloudRequestPreparation) prepareActive(ctx context.Context, record cloudstate.Record, caller llm.RequestContext, parent, checkpointScope string, initial *cloudstate.RequestPreparation) (PreparedCloudRequest, error) {
	scope := record.Request.Scope
	preparation, err := p.store.LoadRequestPreparation(ctx, scope, record.Request.ID)
	if errors.Is(err, cloudstate.ErrRequestPreparationMissing) {
		// Reuse the input validated before BeginOperation. BeginOperation has
		// verified the immutable manifest binding, so a record created by a
		// competing initializer names the same parent and input. Advance
		// PreparedAt to that record's creation time (PreparedAt must not precede
		// CreatedAt) instead of reopening a parent that may since have expired.
		if initial != nil {
			preparation = *initial
			if preparation.PreparedAt.Before(record.Request.CreatedAt) {
				preparation.PreparedAt = record.Request.CreatedAt.UTC()
			}
		} else {
			preparation, err = p.materialize(ctx, record, caller, parent, checkpointScope, p.clock().UTC())
			if err != nil {
				return PreparedCloudRequest{}, err
			}
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

// materialize reads the authorized parent and validates the input against it
// without writing anything.
func (p *CloudRequestPreparation) materialize(ctx context.Context, record cloudstate.Record, caller llm.RequestContext, parent, checkpointScope string, at time.Time) (cloudstate.RequestPreparation, error) {
	materialized, err := p.replay.materializeAuthorized(ctx, caller, parent, checkpointScope)
	if err != nil {
		return cloudstate.RequestPreparation{}, err
	}
	preparation := cloudstate.RequestPreparation{Version: 1, ConfigDigest: p.digest, CheckpointScope: checkpointScope, PreparedAt: at}
	if parent != "" {
		codec := state.CheckpointBlobCodec{MaxBytes: cloudstate.MaxPreparedParentBytes}
		preparation.ParentSnapshot, err = codec.EncodeSnapshot(*state.NewCheckpointSnapshot(materialized))
		if err != nil {
			return cloudstate.RequestPreparation{}, checkpointReplayError(provider.CodeInvalidArgument)
		}
		preparation.ParentProvenance = materialized.ProviderStateProvenance
	}
	// Validate the input against the materialized parent before writing it.
	prepared, err := p.restore(ctx, record, preparation, checkpointScope)
	if err != nil {
		return cloudstate.RequestPreparation{}, err
	}
	// A Generate child holds the parent, the append and at least one output
	// item; a compaction child never holds more than its parent. Reject a
	// lineage that cannot publish before any budget or provider effect.
	items := len(materialized.Items)
	if prepared.Generate != nil {
		items += len(prepared.Generate.Append) + 1
	}
	if err := validateLineageCapacity(p.replay.limits, materialized, items); err != nil {
		return cloudstate.RequestPreparation{}, err
	}
	// Refuse, before anything is paid, a turn whose child could not be
	// prepared as a parent again. Compaction stays available on this parent.
	if prepared.Generate != nil && parent != "" {
		input, err := PrepareGenerateInput(ctx, *prepared.Generate, prepared.GenerateReplay)
		if err != nil {
			return cloudstate.RequestPreparation{}, err
		}
		fits, err := extendedParentFits(prepared.GenerateReplay.State, input)
		if err != nil {
			return cloudstate.RequestPreparation{}, err
		}
		if !fits {
			return cloudstate.RequestPreparation{}, provider.NewError(provider.CodeInvalidArgument, provider.PhasePlan, provider.DispatchNotDispatched, provider.RetryNever, "parent checkpoint is too large to extend; compact it first")
		}
	}
	return preparation, nil
}

// extendedParentFits reports whether a Generate's whole transcript (the
// parent plus its appended input) under the settings the child will publish
// stays within MaxExtendableParentBytes.
func extendedParentFits(parent state.MaterializedState, input PreparedGenerateInput) (bool, error) {
	extended := parent
	extended.Items = input.Request.Input
	extended.Settings = input.Settings
	codec := state.CheckpointBlobCodec{MaxBytes: cloudstate.MaxExtendableParentBytes}
	if _, err := codec.EncodeSnapshot(*state.NewCheckpointSnapshot(extended)); err != nil {
		if errors.Is(err, state.ErrLimitExceeded) {
			return false, nil
		}
		return false, checkpointReplayError(provider.CodeInvalidArgument)
	}
	return true, nil
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
		materialized = state.MaterializedState{Handle: state.Handle(parent), Tenant: caller.Tenant, Project: caller.Project, Depth: snapshot.Depth, Items: snapshot.Items, Settings: snapshot.Settings, Lineage: snapshot.Lineage, PendingToolCalls: pending,
			ProviderStateProvenance: append([]state.ProviderStateProvenance(nil), preparation.ParentProvenance...)}
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
