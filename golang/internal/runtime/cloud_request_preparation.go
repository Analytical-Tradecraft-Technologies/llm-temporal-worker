package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
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
	// input is the Generate or Compact input that restoring this request
	// prepared, in the current Activity step, from Record's manifest and the
	// materialized parent (nil when nothing was prepared). Admission,
	// planning, recovery and finalization reuse it instead of preparing the
	// same transcript again (#1112). It never outlives the step.
	input *cloudPreparedInput
	// parentSnapshot is the parent snapshot restoring this request decoded,
	// or was given, in the current Activity step (nil without a parent).
	parentSnapshot *state.CheckpointSnapshot
	// loaded reports that Preparation was loaded from the store and validated,
	// parent included, in the current Activity step.
	loaded bool
}

// cloudPreparedInput is one step's prepared input of a request, bound to the
// manifest it was prepared from.
type cloudPreparedInput struct {
	manifest []byte
	generate *PreparedGenerateInput
	compact  *PreparedCompactInput
}

// generateInput returns what PrepareGenerateInput(ctx, request, replay)
// returns. When request was decoded from manifest and that manifest is the
// one this step's input was prepared from, with the same replay, the result
// is a copy of that input instead of a second preparation of the whole
// transcript. Callers must pass the replay the input was prepared with.
func (in *cloudPreparedInput) generateInput(ctx context.Context, manifest []byte, request llm.GenerateRequestV1, replay durable.GenerateReplay) (PreparedGenerateInput, error) {
	if in == nil || in.generate == nil || !bytes.Equal(manifest, in.manifest) {
		return PrepareGenerateInput(ctx, request, replay)
	}
	if err := validatePreparationContext(ctx, replay.Completed != nil || replay.ReconciliationPending != nil); err != nil {
		return PreparedGenerateInput{}, err
	}
	// The copy keeps callers from sharing the request's collections, exactly
	// as separately prepared inputs never did. Normalizing an already
	// normalized request is a typed copy that returns the same value.
	prepared := *in.generate
	normalized, err := llm.NormalizeRequest(in.generate.Request)
	if err != nil {
		return PrepareGenerateInput(ctx, request, replay)
	}
	prepared.Request = normalized
	return prepared, nil
}

// compactInput is generateInput for PrepareCompactInput.
func (in *cloudPreparedInput) compactInput(ctx context.Context, manifest []byte, request llm.CompactRequestV1, replay durable.CompactReplay) (PreparedCompactInput, error) {
	if in == nil || in.compact == nil || !bytes.Equal(manifest, in.manifest) {
		return PrepareCompactInput(ctx, request, replay)
	}
	if err := validatePreparationContext(ctx, replay.Completed != nil || replay.ReconciliationPending != nil); err != nil {
		return PreparedCompactInput{}, err
	}
	prepared := *in.compact
	if in.compact.Request != nil {
		summary, err := llm.NormalizeRequest(*in.compact.Request)
		if err != nil {
			return PrepareCompactInput(ctx, request, replay)
		}
		prepared.Request = &summary
	}
	return prepared, nil
}

// generateInput is PrepareGenerateInput(ctx, *p.Generate, p.GenerateReplay),
// reusing the input restoring p prepared in this step.
func (p PreparedCloudRequest) generateInput(ctx context.Context) (PreparedGenerateInput, error) {
	return p.input.generateInput(ctx, p.Record.Request.Manifest, *p.Generate, p.GenerateReplay)
}

// compactInput is PrepareCompactInput(ctx, *p.Compact, p.CompactReplay),
// reusing the input restoring p prepared in this step.
func (p PreparedCloudRequest) compactInput(ctx context.Context) (PreparedCompactInput, error) {
	return p.input.compactInput(ctx, p.Record.Request.Manifest, *p.Compact, p.CompactReplay)
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
	var initial *cloudMaterializedPreparation
	if _, err := p.store.LookupOperation(ctx, operation); errors.Is(err, contracts.ErrNotFound) {
		// The configured request limits bound new input only. An operation
		// admitted under an earlier snapshot keeps replaying its saved result.
		if err := p.requestLimits.validate(input); err != nil {
			return PreparedCloudRequest{}, err
		}
		candidate := cloudstate.Record{Status: cloudstate.StatusRunning, Request: cloudstate.CreateRequest{Scope: scope, Kind: kind, RequestIndex: index, Manifest: manifest, CreatedAt: operation.Now}}
		initial, err = p.materialize(ctx, candidate, caller, parent, checkpointScope, operation.Now.UTC())
		if err != nil {
			return PreparedCloudRequest{}, err
		}
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

func (p *CloudRequestPreparation) prepareActive(ctx context.Context, record cloudstate.Record, caller llm.RequestContext, parent, checkpointScope string, initial *cloudMaterializedPreparation) (PreparedCloudRequest, error) {
	scope := record.Request.Scope
	preparation, parentSnapshot, validated, err := loadRequestPreparation(ctx, p.store, scope, record.Request.ID, nil)
	if errors.Is(err, cloudstate.ErrRequestPreparationMissing) {
		// Reuse the input validated before BeginOperation. BeginOperation has
		// verified the immutable manifest binding, so a record created by a
		// competing initializer names the same parent and input. Advance
		// PreparedAt to that record's creation time (PreparedAt must not precede
		// CreatedAt) instead of reopening a parent that may since have expired.
		if initial != nil {
			preparation = initial.preparation
			if preparation.PreparedAt.Before(record.Request.CreatedAt) {
				preparation.PreparedAt = record.Request.CreatedAt.UTC()
			}
		} else {
			initial, err = p.materialize(ctx, record, caller, parent, checkpointScope, p.clock().UTC())
			if err != nil {
				return PreparedCloudRequest{}, err
			}
			preparation = initial.preparation
		}
		if err := ctx.Err(); err != nil {
			return PreparedCloudRequest{}, err
		}
		err = saveRequestPreparation(ctx, p.store, scope, record.Request.ID, preparation, initial.known)
		if err != nil && !errors.Is(err, contracts.ErrConflict) {
			return PreparedCloudRequest{}, cloudRuntimeError(err, false)
		}
		// A competing initializer may have won. Only the durable winner is used.
		// Its parent is read and verified again; only an identical parent is
		// not decoded a second time in this step.
		preparation, parentSnapshot, validated, err = loadRequestPreparation(ctx, p.store, scope, record.Request.ID, initial.known)
	}
	if err != nil {
		return PreparedCloudRequest{}, cloudRuntimeError(err, false)
	}
	prepared, err := p.restoreLoaded(ctx, record, preparation, parentSnapshot, validated, checkpointScope)
	if err != nil {
		return PreparedCloudRequest{}, err
	}
	prepared.loaded = true
	return prepared, nil
}

// cloudMaterializedPreparation is a preparation materialized and validated in
// the current Activity step, with its parent snapshot as validation decoded
// it (known is nil without a parent).
type cloudMaterializedPreparation struct {
	preparation cloudstate.RequestPreparation
	known       *cloudstate.KnownParent
}

// cloudPreparationKnownLoader is implemented by cloudstate.Repository, which
// validates a preparation, decoding its parent snapshot, while loading it.
type cloudPreparationKnownLoader interface {
	LoadRequestPreparationKnown(context.Context, cloudstate.Scope, cloudstate.RequestID, *cloudstate.KnownParent) (cloudstate.RequestPreparation, *state.CheckpointSnapshot, error)
}

// cloudPreparationKnownSaver is implemented by cloudstate.Repository.
type cloudPreparationKnownSaver interface {
	SaveRequestPreparationKnown(context.Context, cloudstate.Scope, cloudstate.RequestID, cloudstate.RequestPreparation, *cloudstate.KnownParent) error
}

// loadRequestPreparation loads a preparation. validated reports that the store
// already validated it exactly as RequestPreparation.Validate does, and then
// parent is the snapshot that validation decoded (nil without a parent), owned
// by the caller. Restoring the parent from it instead of validating and
// decoding the same bytes twice more is the per-step saving of #1112. Other
// stores leave validation to the caller. known, when not nil, is a parent this
// step already validated (see cloudstate.KnownParent); the store does not
// decode, or when verified read, an identical parent again.
func loadRequestPreparation(ctx context.Context, store interface {
	LoadRequestPreparation(context.Context, cloudstate.Scope, cloudstate.RequestID) (cloudstate.RequestPreparation, error)
}, scope cloudstate.Scope, id cloudstate.RequestID, known *cloudstate.KnownParent) (preparation cloudstate.RequestPreparation, parent *state.CheckpointSnapshot, validated bool, err error) {
	if loader, ok := store.(cloudPreparationKnownLoader); ok {
		preparation, parent, err = loader.LoadRequestPreparationKnown(ctx, scope, id, known)
		return preparation, parent, err == nil, err
	}
	preparation, err = store.LoadRequestPreparation(ctx, scope, id)
	return preparation, nil, false, err
}

// saveRequestPreparation saves a preparation whose parent, known, this step
// validated, without decoding it again when the store supports that.
func saveRequestPreparation(ctx context.Context, store CloudRequestPreparationStore, scope cloudstate.Scope, id cloudstate.RequestID, preparation cloudstate.RequestPreparation, known *cloudstate.KnownParent) error {
	if saver, ok := store.(cloudPreparationKnownSaver); ok {
		return saver.SaveRequestPreparationKnown(ctx, scope, id, preparation, known)
	}
	return store.SaveRequestPreparation(ctx, scope, id, preparation)
}

// materialize reads the authorized parent and validates the input against it
// without writing anything.
func (p *CloudRequestPreparation) materialize(ctx context.Context, record cloudstate.Record, caller llm.RequestContext, parent, checkpointScope string, at time.Time) (*cloudMaterializedPreparation, error) {
	materialized, err := p.replay.materializeAuthorized(ctx, caller, parent, checkpointScope)
	if err != nil {
		return nil, err
	}
	preparation := cloudstate.RequestPreparation{Version: 1, ConfigDigest: p.digest, CheckpointScope: checkpointScope, PreparedAt: at}
	if parent != "" {
		codec := state.CheckpointBlobCodec{MaxBytes: cloudstate.MaxPreparedParentBytes}
		preparation.ParentSnapshot, err = codec.EncodeSnapshot(*state.NewCheckpointSnapshot(materialized))
		if err != nil {
			return nil, checkpointReplayError(provider.CodeInvalidArgument)
		}
		preparation.ParentProvenance = materialized.ProviderStateProvenance
	}
	// Validate the input against the materialized parent before writing it.
	prepared, err := p.restore(ctx, record, preparation, checkpointScope)
	if err != nil {
		return nil, err
	}
	// A Generate child holds the parent, the append and at least one output
	// item; a compaction child never holds more than its parent. Reject a
	// lineage that cannot publish before any budget or provider effect.
	items := len(materialized.Items)
	if prepared.Generate != nil {
		items += len(prepared.Generate.Append) + 1
	}
	if err := validateLineageCapacity(p.replay.limits, materialized, items); err != nil {
		return nil, err
	}
	// Refuse, before anything is paid, a turn whose child could not be
	// prepared as a parent again. Compaction stays available on this parent.
	if prepared.Generate != nil && parent != "" {
		input, err := prepared.generateInput(ctx)
		if err != nil {
			return nil, err
		}
		fits, err := extendedParentFits(prepared.GenerateReplay.State, input)
		if err != nil {
			return nil, err
		}
		if !fits {
			return nil, provider.NewError(provider.CodeInvalidArgument, provider.PhasePlan, provider.DispatchNotDispatched, provider.RetryNever, "parent checkpoint is too large to extend; compact it first")
		}
	}
	result := &cloudMaterializedPreparation{preparation: preparation}
	if len(preparation.ParentSnapshot) != 0 {
		// restore validated the parent snapshot just now, decoding it once.
		// Saving and reloading it in this step need not decode it again.
		result.known = &cloudstate.KnownParent{Snapshot: preparation.ParentSnapshot, Decoded: prepared.parentSnapshot}
	}
	return result, nil
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
	preparation, parentSnapshot, validated, err := loadRequestPreparation(ctx, p.store, scope, record.Request.ID, nil)
	if err != nil {
		return p.completedAfterError(ctx, record, checkpointScope, cloudRuntimeError(err, false))
	}
	prepared, err := p.restoreLoaded(ctx, record, preparation, parentSnapshot, validated, checkpointScope)
	if err != nil {
		return p.completedAfterError(ctx, record, checkpointScope, err)
	}
	prepared.loaded = true
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
	return p.restoreLoaded(ctx, record, preparation, nil, false, checkpointScope)
}

// restoreLoaded is restore for a preparation that loadRequestPreparation may
// already have validated (validated), with the parent snapshot that
// validation decoded. Otherwise it validates the preparation here, decoding
// the parent once for both validation and restoration.
func (p *CloudRequestPreparation) restoreLoaded(ctx context.Context, record cloudstate.Record, preparation cloudstate.RequestPreparation, parentSnapshot *state.CheckpointSnapshot, validated bool, checkpointScope string) (PreparedCloudRequest, error) {
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
	var invalid error
	if !validated {
		parentSnapshot, invalid = preparation.ValidateParent()
	}
	if invalid != nil || preparation.CheckpointScope != checkpointScope || preparation.PreparedAt.Before(record.Request.CreatedAt) ||
		(parent == "") != (len(preparation.ParentSnapshot) == 0) {
		return PreparedCloudRequest{}, cloudRuntimeError(cloudstate.ErrCorrupt, false)
	}
	var materialized state.MaterializedState
	if parent != "" {
		// Validation decoded the parent with the same codec and limit.
		if parentSnapshot == nil || len(parentSnapshot.Lineage) == 0 {
			return PreparedCloudRequest{}, cloudRuntimeError(cloudstate.ErrCorrupt, false)
		}
		snapshot := *parentSnapshot
		pending, err := state.ValidateTranscript(snapshot.Items)
		if err != nil {
			return PreparedCloudRequest{}, cloudRuntimeError(cloudstate.ErrCorrupt, false)
		}
		materialized = state.MaterializedState{Handle: state.Handle(parent), Tenant: caller.Tenant, Project: caller.Project, Depth: snapshot.Depth, Items: snapshot.Items, Settings: snapshot.Settings, Lineage: snapshot.Lineage, PendingToolCalls: pending,
			ProviderStateProvenance: append([]state.ProviderStateProvenance(nil), preparation.ParentProvenance...)}
	}
	result.GenerateReplay.State, result.CompactReplay.State = materialized, materialized
	// Prepare the input once for the whole step; later stages reuse it.
	input := &cloudPreparedInput{manifest: record.Request.Manifest}
	if result.Generate != nil {
		prepared, err := PrepareGenerateInput(ctx, *result.Generate, result.GenerateReplay)
		if err != nil {
			return PreparedCloudRequest{}, err
		}
		input.generate = &prepared
	} else {
		prepared, err := PrepareCompactInput(ctx, *result.Compact, result.CompactReplay)
		if err != nil {
			return PreparedCloudRequest{}, err
		}
		input.compact = &prepared
	}
	result.input = input
	if parent != "" {
		result.parentSnapshot = parentSnapshot
	}
	return result, nil
}
