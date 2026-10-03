package runtime

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// CheckpointScopeResolver authorizes the caller and resolves its opaque durable
// scope. It must use the same binding as checkpoint publication and handle signing.
// Caller-provided tenant/project names alone are not authorization.
type CheckpointScopeResolver func(context.Context, llm.RequestContext) (string, error)

// CheckpointReplay supplies the parent-materialization portion of Generate and
// Compact replay from one immutable snapshot. Completed operation replay and
// pending attempt recovery belong to the surrounding operation runtime; these
// methods never grant permission to dispatch or settle a budget.
type CheckpointReplay struct {
	materializer state.CheckpointHandleMaterializer
	resolve      CheckpointScopeResolver
	limits       state.MaterializeLimits
}

// NewCheckpointReplay binds the snapshot's generic checkpoint interfaces. It
// creates no clients and never falls back to a legacy continuation store. Zero
// limits use the materializer defaults; negative limits are rejected.
func (capabilities V1RuntimeCapabilities) NewCheckpointReplay(resolve CheckpointScopeResolver, limits state.MaterializeLimits) (*CheckpointReplay, error) {
	if err := capabilities.Checkpoints.RequireMaterializer(); err != nil {
		return nil, err
	}
	if resolve == nil {
		return nil, errors.New("checkpoint replay scope resolver is required")
	}
	if limits.MaxDepth < 0 || limits.MaxRows < 0 || limits.MaxItems < 0 || limits.MaxBytes < 0 {
		return nil, errors.New("checkpoint replay limits must be nonnegative")
	}
	return &CheckpointReplay{materializer: capabilities.Checkpoints.Materializer, resolve: resolve, limits: limits}, nil
}

// Generate materializes only the parent; Append and SettingsPatch remain the
// current operation's delta. A root returns an empty base after authorization.
func (replay *CheckpointReplay) Generate(ctx context.Context, request llm.GenerateRequestV1) (durable.GenerateReplay, error) {
	if _, err := request.MarshalJSON(); err != nil {
		return durable.GenerateReplay{}, checkpointReplayError(provider.CodeInvalidArgument)
	}
	parent := ""
	if request.Parent != nil {
		parent = string(*request.Parent)
	}
	materialized, err := replay.materialize(ctx, request.Context, parent)
	return durable.GenerateReplay{State: materialized}, err
}

// Compact requires an authenticated parent and never aliases Generate's root
// behavior. Both phases preserve the opaque caller handle, not its internal ID.
func (replay *CheckpointReplay) Compact(ctx context.Context, request llm.CompactRequestV1) (durable.CompactReplay, error) {
	if _, err := request.MarshalJSON(); err != nil {
		return durable.CompactReplay{}, checkpointReplayError(provider.CodeInvalidArgument)
	}
	materialized, err := replay.materialize(ctx, request.Context, string(request.Parent))
	return durable.CompactReplay{State: materialized}, err
}

func (replay *CheckpointReplay) materialize(ctx context.Context, caller llm.RequestContext, parent string) (state.MaterializedState, error) {
	if ctx == nil || replay == nil || replay.resolve == nil || isNilCapability(replay.materializer) {
		return state.MaterializedState{}, checkpointReplayError(provider.CodeConfiguration)
	}
	if err := ctx.Err(); err != nil {
		return state.MaterializedState{}, err
	}
	scope, err := replay.resolve(ctx, caller)
	if ctx.Err() != nil {
		return state.MaterializedState{}, ctx.Err()
	}
	if err != nil {
		return state.MaterializedState{}, checkpointReplayError(provider.CodePermissionDenied)
	}
	if !utf8.ValidString(scope) || scope == "" || len(scope) > 512 || strings.TrimSpace(scope) != scope || strings.ContainsAny(scope, "\x00\r\n") {
		return state.MaterializedState{}, checkpointReplayError(provider.CodeInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return state.MaterializedState{}, err
	}
	if parent == "" {
		return state.MaterializedState{}, nil
	}
	materialized, err := replay.materializer.MaterializeHandle(ctx, scope, parent, replay.limits)
	if err != nil {
		if ctx.Err() != nil {
			return state.MaterializedState{}, ctx.Err()
		}
		code := provider.CodeStateUnavailable
		if errors.Is(err, state.ErrInvalidHandle) || errors.Is(err, state.ErrNotFound) || errors.Is(err, state.ErrExpired) || errors.Is(err, state.ErrTenantMismatch) {
			code = provider.CodeInvalidArgument
		}
		return state.MaterializedState{}, checkpointReplayError(code)
	}
	// The generic materializer knows only the authorized storage scope. Raw
	// caller labels are attached here, after the scoped handle and graph read.
	if materialized.Handle != state.Handle(parent) || materialized.Tenant != scope || materialized.Project != "" {
		return state.MaterializedState{}, checkpointReplayError(provider.CodeStateCorrupt)
	}
	materialized.Tenant, materialized.Project = caller.Tenant, caller.Project
	pending, err := state.ValidateTranscript(materialized.Items)
	if err != nil || !equalCheckpointFrontier(pending, materialized.PendingToolCalls) {
		return state.MaterializedState{}, checkpointReplayError(provider.CodeStateCorrupt)
	}
	if err := ctx.Err(); err != nil {
		return state.MaterializedState{}, err
	}
	return materialized, nil
}

func equalCheckpointFrontier(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func checkpointReplayError(code provider.Code) error {
	retry := provider.RetryNever
	if code == provider.CodeStateUnavailable {
		retry = provider.RetrySameOperation
	}
	return provider.NewError(code, provider.PhaseStateLoad, provider.DispatchNotDispatched, retry, "checkpoint replay failed")
}
