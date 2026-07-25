package activity

import (
	"context"
	"errors"
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

// CheckpointHandleMaterializer resolves an opaque caller-facing checkpoint
// handle in its already-authorized scope. The handle-capable form is kept
// separate from state.CheckpointMaterializer because repositories use opaque
// handles while the storage-neutral graph uses internal checkpoint IDs.
type CheckpointHandleMaterializer interface {
	MaterializeHandle(context.Context, string, string, state.MaterializeLimits) (state.MaterializedState, error)
}

// MaterializedGenerateRuntime receives the validated replay state before it
// is allowed to dispatch Generate. Keeping this as an optional extension of
// V1Runtime preserves the existing Activity contract for runtimes that do not
// opt into checkpoint-aware requests.
type MaterializedGenerateRuntime interface {
	GenerateV1Materialized(context.Context, llm.GenerateRequestV1, state.MaterializedState) (llm.GenerateResponseV1, error)
}

// MaterializedCompactRuntime receives the validated parent state before it is
// allowed to dispatch Compact.
type MaterializedCompactRuntime interface {
	CompactV1Materialized(context.Context, llm.CompactRequestV1, state.MaterializedState) (llm.CompactResponseV1, error)
}

// ScopeResolver maps the authenticated request context to the opaque durable
// state scope. There is deliberately no default concatenation: PostgreSQL
// scopes are stable identifiers, not raw tenant/project strings.
type ScopeResolver func(llm.RequestContext) (string, error)

// MaterializingV1Runtime is a narrow composition seam for the one-shot v1
// Activity boundary. A request carrying a parent checkpoint is materialized
// and validated before the runtime receives it. If the runtime does not
// implement the corresponding state-aware extension, dispatch fails closed.
// Root Generate requests (which have no parent) use the existing V1Runtime
// method because there is no checkpoint to replay.
//
// This seam intentionally does not perform provider, cache, budget, or
// checkpoint publication work. Those operations remain owned by the durable
// runtime implementation behind MaterializedGenerateRuntime and
// MaterializedCompactRuntime.
type MaterializingV1Runtime struct {
	Runtime      V1Runtime
	Materializer CheckpointHandleMaterializer
	Scope        ScopeResolver
	Limits       state.MaterializeLimits
}

// errMaterializedStateInvalid marks a repository/materializer contract
// violation. A checkpoint materializer is trusted to enforce scope and
// lineage invariants before its result crosses into the runtime; accepting a
// mismatched handle or an invalid transcript here could route an operation
// against another checkpoint or produce a response from corrupted state.
// Keep this marker private so callers receive only the stable state-corrupt
// provider error below.
var errMaterializedStateInvalid = errors.New("materialized checkpoint state is invalid")

var _ V1Runtime = (*MaterializingV1Runtime)(nil)

func (runtime *MaterializingV1Runtime) GenerateV1(ctx context.Context, request llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
	if runtime == nil || runtime.Runtime == nil {
		return llm.GenerateResponseV1{}, runtimeConfigurationError("runtime is not configured")
	}
	if request.Parent == nil {
		return runtime.Runtime.GenerateV1(ctx, request)
	}
	materialized, err := runtime.materialize(ctx, request.Context, string(*request.Parent))
	if err != nil {
		return llm.GenerateResponseV1{}, err
	}
	aware, ok := runtime.Runtime.(MaterializedGenerateRuntime)
	if !ok {
		return llm.GenerateResponseV1{}, runtimeConfigurationError("Generate runtime does not accept materialized checkpoints")
	}
	return aware.GenerateV1Materialized(ctx, request, materialized)
}

func (runtime *MaterializingV1Runtime) CompactV1(ctx context.Context, request llm.CompactRequestV1) (llm.CompactResponseV1, error) {
	if runtime == nil || runtime.Runtime == nil {
		return llm.CompactResponseV1{}, runtimeConfigurationError("runtime is not configured")
	}
	materialized, err := runtime.materialize(ctx, request.Context, string(request.Parent))
	if err != nil {
		return llm.CompactResponseV1{}, err
	}
	aware, ok := runtime.Runtime.(MaterializedCompactRuntime)
	if !ok {
		return llm.CompactResponseV1{}, runtimeConfigurationError("Compact runtime does not accept materialized checkpoints")
	}
	return aware.CompactV1Materialized(ctx, request, materialized)
}

func (runtime *MaterializingV1Runtime) QueryV1(ctx context.Context, request llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	if runtime == nil || runtime.Runtime == nil {
		return llm.QueryResponseV1{}, runtimeConfigurationError("runtime is not configured")
	}
	return runtime.Runtime.QueryV1(ctx, request)
}

func (runtime *MaterializingV1Runtime) materialize(ctx context.Context, requestContext llm.RequestContext, handle string) (state.MaterializedState, error) {
	if ctx == nil {
		return state.MaterializedState{}, runtimeConfigurationError("Activity context is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return state.MaterializedState{}, err
	}
	if runtime.Materializer == nil || runtime.Scope == nil {
		return state.MaterializedState{}, runtimeConfigurationError("checkpoint materialization is not configured")
	}
	scopeID, err := runtime.Scope(requestContext)
	if err != nil {
		return state.MaterializedState{}, provider.NewError(provider.CodeInvalidArgument, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever, "checkpoint scope is invalid")
	}
	if scopeID == "" {
		return state.MaterializedState{}, provider.NewError(provider.CodeInvalidArgument, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever, "checkpoint scope is invalid")
	}
	materialized, err := runtime.Materializer.MaterializeHandle(ctx, scopeID, handle, runtime.Limits)
	if err == nil {
		if err := validateMaterializedState(materialized, scopeID, handle); err != nil {
			return state.MaterializedState{}, mapMaterializationError(err)
		}
		return materialized, nil
	}
	return state.MaterializedState{}, mapMaterializationError(err)
}

// validateMaterializedState checks the part of the materializer contract that
// is observable at the Activity boundary. Storage implementations perform the
// complete graph/blob validation; this second check prevents a faulty adapter
// or future implementation from silently substituting a different checkpoint
// or an invalid tool-call frontier after the read has succeeded.
func validateMaterializedState(materialized state.MaterializedState, scopeID, handle string) error {
	fail := func(format string, args ...any) error {
		return fmt.Errorf("%w: %s", errMaterializedStateInvalid, fmt.Sprintf(format, args...))
	}
	if handle == "" || materialized.Handle != state.Handle(handle) {
		return fail("materialized handle does not match requested checkpoint")
	}
	if scopeID == "" || materialized.Tenant != scopeID {
		return fail("materialized scope does not match requested scope")
	}
	if materialized.Depth < 0 {
		return fail("materialized depth is negative")
	}
	if len(materialized.Lineage) == 0 || materialized.Lineage[len(materialized.Lineage)-1] != materialized.Handle {
		return fail("materialized lineage does not terminate at requested checkpoint")
	}
	seen := make(map[state.Handle]struct{}, len(materialized.Lineage))
	for index, lineageHandle := range materialized.Lineage {
		if lineageHandle == "" {
			return fail("materialized lineage entry %d is empty", index)
		}
		if _, exists := seen[lineageHandle]; exists {
			return fail("materialized lineage contains a cycle")
		}
		seen[lineageHandle] = struct{}{}
	}
	if materialized.Settings.Model == "" {
		return fail("materialized root model is missing")
	}
	pending, err := state.ValidateTranscript(materialized.Items)
	if err != nil {
		return fail("materialized transcript: %v", err)
	}
	if len(pending) != len(materialized.PendingToolCalls) {
		return fail("materialized tool frontier does not match transcript")
	}
	for index := range pending {
		if pending[index] != materialized.PendingToolCalls[index] {
			return fail("materialized tool frontier does not match transcript")
		}
	}
	return nil
}

func runtimeConfigurationError(message string) error {
	return provider.NewError(provider.CodeConfiguration, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever, message)
}

func mapMaterializationError(err error) error {
	if err == nil {
		return nil
	}
	// Storage adapters frequently wrap context errors with the checkpoint
	// operation they were performing. Preserve cancellation at this boundary so
	// Temporal observes an Activity cancellation rather than retrying a request
	// whose caller has already stopped waiting.
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return provider.NewError(provider.CodeDeadlineExceeded, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetrySameOperation, "checkpoint materialization deadline exceeded")
	}
	if errors.Is(err, state.ErrNotFound) || errors.Is(err, state.ErrTenantMismatch) || errors.Is(err, state.ErrExpired) {
		return provider.NewError(provider.CodeInvalidArgument, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever, "checkpoint could not be materialized")
	}
	if errors.Is(err, errMaterializedStateInvalid) {
		return provider.NewError(provider.CodeStateCorrupt, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever, "checkpoint materialization returned invalid state")
	}
	// Corrupt lineage and blob failures are worker/state failures. Preserve the
	// retryable distinction only at this sanitized provider boundary.
	return provider.NewError(provider.CodeStateUnavailable, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetrySameOperation, "checkpoint state is unavailable")
}
