package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	blobstore "github.com/mfow/llm-temporal-worker/golang/storage/blob"
	durablestore "github.com/mfow/llm-temporal-worker/golang/storage/durable"
	redisstore "github.com/mfow/llm-temporal-worker/golang/storage/redis"
)

const checkpointCompilerEpoch = "llmtw-runtime-v1"

const (
	checkpointBoundsFailureReason      = "checkpoint_bounds_exceeded"
	checkpointInvalidFailureReason     = "checkpoint_invalid"
	payloadBoundsFailureReason         = "provider_payload_limit_exceeded"
	tokenBoundsFailureReason           = "provider_token_limit_exceeded"
	callerCanceledFailureReason        = "caller_canceled"
	callerDeadlineFailureReason        = "caller_deadline_exceeded"
	callerCanceledPreReservationReason = "caller_canceled_pre_reservation"
	callerDeadlinePreReservationReason = "caller_deadline_pre_reservation"
	providerCanceledPreDispatchReason  = "provider_canceled_pre_dispatch"
	providerDeadlinePreDispatchReason  = "provider_deadline_pre_dispatch"
)

var errProviderPayloadLimit = errors.New("provider request exceeds configured payload limit")

// NewProductionGeneratePortsFactory builds the complete snapshot-owned durable
// Generate phase. Construction is side-effect free; every dependency is
// validated before the returned callbacks can be registered with Temporal.
func NewProductionGeneratePortsFactory() GeneratePortsFactory {
	return func(_ context.Context, capabilities V1RuntimeCapabilities) (durablestore.GeneratePorts, error) {
		binding, err := newProductionPhaseBinding(capabilities)
		if err != nil {
			return durablestore.GeneratePorts{}, err
		}
		return binding.generatePorts(), nil
	}
}

type productionPhaseBinding struct {
	cap         V1RuntimeCapabilities
	composition durablestore.Composition
	codec       state.CheckpointBlobCodec
}

func newProductionPhaseBinding(capabilities V1RuntimeCapabilities) (*productionPhaseBinding, error) {
	composition, ok := capabilities.DurableComposition()
	if !ok {
		return nil, errors.New("durable composition is unavailable")
	}
	if err := composition.Validate(); err != nil {
		return nil, fmt.Errorf("validate durable composition: %w", err)
	}
	if _, ok := composition.Operations.(admission.PreWriteFailureStore); !ok {
		return nil, errors.New("atomic deterministic no-dispatch failure repository is required")
	}
	if composition.Finalizer == nil {
		return nil, errors.New("atomic PostgreSQL finalizer is required")
	}
	if capabilities.Snapshot == nil || capabilities.Planner == nil ||
		capabilities.Estimator.MaxInput <= 0 || capabilities.Estimator.MaxOutput <= 0 ||
		capabilities.Estimator.MaxReasoning <= 0 || capabilities.Adapters == nil {
		return nil, errors.New("routing, pricing, explicit token ceilings, and provider capabilities are required")
	}
	if err := capabilities.Checkpoints.RequirePublication(); err != nil {
		return nil, fmt.Errorf("complete PostgreSQL/S3 checkpoint capabilities are required: %w", err)
	}
	if capabilities.ResolveScope == nil {
		return nil, errors.New("PostgreSQL scope resolver is required")
	}
	if err := capabilities.BudgetGenerationID.Validate(); err != nil {
		return nil, err
	}
	if err := capabilities.BudgetIncarnationID.Validate(); err != nil {
		return nil, err
	}
	if capabilities.ReservationLease <= 0 || capabilities.OperationRetention <= 0 || capabilities.CheckpointRetention <= 0 || capabilities.MaxRequestBytes <= 0 {
		return nil, errors.New("durable reservation lease, retention, and request bounds are required")
	}
	if capabilities.ResourceCapacity.GenerationID != "" && capabilities.CapacityLeases == nil {
		return nil, errors.New("verified resource capacity requires Redis lease storage")
	}
	if capabilities.CheckpointLimits.MaxDepth <= 0 || capabilities.CheckpointLimits.MaxRows <= 0 || capabilities.CheckpointLimits.MaxItems <= 0 || capabilities.CheckpointLimits.MaxBytes <= 0 {
		return nil, errors.New("durable checkpoint bounds are required")
	}
	return &productionPhaseBinding{cap: capabilities, composition: composition, codec: state.CheckpointBlobCodec{MaxBytes: int(capabilities.CheckpointLimits.MaxBytes), MaxDepth: int(capabilities.CheckpointLimits.MaxDepth)}}, nil
}

func (binding *productionPhaseBinding) generatePorts() durablestore.GeneratePorts {
	return durablestore.GeneratePorts{
		Replay:             binding.replayGenerate,
		CacheLookup:        binding.generateCacheLookup,
		CompactionDecision: binding.generateCompactionDecision,
		Compact:            binding.compactForGenerate,
		Route:              binding.routeGenerate,
		Reserve:            binding.reserveGenerate,
		Journal:            binding.journalGenerate,
		Dispatch:           binding.dispatchGenerate,
		Finalize:           binding.finalizeGenerate,
		FinalizeCache:      binding.finalizeGenerateCache,
		Reconcile:          binding.reconcileGenerate,
	}
}

func batchResourceIdentity(scope, kind, operationKey string) durablestore.OperationID {
	material := make([]byte, 0, len(scope)+len(kind)+len(operationKey)+2)
	material = append(material, scope...)
	material = append(material, 0)
	material = append(material, kind...)
	material = append(material, 0)
	material = append(material, operationKey...)
	return durablestore.OperationID(uuid.NewSHA1(uuid.NameSpaceOID, material).String())
}

func strictOperationIdentity(scope, actor, kind, apiVersion, operationKey string) durablestore.OperationID {
	components := []string{scope, actor, kind, apiVersion, operationKey}
	size := len("llmtw/operation/v2\x00")
	for _, component := range components {
		size += len(component) + 1
	}
	material := make([]byte, 0, size)
	material = append(material, "llmtw/operation/v2\x00"...)
	for _, component := range components {
		material = append(material, component...)
		material = append(material, 0)
	}
	return durablestore.OperationID(uuid.NewSHA1(uuid.NameSpaceOID, material).String())
}

func releasedOperationIdentity(kind, operationKey string, digest [32]byte) durablestore.OperationID {
	material := make([]byte, 0, len(kind)+len(operationKey)+len(digest)+2)
	material = append(material, kind...)
	material = append(material, 0)
	material = append(material, operationKey...)
	material = append(material, 0)
	material = append(material, digest[:]...)
	return durablestore.OperationID(uuid.NewSHA1(uuid.NameSpaceOID, material).String())
}

func requestManifest(value any) ([]byte, []byte, [32]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, nil, [32]byte{}, err
	}
	canonical, err := llm.CanonicalJSON(encoded)
	if err != nil {
		return nil, nil, [32]byte{}, err
	}
	digest := sha256.Sum256(canonical)
	manifest, err := json.Marshal(map[string]any{
		"schema_version":    2,
		"payload_sha256":    hex.EncodeToString(digest[:]),
		"payload_bytes":     len(canonical),
		"payload_reference": "inline",
	})
	if err != nil {
		return nil, nil, [32]byte{}, err
	}
	return manifest, canonical, digest, nil
}

func rawScope(value llm.RequestContext) string { return value.Tenant + "\x00" + value.Project }

func (binding *productionPhaseBinding) beginOperation(ctx context.Context, kind, apiVersion, operationKey string, requestContext llm.RequestContext, manifest, payload []byte, digest [32]byte) (durablestore.OperationID, admission.BeginResult, error) {
	operationID := strictOperationIdentity(rawScope(requestContext), requestContext.Actor, kind, apiVersion, operationKey)
	releasedID := releasedOperationIdentity(kind, operationKey, digest)
	now := binding.now()
	result, err := binding.composition.Operations.Begin(ctx, admission.BeginRequest{
		ID: string(operationID), ReleasedID: string(releasedID), OperationKey: operationKey, Actor: requestContext.Actor,
		ScopeKey: rawScope(requestContext), RequestDigest: digest,
		ConfigVersion: hex.EncodeToString(binding.cap.ConfigDigest[:]), ExpiresAt: now.Add(binding.cap.OperationRetention),
		OperationKind: kind, APIVersion: apiVersion, RequestSchemaVersion: 1,
		RequestManifest: manifest, RequestPayload: payload, ConfigDigest: binding.cap.ConfigDigest,
	})
	if err != nil {
		return operationID, admission.BeginResult{}, err
	}
	resolved := durablestore.OperationID(result.Operation.ID)
	if err := resolved.Validate(); err != nil {
		return operationID, admission.BeginResult{}, fmt.Errorf("resolved durable operation identity: %w", err)
	}
	return resolved, result, nil
}

type deterministicNoDispatchClass struct {
	code    provider.Code
	phase   provider.Phase
	message string
}

func deterministicNoDispatchClassification(reason string) (deterministicNoDispatchClass, bool) {
	switch reason {
	case checkpointBoundsFailureReason:
		return deterministicNoDispatchClass{provider.CodeInvalidArgument, provider.PhaseStateLoad, "checkpoint exceeds configured history limit"}, true
	case checkpointInvalidFailureReason:
		return deterministicNoDispatchClass{provider.CodeInvalidArgument, provider.PhaseStateLoad, "checkpoint is invalid or unavailable"}, true
	case payloadBoundsFailureReason:
		return deterministicNoDispatchClass{provider.CodeInvalidArgument, provider.PhaseNormalize, "provider request exceeds configured payload limit"}, true
	case tokenBoundsFailureReason:
		return deterministicNoDispatchClass{provider.CodeInvalidArgument, provider.PhasePlan, "provider request exceeds configured token limit"}, true
	case callerCanceledFailureReason, callerCanceledPreReservationReason:
		return deterministicNoDispatchClass{provider.CodeCanceled, provider.PhaseStateLoad, "request canceled before provider dispatch"}, true
	case callerDeadlineFailureReason, callerDeadlinePreReservationReason:
		return deterministicNoDispatchClass{provider.CodeDeadlineExceeded, provider.PhaseStateLoad, "request deadline exceeded before provider dispatch"}, true
	default:
		return deterministicNoDispatchClass{}, false
	}
}

func deterministicNoDispatchFailure(operationID, reason string, cause error) *provider.Error {
	class, ok := deterministicNoDispatchClassification(reason)
	if !ok {
		class = deterministicNoDispatchClass{provider.CodeInternal, provider.PhaseFinalize, "durable no-dispatch failure classification is invalid"}
	}
	failure := provider.NewError(class.code, class.phase, provider.DispatchNotDispatched, provider.RetryNever, class.message)
	failure.OperationID = operationID
	failure.Cause = cause
	return failure
}

func validateDeterministicNoDispatchReceipt(operation admission.Operation, reason string) error {
	if operation.State != admission.StateDefiniteFailed || operation.FailureReason != reason {
		return errors.New("durable no-dispatch failure identity does not match")
	}
	if operation.CostStatus != "exact" || operation.CostMethod != "worker_cache_zero" ||
		operation.IncurredMicroUSD != 0 || operation.FinalMicroUSD != 0 {
		return errors.New("durable no-dispatch failure is not an exact zero-usage receipt")
	}
	if operation.IncurredCostUSD != nil && !operation.IncurredCostUSD.IsZero() {
		return errors.New("durable no-dispatch failure has incurred provider cost")
	}
	if operation.ActualCostUSD != nil && !operation.ActualCostUSD.IsZero() {
		return errors.New("durable no-dispatch failure has actual provider cost")
	}
	if operation.Attempt.Provider != "" || operation.Attempt.ProviderRequestID != "" {
		return errors.New("durable no-dispatch failure unexpectedly identifies a provider")
	}
	return nil
}

// persistDeterministicNoDispatch atomically closes the operation and its
// zero-usage attempt before returning the typed caller failure. Callers that
// already accepted a Redis reservation must reconcile that reservation after
// this PostgreSQL terminal receipt commits.
func (binding *productionPhaseBinding) persistDeterministicNoDispatch(ctx context.Context, operation admission.Operation, attemptNumber int, reason string, cause error) error {
	if _, ok := deterministicNoDispatchClassification(reason); !ok {
		return errors.New("deterministic no-dispatch failure classification is invalid")
	}
	persistCtx := context.WithoutCancel(ctx)
	if operation.State == admission.StateDefiniteFailed {
		if err := validateDeterministicNoDispatchReceipt(operation, reason); err != nil {
			return err
		}
		return deterministicNoDispatchFailure(operation.ID, reason, cause)
	}
	if operation.State != admission.StateReserved {
		return errors.New("deterministic no-dispatch failure operation is not reserved")
	}
	store, ok := binding.composition.Operations.(admission.PreWriteFailureStore)
	if !ok {
		return errors.New("operation repository cannot atomically persist a deterministic no-dispatch failure")
	}
	if operation.Attempt.AttemptNumber > 0 {
		// A prior provider route already consumed the gateway-owned starting
		// ordinal. Derive the next identity from the durable latest attempt.
		attemptNumber = operation.Attempt.AttemptNumber + 1
	}
	failure := admission.FailRequest{
		OperationID: operation.ID, DispatchToken: operation.DispatchToken,
		Certainty: admission.NotDispatched,
		Attempt:   admission.AttemptFacts{Dispatch: admission.NotDispatched, AttemptNumber: attemptNumber},
		Reason:    reason,
	}
	if err := store.FailBeforeDispatch(persistCtx, failure); err != nil {
		return fmt.Errorf("persist deterministic no-dispatch failure: %w", err)
	}
	terminal, err := binding.composition.Operations.Get(persistCtx, operation.ID)
	if err != nil {
		return fmt.Errorf("reload deterministic no-dispatch failure: %w", err)
	}
	if err := validateDeterministicNoDispatchReceipt(terminal, reason); err != nil {
		return err
	}
	return deterministicNoDispatchFailure(terminal.ID, reason, cause)
}

func (binding *productionPhaseBinding) replayGenerate(ctx context.Context, request llm.GenerateRequestV1) (durablestore.GenerateReplay, error) {
	manifest, payload, digest, err := requestManifest(request)
	if err != nil {
		return durablestore.GenerateReplay{}, err
	}
	operationID, begun, err := binding.beginOperation(ctx, "generate", llm.APIVersion, request.OperationKey, request.Context, manifest, payload, digest)
	if err != nil {
		return durablestore.GenerateReplay{}, err
	}
	replay := durablestore.GenerateReplay{
		OperationID:        operationID,
		ImmutableFacts:     append([]byte(nil), begun.Operation.ImmutableFacts...),
		OperationExpiresAt: begun.Operation.ExpiresAt,
	}
	if begun.Existing && begun.Operation.State == admission.StateDefiniteFailed {
		if _, ok := deterministicNoDispatchClassification(begun.Operation.FailureReason); ok {
			if err := validateDeterministicNoDispatchReceipt(begun.Operation, begun.Operation.FailureReason); err != nil {
				return durablestore.GenerateReplay{}, err
			}
			if len(replay.ImmutableFacts) != 0 {
				route, err := routeFromPersistedFacts(replay.ImmutableFacts, llm.Request{})
				if err != nil {
					return durablestore.GenerateReplay{}, err
				}
				if route.OperationID != replay.OperationID || route.Reservation == nil {
					return durablestore.GenerateReplay{}, errors.New("deterministic no-dispatch reservation identity does not match replay")
				}
				if err := binding.finalizeFailedBudget(ctx, route, *route.Reservation, begun.Operation); err != nil {
					return durablestore.GenerateReplay{}, fmt.Errorf("replay deterministic no-dispatch budget compensation: %w", err)
				}
			}
			return durablestore.GenerateReplay{}, deterministicNoDispatchFailure(begun.Operation.ID, begun.Operation.FailureReason, nil)
		}
	}
	if begun.Existing {
		switch begun.Operation.State {
		case admission.StateCompleted, admission.StateDefiniteFailed, admission.StateAmbiguous:
			// Terminal operations cannot dispatch or publish new provider output.
			// Rebuild their accounting handoff entirely from the immutable
			// PostgreSQL facts so an expired parent checkpoint cannot strand a
			// committed result or failure.
			route, err := routeFromPersistedFacts(replay.ImmutableFacts, llm.Request{})
			if err != nil {
				return durablestore.GenerateReplay{}, err
			}
			if route.OperationID != replay.OperationID {
				return durablestore.GenerateReplay{}, errors.New("persisted terminal route operation identity does not match replay")
			}
			reservation := *route.Reservation
			if begun.Operation.State != admission.StateCompleted {
				if err := binding.finalizeFailedBudget(ctx, route, reservation, begun.Operation); err != nil {
					return durablestore.GenerateReplay{}, fmt.Errorf("replay durable budget compensation: %w", err)
				}
				reason := begun.Operation.FailureReason
				if reason == "" {
					reason = "provider_dispatch_failed"
				}
				return durablestore.GenerateReplay{}, fmt.Errorf("durable operation previously failed: %s", reason)
			}
			response, err := binding.composition.Results.Get(ctx, begun.Operation.ID)
			if err != nil {
				return durablestore.GenerateReplay{}, fmt.Errorf("load completed result: %w", err)
			}
			mapped, err := generateResponseFromNormalized(response, request.Parent, "miss_populated")
			if err != nil {
				return durablestore.GenerateReplay{}, err
			}
			if request.CostAdmission != nil {
				admission := *request.CostAdmission
				mapped.CostAdmission = &admission
			}
			finalization := durablestore.GenerateFinalization{Response: mapped}
			replay.Completed = &mapped
			replay.ReconciliationPending = &durablestore.GenerateReconciliation{Route: route, Reservation: reservation, Finalization: finalization}
			return replay, nil
		case admission.StateDispatching, admission.StateProviderPending:
			return durablestore.GenerateReplay{}, errors.New("durable provider outcome requires operator-safe recovery")
		}
	}
	if request.Parent != nil {
		scopeID, err := binding.cap.ResolveScope(ctx, request.Context)
		if err != nil {
			return durablestore.GenerateReplay{}, err
		}
		materialized, err := binding.cap.Checkpoints.Materializer.MaterializeHandle(ctx, scopeID, string(*request.Parent), binding.cap.CheckpointLimits)
		if err != nil {
			switch {
			case errors.Is(err, state.ErrMaterializeLimit):
				return durablestore.GenerateReplay{}, binding.persistDeterministicNoDispatch(ctx, begun.Operation, gatewayAttemptNumber(request.CostAdmission), checkpointBoundsFailureReason, err)
			case errors.Is(err, state.ErrInvalidCheckpoint), errors.Is(err, state.ErrNotFound), errors.Is(err, state.ErrExpired), errors.Is(err, state.ErrTenantMismatch), errors.Is(err, state.ErrInvalidHandle):
				return durablestore.GenerateReplay{}, binding.persistDeterministicNoDispatch(ctx, begun.Operation, gatewayAttemptNumber(request.CostAdmission), checkpointInvalidFailureReason, err)
			default:
				return durablestore.GenerateReplay{}, err
			}
		}
		// MaterializeHandle authenticates the opaque storage scope resolved from
		// this request. Restore the caller-facing scope labels expected by the
		// provider-neutral replay contract; the checkpoint row intentionally
		// does not persist plaintext tenant or project names.
		materialized.Tenant = request.Context.Tenant
		materialized.Project = request.Context.Project
		replay.State = materialized
	}
	if err := validateGenerateCheckpointBounds(request, replay, binding.cap.CheckpointLimits); err != nil {
		return durablestore.GenerateReplay{}, binding.persistDeterministicNoDispatch(ctx, begun.Operation, gatewayAttemptNumber(request.CostAdmission), checkpointBoundsFailureReason, err)
	}
	providerRequest, _, err := requestFromReplay(request, replay)
	if err != nil {
		return durablestore.GenerateReplay{}, err
	}
	normalized, err := llm.NormalizeRequest(providerRequest)
	if err != nil {
		return durablestore.GenerateReplay{}, err
	}
	if err := validateProviderPayloadBounds(normalized, binding.cap.MaxRequestBytes); err != nil {
		return durablestore.GenerateReplay{}, binding.persistDeterministicNoDispatch(ctx, begun.Operation, gatewayAttemptNumber(request.CostAdmission), payloadBoundsFailureReason, err)
	}
	return replay, nil
}

func (binding *productionPhaseBinding) generateCacheLookup(_ context.Context, request llm.GenerateRequestV1, _ durablestore.GenerateReplay) (durablestore.CacheDecision, error) {
	if request.Cache != nil {
		return durablestore.CacheDecision{}, errors.New("durable response cache is not configured for this snapshot")
	}
	return durablestore.CacheDecision{Disposition: durablestore.CacheDisabled}, nil
}

func (binding *productionPhaseBinding) generateCompactionDecision(_ context.Context, _ llm.GenerateRequestV1, replay durablestore.GenerateReplay, _ durablestore.CacheDecision) (durablestore.CompactionDecision, error) {
	if len(replay.State.Settings.CompactionPolicy) != 0 {
		return durablestore.CompactionDecision{}, errors.New("automatic compaction policy requires an explicit compact child")
	}
	return durablestore.CompactionDecision{}, nil
}

func (binding *productionPhaseBinding) compactForGenerate(context.Context, llm.GenerateRequestV1, durablestore.GenerateReplay) (durablestore.GenerateReplay, error) {
	return durablestore.GenerateReplay{}, errors.New("automatic compaction was not selected")
}

func requestFromReplay(request llm.GenerateRequestV1, replay durablestore.GenerateReplay) (llm.Request, state.ModelState, error) {
	base := replay.State.Settings
	if request.Parent == nil {
		base = state.RootModelState("")
	}
	patch, err := state.SettingsPatchFromV1(request.SettingsPatch)
	if err != nil {
		return llm.Request{}, state.ModelState{}, err
	}
	settings, err := state.ApplySettingsPatch(base, patch)
	if err != nil {
		return llm.Request{}, state.ModelState{}, err
	}
	if err := settings.Validate(); err != nil {
		return llm.Request{}, state.ModelState{}, err
	}
	input := make([]llm.Item, 0, len(replay.State.Items)+len(request.Append))
	input = append(input, replay.State.Items...)
	input = append(input, request.Append...)
	providerRequest := llm.Request{
		APIVersion: llm.APIVersion, OperationKey: request.OperationKey, Context: request.Context,
		Model: settings.Model, ServiceClass: settings.ServiceClass, ServiceClassFallbacks: append([]llm.ServiceClass(nil), settings.ServiceClassFallbacks...),
		Portability: settings.Portability, Instructions: append([]llm.Instruction(nil), settings.Instructions...), Input: input,
		Tools: append([]llm.Tool(nil), settings.Tools...), ToolPolicy: settings.ToolPolicy, Output: settings.Output,
		Sampling:   &llm.SamplingSpec{Temperature: settings.Temperature},
		Reasoning:  &llm.ReasoningSpec{Effort: settings.ReasoningEffort, Summary: settings.ReasoningSummary},
		Extensions: cloneRawMessages(settings.Extensions),
	}
	return providerRequest, settings, nil
}

func validateGenerateCheckpointBounds(request llm.GenerateRequestV1, replay durablestore.GenerateReplay, limits state.MaterializeLimits) error {
	if err := validateCheckpointBounds(replay.State, request.Parent != nil, len(request.Append), limits); err != nil {
		return err
	}
	items := make([]llm.Item, 0, len(replay.State.Items)+len(request.Append))
	items = append(items, replay.State.Items...)
	items = append(items, request.Append...)
	return validateCanonicalPayloadBytes(items, limits.MaxBytes, "checkpoint history")
}

func validateCheckpointBounds(materialized state.MaterializedState, hasParent bool, appendedItems int, limits state.MaterializeLimits) error {
	if hasParent && (materialized.Depth < 0 || materialized.Depth >= limits.MaxDepth) {
		return fmt.Errorf("checkpoint continuation depth limit reached: %d >= %d", materialized.Depth, limits.MaxDepth)
	}
	if len(materialized.Lineage) >= limits.MaxRows {
		return fmt.Errorf("checkpoint row limit reached: %d >= %d", len(materialized.Lineage), limits.MaxRows)
	}
	_, err := boundedCheckpointItemCount(limits.MaxItems, len(materialized.Items), appendedItems)
	return err
}

func validateProviderPayloadBounds(request llm.Request, maxBytes int64) error {
	if maxBytes <= 0 {
		return nil
	}
	if err := validateCanonicalPayloadBytes(request, maxBytes, "provider request"); err != nil {
		return fmt.Errorf("%w: %v", errProviderPayloadLimit, err)
	}
	return nil
}

func validateCanonicalPayloadBytes(value any, maxBytes int64, label string) error {
	maxInt := int64(int(^uint(0) >> 1))
	if maxBytes <= 0 || maxBytes > maxInt {
		return fmt.Errorf("%s byte limit is invalid", label)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode %s: %w", label, err)
	}
	if _, err := llm.CanonicalJSONWithLimits(encoded, int(maxBytes), llm.DefaultCanonicalMaxDepth); err != nil {
		return fmt.Errorf("%s exceeds configured byte/depth limit: %w", label, err)
	}
	return nil
}

func boundedCheckpointItemCount(maxItems int, counts ...int) (int, error) {
	if maxItems <= 0 {
		return 0, errors.New("checkpoint item limit is invalid")
	}
	total := 0
	for _, count := range counts {
		if count < 0 || count > maxItems-total {
			return 0, fmt.Errorf("checkpoint item limit exceeded: combined item count exceeds %d", maxItems)
		}
		total += count
	}
	return total, nil
}

func cloneRawMessages(source map[string]json.RawMessage) map[string]json.RawMessage {
	if source == nil {
		return nil
	}
	result := make(map[string]json.RawMessage, len(source))
	for key, value := range source {
		result[key] = append(json.RawMessage(nil), value...)
	}
	return result
}

func gatewayAttemptNumber(admission *llm.CostAdmissionV1) int {
	if admission == nil {
		return 0
	}
	return int(admission.GatewayAttemptOrdinal)
}

// prepareDurableDispatchRequest enforces the provider-neutral hosted-state
// boundary. Durable continuations replay caller-owned PostgreSQL/S3
// checkpoints, never a provider response or thread identifier. Individual
// adapters are responsible for emitting their explicit stateless wire policy.
func prepareDurableDispatchRequest(request llm.Request, _ routing.Candidate) (llm.Request, error) {
	if request.Continuation != nil {
		return llm.Request{}, errors.New("durable dispatch cannot consume provider-hosted continuation state")
	}
	return request, nil
}

type persistedReservationFacts struct {
	Version                        int                            `json:"version"`
	OperationID                    durablestore.OperationID       `json:"operation_id"`
	GenerationID                   durablestore.GenerationID      `json:"generation_id"`
	ExpiresAt                      time.Time                      `json:"lease_expires_at"`
	RouteID                        string                         `json:"route_id"`
	EndpointID                     string                         `json:"endpoint_id"`
	Provider                       string                         `json:"provider"`
	Model                          string                         `json:"model"`
	PriceVersion                   string                         `json:"price_version"`
	PricingGenerationID            string                         `json:"pricing_generation_id,omitempty"`
	PricingManifestSHA256          string                         `json:"pricing_manifest_sha256,omitempty"`
	ResourceCapacityGenerationID   string                         `json:"resource_capacity_generation_id"`
	ResourceCapacityManifestSHA256 string                         `json:"resource_capacity_manifest_sha256"`
	Candidate                      routing.Candidate              `json:"candidate"`
	Price                          persistedPricingEntry          `json:"price"`
	Reservations                   []admission.WindowReservation  `json:"reservations"`
	Bounds                         durablestore.ReservationBounds `json:"bounds,omitempty"`
	EstimatedUSD                   pricing.USD                    `json:"estimated_usd"`
	Reservation                    durablestore.ReserveResult     `json:"reservation"`
}

type persistedPricingEntry struct {
	Provider, Family, EndpointID, Region, Model, ProviderTier string
	InputPerMillion, OutputPerMillion, CacheReadPerMillion    string
	CacheWritePerMillion, ReasoningPerMillion, PerRequest     string
	UnknownComponents                                         []pricing.PriceComponent
	EffectiveFrom, EffectiveUntil                             time.Time
	Provenance, Version                                       string
}

func decimalText(value pricing.DecimalUSD) string {
	if text := value.String(); text != "" {
		return text
	}
	return "0"
}

func persistPricingEntry(entry pricing.Entry) persistedPricingEntry {
	return persistedPricingEntry{
		Provider: entry.Provider, Family: entry.Family, EndpointID: entry.EndpointID,
		Region: entry.Region, Model: entry.Model, ProviderTier: entry.ProviderTier,
		InputPerMillion:      decimalText(entry.Prices.InputPerMillion),
		OutputPerMillion:     decimalText(entry.Prices.OutputPerMillion),
		CacheReadPerMillion:  decimalText(entry.Prices.CacheReadPerMillion),
		CacheWritePerMillion: decimalText(entry.Prices.CacheWritePerMillion),
		ReasoningPerMillion:  decimalText(entry.Prices.ReasoningPerMillion),
		PerRequest:           decimalText(entry.Prices.PerRequest),
		UnknownComponents:    append([]pricing.PriceComponent(nil), entry.UnknownComponents...),
		EffectiveFrom:        entry.EffectiveFrom, EffectiveUntil: entry.EffectiveUntil,
		Provenance: entry.Provenance, Version: entry.Version,
	}
}

func (entry persistedPricingEntry) restore() (pricing.Entry, error) {
	values := []string{entry.InputPerMillion, entry.OutputPerMillion, entry.CacheReadPerMillion, entry.CacheWritePerMillion, entry.ReasoningPerMillion, entry.PerRequest}
	parsed := make([]pricing.DecimalUSD, len(values))
	for index, value := range values {
		price, err := pricing.ParseDecimalUSD(value)
		if err != nil {
			return pricing.Entry{}, fmt.Errorf("decode persisted price component: %w", err)
		}
		parsed[index] = price
	}
	return pricing.Entry{
		Provider: entry.Provider, Family: entry.Family, EndpointID: entry.EndpointID,
		Region: entry.Region, Model: entry.Model, ProviderTier: entry.ProviderTier,
		Prices:            pricing.UnitPrices{InputPerMillion: parsed[0], OutputPerMillion: parsed[1], CacheReadPerMillion: parsed[2], CacheWritePerMillion: parsed[3], ReasoningPerMillion: parsed[4], PerRequest: parsed[5]},
		UnknownComponents: append([]pricing.PriceComponent(nil), entry.UnknownComponents...),
		EffectiveFrom:     entry.EffectiveFrom, EffectiveUntil: entry.EffectiveUntil,
		Provenance: entry.Provenance, Version: entry.Version,
	}, nil
}

func persistedFactsForRoute(route durablestore.RoutePlan, reservation durablestore.ReserveResult) (persistedReservationFacts, error) {
	if route.Execution == nil {
		return persistedReservationFacts{}, errors.New("route execution is unavailable")
	}
	facts := persistedReservationFacts{
		Version: 4, OperationID: route.OperationID, GenerationID: route.GenerationID,
		ExpiresAt: route.ReservationExpiresAt, RouteID: route.RouteID, EndpointID: route.EndpointID,
		Provider: route.Provider, Model: route.Model, PriceVersion: route.PriceVersion,
		PricingGenerationID: route.PricingGenerationID, PricingManifestSHA256: route.PricingManifestSHA256,
		ResourceCapacityGenerationID:   route.ResourceCapacityGenerationID,
		ResourceCapacityManifestSHA256: route.ResourceCapacityManifestSHA256,
		Candidate:                      route.Execution.Candidate, Price: persistPricingEntry(route.Execution.Price),
		Reservations: append([]admission.WindowReservation(nil), route.Execution.Reservations...),
		EstimatedUSD: route.Execution.EstimatedUSD, Bounds: route.Execution.Bounds, Reservation: reservation,
	}
	if facts.ExpiresAt.IsZero() {
		return persistedReservationFacts{}, errors.New("reservation lease deadline is unavailable")
	}
	return facts, nil
}

func routeFromPersistedFacts(raw []byte, request llm.Request) (durablestore.RoutePlan, error) {
	var facts persistedReservationFacts
	if err := json.Unmarshal(raw, &facts); err != nil {
		return durablestore.RoutePlan{}, fmt.Errorf("decode immutable reservation facts: %w", err)
	}
	if facts.Version != 4 {
		return durablestore.RoutePlan{}, errors.New("unsupported immutable reservation facts version")
	}
	price, err := facts.Price.restore()
	if err != nil {
		return durablestore.RoutePlan{}, err
	}
	if facts.Candidate.RouteID != facts.RouteID || facts.Candidate.EndpointID != facts.EndpointID || facts.Candidate.Provider != facts.Provider || facts.Candidate.Model != facts.Model {
		return durablestore.RoutePlan{}, errors.New("immutable candidate facts do not match selected route")
	}
	if price.Version != facts.PriceVersion {
		return durablestore.RoutePlan{}, errors.New("immutable pricing facts do not match selected route")
	}
	route := durablestore.RoutePlan{
		OperationID: facts.OperationID, GenerationID: facts.GenerationID,
		RouteID: facts.RouteID, EndpointID: facts.EndpointID, Provider: facts.Provider,
		Model: facts.Model, PriceVersion: facts.PriceVersion,
		PricingGenerationID: facts.PricingGenerationID, PricingManifestSHA256: facts.PricingManifestSHA256,
		ResourceCapacityGenerationID:   facts.ResourceCapacityGenerationID,
		ResourceCapacityManifestSHA256: facts.ResourceCapacityManifestSHA256,
		Execution: &durablestore.RouteExecution{
			Request: request, Candidate: facts.Candidate, Price: price,
			Reservations: append([]admission.WindowReservation(nil), facts.Reservations...),
			EstimatedUSD: facts.EstimatedUSD, Bounds: facts.Bounds,
		},
		Reservation: &facts.Reservation, ReservationExpiresAt: facts.ExpiresAt, ReservationRecovered: true,
	}
	if err := route.Validate(); err != nil {
		return durablestore.RoutePlan{}, fmt.Errorf("validate immutable route facts: %w", err)
	}
	if _, err := reservationRequest(route); err != nil {
		return durablestore.RoutePlan{}, fmt.Errorf("validate immutable reservation facts: %w", err)
	}
	return route, nil
}

func reservationRequest(route durablestore.RoutePlan) (durablestore.ReserveRequest, error) {
	if route.Execution == nil || route.Reservation == nil || len(route.Reservation.Events) == 0 {
		return durablestore.ReserveRequest{}, errors.New("immutable reservation facts are unavailable")
	}
	occurredAt := route.Reservation.Events[0].OccurredAt
	for _, event := range route.Reservation.Events {
		if !event.OccurredAt.Equal(occurredAt) {
			return durablestore.ReserveRequest{}, errors.New("immutable reservation event timestamps differ")
		}
	}
	request := durablestore.ReserveRequest{
		OperationID: route.OperationID, GenerationID: route.GenerationID,
		IncarnationID: route.Reservation.IncarnationID,
		Reservations:  route.Execution.Reservations,
		ExpiresAt:     route.ReservationExpiresAt, OccurredAt: occurredAt,
		Route: durablestore.DispatchRouteFacts{
			RouteID: route.RouteID, EndpointID: route.EndpointID, Provider: route.Provider,
			ResolvedModel: route.Model, ServiceClass: string(route.Execution.Candidate.AttemptedClass),
			PriceVersion: route.PriceVersion,
		},
		Bounds: route.Execution.Bounds,
	}
	if err := route.Reservation.Validate(request); err != nil {
		return durablestore.ReserveRequest{}, err
	}
	if err := durablestore.ValidatePlannedReserveResult(request, *route.Reservation); err != nil {
		return durablestore.ReserveRequest{}, err
	}
	return request, nil
}

func (binding *productionPhaseBinding) prepareReservationRoute(ctx context.Context, kind, apiVersion, operationKey string, requestContext llm.RequestContext, requestValue any, route durablestore.RoutePlan) (durablestore.RoutePlan, error) {
	if route.Execution == nil {
		return durablestore.RoutePlan{}, errors.New("route execution is unavailable")
	}
	now := binding.now()
	route.ReservationExpiresAt = now.Add(binding.cap.ReservationLease)
	reserveRequest := durablestore.ReserveRequest{
		OperationID: route.OperationID, GenerationID: route.GenerationID,
		IncarnationID: binding.cap.BudgetIncarnationID,
		Reservations:  route.Execution.Reservations,
		ExpiresAt:     route.ReservationExpiresAt, OccurredAt: now,
	}
	reservation, err := durablestore.PlannedReserveResult(reserveRequest)
	if err != nil {
		return durablestore.RoutePlan{}, err
	}
	route.Reservation = &reservation
	if err := binding.persistReservationFacts(ctx, kind, apiVersion, operationKey, requestContext, requestValue, route, reservation); err != nil {
		return durablestore.RoutePlan{}, err
	}
	return route, nil
}

func (binding *productionPhaseBinding) persistReservationFacts(ctx context.Context, kind, apiVersion, operationKey string, requestContext llm.RequestContext, requestValue any, route durablestore.RoutePlan, reservation durablestore.ReserveResult) error {
	facts, err := persistedFactsForRoute(route, reservation)
	if err != nil {
		return err
	}
	factsJSON, err := json.Marshal(facts)
	if err != nil {
		return fmt.Errorf("encode immutable reservation facts: %w", err)
	}
	manifest, payload, digest, err := requestManifest(requestValue)
	if err != nil {
		return err
	}
	result, err := binding.composition.Operations.Begin(ctx, admission.BeginRequest{
		ID: string(route.OperationID), ReleasedID: string(releasedOperationIdentity(kind, operationKey, digest)),
		OperationKey: operationKey, Actor: requestContext.Actor,
		ScopeKey: rawScope(requestContext), RequestDigest: digest,
		ConfigVersion: hex.EncodeToString(binding.cap.ConfigDigest[:]), ExpiresAt: route.ReservationExpiresAt,
		OperationKind: kind, APIVersion: apiVersion, RequestSchemaVersion: 1,
		RequestManifest: manifest, RequestPayload: payload, ConfigDigest: binding.cap.ConfigDigest,
		ReservationUSD: route.Execution.EstimatedUSD, ImmutableFacts: factsJSON,
	})
	if err != nil {
		return err
	}
	if !result.Existing || result.Operation.ID != string(route.OperationID) || len(result.Operation.ImmutableFacts) == 0 {
		return errors.New("immutable reservation facts were not durably recovered")
	}
	return nil
}

func (binding *productionPhaseBinding) routeGenerate(ctx context.Context, request llm.GenerateRequestV1, replay durablestore.GenerateReplay, _ durablestore.CompactionDecision) (durablestore.RoutePlan, error) {
	providerRequest, _, err := requestFromReplay(request, replay)
	if err != nil {
		return durablestore.RoutePlan{}, err
	}
	normalized, err := llm.NormalizeRequest(providerRequest)
	if err != nil {
		return durablestore.RoutePlan{}, err
	}
	if len(replay.ImmutableFacts) != 0 {
		route, err := routeFromPersistedFacts(replay.ImmutableFacts, normalized)
		if err != nil {
			return durablestore.RoutePlan{}, err
		}
		if route.OperationID != replay.OperationID {
			return durablestore.RoutePlan{}, errors.New("persisted route operation identity does not match replay")
		}
		dispatchRequest, err := prepareDurableDispatchRequest(normalized, route.Execution.Candidate)
		if err != nil {
			return durablestore.RoutePlan{}, err
		}
		route.Execution.Request = dispatchRequest
		return route, nil
	}
	if err := replay.OperationID.Validate(); err != nil {
		return durablestore.RoutePlan{}, fmt.Errorf("resolved replay operation identity: %w", err)
	}
	snapshot, err := binding.cap.Snapshot.Current(ctx)
	if err != nil {
		return durablestore.RoutePlan{}, err
	}
	if snapshot.Prices == nil {
		return durablestore.RoutePlan{}, errors.New("pricing resolver is unavailable")
	}
	now := binding.now()
	plan, err := binding.cap.Planner.Plan(ctx, routing.Input{Request: normalized, Catalog: snapshot.Routes, Health: snapshot.Health, Now: now})
	if err != nil {
		return durablestore.RoutePlan{}, err
	}
	type pricedCandidate struct {
		candidate routing.Candidate
		request   llm.Request
		entry     pricing.Entry
		quote     pricing.Quote
		estimate  budget.Estimate
		matches   []budget.MatchedWindow
	}
	priced := make([]pricedCandidate, 0, len(plan.Candidates))
	var tokenLimitErr error
	for _, candidate := range plan.Candidates {
		dispatchRequest, policyErr := prepareDurableDispatchRequest(normalized, candidate)
		if policyErr != nil {
			return durablestore.RoutePlan{}, policyErr
		}
		matches := budget.MatchPolicies(snapshot.BudgetPolicies, budget.ContextFor(normalized, candidate, snapshot.Environment))
		if snapshot.RequireBudgetMatch && len(matches) == 0 {
			continue
		}
		quote, quoteErr := snapshot.Prices.Resolve(pricing.Query{Provider: candidate.Provider, Family: candidate.Family, EndpointID: candidate.EndpointID, Region: candidate.Region, Model: candidate.Model, ProviderTier: candidate.ProviderTier, At: now})
		if quoteErr != nil {
			if request.CostAdmission != nil {
				return durablestore.RoutePlan{}, fmt.Errorf("cost admission route %q has no immutable price: %w", candidate.RouteID, quoteErr)
			}
			continue
		}
		if request.CostAdmission != nil &&
			(quote.CatalogVersion != request.CostAdmission.PricingGenerationID ||
				quote.CatalogDigest != request.CostAdmission.PricingManifestSHA256) {
			return durablestore.RoutePlan{}, fmt.Errorf("cost admission pricing snapshot does not match route %q", candidate.RouteID)
		}
		entry := quote.Entry
		if entry.Version == "" {
			entry.Version = quote.CatalogVersion
		}
		estimator := binding.cap.Estimator
		if request.CostAdmission != nil && request.CostAdmission.BatchID != "" {
			estimator.MaxInput = 0
			estimator.MaxOutput = 0
			estimator.MaxReasoning = 0
		}
		estimate, estimateErr := estimator.EstimateCandidate(dispatchRequest, candidate, entry)
		if estimateErr != nil {
			if errors.Is(estimateErr, budget.ErrTokenLimit) {
				tokenLimitErr = estimateErr
				continue
			}
			if request.CostAdmission != nil {
				return durablestore.RoutePlan{}, fmt.Errorf("cost admission route %q cannot be safely priced: %w", candidate.RouteID, estimateErr)
			}
			continue
		}
		priced = append(priced, pricedCandidate{candidate: candidate, request: dispatchRequest, entry: entry, quote: quote, estimate: estimate, matches: matches})
	}
	if len(priced) == 0 {
		if tokenLimitErr != nil {
			operation, loadErr := binding.composition.Operations.Get(ctx, string(replay.OperationID))
			if loadErr != nil {
				return durablestore.RoutePlan{}, fmt.Errorf("reload token-bound operation: %w", loadErr)
			}
			return durablestore.RoutePlan{}, binding.persistDeterministicNoDispatch(ctx, operation, gatewayAttemptNumber(request.CostAdmission), tokenBoundsFailureReason, tokenLimitErr)
		}
		return durablestore.RoutePlan{}, errors.New("no eligible priced route")
	}
	selected := priced[0]
	maximum := selected.estimate
	if request.CostAdmission != nil {
		for _, candidate := range priced[1:] {
			if candidate.estimate.CostUSD.Cmp(maximum.CostUSD) > 0 {
				maximum = candidate.estimate
			}
		}
	}
	reservations := make([]admission.WindowReservation, 0, len(selected.matches))
	for _, match := range selected.matches {
		_, bucket := match.Window.Range(now)
		reservations = append(reservations, admission.WindowReservation{PolicyID: match.PolicyID, WindowID: match.Window.ID, Bucket: bucket, Amount: maximum.MicroUSD, Limit: match.Window.Limit, AmountUSD: maximum.CostUSD, LimitUSD: match.Window.LimitUSD, BucketNanos: match.Window.Bucket.Nanoseconds(), DurationNanos: match.Window.Duration.Nanoseconds()})
	}
	route := durablestore.RoutePlan{
		OperationID: replay.OperationID, GenerationID: binding.cap.BudgetGenerationID,
		RouteID: selected.candidate.RouteID, EndpointID: selected.candidate.EndpointID,
		Provider: selected.candidate.Provider, Model: selected.candidate.Model,
		PriceVersion: selected.entry.Version, PricingGenerationID: selected.quote.CatalogVersion,
		PricingManifestSHA256:          selected.quote.CatalogDigest,
		ResourceCapacityGenerationID:   binding.cap.ResourceCapacity.GenerationID,
		ResourceCapacityManifestSHA256: binding.cap.ResourceCapacity.ManifestSHA256,
		Execution:                      &durablestore.RouteExecution{Request: selected.request, Candidate: selected.candidate, Price: selected.entry, Reservations: reservations, EstimatedUSD: maximum.CostUSD},
	}
	if request.CostAdmission != nil && request.CostAdmission.BatchID != "" {
		granted, err := binding.prepareGrantedReservationRoute(ctx, request, route)
		if errors.Is(err, budget.ErrTokenLimit) {
			operation, loadErr := binding.composition.Operations.Get(ctx, string(replay.OperationID))
			if loadErr != nil {
				return durablestore.RoutePlan{}, fmt.Errorf("reload signed token-bound operation: %w", loadErr)
			}
			terminalErr := binding.persistDeterministicNoDispatch(ctx, operation, gatewayAttemptNumber(request.CostAdmission), tokenBoundsFailureReason, err)
			var mapped *provider.Error
			if !errors.As(terminalErr, &mapped) {
				return durablestore.RoutePlan{}, terminalErr
			}
			terminal, loadErr := binding.composition.Operations.Get(context.WithoutCancel(ctx), operation.ID)
			if loadErr != nil {
				return durablestore.RoutePlan{}, fmt.Errorf("reload terminal signed token-bound operation: %w", loadErr)
			}
			if granted.Reservation == nil {
				return durablestore.RoutePlan{}, errors.New("signed token-bound operation has no confirmed reservation")
			}
			if reconcileErr := binding.finalizeFailedBudget(ctx, granted, *granted.Reservation, terminal); reconcileErr != nil {
				return durablestore.RoutePlan{}, fmt.Errorf("reconcile signed token-bound operation: %w", reconcileErr)
			}
			return durablestore.RoutePlan{}, terminalErr
		}
		return granted, err
	}
	return binding.prepareReservationRoute(ctx, "generate", llm.APIVersion, request.OperationKey, request.Context, request, route)
}

func (binding *productionPhaseBinding) reserveGenerate(ctx context.Context, _ llm.GenerateRequestV1, route durablestore.RoutePlan) (durablestore.ReserveResult, error) {
	request, err := reservationRequest(route)
	if err != nil {
		return durablestore.ReserveResult{}, err
	}
	if route.ReservationRecovered {
		confirmed, confirmErr := binding.composition.Materializer.Confirm(ctx, request)
		if confirmErr == nil {
			if err := durablestore.ValidatePlannedReserveResult(request, confirmed); err != nil {
				return durablestore.ReserveResult{}, err
			}
			return confirmed, nil
		}
		if !errors.Is(confirmErr, durablestore.ErrReservationNotFound) {
			return durablestore.ReserveResult{}, confirmErr
		}
	}
	result, err := binding.composition.Materializer.Accept(ctx, request)
	if err != nil {
		return durablestore.ReserveResult{}, err
	}
	if err := durablestore.ValidatePlannedReserveResult(request, result); err != nil {
		return durablestore.ReserveResult{}, err
	}
	return result, nil
}

func (binding *productionPhaseBinding) journalGenerate(ctx context.Context, _ llm.GenerateRequestV1, route durablestore.RoutePlan, reservation durablestore.ReserveResult) (durablestore.JournalReceipt, error) {
	for _, event := range reservation.Events {
		if _, err := binding.composition.Journal.AppendReservation(ctx, event); err != nil {
			return durablestore.JournalReceipt{}, err
		}
	}
	return durablestore.JournalReceipt{OperationID: route.OperationID, GenerationID: route.GenerationID}, nil
}

func (binding *productionPhaseBinding) confirmReservation(ctx context.Context, route durablestore.RoutePlan) (durablestore.ReserveResult, error) {
	request, err := reservationRequest(route)
	if err != nil {
		return durablestore.ReserveResult{}, err
	}
	if !request.ExpiresAt.After(binding.now()) {
		return durablestore.ReserveResult{}, errors.New("immutable reservation lease expired before provider dispatch")
	}
	result, err := binding.composition.Materializer.Confirm(ctx, request)
	if err != nil {
		return durablestore.ReserveResult{}, err
	}
	if err := durablestore.ValidatePlannedReserveResult(request, result); err != nil {
		return durablestore.ReserveResult{}, err
	}
	return result, nil
}

type productionDispatchObserver struct {
	binding       *productionPhaseBinding
	operation     admission.Operation
	route         durablestore.RoutePlan
	candidate     routing.Candidate
	attemptNumber int
	capacity      redisstore.ThrottleReservation
	capacityStop  chan struct{}
	capacityDone  chan struct{}
	cancelInvoke  context.CancelCauseFunc
	marked        bool
}

func (observer *productionDispatchObserver) BeforePossibleWrite(ctx context.Context) error {
	if observer.marked {
		return nil
	}
	if !observer.route.ReservationExpiresAt.After(observer.binding.now()) {
		return errors.New("immutable reservation lease expired before provider write")
	}
	reservation, err := reservationRequest(observer.route)
	if err != nil {
		return err
	}
	// Fence the accepted Redis reservation first, then acquire the exact signed
	// capacity generation. PostgreSQL is deliberately the final gate: capacity
	// queueing cannot consume the immutable lease and then write provider bytes
	// under a stale dispatch transition.
	if err := observer.binding.composition.Materializer.FenceDispatch(ctx, durablestore.DispatchFenceRequest{Reservation: reservation, RetainUntil: observer.operation.ExpiresAt}); err != nil {
		return fmt.Errorf("fence live Redis reservation before provider write: %w", err)
	}
	capacity, err := observer.acquireCapacity(ctx)
	if err != nil {
		return err
	}
	observer.capacity = capacity
	observer.startCapacityRenewal(ctx)
	if !observer.route.ReservationExpiresAt.After(observer.binding.now()) {
		releaseErr := observer.releaseCapacity(ctx)
		return errors.Join(errors.New("immutable reservation lease expired while awaiting provider capacity"), releaseErr)
	}
	err = observer.binding.composition.Operations.MarkDispatching(ctx, admission.DispatchRequest{OperationID: observer.operation.ID, DispatchToken: observer.operation.DispatchToken, Attempt: admission.AttemptFacts{RouteID: observer.candidate.RouteID, EndpointID: observer.candidate.EndpointID, Provider: observer.candidate.Provider, ResolvedModel: observer.candidate.Model, ServiceClass: string(observer.candidate.AttemptedClass), AttemptNumber: observer.attemptNumber}, LeaseUntil: observer.route.ReservationExpiresAt})
	if err != nil {
		releaseErr := observer.releaseCapacity(ctx)
		return errors.Join(err, releaseErr)
	}
	if !observer.route.ReservationExpiresAt.After(observer.binding.now()) {
		releaseErr := observer.releaseCapacity(ctx)
		return errors.Join(errors.New("immutable reservation lease expired during durable dispatch transition"), releaseErr)
	}
	if err := ctx.Err(); err != nil {
		releaseErr := observer.releaseCapacity(ctx)
		return errors.Join(err, releaseErr)
	}
	observer.marked = true
	return nil
}
func (*productionDispatchObserver) AfterResponseHeaders(context.Context, provider.ResponseMetadata) error {
	return nil
}
func (*productionDispatchObserver) OnProgress(context.Context, provider.Progress) {}

func (observer *productionDispatchObserver) acquireCapacity(ctx context.Context) (redisstore.ThrottleReservation, error) {
	capacity := observer.binding.cap.ResourceCapacity
	if err := validateProviderCapacityBinding(observer.route, capacity); err != nil {
		return redisstore.ThrottleReservation{}, err
	}
	if capacity.GenerationID == "" {
		return redisstore.ThrottleReservation{}, nil
	}
	if observer.binding.cap.CapacityLeases == nil {
		return redisstore.ThrottleReservation{}, errors.New("verified resource capacity has no Redis lease store")
	}
	limits, queueTimeout, err := providerCapacityLimits(capacity, observer.candidate.RouteID, observer.binding.cap.ReservationLease)
	if err != nil {
		return redisstore.ThrottleReservation{}, err
	}
	leaseID := providerCapacityLeaseID(observer.route, observer.operation, observer.attemptNumber)
	scopePrefix := capacity.GenerationID + ":"
	acquired, err := observer.binding.cap.CapacityLeases.AcquireFair(
		ctx,
		leaseID,
		scopePrefix+"provider-call-queue",
		queueTimeout,
		25*time.Millisecond,
		limits,
	)
	if err != nil {
		return redisstore.ThrottleReservation{}, fmt.Errorf("acquire signed provider capacity: %w", err)
	}
	if acquired.Reservation.ID != leaseID {
		return redisstore.ThrottleReservation{}, errors.New("signed provider capacity lease identity mismatch")
	}
	return acquired.Reservation, nil
}
func providerCapacityLeaseID(route durablestore.RoutePlan, operation admission.Operation, attemptNumber int) string {
	return "provider-call:" + digestCanonical(map[string]string{
		"schema":          "llmtw/provider-capacity-lease/v1",
		"generation_id":   route.ResourceCapacityGenerationID,
		"manifest_sha256": route.ResourceCapacityManifestSHA256,
		"operation_id":    operation.ID,
		"dispatch_token":  operation.DispatchToken,
		"attempt_ordinal": fmt.Sprintf("%d", attemptNumber),
		"route_id":        route.RouteID,
	})
}

func validateProviderCapacityBinding(route durablestore.RoutePlan, capacity config.VerifiedResourceCapacity) error {
	if capacity.GenerationID == "" {
		if route.ResourceCapacityGenerationID != "" || route.ResourceCapacityManifestSHA256 != "" {
			return errors.New("immutable route requires signed provider capacity unavailable in this snapshot")
		}
		return nil
	}
	if route.ResourceCapacityGenerationID != capacity.GenerationID ||
		route.ResourceCapacityManifestSHA256 != capacity.ManifestSHA256 {
		return errors.New("immutable route does not match the signed provider capacity generation")
	}
	return nil
}

func providerCapacityLimits(capacity config.VerifiedResourceCapacity, routeID string, lease time.Duration) ([]redisstore.ThrottleLimit, time.Duration, error) {
	limits := capacity.Limits
	var routeLimit *config.ProviderInflightLimit
	for index := range limits.ProviderMaxInflight {
		if limits.ProviderMaxInflight[index].RouteID == routeID {
			routeLimit = &limits.ProviderMaxInflight[index]
			break
		}
	}
	if routeLimit == nil {
		return nil, 0, fmt.Errorf("resolved route %q is absent from signed resource capacity", routeID)
	}
	scopePrefix := capacity.GenerationID + ":"
	return []redisstore.ThrottleLimit{
		{Kind: redisstore.ThrottleConcurrency, Scope: scopePrefix + "global", Amount: 1, Limit: int64(limits.LLMGlobalMaxInflight), Window: lease},
		{Kind: redisstore.ThrottleRequests, Scope: scopePrefix + "global", Amount: 1, Limit: int64(limits.LLMGlobalMaxRequestsPerWindow), Window: time.Duration(limits.LLMGlobalWindowSeconds) * time.Second},
		{Kind: redisstore.ThrottleConcurrency, Scope: scopePrefix + "route:" + routeLimit.RouteID, Amount: 1, Limit: int64(routeLimit.MaxInflight), Window: lease},
		{Kind: redisstore.ThrottleRequests, Scope: scopePrefix + "route:" + routeLimit.RouteID, Amount: 1, Limit: int64(routeLimit.MaxRequestsPerWindow), Window: time.Duration(routeLimit.WindowSeconds) * time.Second},
	}, time.Duration(limits.ProviderCallQueueSeconds) * time.Second, nil
}

func (observer *productionDispatchObserver) startCapacityRenewal(ctx context.Context) {
	if observer.capacity.ID == "" {
		return
	}
	interval := observer.binding.cap.ReservationLease / 3
	if interval < time.Second {
		interval = time.Second
	}
	observer.capacityStop = make(chan struct{})
	observer.capacityDone = make(chan struct{})
	go func() {
		defer close(observer.capacityDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-observer.capacityStop:
				return
			case <-ticker.C:
				renewCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), interval)
				err := observer.binding.cap.CapacityLeases.Renew(renewCtx, observer.capacity, observer.binding.cap.ReservationLease)
				cancel()
				if err != nil {
					if observer.cancelInvoke != nil {
						observer.cancelInvoke(fmt.Errorf("renew signed provider capacity: %w", err))
					}
					return
				}
			}
		}
	}()
}

func (observer *productionDispatchObserver) releaseCapacity(ctx context.Context) error {
	if observer.capacity.ID == "" {
		return nil
	}
	if observer.capacityStop != nil {
		close(observer.capacityStop)
		<-observer.capacityDone
		observer.capacityStop = nil
	}
	err := observer.binding.cap.CapacityLeases.Release(context.WithoutCancel(ctx), observer.capacity)
	observer.capacity = redisstore.ThrottleReservation{}
	return err
}

func (binding *productionPhaseBinding) dispatchGenerate(ctx context.Context, request llm.GenerateRequestV1, _ durablestore.GenerateReplay, route durablestore.RoutePlan, _ durablestore.JournalReceipt) (durablestore.DispatchResult, error) {
	if route.Execution == nil {
		return durablestore.DispatchResult{}, errors.New("route execution is unavailable")
	}
	operation, err := binding.composition.Operations.Get(ctx, string(route.OperationID))
	if err != nil {
		return durablestore.DispatchResult{}, err
	}
	candidate := route.Execution.Candidate
	attemptNumber := gatewayAttemptNumber(request.CostAdmission)
	if operation.Attempt.AttemptNumber > 0 {
		attemptNumber = operation.Attempt.AttemptNumber + 1
	}
	adapter, err := binding.cap.Adapters.Adapter(ctx, candidate)
	if err != nil {
		return durablestore.DispatchResult{}, binding.failProviderAttempt(ctx, route, operation, attemptNumber, admission.NotDispatched, err)
	}
	query := provider.CapabilityQuery{EndpointID: candidate.EndpointID, Family: provider.Family(candidate.Family), Model: candidate.Model, ServiceClass: candidate.AttemptedClass}
	capability, err := adapter.Capabilities(ctx, query)
	if err != nil {
		return durablestore.DispatchResult{}, binding.failProviderAttempt(ctx, route, operation, attemptNumber, admission.NotDispatched, err)
	}
	digest, err := llm.RequestDigest(route.Execution.Request)
	if err != nil {
		return durablestore.DispatchResult{}, binding.failProviderAttempt(ctx, route, operation, attemptNumber, admission.NotDispatched, err)
	}
	compileRequest := route.Execution.Request
	compileRequest.Model = candidate.Model
	call, err := adapter.Compile(ctx, provider.CompileInput{Request: compileRequest, Query: query, Capability: capability, Strict: route.Execution.Request.Portability != llm.PortabilityBestEffort, Metadata: provider.CallMetadata{SchemaDigest: digest, CapabilityVersion: candidate.CapabilityVersion, ProviderTier: candidate.ProviderTier}})
	if err != nil {
		return durablestore.DispatchResult{}, binding.failProviderAttempt(ctx, route, operation, attemptNumber, admission.NotDispatched, err)
	}
	if _, err := binding.confirmReservation(ctx, route); err != nil {
		cause := fmt.Errorf("confirm live immutable reservation: %w", err)
		return durablestore.DispatchResult{}, binding.failProviderAttempt(ctx, route, operation, attemptNumber, admission.NotDispatched, cause)
	}
	invokeCtx, cancelInvoke := context.WithCancelCause(ctx)
	defer cancelInvoke(context.Canceled)
	observer := &productionDispatchObserver{binding: binding, operation: operation, route: route, candidate: candidate, attemptNumber: attemptNumber, cancelInvoke: cancelInvoke}
	defer func() {
		_ = observer.releaseCapacity(context.WithoutCancel(ctx))
	}()
	result, err := adapter.Invoke(invokeCtx, call, observer)
	releaseErr := observer.releaseCapacity(ctx)
	if err == nil && context.Cause(invokeCtx) != nil {
		err = context.Cause(invokeCtx)
	}
	if err == nil && releaseErr != nil {
		err = fmt.Errorf("release signed provider capacity: %w", releaseErr)
	}
	if err != nil {
		certainty := admission.Ambiguous
		var mapped *provider.Error
		if errors.As(err, &mapped) && (mapped.Dispatch == provider.DispatchNotDispatched || mapped.Dispatch == provider.DispatchRejected) {
			certainty = admission.Rejected
		}
		if !observer.marked {
			certainty = admission.NotDispatched
		}
		if observer.marked {
			operation.State = admission.StateDispatching
		}
		return durablestore.DispatchResult{}, binding.failProviderAttempt(ctx, route, operation, attemptNumber, certainty, err)
	}
	if !observer.marked {
		return durablestore.DispatchResult{}, binding.failProviderAttempt(ctx, route, operation, attemptNumber, admission.Ambiguous, errors.New("provider adapter returned without marking possible write"))
	}
	if result.Response.Cost.ActualCostUSD == nil {
		usageCost, costErr := pricing.CostFromUsage(route.Execution.Price, pricing.Usage{InputTokens: result.Response.Usage.InputTokens, OutputTokens: result.Response.Usage.OutputTokens, ReasoningTokens: result.Response.Usage.ReasoningTokens, CacheReadTokens: result.Response.Usage.CacheReadTokens, CacheWriteTokens: result.Response.Usage.CacheWriteTokens})
		if costErr == nil {
			actual := usageCost.USD
			result.Response.Cost.Status = llm.CostStatusKnown
			result.Response.Cost.ActualCostUSD = &actual
			result.Response.Cost.Method = string(usageCost.Method)
			result.Response.Cost.CatalogVersion = usageCost.CatalogVersion
		} else {
			result.Response.Cost.Status = llm.CostStatusUnknown
			result.Response.Cost.Method = ""
			result.Response.Cost.CatalogVersion = ""
		}
	}
	bindRouteProvenance(&result.Response, route.Execution.Request, candidate)
	result.Response.OperationID = string(route.OperationID)
	result.Response.OperationKey = route.Execution.Request.OperationKey
	return durablestore.DispatchResult{Response: result.Response}, nil
}

func bindRouteProvenance(response *llm.Response, request llm.Request, candidate routing.Candidate) {
	response.Route = llm.RouteFacts{
		RouteID: candidate.RouteID, EndpointID: candidate.EndpointID,
		APIFamily: candidate.Family, RequestedModel: request.Model,
		ResolvedModel: candidate.Model,
	}
	response.Service.Requested = candidate.RequestedClass
	response.Service.Attempted = candidate.AttemptedClass
	response.Service.FallbackIndex = candidate.FallbackIndex
}

func (binding *productionPhaseBinding) failProviderAttempt(ctx context.Context, route durablestore.RoutePlan, operation admission.Operation, attemptNumber int, certainty admission.DispatchCertainty, cause error) error {
	persistCtx := context.WithoutCancel(ctx)
	current, err := binding.composition.Operations.Get(persistCtx, operation.ID)
	if err != nil {
		return fmt.Errorf("reload provider attempt before failure: %w", err)
	}
	operation = current
	if operation.State == admission.StateReserved && operation.Attempt.AttemptNumber > 0 {
		attemptNumber = operation.Attempt.AttemptNumber + 1
	} else if operation.Attempt.AttemptNumber > 0 {
		attemptNumber = operation.Attempt.AttemptNumber
	}
	reason := "provider_dispatch_failed"
	if certainty == admission.NotDispatched || certainty == admission.Rejected {
		switch {
		case errors.Is(cause, context.Canceled):
			reason = providerCanceledPreDispatchReason
		case errors.Is(cause, context.DeadlineExceeded):
			reason = providerDeadlinePreDispatchReason
		}
	}
	failure := admission.FailRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Certainty: certainty, Attempt: admission.AttemptFacts{RouteID: route.RouteID, EndpointID: route.EndpointID, Provider: route.Provider, ResolvedModel: route.Model, Dispatch: certainty, AttemptNumber: attemptNumber}, Reason: reason}
	var failErr error
	if operation.State == admission.StateReserved && (certainty == admission.NotDispatched || certainty == admission.Rejected) {
		store, ok := binding.composition.Operations.(admission.PreWriteFailureStore)
		if !ok {
			return errors.New("operation repository cannot atomically persist a proven pre-write failure")
		}
		failErr = store.FailBeforeDispatch(persistCtx, failure)
	} else {
		if operation.State == admission.StateReserved {
			if err := binding.composition.Operations.MarkDispatching(persistCtx, admission.DispatchRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Attempt: failure.Attempt, LeaseUntil: route.ReservationExpiresAt}); err != nil {
				return fmt.Errorf("mark failed provider attempt: %w", err)
			}
		}
		failErr = binding.composition.Operations.Fail(persistCtx, failure)
	}
	if failErr != nil {
		return fmt.Errorf("record failed provider attempt: %w", failErr)
	}
	terminal, err := binding.composition.Operations.Get(persistCtx, operation.ID)
	if err != nil {
		return fmt.Errorf("reload failed provider attempt: %w", err)
	}
	if err := binding.finalizeFailedBudget(ctx, route, *route.Reservation, terminal); err != nil {
		return fmt.Errorf("finalize durable budget outcome: %w", err)
	}
	return cause
}

func (binding *productionPhaseBinding) finalizeFailedBudget(ctx context.Context, route durablestore.RoutePlan, reservation durablestore.ReserveResult, operation admission.Operation) error {
	if route.Execution == nil {
		return errors.New("route execution is unavailable")
	}
	if operation.CompletedAt.IsZero() {
		return errors.New("persisted failure timestamp is unavailable")
	}
	zero := pricing.MustUSD("0")
	events := make([]budget.CompletionEvent, 0, len(reservation.Events))
	for index, reserved := range reservation.Events {
		event := budget.CompletionEvent{
			EventID:      uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("llmtw/budget-failed/v2\x00%s\x00%s\x00%d", route.OperationID, reserved.EventID, index))).String(),
			GenerationID: string(route.GenerationID), OperationID: string(route.OperationID),
			WindowID: reserved.WindowID, BucketStart: reserved.BucketStart,
			ReservationRevision: reserved.ReservationRevision + 1, OccurredAt: operation.CompletedAt,
		}
		switch {
		case operation.State == admission.StateAmbiguous:
			event.Kind, event.CostStatus, event.UnknownReasonCode = budget.JournalRetainAmbiguous, budget.CostUnknown, operation.CostUnknownReason
			if event.UnknownReasonCode == "" {
				event.UnknownReasonCode = "ambiguous_dispatch"
			}
		case operation.State == admission.StateCanceled:
			event.Kind, event.CostStatus, event.ActualCostUSD = budget.JournalRelease, budget.CostExact, &zero
			event.ReservedDecreaseUSD = reserved.AmountUSD
		case operation.CostStatus == "unknown":
			event.Kind, event.CostStatus, event.UnknownReasonCode = budget.JournalFinalizeUnknown, budget.CostUnknown, operation.CostUnknownReason
			if event.UnknownReasonCode == "" {
				event.UnknownReasonCode = "provider_did_not_report_cost"
			}
			event.ReservedDecreaseUSD, event.AccountedIncreaseUSD = reserved.AmountUSD, reserved.AmountUSD
		case operation.CostMethod == "worker_cache_zero":
			event.Kind, event.CostStatus, event.ActualCostUSD = budget.JournalRelease, budget.CostExact, &zero
			event.ReservedDecreaseUSD = reserved.AmountUSD
		default:
			if operation.ActualCostUSD == nil {
				return errors.New("persisted failed operation exact cost is unavailable")
			}
			actual := *operation.ActualCostUSD
			event.Kind, event.CostStatus, event.ActualCostUSD = budget.JournalFinalizeExact, budget.CostExact, &actual
			event.ReservedDecreaseUSD, event.AccountedIncreaseUSD = reserved.AmountUSD, actual
		}
		events = append(events, event)
	}
	for _, event := range events {
		if _, err := binding.composition.Journal.AppendCompletion(context.WithoutCancel(ctx), event); err != nil {
			return err
		}
	}
	return binding.composition.Materializer.Reconcile(context.WithoutCancel(ctx), durablestore.ReconcileRequest{OperationID: route.OperationID, GenerationID: route.GenerationID, IncarnationID: reservation.IncarnationID, Events: events})
}

func (binding *productionPhaseBinding) finalizeGenerate(ctx context.Context, request llm.GenerateRequestV1, replay durablestore.GenerateReplay, route durablestore.RoutePlan, reservation durablestore.ReserveResult, dispatch durablestore.DispatchResult) (durablestore.GenerateFinalization, error) {
	if request.CostAdmission != nil {
		if dispatch.Response.Cost.ActualCostUSD == nil {
			return durablestore.GenerateFinalization{}, binding.failPostResponseCheckpoint(ctx, route, reservation, dispatch.Response, errors.New("cost admission requires exact provider receipt"))
		}
		bounds := route.Execution.Bounds
		usage := dispatch.Response.Usage
		if bounds.OperationSHA256 != "" && (usage.InputTokens > bounds.MaxInputTokens ||
			usage.OutputTokens > bounds.MaxOutputTokens ||
			usage.ReasoningTokens > bounds.MaxReasoningTokens ||
			usage.CacheReadTokens > bounds.MaxCacheReadTokens ||
			usage.CacheWriteTokens > bounds.MaxCacheWriteTokens) {
			return durablestore.GenerateFinalization{}, binding.failPostResponseCheckpoint(ctx, route, reservation, dispatch.Response, errors.New("exact provider usage exceeds signed batch grant component ceiling"))
		}
		allowance, allowanceErr := pricing.USDFromMicro(pricing.MicroUSD(request.CostAdmission.RemainingMaxCostMicrounits))
		if allowanceErr != nil {
			return durablestore.GenerateFinalization{}, binding.failPostResponseCheckpoint(ctx, route, reservation, dispatch.Response, fmt.Errorf("cost admission allowance: %w", allowanceErr))
		}
		actual := *dispatch.Response.Cost.ActualCostUSD
		if actual.Cmp(route.Execution.EstimatedUSD) > 0 || actual.Cmp(allowance) > 0 {
			return durablestore.GenerateFinalization{}, binding.failPostResponseCheckpoint(ctx, route, reservation, dispatch.Response, errors.New("exact provider cost exceeds admitted reservation"))
		}
		dispatch.Response.Cost.CatalogVersion = route.PricingGenerationID
	}
	scopeID, err := binding.cap.ResolveScope(ctx, request.Context)
	if err != nil {
		return durablestore.GenerateFinalization{}, err
	}
	checkpoint, handle, checkpointObjects, err := binding.publishCheckpoint(ctx, scopeID, request.Parent, route.OperationID, state.CheckpointGeneration, replay, request.Append, request.SettingsPatch, dispatch.Response.Output)
	if err != nil {
		return durablestore.GenerateFinalization{}, binding.failPostResponseCheckpoint(ctx, route, reservation, dispatch.Response, err)
	}
	dispatch.Response.Continuation = &llm.Continuation{Handle: handle}
	resultRef, err := binding.composition.Results.Put(ctx, string(route.OperationID), dispatch.Response)
	if err != nil {
		return durablestore.GenerateFinalization{}, err
	}
	actualMicro := pricing.MicroUSD(0)
	if dispatch.Response.Cost.ActualCostUSD != nil {
		actualMicro, err = pricing.CeilMicroFromUSD(*dispatch.Response.Cost.ActualCostUSD)
		if err != nil {
			return durablestore.GenerateFinalization{}, err
		}
	}
	operation, err := binding.composition.Operations.Get(ctx, string(route.OperationID))
	if err != nil {
		return durablestore.GenerateFinalization{}, err
	}
	actualUSD, unknownReason := operationCompletionCost(dispatch.Response.Cost)
	checkpoint, err = binding.composition.Finalizer.Finalize(ctx, admission.AtomicFinalization{
		ScopeID: scopeID, Checkpoint: checkpoint, CheckpointObjects: checkpointObjects,
		Complete: admission.CompleteRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Actual: actualMicro, ActualCostUSD: actualUSD, ResultRef: &resultRef, Attempt: admission.AttemptFacts{RouteID: route.RouteID, EndpointID: route.EndpointID, Provider: route.Provider, ResolvedModel: route.Model, ServiceClass: string(route.Execution.Candidate.AttemptedClass), Dispatch: admission.Accepted, AttemptNumber: operation.Attempt.AttemptNumber}, CostStatus: costStatus(dispatch.Response.Cost), CostMethod: dispatch.Response.Cost.Method, CostCatalogVersion: dispatch.Response.Cost.CatalogVersion, UnknownReason: unknownReason},
	})
	if err != nil {
		return durablestore.GenerateFinalization{}, err
	}
	mapped, err := generateResponseFromNormalized(dispatch.Response, request.Parent, "miss_populated")
	if err != nil {
		return durablestore.GenerateFinalization{}, err
	}
	if request.CostAdmission != nil {
		admission := *request.CostAdmission
		mapped.CostAdmission = &admission
	}
	mapped.Checkpoint.Depth = checkpoint.Depth
	_ = reservation
	return durablestore.GenerateFinalization{Response: mapped}, nil
}

func (binding *productionPhaseBinding) failPostResponse(
	ctx context.Context,
	route durablestore.RoutePlan,
	reservation durablestore.ReserveResult,
	response llm.Response,
	reason string,
	cause error,
) error {
	if route.Execution == nil {
		return errors.New("route execution is unavailable")
	}
	persistCtx := context.WithoutCancel(ctx)
	operation, err := binding.composition.Operations.Get(persistCtx, string(route.OperationID))
	if err != nil {
		return fmt.Errorf("load post-response failure: %w", err)
	}
	if operation.State == admission.StateCompleted {
		if err := binding.reconcileCompletedResponseBudget(persistCtx, route, reservation, response, operation); err != nil {
			return fmt.Errorf("reconcile committed post-response outcome: %w", err)
		}
		return cause
	}
	if operation.State.Terminal() {
		if err := binding.finalizeFailedBudget(persistCtx, route, reservation, operation); err != nil {
			return fmt.Errorf("reconcile persisted post-response failure: %w", err)
		}
		return cause
	}
	status := costStatus(response.Cost)
	actual, unknownReason := operationCompletionCost(response.Cost)
	actualMicro := pricing.MicroUSD(0)
	if response.Cost.ActualCostUSD != nil {
		// Exact USD is authoritative. The microUSD field is compatibility-only;
		// leave it at zero if an otherwise valid provider cost is outside the
		// legacy Redis integer range so the terminal transition cannot strand.
		if compatible, compatibilityErr := pricing.CeilMicroFromUSD(actual); compatibilityErr == nil {
			actualMicro = compatible
		}
	}
	attempt := admission.AttemptFacts{
		RouteID: route.RouteID, EndpointID: route.EndpointID, Provider: route.Provider,
		ResolvedModel: route.Model, ProviderRequestID: response.Provider.RequestID,
		ServiceClass: string(route.Execution.Candidate.AttemptedClass),
		Dispatch:     admission.Accepted, AttemptNumber: operation.Attempt.AttemptNumber,
	}
	if operation.State == admission.StateReserved {
		if err := binding.composition.Operations.MarkDispatching(persistCtx, admission.DispatchRequest{
			OperationID: operation.ID, DispatchToken: operation.DispatchToken,
			Attempt: attempt, LeaseUntil: route.ReservationExpiresAt,
		}); err != nil {
			return fmt.Errorf("mark post-response failure as dispatched: %w", err)
		}
	}
	fail := admission.FailRequest{
		OperationID: operation.ID, DispatchToken: operation.DispatchToken,
		Certainty: admission.Accepted, Incurred: actualMicro, IncurredCostUSD: actual,
		PostResponse: true, CostStatus: status, CostMethod: response.Cost.Method, CostCatalogVersion: response.Cost.CatalogVersion, UnknownReason: unknownReason,
		Attempt: attempt, Reason: reason,
	}
	if err := binding.composition.Operations.Fail(persistCtx, fail); err != nil {
		return fmt.Errorf("persist post-response failure: %w", err)
	}
	terminal, err := binding.composition.Operations.Get(persistCtx, operation.ID)
	if err != nil {
		return fmt.Errorf("reload post-response failure: %w", err)
	}
	if err := binding.finalizeFailedBudget(persistCtx, route, reservation, terminal); err != nil {
		return fmt.Errorf("reconcile post-response failure: %w", err)
	}
	return cause
}

func (binding *productionPhaseBinding) failPostResponseCheckpoint(ctx context.Context, route durablestore.RoutePlan, reservation durablestore.ReserveResult, response llm.Response, cause error) error {
	return binding.failPostResponse(ctx, route, reservation, response, "checkpoint_publication_failed", cause)
}

func (binding *productionPhaseBinding) reconcileCompletedResponseBudget(ctx context.Context, route durablestore.RoutePlan, reservation durablestore.ReserveResult, response llm.Response, operation admission.Operation) error {
	if operation.CompletedAt.IsZero() {
		return errors.New("persisted completion timestamp is unavailable")
	}
	cost := llm.CostV1{Status: "unknown", UnknownReason: "provider_did_not_report_cost"}
	if response.Cost.ActualCostUSD != nil {
		value := response.Cost.ActualCostUSD.String()
		cost = llm.CostV1{Status: "exact", ActualCostUSD: &value, Method: response.Cost.Method, CatalogVersion: response.Cost.CatalogVersion}
	}
	events, err := completionEvents(route, reservation, cost, operation.CompletedAt)
	if err != nil {
		return err
	}
	for _, event := range events {
		if _, err := binding.composition.Journal.AppendCompletion(context.WithoutCancel(ctx), event); err != nil {
			return err
		}
	}
	return binding.composition.Materializer.Reconcile(context.WithoutCancel(ctx), durablestore.ReconcileRequest{
		OperationID: route.OperationID, GenerationID: route.GenerationID,
		IncarnationID: reservation.IncarnationID, Events: events,
	})
}

func costStatus(cost llm.Cost) string {
	if cost.ActualCostUSD == nil {
		return "unknown"
	}
	return "exact"
}

func operationCompletionCost(cost llm.Cost) (pricing.USD, string) {
	if cost.ActualCostUSD == nil {
		return pricing.USD{}, "provider_did_not_report_cost"
	}
	return *cost.ActualCostUSD, ""
}

func generateResponseFromNormalized(response llm.Response, parent *llm.CheckpointHandle, cacheDisposition string) (llm.GenerateResponseV1, error) {
	if response.Continuation == nil || strings.TrimSpace(response.Continuation.Handle) == "" {
		return llm.GenerateResponseV1{}, errors.New("stored response has no checkpoint handle")
	}
	handle := llm.CheckpointHandle(response.Continuation.Handle)
	cost := llm.CostV1{Status: "unknown", UnknownReason: "provider_did_not_report_cost"}
	if response.Cost.ActualCostUSD != nil {
		value := response.Cost.ActualCostUSD.String()
		cost = llm.CostV1{Status: "exact", ActualCostUSD: &value, Method: response.Cost.Method, CatalogVersion: response.Cost.CatalogVersion}
	}
	return llm.GenerateResponseV1{APIVersion: llm.APIVersion, OperationKey: response.OperationKey, OperationID: response.OperationID, Status: response.Status, Output: response.Output, Checkpoint: llm.CheckpointMetadata{Handle: handle, Parent: parent, Kind: "generation"}, Cache: llm.CacheDispositionV1{Disposition: cacheDisposition}, Route: &response.Route, Usage: &response.Usage, Cost: cost, Diagnostics: response.Diagnostics}, nil
}

func (binding *productionPhaseBinding) finalizeGenerateCache(context.Context, llm.GenerateRequestV1, durablestore.GenerateReplay, durablestore.CacheDecision) (durablestore.GenerateFinalization, error) {
	return durablestore.GenerateFinalization{}, errors.New("cache finalization cannot run when durable cache is disabled")
}

func (binding *productionPhaseBinding) reconcileGenerate(ctx context.Context, _ llm.GenerateRequestV1, route durablestore.RoutePlan, reservation durablestore.ReserveResult, finalization durablestore.GenerateFinalization) error {
	operation, err := binding.composition.Operations.Get(ctx, string(route.OperationID))
	if err != nil {
		return err
	}
	if operation.CompletedAt.IsZero() {
		return errors.New("persisted completion timestamp is unavailable")
	}
	events, err := completionEvents(route, reservation, finalization.Response.Cost, operation.CompletedAt)
	if err != nil {
		return err
	}
	for _, event := range events {
		if _, err := binding.composition.Journal.AppendCompletion(ctx, event); err != nil {
			return err
		}
	}
	return binding.composition.Materializer.Reconcile(ctx, durablestore.ReconcileRequest{OperationID: route.OperationID, GenerationID: route.GenerationID, IncarnationID: reservation.IncarnationID, Events: events})
}

func completionEvents(route durablestore.RoutePlan, reservation durablestore.ReserveResult, cost llm.CostV1, now time.Time) ([]budget.CompletionEvent, error) {
	if route.Execution == nil {
		return nil, errors.New("route execution is unavailable")
	}
	var actual *pricing.USD
	kind := budget.JournalFinalizeUnknown
	status := budget.CostUnknown
	unknown := cost.UnknownReason
	if cost.ActualCostUSD != nil {
		parsed, err := pricing.ParseUSD(*cost.ActualCostUSD)
		if err != nil {
			return nil, err
		}
		actual = &parsed
		kind = budget.JournalFinalizeExact
		status = budget.CostExact
		unknown = ""
	}
	events := make([]budget.CompletionEvent, 0, len(reservation.Events))
	for index, reserved := range reservation.Events {
		increase := reserved.AmountUSD
		if actual != nil {
			increase = *actual
		}
		eventID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("llmtw/budget-complete/v1\x00%s\x00%s\x00%d", route.OperationID, reserved.EventID, index))).String()
		events = append(events, budget.CompletionEvent{EventID: eventID, GenerationID: string(route.GenerationID), OperationID: string(route.OperationID), WindowID: reserved.WindowID, BucketStart: reserved.BucketStart, ReservationRevision: reserved.ReservationRevision + 1, Kind: kind, ReservedDecreaseUSD: reserved.AmountUSD, AccountedIncreaseUSD: increase, ActualCostUSD: actual, CostStatus: status, UnknownReasonCode: unknown, OccurredAt: now})
	}
	return events, nil
}

func (binding *productionPhaseBinding) publishCheckpoint(ctx context.Context, scopeID string, parentHandle *llm.CheckpointHandle, operationID durablestore.OperationID, kind state.CheckpointKind, replay durablestore.GenerateReplay, delta []llm.Item, patchWire llm.SettingsPatchV1, output []llm.Item) (state.DurableCheckpoint, string, []blobstore.Ref, error) {
	parsedScope, err := uuid.Parse(scopeID)
	if err != nil || parsedScope == uuid.Nil || parsedScope.String() != scopeID {
		return state.DurableCheckpoint{}, "", nil, errors.New("invalid PostgreSQL scope identity")
	}
	itemCount, err := boundedCheckpointItemCount(binding.cap.CheckpointLimits.MaxItems, len(output))
	if kind != state.CheckpointCompaction && err == nil {
		itemCount, err = boundedCheckpointItemCount(binding.cap.CheckpointLimits.MaxItems, itemCount, len(replay.State.Items), len(delta))
	}
	if err != nil {
		return state.DurableCheckpoint{}, "", nil, err
	}
	if parentHandle != nil {
		if replay.State.Depth < 0 || replay.State.Depth >= binding.cap.CheckpointLimits.MaxDepth {
			return state.DurableCheckpoint{}, "", nil, fmt.Errorf("checkpoint continuation depth limit reached: %d >= %d", replay.State.Depth, binding.cap.CheckpointLimits.MaxDepth)
		}
	}
	if len(replay.State.Lineage) >= binding.cap.CheckpointLimits.MaxRows {
		return state.DurableCheckpoint{}, "", nil, fmt.Errorf("checkpoint row limit reached: %d >= %d", len(replay.State.Lineage), binding.cap.CheckpointLimits.MaxRows)
	}
	checkpointID := state.CheckpointID(uuid.NewSHA1(uuid.NameSpaceOID, []byte("llmtw/checkpoint/v1\x00"+string(operationID))).String())
	handle, keyID, publicMAC, err := binding.cap.Checkpoints.IssueHandle(scopeID, checkpointID)
	if err != nil {
		return state.DurableCheckpoint{}, "", nil, err
	}
	var patch state.SettingsPatch
	if kind == state.CheckpointCompaction {
		patch = state.SettingsPatchForModel(replay.State.Settings)
	} else {
		patch, err = state.SettingsPatchFromV1(patchWire)
		if err != nil {
			return state.DurableCheckpoint{}, "", nil, err
		}
	}
	bundleBytes, err := binding.codec.EncodeBundle(state.CheckpointBundle{
		Delta: delta, Response: output, SettingsPatch: patch,
	})
	if err != nil {
		return state.DurableCheckpoint{}, "", nil, err
	}
	var parentID *state.CheckpointID
	depth := int32(0)
	if parentHandle != nil {
		resolved, err := binding.cap.Checkpoints.VerifyHandle(ctx, scopeID, string(*parentHandle))
		if err != nil {
			return state.DurableCheckpoint{}, "", nil, err
		}
		parentID = &resolved
		depth = replay.State.Depth + 1
	}
	expires := binding.now().Add(binding.cap.CheckpointRetention)
	objects := make([]blobstore.Ref, 0, 2)
	bundleRef, err := binding.putCheckpointBlob(ctx, scopeID, bundleBytes, expires)
	if err != nil {
		return state.DurableCheckpoint{}, "", nil, err
	}
	objects = append(objects, bundleRef)

	lineage := append([]state.Handle(nil), replay.State.Lineage...)
	lineage = append(lineage, state.Handle(checkpointID))
	var transcript []llm.Item
	if kind == state.CheckpointCompaction {
		transcript = make([]llm.Item, len(output))
		copy(transcript, output)
	} else {
		transcript = make([]llm.Item, 0, len(replay.State.Items)+len(delta)+len(output))
		transcript = append(transcript, replay.State.Items...)
		transcript = append(transcript, delta...)
		transcript = append(transcript, output...)
	}
	materializedSettings := replay.State.Settings
	if kind != state.CheckpointCompaction {
		materializedSettings, err = state.ApplySettingsPatch(replay.State.Settings, patch)
		if err != nil {
			return state.DurableCheckpoint{}, "", nil, err
		}
	}
	// Exact materialized snapshots are bounded replay accelerators. They are
	// immutable content-addressed objects and never replace the authoritative
	// lineage deltas; readers can fall back if a snapshot fails verification.
	const snapshotInterval int32 = 16
	if kind == state.CheckpointCompaction || depth%snapshotInterval == 0 {
		snapshot := state.NewCheckpointSnapshot(state.MaterializedState{
			Items: transcript, Settings: materializedSettings, Depth: depth, Lineage: lineage,
		})
		encoded, err := binding.codec.EncodeSnapshot(*snapshot)
		if err != nil {
			return state.DurableCheckpoint{}, "", nil, err
		}
		ref, err := binding.putCheckpointBlob(ctx, scopeID, encoded, expires)
		if err != nil {
			return state.DurableCheckpoint{}, "", nil, err
		}
		objects = append(objects, ref)
	}
	lineageBytes, _ := json.Marshal(lineage)
	settingsDigest, err := binding.codec.DigestMaterializedSettings(materializedSettings)
	if err != nil {
		return state.DurableCheckpoint{}, "", nil, err
	}
	frontier, err := state.ValidateTranscript(transcript)
	if err != nil {
		return state.DurableCheckpoint{}, "", nil, err
	}
	frontierBytes, _ := json.Marshal(frontier)
	now := binding.now()
	checkpoint := state.DurableCheckpoint{ID: checkpointID, ScopeID: scopeID, PublicIDHMAC: publicMAC, HandleKeyID: keyID, ParentID: parentID, Kind: kind, Depth: depth, OriginOperationID: state.OperationID(operationID), CanonicalLineageDigest: sha256.Sum256(lineageBytes), MaterializedSettingsDigest: settingsDigest, ToolFrontierDigest: sha256.Sum256(frontierBytes), SchemaVersion: 1, CompilerEpoch: checkpointCompilerEpoch, CreatedAt: now, ExpiresAt: expires}
	if kind == state.CheckpointCompaction {
		checkpoint.CompactedThroughID = parentID
	}
	return checkpoint, handle, objects, nil
}

func (binding *productionPhaseBinding) putCheckpointBlob(ctx context.Context, scopeID string, data []byte, expires time.Time) (blobstore.Ref, error) {
	return binding.cap.Checkpoints.BlobStore.Put(ctx, blobstore.PutRequest{Tenant: scopeID, MediaType: "application/json", Data: data, ExpiresAt: expires})
}

func (binding *productionPhaseBinding) now() time.Time {
	if binding.cap.Clock != nil {
		return binding.cap.Clock().UTC()
	}
	return time.Now().UTC()
}

var _ provider.Observer = (*productionDispatchObserver)(nil)
var _ engine.AdapterRegistry = (engine.AdapterMap)(nil)
