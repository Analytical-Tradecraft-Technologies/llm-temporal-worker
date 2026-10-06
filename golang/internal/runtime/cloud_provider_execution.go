package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

type CloudProviderExecutionStore interface {
	Read(context.Context, cloudstate.Scope, cloudstate.RequestID) (cloudstate.Record, error)
	BeginProviderExecution(context.Context, cloudstate.Scope, cloudstate.RequestID, durable.ReserveResult, time.Time) (cloudstate.SavedProviderExecution, bool, error)
	LoadProviderExecution(context.Context, cloudstate.Scope, cloudstate.RequestID) (cloudstate.SavedProviderExecution, error)
	SaveProviderExecution(context.Context, cloudstate.Scope, cloudstate.RequestID, uint64, cloudstate.ProviderExecution) error
}

// CloudProviderExecution makes at most one provider call per invocation. It
// never sleeps, selects a replacement route, or retries paid submission. Scope
// is supplied by the authorized runtime; provider IDs never leave this layer.
type CloudProviderExecution struct {
	statusRecorder engine.ProviderStatusRecorder
	store          CloudProviderExecutionStore
	admission      *CloudBudgetAdmission
	clock          func() time.Time
	saveBackoff    []time.Duration
	// finalizationTimeout bounds the detached settlement after a provider
	// result; zero means defaultCloudFinalizationTimeout.
	finalizationTimeout time.Duration
}

// defaultCloudFinalizationTimeout is server.finalization_timeout's default.
const defaultCloudFinalizationTimeout = 10 * time.Second

// One execution save may take executionSaveAttemptTimeout. A failed save is
// retried after each executionSaveBackoff delay while executionSaveBudget lasts.
const (
	executionSaveAttemptTimeout = 10 * time.Second
	executionSaveBudget         = 30 * time.Second
)

var executionSaveBackoff = []time.Duration{200 * time.Millisecond, time.Second, 3 * time.Second}

// ProviderExecutionResult is internal runtime state, not a Temporal payload.
// Workflow-facing payloads use only the internal request ID and safe status.
type ProviderExecutionResult struct {
	Saved      cloudstate.SavedProviderExecution
	RetryAfter time.Duration
}

func (capabilities V1RuntimeCapabilities) NewCloudProviderExecution(ctx context.Context) (*CloudProviderExecution, error) {
	store, ok := capabilities.Requests.(CloudProviderExecutionStore)
	if !ok || isNilCapability(store) {
		return nil, budgetPlanningError(provider.CodeConfiguration)
	}
	admission, err := capabilities.NewCloudBudgetAdmission(ctx)
	if err != nil {
		return nil, err
	}
	clock := capabilities.Clock
	if clock == nil {
		clock = time.Now
	}
	return &CloudProviderExecution{statusRecorder: capabilities.ProviderStatusRecorder, store: store, admission: admission, clock: clock, saveBackoff: executionSaveBackoff}, nil
}

// Submit accepts only a prepared local call and its accepted reservation. The
// durable fence is written before Claim, and BeforePossibleWrite persists the
// claim before allowing HTTP. A saved claim is never reused as a dispatch grant.
func (executor *CloudProviderExecution) Submit(ctx context.Context, call *CloudBudgetCall, reservation durable.ReserveResult) (ProviderExecutionResult, error) {
	return executor.submit(ctx, call, reservation, nil)
}

// The optional cache gate runs only for the durable execution-fence winner,
// before a budget claim or HTTP request. A crash or uncertain gate reply stays
// recoverable as an unknown child; retries cannot regain dispatch permission.
func (executor *CloudProviderExecution) submit(ctx context.Context, call *CloudBudgetCall, reservation durable.ReserveResult, start func(context.Context) error) (ProviderExecutionResult, error) {
	if executor == nil || call == nil || call.owner != executor.admission || ctx == nil {
		return ProviderExecutionResult{}, executionError(provider.CodeConfiguration)
	}
	if err := ctx.Err(); err != nil {
		return ProviderExecutionResult{}, err
	}
	// An already-started attempt is resumed through Resume, including after a
	// lost acknowledgement of Begin. It can never enter initial admission.
	saved, fresh, err := executor.store.BeginProviderExecution(ctx, call.scope, call.id, reservation, executor.clock())
	if err != nil {
		return ProviderExecutionResult{}, cloudRuntimeError(err, false)
	}
	if cloudPlanDigest(saved.Plan) != cloudPlanDigest(call.plan) {
		return ProviderExecutionResult{}, executionError(provider.CodeStateCorrupt)
	}
	if !fresh {
		return executor.settle(ctx, call.scope, call.id, saved)
	}
	if start != nil {
		if err := start(ctx); err != nil {
			next := saved.Execution
			next.Stage = cloudstate.ExecutionUnknown
			next.Failure = &cloudstate.ExecutionFailure{Code: provider.CodeAmbiguousDispatch, Dispatch: provider.DispatchAmbiguous}
			return executor.save(ctx, call.scope, call.id, saved, next)
		}
	}
	var claim *durable.ClaimReceipt
	if saved.Plan.RequiresReservation() {
		value, claimErr := executor.admission.admit.claim(ctx, saved.Plan.Route, saved.Execution.Reservation)
		err = claimErr
		if err == nil {
			claim = &value
		}
	}
	if err != nil {
		// Even a lost claim reply remains charged. Do not infer that the start
		// permission was unused, and never refund or acquire a replacement here.
		next := saved.Execution
		next.Stage = cloudstate.ExecutionUnknown
		next.Failure = &cloudstate.ExecutionFailure{Code: provider.CodeAmbiguousDispatch, Dispatch: provider.DispatchAmbiguous}
		return executor.save(ctx, call.scope, call.id, saved, next)
	}
	observer := &executionObserver{executor: executor, scope: call.scope, id: call.id, saved: saved, claim: claim}
	// Bound an individual HTTP call independently of workflow waiting. Saving
	// its outcome has a separate bounded context if this context expires.
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	var outcome provider.ResumableResult
	if resumable, ok := call.provider.Adapter.(provider.ResumableAdapter); ok {
		outcome, err = resumable.Submit(callCtx, call.provider.Call, observer)
	} else {
		var result provider.Result
		result, err = call.provider.Adapter.Invoke(callCtx, call.provider.Call, observer)
		outcome = provider.ResumableResult{State: provider.ResumableCompleted, Dispatch: provider.DispatchAccepted, Result: result}
	}
	saved = observer.saved
	if observer.saveErr != nil {
		// The observer refused the possible-write boundary and never marked it,
		// so the adapter was not allowed to send anything. Settle the consumed
		// claim as a zero-cost retryable failure instead of leaving the attempt
		// in claiming/submitting until RecoverAfter. The refused save may still
		// have been applied, so continue from whichever revision is durable.
		current, ok := executor.refusedDispatch(ctx, call.scope, call.id, saved)
		if !ok {
			return ProviderExecutionResult{}, cloudRuntimeError(observer.saveErr, true)
		}
		saved = current
		err = provider.NewError(provider.CodeStateUnavailable, provider.PhaseDispatch, provider.DispatchNotDispatched, provider.RetrySameOperation, "provider execution not dispatched")
	}
	// A preflight failure may precede the observer. Retain the fresh claim so
	// an explicitly proven pre-dispatch failure can be settled without another
	// HTTP call. A successful adapter must have crossed the durable boundary.
	if !observer.marked {
		if err == nil {
			err = executionError(provider.CodeProviderInvalidResponse)
		}
		saved.Execution.Claim = claim
	}
	// An adapter cannot undo the durable possible-write boundary without
	// transport evidence that the request never reached a writable connection.
	var classified *provider.Error
	if observer.marked && errors.As(err, &classified) && classified.Dispatch == provider.DispatchNotDispatched &&
		!errors.Is(err, provider.ErrProviderPreDispatch) && !errors.Is(err, provider.ErrProviderEgressDenied) {
		copy := *classified
		copy.Dispatch = provider.DispatchAmbiguous
		copy.Retry = provider.RetryNever
		err = &copy
	}
	result, err := executor.completeCall(ctx, call.scope, call.id, saved, call.provider.Call, call.transcript, outcome, err, false)
	if err != nil && observer.saveErr != nil {
		return ProviderExecutionResult{}, cloudRuntimeError(observer.saveErr, true)
	}
	return result, err
}

// Resume reconstructs the same saved provider call using original input. It
// polls once if an ID exists, or uses documented idempotency recovery once
// after the bounded submission interval. It never calls Submit/Invoke.
func (executor *CloudProviderExecution) Resume(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, generate durable.GenerateReplay, compact durable.CompactReplay) (ProviderExecutionResult, error) {
	if executor == nil || ctx == nil {
		return ProviderExecutionResult{}, executionError(provider.CodeConfiguration)
	}
	saved, err := executor.store.LoadProviderExecution(ctx, scope, id)
	if err != nil {
		return ProviderExecutionResult{}, cloudRuntimeError(err, false)
	}
	stage := saved.Execution.Stage
	if stage == cloudstate.ExecutionSucceeded || stage == cloudstate.ExecutionFailed {
		return executor.settle(ctx, scope, id, saved)
	}
	if stage == cloudstate.ExecutionPending && executor.clock().Before(saved.Execution.PollAfter) {
		return executor.result(saved), nil
	}
	if (stage == cloudstate.ExecutionClaiming || stage == cloudstate.ExecutionSubmitting) && executor.clock().Before(saved.Execution.RecoverAfter) {
		return executor.result(saved), nil
	}
	planned, transcript, err := executor.reconstruct(ctx, scope, id, saved.Plan, generate, compact)
	if err != nil {
		return ProviderExecutionResult{}, err
	}
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	var outcome provider.ResumableResult
	if saved.Execution.ProviderOperationID != "" {
		adapter, ok := planned.Adapter.(provider.ResumableAdapter)
		if !ok {
			return ProviderExecutionResult{}, executionError(provider.CodeConfiguration)
		}
		outcome, err = adapter.Poll(callCtx, planned.Call, saved.Execution.ProviderOperationID, provider.NopObserver{})
		// A transport failure during a read does not destroy the durable job.
		if err != nil {
			return ProviderExecutionResult{}, executionError(provider.CodeProviderUnavailable)
		}
	} else if recovery, ok := planned.Adapter.(provider.IdempotencyRecovery); ok && (saved.Execution.Claim != nil || !saved.Plan.RequiresReservation()) {
		outcome, err = recovery.RecoverByIdempotencyKey(callCtx, planned.Call, provider.NopObserver{})
		if err != nil {
			return ProviderExecutionResult{}, executionError(provider.CodeProviderUnavailable)
		}
	} else {
		if stage == cloudstate.ExecutionUnknown {
			return executor.result(saved), nil
		}
		next := saved.Execution
		next.Stage = cloudstate.ExecutionUnknown
		next.Failure = &cloudstate.ExecutionFailure{Code: provider.CodeAmbiguousDispatch, Dispatch: provider.DispatchAmbiguous}
		return executor.save(ctx, scope, id, saved, next)
	}
	return executor.completeCall(ctx, scope, id, saved, planned.Call, transcript, outcome, nil, true)
}

func (executor *CloudProviderExecution) reconstruct(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, plan cloudstate.BudgetPlan, generate durable.GenerateReplay, compact durable.CompactReplay) (PlannedProviderCall, []llm.Item, error) {
	record, err := executor.store.Read(ctx, scope, id)
	if err != nil {
		return PlannedProviderCall{}, nil, cloudRuntimeError(err, false)
	}
	if record.Request.Scope != scope || record.Request.ID != id || record.Request.Kind != plan.Kind {
		return PlannedProviderCall{}, nil, executionError(provider.CodeStateCorrupt)
	}
	linked, err := cloudRequestAttempt(record)
	if err != nil {
		return PlannedProviderCall{}, nil, err
	}
	if linked != nil && plan.Route.OperationID != durable.OperationID(linked.ID) {
		return PlannedProviderCall{}, nil, executionError(provider.CodeStateCorrupt)
	}
	binding := ProviderRecoveryBinding{ConfigDigest: plan.ConfigDigest, ConfigEpoch: plan.ConfigEpoch, RequestDigest: plan.RequestDigest, CandidateID: plan.Estimate.CandidateID,
		Route: plan.Route, Family: plan.Family, CapabilityVersion: plan.CapabilityVersion, ProviderTier: plan.ProviderTier, RequestedClass: plan.RequestedClass, AttemptedClass: plan.AttemptedClass}
	if plan.Kind == "generate" {
		var request llm.GenerateRequestV1
		if json.Unmarshal(record.Request.Manifest, &request) != nil || request.Context.Tenant != scope.Tenant || request.Context.Project != scope.Project {
			return PlannedProviderCall{}, nil, executionError(provider.CodeStateCorrupt)
		}
		prepared, err := PrepareGenerateInput(ctx, request, generate)
		if err != nil {
			return PlannedProviderCall{}, nil, err
		}
		key, err := cloudProviderOperationKey(record, request.OperationKey)
		if err != nil {
			return PlannedProviderCall{}, nil, err
		}
		prepared.Request.OperationKey = key
		binding.OperationKeyDigest = ProviderRecoveryOperationKeyDigest(key)
		planned, err := executor.admission.recovery.Generate(ctx, prepared, binding)
		return planned, prepared.Request.Input, err
	}
	var request llm.CompactRequestV1
	if json.Unmarshal(record.Request.Manifest, &request) != nil || request.Context.Tenant != scope.Tenant || request.Context.Project != scope.Project {
		return PlannedProviderCall{}, nil, executionError(provider.CodeStateCorrupt)
	}
	prepared, err := PrepareCompactInput(ctx, request, compact)
	if err != nil {
		return PlannedProviderCall{}, nil, err
	}
	key, err := cloudProviderOperationKey(record, request.OperationKey)
	if err != nil {
		return PlannedProviderCall{}, nil, err
	}
	if prepared.Request != nil {
		prepared.Request.OperationKey = key
	}
	binding.OperationKeyDigest = ProviderRecoveryOperationKeyDigest(key)
	planned, err := executor.admission.recovery.Compact(ctx, prepared, binding)
	return planned, nil, err
}

type executionObserver struct {
	executor *CloudProviderExecution
	scope    cloudstate.Scope
	id       cloudstate.RequestID
	saved    cloudstate.SavedProviderExecution
	claim    *durable.ClaimReceipt
	marked   bool
	saveErr  error
}

func (observer *executionObserver) BeforePossibleWrite(ctx context.Context) error {
	if observer.marked || observer.saveErr != nil {
		return executionError(provider.CodeAmbiguousDispatch)
	}
	next := observer.saved.Execution
	next.Stage = cloudstate.ExecutionSubmitting
	next.Claim = observer.claim
	result, err := observer.executor.save(ctx, observer.scope, observer.id, observer.saved, next)
	if err != nil {
		observer.saveErr = err
		return err
	}
	observer.saved, observer.marked = result.Saved, true
	return nil
}
func (*executionObserver) AfterResponseHeaders(context.Context, provider.ResponseMetadata) error {
	return nil
}
func (*executionObserver) OnProgress(context.Context, provider.Progress) {}

func (executor *CloudProviderExecution) completeCall(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, saved cloudstate.SavedProviderExecution, call provider.Call, transcript []llm.Item, outcome provider.ResumableResult, callErr error, polling bool) (ProviderExecutionResult, error) {
	next := saved.Execution
	next.PollAfter, next.Failure = time.Time{}, nil
	if callErr == nil {
		callErr = outcome.ValidateForCall(call)
		if callErr == nil && outcome.State == provider.ResumableCompleted {
			if _, err := outcome.Result.Response.MarshalJSON(); err != nil {
				callErr = provider.NewError(provider.CodeProviderInvalidResponse, provider.PhaseLift, provider.DispatchAccepted, provider.RetryNever, "provider response cannot be saved")
			}
		}
		if saved.Execution.ProviderOperationID != "" && outcome.State != provider.ResumableNotFound && outcome.ProviderOperationID != saved.Execution.ProviderOperationID {
			callErr = executionError(provider.CodeProviderInvalidResponse)
		}
	}
	if callErr != nil {
		if polling {
			return ProviderExecutionResult{}, executionError(provider.CodeProviderInvalidResponse)
		}
		next.Stage = cloudstate.ExecutionUnknown
		next.Failure = &cloudstate.ExecutionFailure{Code: provider.CodeAmbiguousDispatch, Dispatch: provider.DispatchAmbiguous}
		var classified *provider.Error
		// A one-shot Invoke always reports a completed outcome. A resumable
		// submission that failed may still have a running job to recover.
		received := outcome.State == provider.ResumableCompleted
		if errors.As(callErr, &classified) && classified.Code.Valid() && (classified.Dispatch == provider.DispatchRejected || classified.Dispatch == provider.DispatchNotDispatched || (received && acceptedInvalidResponse(classified))) {
			next.Stage = cloudstate.ExecutionFailed
			next.Failure = executionFailure(classified.Code, classified.Dispatch, classified, executor.clock())
		}
	} else {
		if outcome.ProviderOperationID != "" {
			next.ProviderOperationID = outcome.ProviderOperationID
		}
		switch outcome.State {
		case provider.ResumablePending:
			next.Stage = cloudstate.ExecutionPending
			delay := outcome.NextPollAfter
			if delay < time.Second {
				delay = time.Second
			}
			if delay > time.Minute {
				delay = time.Minute
			}
			next.PollAfter = executor.clock().Add(delay).UTC()
		case provider.ResumableCompleted:
			next.Stage, next.Response = cloudstate.ExecutionSucceeded, &outcome.Result.Response
			if saved.Plan.Kind == "generate" {
				next.Response.Output = uniqueToolCallIDs(saved.Plan.Route.OperationID, transcript, next.Response.Output)
			}
			priceExecutionResponse(saved.Plan, next.Response)
		case provider.ResumableFailed:
			next.Stage = cloudstate.ExecutionFailed
			code := outcome.Failure.Code
			if !code.Valid() {
				code = provider.CodeProviderUnavailable
			}
			next.Failure = executionFailure(code, outcome.Dispatch, outcome.Failure, executor.clock())
		case provider.ResumableNotFound:
			next.Stage = cloudstate.ExecutionUnknown
			next.Failure = &cloudstate.ExecutionFailure{Code: provider.CodeAmbiguousDispatch, Dispatch: provider.DispatchAmbiguous}
		}
	}
	next.UpdatedAt = executor.clock().UTC()
	if next.Stage == cloudstate.ExecutionSucceeded || next.Stage == cloudstate.ExecutionFailed {
		next.CompletedAt = next.UpdatedAt
		if saved.Plan.RequiresReservation() {
			next.Settlement = executionSettlement(next)
		} else {
			next.Settled = true
		}
	}
	result, err := executor.save(ctx, scope, id, saved, next)
	if err != nil {
		return result, err
	}
	return executor.settle(ctx, scope, id, result.Saved)
}

// refusedDispatch loads the durable state left by a failed pre-HTTP marker
// save: either the untouched claiming record or this caller's own applied
// marker. Anything else was advanced elsewhere and is left to Resume.
func (executor *CloudProviderExecution) refusedDispatch(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, saved cloudstate.SavedProviderExecution) (cloudstate.SavedProviderExecution, bool) {
	loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), executionSaveAttemptTimeout)
	defer cancel()
	current, err := executor.store.LoadProviderExecution(loadCtx, scope, id)
	if err != nil || current.Execution.StartToken != saved.Execution.StartToken {
		return cloudstate.SavedProviderExecution{}, false
	}
	untouched := current.Execution.Revision == saved.Execution.Revision && current.Execution.Stage == cloudstate.ExecutionClaiming
	marker := current.Execution.Revision == saved.Execution.Revision+1 && current.Execution.Stage == cloudstate.ExecutionSubmitting
	return current, untouched || marker
}

// save writes one revision. A provider outcome exists only in memory, so a
// transient storage failure is retried with the identical revision under a
// bounded context detached from the Activity. A retry is a compare-and-set of
// the same content: it cannot dispatch, and it treats an earlier attempt whose
// acknowledgement was lost as success.
func (executor *CloudProviderExecution) save(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, saved cloudstate.SavedProviderExecution, next cloudstate.ProviderExecution) (ProviderExecutionResult, error) {
	finalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), executionSaveBudget)
	defer cancel()
	return executor.saveWithin(finalCtx, scope, id, saved, next)
}

// saveWithin is save under a caller-supplied detached context, whose deadline
// bounds every attempt and backoff; settlement passes its finalization context
// so server.finalization_timeout bounds the whole settlement path.
func (executor *CloudProviderExecution) saveWithin(finalCtx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, saved cloudstate.SavedProviderExecution, next cloudstate.ProviderExecution) (ProviderExecutionResult, error) {
	next.Revision = saved.Execution.Revision + 1
	if now := executor.clock().UTC(); now.After(next.UpdatedAt) {
		next.UpdatedAt = now
	}
	err := executor.saveOnce(finalCtx, scope, id, saved.Execution.Revision, next, false)
	for _, delay := range executor.saveBackoff {
		if err == nil || !retryableExecutionSave(err) {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-finalCtx.Done():
			timer.Stop()
		case <-timer.C:
		}
		if finalCtx.Err() != nil {
			break
		}
		err = executor.saveOnce(finalCtx, scope, id, saved.Execution.Revision, next, true)
	}
	if err != nil {
		// Concurrent polls or settlement acknowledgements may advance this same
		// execution. Retry observation through the durable fence; this is not a
		// conflicting public request. The pre-HTTP write must still fail closed.
		if errors.Is(err, contracts.ErrConflict) && next.Stage != cloudstate.ExecutionSubmitting {
			return ProviderExecutionResult{}, executionError(provider.CodeStateUnavailable)
		}
		return ProviderExecutionResult{}, cloudRuntimeError(err, true)
	}
	saved.Execution = next
	return executor.result(saved), nil
}

func (executor *CloudProviderExecution) saveOnce(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, previous uint64, next cloudstate.ProviderExecution, retried bool) error {
	attemptCtx, cancel := context.WithTimeout(ctx, executionSaveAttemptTimeout)
	defer cancel()
	err := executor.store.SaveProviderExecution(attemptCtx, scope, id, previous, next)
	if !retried || !errors.Is(err, contracts.ErrConflict) {
		return err
	}
	// An earlier attempt may have been applied without an acknowledgement.
	// Stored progress is re-encoded, so compare canonical forms after a reload
	// rather than relying on the repository's byte comparison.
	current, loadErr := executor.store.LoadProviderExecution(attemptCtx, scope, id)
	if loadErr != nil {
		return loadErr
	}
	if sameExecution(current.Execution, next) {
		return nil
	}
	return err
}

func retryableExecutionSave(err error) bool {
	for _, permanent := range []error{contracts.ErrConflict, contracts.ErrNotFound, cloudstate.ErrInvalid, cloudstate.ErrCorrupt, cloudstate.ErrProviderExecutionMissing, cloudstate.ErrBudgetPlanMissing} {
		if errors.Is(err, permanent) {
			return false
		}
	}
	return true
}

func sameExecution(left, right cloudstate.ProviderExecution) bool {
	canonical := func(execution cloudstate.ProviderExecution) []byte {
		data, err := json.Marshal(execution)
		if err != nil {
			return nil
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		var tree any
		if decoder.Decode(&tree) != nil {
			return nil
		}
		// Encoding a decoded tree sorts every object's keys.
		data, _ = json.Marshal(tree)
		return data
	}
	l, r := canonical(left), canonical(right)
	return l != nil && bytes.Equal(l, r)
}

func (executor *CloudProviderExecution) settle(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, saved cloudstate.SavedProviderExecution) (ProviderExecutionResult, error) {
	executor.recordRouteStatus(ctx, saved)
	if saved.Execution.Settlement == nil || saved.Execution.Settled {
		return executor.result(saved), nil
	}
	timeout := executor.finalizationTimeout
	if timeout <= 0 {
		timeout = defaultCloudFinalizationTimeout
	}
	finalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	// Persisted response + exact event batch precede Redis settlement. A lost
	// reply only retries this identical batch; it cannot poll/submit/refund twice.
	if err := executor.admission.admit.boundary.Materializer.Reconcile(finalCtx, *saved.Execution.Settlement); err != nil {
		return ProviderExecutionResult{}, executionError(provider.CodeStateUnavailable)
	}
	next := saved.Execution
	next.Settled = true
	return executor.saveWithin(finalCtx, scope, id, saved, next)
}

func (executor *CloudProviderExecution) result(saved cloudstate.SavedProviderExecution) ProviderExecutionResult {
	result := ProviderExecutionResult{Saved: saved}
	until := saved.Execution.PollAfter
	if saved.Execution.Stage == cloudstate.ExecutionClaiming || saved.Execution.Stage == cloudstate.ExecutionSubmitting {
		// Another caller may finish submission at any moment. Recheck saved
		// progress promptly without changing the deadline that permits recovery.
		until = minExecutionObservationTime(executor.clock(), saved.Execution.RecoverAfter)
	}
	if !until.IsZero() {
		result.RetryAfter = until.Sub(executor.clock())
		if result.RetryAfter < time.Second {
			result.RetryAfter = time.Second
		}
	}
	return result
}

func minExecutionObservationTime(now, recoverAfter time.Time) time.Time {
	if next := now.Add(5 * time.Second); next.Before(recoverAfter) {
		return next
	}
	return recoverAfter
}

// uniqueToolCallIDs keeps tool-call IDs unique for a lineage. Providers that
// number calls from zero in every response reuse the ID of an earlier,
// already resolved call; the saved response replaces such an ID, and any
// result for it in the same output, before settlement so the paid output can
// be published and continued. The replacement depends only on the attempt and
// the provider's ID, so a repeated poll saves the same response. Two calls
// sharing an ID within one response are ambiguous: they still share one ID
// afterwards and the output stays invalid.
func uniqueToolCallIDs(operation durable.OperationID, transcript, output []llm.Item) []llm.Item {
	lineage := make(map[string]struct{})
	for _, item := range transcript {
		if call, ok := toolCallValue(item); ok {
			lineage[call.ID] = struct{}{}
		}
	}
	used := make(map[string]struct{}, len(lineage))
	for id := range lineage {
		used[id] = struct{}{}
	}
	var reused []string
	for _, item := range output {
		call, ok := toolCallValue(item)
		if !ok {
			continue
		}
		if _, exists := lineage[call.ID]; exists && call.ID != "" {
			reused = append(reused, call.ID)
		}
		used[call.ID] = struct{}{}
	}
	if len(reused) == 0 {
		return output
	}
	renamed := make(map[string]string, len(reused))
	for _, id := range reused {
		if _, done := renamed[id]; done {
			continue
		}
		for salt := 0; ; salt++ {
			digest := sha256.Sum256([]byte(fmt.Sprintf("tool-call-id-v1/%s/%d/%s", operation, salt, id)))
			// 37 characters of [a-z0-9_] fit every provider's tool-call ID rules.
			candidate := "call_" + hex.EncodeToString(digest[:16])
			if _, taken := used[candidate]; !taken {
				renamed[id], used[candidate] = candidate, struct{}{}
				break
			}
		}
	}
	result := make([]llm.Item, len(output))
	for index, item := range output {
		if call, ok := toolCallValue(item); ok {
			if id, found := renamed[call.ID]; found {
				call.ID = id
			}
			item = call
		} else if value, ok := item.(llm.ToolResult); ok {
			if id, found := renamed[value.CallID]; found {
				value.CallID = id
			}
			item = value
		} else if value, ok := item.(*llm.ToolResult); ok && value != nil {
			detached := *value
			if id, found := renamed[detached.CallID]; found {
				detached.CallID = id
			}
			item = detached
		}
		result[index] = item
	}
	return result
}

func toolCallValue(item llm.Item) (llm.ToolCall, bool) {
	switch value := item.(type) {
	case llm.ToolCall:
		return value, true
	case *llm.ToolCall:
		if value != nil {
			return *value, true
		}
	}
	return llm.ToolCall{}, false
}

func executionError(code provider.Code) error {
	return provider.NewError(code, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetrySameOperation, "provider execution unavailable")
}

func priceExecutionResponse(plan cloudstate.BudgetPlan, response *llm.Response) {
	response.OperationID = string(plan.Route.OperationID)
	if plan.RequiresReservation() {
		response.Cost.ReservedCostUSD = &plan.Estimate.CostUSD
	} else {
		response.Cost.ReservedCostUSD = nil
	}
	response.Cost.CatalogVersion = plan.Quote.Entry.Version
	if response.Cost.Status == llm.CostStatusUnknown {
		response.Cost.ActualCostUSD = nil
		return
	}
	// A provider-reported amount is not promoted to a known cost without a
	// catalog quote (docs/architecture/pricing-and-budgets.md).
	if plan.Unpriced {
		response.Cost.Status, response.Cost.ActualCostUSD = llm.CostStatusUnknown, nil
		return
	}
	if response.Cost.ActualCostUSD != nil && response.Cost.ActualCostUSD.Validate() == nil {
		response.Cost.Status, response.Cost.Method = llm.CostStatusKnown, string(pricing.CostProviderReported)
		return
	}
	usage := response.Usage
	// A provider call always consumes input tokens. All-zero usage means the
	// provider omitted usage, so pricing it would settle a near-zero exact
	// cost and release the reservation; keep the cost unknown instead.
	if usage.InputTokens == 0 && usage.OutputTokens == 0 && usage.ReasoningTokens == 0 && usage.CacheReadTokens == 0 && usage.CacheWriteTokens == 0 {
		response.Cost.Status, response.Cost.ActualCostUSD = llm.CostStatusUnknown, nil
		return
	}
	cost, err := pricing.CostFromUsage(plan.Quote.Entry, pricing.Usage{InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens,
		ReasoningTokens: usage.ReasoningTokens, CacheReadTokens: usage.CacheReadTokens, CacheWriteTokens: usage.CacheWriteTokens})
	if err != nil || response.Cost.Method != "" {
		response.Cost.Status, response.Cost.ActualCostUSD = llm.CostStatusUnknown, nil
		return
	}
	response.Cost.Status, response.Cost.ActualCostUSD, response.Cost.Method = llm.CostStatusKnown, &cost.USD, string(cost.Method)
}

func executionSettlement(execution cloudstate.ProviderExecution) *durable.ReconcileRequest {
	if execution.Claim == nil {
		return nil
	}
	var actual *pricing.USD
	if execution.Response != nil {
		actual = execution.Response.Cost.ActualCostUSD
	}
	if execution.Failure != nil && (execution.Failure.Dispatch == provider.DispatchRejected || execution.Failure.Dispatch == provider.DispatchNotDispatched) {
		zero := pricing.MustUSD("0")
		actual = &zero
	}
	reservation := execution.Reservation
	result := &durable.ReconcileRequest{OperationID: reservation.OperationID, GenerationID: reservation.GenerationID, IncarnationID: reservation.IncarnationID}
	for _, reserved := range reservation.Events {
		digest := sha256.Sum256([]byte("provider-settlement-v1/" + reserved.EventID))
		event := budget.CompletionEvent{EventID: hex.EncodeToString(digest[:]), OperationID: reserved.OperationID, GenerationID: reserved.GenerationID,
			WindowID: reserved.WindowID, BucketStart: reserved.BucketStart, ReservationRevision: reserved.ReservationRevision + 1,
			ReservedDecreaseUSD: reserved.AmountUSD, AccountedDecreaseUSD: pricing.MustUSD("0"), OccurredAt: execution.UpdatedAt}
		if actual != nil {
			event.Kind, event.CostStatus, event.ActualCostUSD, event.AccountedIncreaseUSD = budget.JournalFinalizeExact, budget.CostExact, actual, *actual
		} else {
			event.Kind, event.CostStatus, event.UnknownReasonCode, event.AccountedIncreaseUSD = budget.JournalFinalizeUnknown, budget.CostUnknown, "provider_cost_unknown", reserved.AmountUSD
		}
		result.Events = append(result.Events, event)
	}
	return result
}

// A response that was received in full and then rejected while lifting has a
// certain outcome: the provider did the work and its output is unusable. It is
// a terminal failure accounted at the reservation, never an unknown outcome
// that stalls and then buys a second provider call.
func acceptedInvalidResponse(failure *provider.Error) bool {
	return failure.Code == provider.CodeProviderInvalidResponse && failure.Phase == provider.PhaseLift &&
		failure.Dispatch == provider.DispatchAccepted && failure.Retry == provider.RetryNever
}

// Only a valid provider retry classification can authorize a new attempt. Raw
// errors remain outcome_unknown; they never become a free or immediate retry.
func executionFailure(code provider.Code, dispatch provider.DispatchCertainty, failure *provider.Error, now time.Time) *cloudstate.ExecutionFailure {
	result := &cloudstate.ExecutionFailure{Code: code, Dispatch: dispatch}
	if preDispatchContextEnded(failure) {
		// The worker's own context ended (shutdown or Activity deadline) before
		// any provider write. Nothing was sent, so this is a zero-cost retryable
		// failure. RetryNever on the provider error only forbids route fallback
		// within this attempt; it must not make the request durably terminal.
		result.Retryable, result.RetryNotBefore = true, now.UTC().Add(time.Second)
		return result
	}
	if failure != nil && failure.Retry.Valid() && failure.Retry != provider.RetryNever {
		delay := failure.RetryAfter
		if delay < time.Second {
			delay = time.Second
		}
		if delay > 24*time.Hour {
			delay = 24 * time.Hour
		}
		result.Retryable, result.RetryNotBefore = true, now.UTC().Add(delay)
	}
	return result
}

func preDispatchContextEnded(failure *provider.Error) bool {
	return failure != nil && failure.Dispatch == provider.DispatchNotDispatched &&
		(failure.Code == provider.CodeCanceled || failure.Code == provider.CodeDeadlineExceeded) &&
		errors.Is(failure, provider.ErrProviderPreDispatch)
}
