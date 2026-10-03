package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/budget"
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
	store     CloudProviderExecutionStore
	admission *CloudBudgetAdmission
	clock     func() time.Time
}

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
	return &CloudProviderExecution{store: store, admission: admission, clock: clock}, nil
}

// Submit accepts only a prepared local call and its accepted reservation. The
// durable fence is written before Claim, and BeforePossibleWrite persists the
// claim before allowing HTTP. A saved claim is never reused as a dispatch grant.
func (executor *CloudProviderExecution) Submit(ctx context.Context, call *CloudBudgetCall, reservation durable.ReserveResult) (ProviderExecutionResult, error) {
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
	if observer.saveErr != nil {
		return ProviderExecutionResult{}, cloudRuntimeError(observer.saveErr, true)
	}
	saved = observer.saved
	// A preflight failure may precede the observer. Retain the fresh claim so
	// an explicitly proven pre-dispatch failure can be settled without another
	// HTTP call. A successful adapter must have crossed the durable boundary.
	if !observer.marked {
		if err == nil {
			err = executionError(provider.CodeProviderInvalidResponse)
		}
		saved.Execution.Claim = claim
	}
	return executor.completeCall(ctx, call.scope, call.id, saved, call.provider.Call, outcome, err, false)
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
	planned, err := executor.reconstruct(ctx, scope, id, saved.Plan, generate, compact)
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
	return executor.completeCall(ctx, scope, id, saved, planned.Call, outcome, nil, true)
}

func (executor *CloudProviderExecution) reconstruct(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, plan cloudstate.BudgetPlan, generate durable.GenerateReplay, compact durable.CompactReplay) (PlannedProviderCall, error) {
	record, err := executor.store.Read(ctx, scope, id)
	if err != nil {
		return PlannedProviderCall{}, cloudRuntimeError(err, false)
	}
	if record.Request.Scope != scope || record.Request.ID != id || record.Request.Kind != plan.Kind {
		return PlannedProviderCall{}, executionError(provider.CodeStateCorrupt)
	}
	linked, err := cloudRequestAttempt(record)
	if err != nil {
		return PlannedProviderCall{}, err
	}
	if linked != nil && plan.Route.OperationID != durable.OperationID(linked.ID) {
		return PlannedProviderCall{}, executionError(provider.CodeStateCorrupt)
	}
	binding := ProviderRecoveryBinding{ConfigDigest: plan.ConfigDigest, ConfigEpoch: plan.ConfigEpoch, RequestDigest: plan.RequestDigest, CandidateID: plan.Estimate.CandidateID,
		Route: plan.Route, Family: plan.Family, CapabilityVersion: plan.CapabilityVersion, ProviderTier: plan.ProviderTier, RequestedClass: plan.RequestedClass, AttemptedClass: plan.AttemptedClass}
	if plan.Kind == "generate" {
		var request llm.GenerateRequestV1
		if json.Unmarshal(record.Request.Manifest, &request) != nil || request.Context.Tenant != scope.Tenant || request.Context.Project != scope.Project {
			return PlannedProviderCall{}, executionError(provider.CodeStateCorrupt)
		}
		prepared, err := PrepareGenerateInput(ctx, request, generate)
		if err != nil {
			return PlannedProviderCall{}, err
		}
		key, err := cloudProviderOperationKey(record, request.OperationKey)
		if err != nil {
			return PlannedProviderCall{}, err
		}
		prepared.Request.OperationKey = key
		binding.OperationKeyDigest = ProviderRecoveryOperationKeyDigest(key)
		return executor.admission.recovery.Generate(ctx, prepared, binding)
	}
	var request llm.CompactRequestV1
	if json.Unmarshal(record.Request.Manifest, &request) != nil || request.Context.Tenant != scope.Tenant || request.Context.Project != scope.Project {
		return PlannedProviderCall{}, executionError(provider.CodeStateCorrupt)
	}
	prepared, err := PrepareCompactInput(ctx, request, compact)
	if err != nil {
		return PlannedProviderCall{}, err
	}
	key, err := cloudProviderOperationKey(record, request.OperationKey)
	if err != nil {
		return PlannedProviderCall{}, err
	}
	if prepared.Request != nil {
		prepared.Request.OperationKey = key
	}
	binding.OperationKeyDigest = ProviderRecoveryOperationKeyDigest(key)
	return executor.admission.recovery.Compact(ctx, prepared, binding)
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

func (executor *CloudProviderExecution) completeCall(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, saved cloudstate.SavedProviderExecution, call provider.Call, outcome provider.ResumableResult, callErr error, polling bool) (ProviderExecutionResult, error) {
	next := saved.Execution
	next.PollAfter, next.Failure = time.Time{}, nil
	if callErr == nil {
		callErr = outcome.ValidateForCall(call)
		if callErr == nil && outcome.State == provider.ResumableCompleted {
			_, callErr = outcome.Result.Response.MarshalJSON()
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
		if errors.As(callErr, &classified) && classified.Code.Valid() && (classified.Dispatch == provider.DispatchRejected || classified.Dispatch == provider.DispatchNotDispatched) {
			next.Stage = cloudstate.ExecutionFailed
			next.Failure = &cloudstate.ExecutionFailure{Code: classified.Code, Dispatch: classified.Dispatch}
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
			priceExecutionResponse(saved.Plan, next.Response)
		case provider.ResumableFailed:
			next.Stage = cloudstate.ExecutionFailed
			code := outcome.Failure.Code
			if !code.Valid() {
				code = provider.CodeProviderUnavailable
			}
			next.Failure = &cloudstate.ExecutionFailure{Code: code, Dispatch: outcome.Dispatch}
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

func (executor *CloudProviderExecution) save(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, saved cloudstate.SavedProviderExecution, next cloudstate.ProviderExecution) (ProviderExecutionResult, error) {
	next.Revision = saved.Execution.Revision + 1
	if now := executor.clock().UTC(); now.After(next.UpdatedAt) {
		next.UpdatedAt = now
	}
	finalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := executor.store.SaveProviderExecution(finalCtx, scope, id, saved.Execution.Revision, next); err != nil {
		return ProviderExecutionResult{}, cloudRuntimeError(err, true)
	}
	saved.Execution = next
	return executor.result(saved), nil
}

func (executor *CloudProviderExecution) settle(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, saved cloudstate.SavedProviderExecution) (ProviderExecutionResult, error) {
	if saved.Execution.Settlement == nil || saved.Execution.Settled {
		return executor.result(saved), nil
	}
	finalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	// Persisted response + exact event batch precede Redis settlement. A lost
	// reply only retries this identical batch; it cannot poll/submit/refund twice.
	if err := executor.admission.admit.boundary.Materializer.Reconcile(finalCtx, *saved.Execution.Settlement); err != nil {
		return ProviderExecutionResult{}, executionError(provider.CodeStateUnavailable)
	}
	next := saved.Execution
	next.Settled = true
	return executor.save(finalCtx, scope, id, saved, next)
}

func (executor *CloudProviderExecution) result(saved cloudstate.SavedProviderExecution) ProviderExecutionResult {
	result := ProviderExecutionResult{Saved: saved}
	until := saved.Execution.PollAfter
	if saved.Execution.Stage == cloudstate.ExecutionClaiming || saved.Execution.Stage == cloudstate.ExecutionSubmitting {
		until = saved.Execution.RecoverAfter
	}
	if !until.IsZero() {
		result.RetryAfter = until.Sub(executor.clock())
		if result.RetryAfter < time.Second {
			result.RetryAfter = time.Second
		}
	}
	return result
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
	if response.Cost.ActualCostUSD != nil && response.Cost.ActualCostUSD.Validate() == nil {
		response.Cost.Status, response.Cost.Method = llm.CostStatusKnown, string(pricing.CostProviderReported)
		return
	}
	if plan.Unpriced {
		response.Cost.Status, response.Cost.ActualCostUSD = llm.CostStatusUnknown, nil
		return
	}
	usage := response.Usage
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
