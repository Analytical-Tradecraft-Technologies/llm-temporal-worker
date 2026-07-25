package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

// invokeAttempt dispatches a provider call through the optional resumable
// port. The third return value means that the operation is already durable in
// provider_pending and the caller must return the polling error without
// transitioning the operation to a terminal state.
func (engine *Engine) invokeAttempt(ctx context.Context, operation admission.Operation, candidate routing.Candidate, adapter provider.Adapter, call provider.Call, observer *dispatchObserver) (provider.Result, error, bool) {
	resumable, ok := adapter.(provider.ResumableAdapter)
	if !ok {
		result, err := adapter.Invoke(ctx, call, observer)
		return result, err, false
	}
	outcome, err := resumable.Submit(ctx, call, observer)
	if err != nil {
		// A Submit failure after the possible-write boundary may still have
		// created provider-owned work. Providers that document an idempotency
		// lookup get exactly one recovery attempt; all other adapters remain
		// fail-closed and the caller records an ambiguous dispatch.
		mapped := classifyProviderError(err, observer != nil && observer.marked)
		if mapped.Dispatch == provider.DispatchAccepted || mapped.Dispatch == provider.DispatchAmbiguous {
			if recovery, recoverable := adapter.(provider.IdempotencyRecovery); recoverable {
				recovered, recoveryErr := recovery.RecoverByIdempotencyKey(ctx, call, observer)
				if recoveryErr != nil {
					mapped.Cause = recoveryErr
					return provider.Result{}, mapped, false
				}
				if validationErr := recovered.ValidateForCall(call); validationErr != nil {
					invalid := provider.NewError(provider.CodeProviderInvalidResponse, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "provider idempotency recovery response is invalid")
					invalid.Cause = validationErr
					return provider.Result{}, invalid, false
				}
				return engine.handleResumableOutcome(ctx, operation, candidate, call, observer, resumable, recovered)
			}
		}
		return provider.Result{}, err, false
	}
	if err := outcome.ValidateForCall(call); err != nil {
		mapped := provider.NewError(provider.CodeProviderInvalidResponse, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "resumable provider response is invalid")
		mapped.Cause = err
		return provider.Result{}, mapped, false
	}
	return engine.handleResumableOutcome(ctx, operation, candidate, call, observer, resumable, outcome)
}

func (engine *Engine) handleResumableOutcome(ctx context.Context, operation admission.Operation, candidate routing.Candidate, call provider.Call, observer *dispatchObserver, resumable provider.ResumableAdapter, outcome provider.ResumableResult) (provider.Result, error, bool) {
	switch outcome.State {
	case provider.ResumableCompleted:
		return outcome.Result, nil, false
	case provider.ResumableFailed:
		return provider.Result{}, outcome.Failure, false
	case provider.ResumableNotFound:
		return provider.Result{}, provider.NewError(provider.CodeAmbiguousDispatch, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "provider operation could not be found"), false
	case provider.ResumablePending:
		// The adapter contract requires a possible-write boundary before an
		// accepted result. Without it there is no safe way to persist the id.
		if observer == nil || !observer.marked {
			return provider.Result{}, engineError(provider.CodeAmbiguousDispatch, provider.PhaseDispatch, provider.DispatchAccepted, provider.RetryNever, "resumable provider accepted work before dispatch was recorded", nil), false
		}
		store, ok := engine.dependencies.Admission.(admission.ProviderPendingStore)
		if !ok {
			return provider.Result{}, engineError(provider.CodeStateUnavailable, provider.PhaseFinalize, provider.DispatchAccepted, provider.RetryNever, "provider operation repository is unavailable", nil), false
		}
		if err := store.MarkProviderPending(ctx, admission.ProviderPendingRequest{
			OperationID: operation.ID, DispatchToken: operation.DispatchToken,
			ProviderOperationID: outcome.ProviderOperationID,
			EndpointID:          candidate.EndpointID, Provider: candidate.Provider,
			PollAfter: providerPollAfter(engine.dependencies.Clock, outcome.NextPollAfter),
		}); err != nil {
			return provider.Result{}, engineError(provider.CodeStateUnavailable, provider.PhaseFinalize, provider.DispatchAccepted, provider.RetryNever, "provider operation persistence failed", err), false
		}
		recordPendingPoll(ctx, "started")
		result, pollErr := PollProviderOperation(ctx, resumable, call, outcome.ProviderOperationID, observer, ProviderPollOptions{InitialDelay: outcome.NextPollAfter})
		if pollErr != nil {
			mapped := pendingPollError(pollErr)
			recordPendingPoll(ctx, pollOutcome(mapped))
			if mapped.Retry != provider.RetrySameOperation && mapped.Code != provider.CodeCanceled {
				// A terminal poll outcome (for example provider not-found or a
				// provider-declared failure) must pass through the normal ledger
				// finalization path rather than strand the reservation in
				// provider_pending.
				return provider.Result{}, mapped, false
			}
			return provider.Result{}, mapped, true
		}
		recordPendingPoll(ctx, "completed")
		return result, nil, false
	default:
		return provider.Result{}, fmt.Errorf("resumable provider state %q was not handled", outcome.State), false
	}
}

func providerPollAfter(now func() time.Time, delay time.Duration) time.Time {
	if delay <= 0 || now == nil {
		return time.Time{}
	}
	return now().Add(delay)
}

func (engine *Engine) resumeProviderPending(ctx context.Context, request, providerRequest llm.Request, snapshot Snapshot, quoted quotedPlan, operation admission.Operation, parent *state.Continuation) (llm.Response, error) {
	store, ok := engine.dependencies.Admission.(admission.ProviderPendingStore)
	if !ok {
		return llm.Response{}, engineError(provider.CodeStateUnavailable, provider.PhaseStateLoad, provider.DispatchAccepted, provider.RetrySameOperation, "provider operation repository is unavailable", nil)
	}
	providerOperationID, err := store.ProviderOperation(ctx, operation.ID)
	if err != nil {
		return llm.Response{}, engineError(provider.CodeStateUnavailable, provider.PhaseStateLoad, provider.DispatchAccepted, provider.RetrySameOperation, "provider operation could not be loaded", err)
	}
	initialDelay := time.Duration(0)
	if schedule, ok := engine.dependencies.Admission.(admission.ProviderPendingSchedule); ok {
		pollAfter, scheduleErr := schedule.ProviderPollAfter(ctx, operation.ID)
		if scheduleErr != nil {
			return llm.Response{}, engineError(provider.CodeStateUnavailable, provider.PhaseStateLoad, provider.DispatchAccepted, provider.RetrySameOperation, "provider poll schedule could not be loaded", scheduleErr)
		}
		if now := engine.dependencies.Clock(); !pollAfter.IsZero() && pollAfter.After(now) {
			initialDelay = pollAfter.Sub(now)
		}
	}
	index := -1
	for i, candidate := range quoted.candidates {
		if candidate.candidate.EndpointID == operation.Attempt.EndpointID {
			index = i
			break
		}
	}
	if index < 0 {
		return llm.Response{}, engineError(provider.CodeStateCorrupt, provider.PhasePlan, provider.DispatchAccepted, provider.RetryNever, "provider pending endpoint is not in the current route plan", nil)
	}
	candidate := quoted.candidates[index]
	adapter, err := engine.dependencies.Adapters.Adapter(ctx, candidate.candidate)
	if err != nil {
		return llm.Response{}, engineError(provider.CodeStateUnavailable, provider.PhaseCompile, provider.DispatchAccepted, provider.RetrySameOperation, "provider pending adapter is unavailable", err)
	}
	resumable, ok := adapter.(provider.ResumableAdapter)
	if !ok {
		return llm.Response{}, engineError(provider.CodeStateCorrupt, provider.PhasePoll, provider.DispatchAccepted, provider.RetryNever, "provider pending adapter is not resumable", nil)
	}
	query := provider.CapabilityQuery{EndpointID: candidate.candidate.EndpointID, Family: provider.Family(candidate.candidate.Family), Model: candidate.candidate.Model, ServiceClass: candidate.candidate.AttemptedClass}
	capability, err := adapter.Capabilities(ctx, query)
	if err != nil {
		return llm.Response{}, engineError(provider.CodeStateUnavailable, provider.PhaseCompile, provider.DispatchAccepted, provider.RetrySameOperation, "provider pending capabilities are unavailable", err)
	}
	call, err := adapter.Compile(ctx, provider.CompileInput{
		Request: providerRequest, Query: query, Capability: capability,
		Strict:   request.Portability != llm.PortabilityBestEffort,
		Metadata: provider.CallMetadata{SchemaDigest: mustRequestDigest(request), CapabilityVersion: candidate.candidate.CapabilityVersion, ProviderTier: candidate.candidate.ProviderTier, OpaqueStateRequired: request.Continuation != nil},
	})
	if err != nil {
		return llm.Response{}, engineError(provider.CodeStateUnavailable, provider.PhaseCompile, provider.DispatchAccepted, provider.RetrySameOperation, "provider pending call could not be compiled", err)
	}
	lease := snapshot.ReservationLease
	if lease <= 0 {
		lease = defaultLease
	}
	attempt := operation.Attempt.AttemptNumber
	if attempt <= 0 {
		attempt = index + 1
	}
	observer := &dispatchObserver{engine: engine, operation: operation, candidate: candidate.candidate, attempt: attempt, leaseUntil: engine.dependencies.Clock().Add(lease), marked: true}
	if err := engine.beat(ctx, Progress{OperationID: operation.ID, Phase: "provider_wait", RouteIndex: candidate.candidate.RouteIndex, ClassIndex: candidate.candidate.FallbackIndex, At: engine.dependencies.Clock()}); err != nil {
		return llm.Response{}, err
	}
	recordPendingPoll(ctx, "started")
	result, pollErr := PollProviderOperation(ctx, resumable, call, providerOperationID, observer, ProviderPollOptions{InitialDelay: initialDelay})
	if pollErr != nil {
		mapped := pendingPollError(pollErr)
		recordPendingPoll(ctx, pollOutcome(mapped))
		engine.recordProviderStatus(ctx, snapshot, operation, candidate.candidate, provider.Result{}, mapped)
		if mapped.Retry == provider.RetrySameOperation || mapped.Code == provider.CodeCanceled {
			mapped.OperationID = operation.ID
			return llm.Response{}, mapped
		}
		return llm.Response{}, engine.finishFailed(ctx, operation, candidate.candidate, mapped, 0)
	}
	recordPendingPoll(ctx, "completed")
	engine.recordProviderStatus(ctx, snapshot, operation, candidate.candidate, result, nil)
	return engine.finalizeSuccess(ctx, request, snapshot, quoted, index, operation, parent, call, result.Response)
}

func recordPendingPoll(ctx context.Context, outcome string) {
	if metrics := observability.MetricsFromContext(ctx); metrics != nil {
		metrics.RecordPendingPoll(outcome)
	}
}

func pollOutcome(err *provider.Error) string {
	if err == nil {
		return "failed"
	}
	if err.Retry == provider.RetrySameOperation || err.Code == provider.CodeCanceled {
		return "retry"
	}
	return "failed"
}

// pendingPollError preserves retry guidance while an operation is already
// durable. classifyProviderError intentionally turns accepted dispatch errors
// into terminal ambiguity for new submissions; doing that for a poll would
// discard safe retry of the same provider-owned operation.
func pendingPollError(err error) *provider.Error {
	var mapped *provider.Error
	if errors.As(err, &mapped) {
		copy := *mapped
		if copy.Retry == provider.RetrySameOperation || copy.Code == provider.CodeCanceled {
			return &copy
		}
		return &copy
	}
	return engineError(provider.CodeStateUnavailable, provider.PhasePoll, provider.DispatchAccepted, provider.RetrySameOperation, "provider poll failed", err)
}
