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
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

type dispatchObserver struct {
	engine       *Engine
	operation    admission.Operation
	candidate    routing.Candidate
	attempt      int
	leaseUntil   time.Time
	streaming    bool
	marked       bool
	heartbeatErr error
}

func (observer *dispatchObserver) BeforePossibleWrite(ctx context.Context) error {
	if observer == nil || observer.engine == nil {
		return fmt.Errorf("dispatch observer is nil")
	}
	if observer.marked {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := observer.engine.dependencies.Admission.MarkDispatching(ctx, admission.DispatchRequest{
		OperationID: observer.operation.ID, DispatchToken: observer.operation.DispatchToken,
		Attempt: admission.AttemptFacts{
			RouteID: observer.candidate.RouteID, EndpointID: observer.candidate.EndpointID,
			Provider: observer.candidate.Provider, ResolvedModel: observer.candidate.Model, ServiceClass: string(observer.candidate.AttemptedClass),
			AttemptNumber: observer.attempt,
		},
		LeaseUntil: observer.leaseUntil,
	})
	if err != nil {
		return err
	}
	observer.marked = true
	recordOperationState(ctx, admission.StateDispatching)
	if err := observer.engine.beat(ctx, Progress{OperationID: observer.operation.ID, Phase: "pre_write", RouteIndex: observer.candidate.RouteIndex, ClassIndex: observer.candidate.FallbackIndex, At: observer.engine.dependencies.Clock()}); err != nil {
		return err
	}
	return nil
}

func (observer *dispatchObserver) AfterResponseHeaders(ctx context.Context, metadata provider.ResponseMetadata) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return observer.engine.beat(ctx, Progress{OperationID: observer.operation.ID, Phase: "response_received", RouteIndex: observer.candidate.RouteIndex, ClassIndex: observer.candidate.FallbackIndex, At: observer.engine.dependencies.Clock()})
}

func (observer *dispatchObserver) OnProgress(ctx context.Context, progress provider.Progress) {
	if observer == nil || observer.engine == nil {
		return
	}
	phase := progress.Phase
	if phase == string(provider.PhaseStream) {
		// An Invoke call is one-shot even if an adapter reports an internal
		// stream phase. Only Engine.Stream owns a token event source, so only
		// that path may expose streaming progress to its heartbeat consumer.
		if !observer.streaming {
			return
		}
		phase = "streaming"
	}
	if err := observer.engine.beat(ctx, Progress{OperationID: observer.operation.ID, Phase: phase, RouteIndex: observer.candidate.RouteIndex, ClassIndex: observer.candidate.FallbackIndex, OutputItems: progress.OutputItems, At: observer.engine.dependencies.Clock()}); err != nil {
		observer.heartbeatErr = err
	}
}

func (engine *Engine) dispatchPlan(ctx context.Context, request, providerRequest llm.Request, snapshot Snapshot, quoted quotedPlan, operation admission.Operation, parent *state.Continuation) (llm.Response, error) {
	if operation.State == admission.StateProviderPending {
		return engine.resumeProviderPending(ctx, request, providerRequest, snapshot, quoted, operation, parent)
	}
	if operation.State == admission.StateDispatching {
		return engine.resumeDispatching(ctx, request, providerRequest, snapshot, quoted, operation, parent)
	}
	for index, candidate := range quoted.candidates {
		if index >= engine.dependencies.MaxAttempts {
			break
		}
		if err := ctx.Err(); err != nil {
			return llm.Response{}, engine.handleCancellation(ctx, operation, candidate.candidate, err)
		}
		adapter, err := engine.dependencies.Adapters.Adapter(ctx, candidate.candidate)
		if err != nil {
			continue
		}
		query := provider.CapabilityQuery{EndpointID: candidate.candidate.EndpointID, Family: provider.Family(candidate.candidate.Family), Model: candidate.candidate.Model, ServiceClass: candidate.candidate.AttemptedClass}
		capability, err := adapter.Capabilities(ctx, query)
		if err != nil {
			continue
		}
		call, err := adapter.Compile(ctx, provider.CompileInput{
			Request: providerRequest, Query: query, Capability: capability, Strict: request.Portability != llm.PortabilityBestEffort,
			Metadata: provider.CallMetadata{SchemaDigest: mustRequestDigest(request), CapabilityVersion: candidate.candidate.CapabilityVersion, ProviderTier: candidate.candidate.ProviderTier, OpaqueStateRequired: request.Continuation != nil},
		})
		if err != nil {
			if mapped, ok := err.(*provider.Error); ok && mapped.Dispatch != provider.DispatchNotDispatched {
				return llm.Response{}, engine.finishFailed(ctx, operation, candidate.candidate, mapped, 0)
			}
			continue
		}
		lease := snapshot.ReservationLease
		if lease <= 0 {
			lease = defaultLease
		}
		observer := &dispatchObserver{engine: engine, operation: operation, candidate: candidate.candidate, attempt: index + 1, leaseUntil: engine.dependencies.Clock().Add(lease)}
		attemptCtx, attemptSpan := engine.startTrace(ctx, "llmtw.provider_attempt", operationTraceAttrs(operation.ID, candidate.candidate)...)
		attemptStarted := time.Now()
		result, invokeErr, providerPending := engine.invokeAttempt(attemptCtx, operation, candidate.candidate, adapter, call, observer)
		var mapped *provider.Error
		if invokeErr != nil {
			mapped = classifyProviderError(invokeErr, observer.marked)
		}
		engine.recordProviderStatus(ctx, snapshot, operation, candidate.candidate, result, mapped)
		if metrics := observability.MetricsFromContext(ctx); metrics != nil {
			outcome := "success"
			if invokeErr != nil {
				outcome = "failure"
			}
			metrics.RecordProviderAttempt(candidate.candidate.EndpointID, candidate.candidate.Model, string(candidate.candidate.AttemptedClass), outcome, time.Since(attemptStarted))
		}
		if invokeErr != nil {
			engine.recordTraceError(attemptCtx, attemptSpan, invokeErr)
		}
		attemptSpan.End()
		if invokeErr != nil && providerPending {
			mapped := pendingPollError(invokeErr)
			mapped.OperationID = operation.ID
			return llm.Response{}, mapped
		}
		if invokeErr == nil {
			if observer.heartbeatErr != nil {
				return llm.Response{}, engine.finishFailed(ctx, operation, candidate.candidate, engineError(provider.CodeStateUnavailable, provider.PhaseFinalize, provider.DispatchAccepted, provider.RetrySameOperation, "progress heartbeat failed", observer.heartbeatErr), candidate.estimate.MicroUSD)
			}
			return engine.finalizeSuccess(ctx, request, snapshot, quoted, index, operation, parent, call, result.Response)
		}
		if mapped == nil {
			mapped = classifyProviderError(invokeErr, observer.marked)
		}
		if !observer.marked && (mapped.Dispatch == provider.DispatchNotDispatched || mapped.Dispatch == provider.DispatchRejected) {
			continue
		}
		certainty := admissionCertainty(mapped.Dispatch)
		if certainty == admission.Accepted || certainty == admission.Ambiguous {
			return llm.Response{}, engine.finishFailed(ctx, operation, candidate.candidate, mapped, candidate.estimate.MicroUSD)
		}
		if mapped.Retry == provider.RetryNever {
			return llm.Response{}, engine.finishFailed(ctx, operation, candidate.candidate, mapped, 0)
		}
		remaining := quoted.candidates[index+1:]
		if len(remaining) == 0 {
			return llm.Response{}, engine.finishFailed(ctx, operation, candidate.candidate, mapped, 0)
		}
		continued, continueErr := engine.continueAfterDefiniteFailure(ctx, operation, candidate.candidate, mapped, remaining, snapshot)
		if continueErr != nil {
			return llm.Response{}, continueErr
		}
		if continued.Denied != nil {
			if metrics := observability.MetricsFromContext(ctx); metrics != nil {
				metrics.RecordBudgetAdmission(continued.Denied.PolicyID, "denied")
			}
			return llm.Response{}, engineError(provider.CodeBudgetDenied, provider.PhaseAdmission, provider.DispatchRejected, provider.RetryAfter, "fallback budget reservation denied", nil)
		}
		recordOperationState(ctx, continued.Operation.State)
		recordAdmission(ctx, continued.Operation.Reservations, "accepted")
		operation = continued.Operation
	}
	return llm.Response{}, engine.finishNoDispatch(ctx, operation, request)
}

func (engine *Engine) continueAfterDefiniteFailure(ctx context.Context, operation admission.Operation, candidate routing.Candidate, failure *provider.Error, remaining []quotedCandidate, snapshot Snapshot) (admission.ContinueResult, error) {
	quoted := quotedPlan{candidates: remaining}
	for _, value := range remaining {
		if value.estimate.MicroUSD > quoted.maximum {
			quoted.maximum = value.estimate.MicroUSD
		}
		if value.estimate.CostUSD.Cmp(quoted.maximumUSD) > 0 {
			quoted.maximumUSD = value.estimate.CostUSD
		}
	}
	lease := snapshot.ReservationLease
	if lease <= 0 {
		lease = defaultLease
	}
	request := admission.ContinueRequest{
		OperationID: operation.ID, DispatchToken: operation.DispatchToken,
		Outcome:   admission.AttemptOutcome{Certainty: admission.Rejected, Incurred: 0, Attempt: admission.AttemptFacts{RouteID: candidate.RouteID, EndpointID: candidate.EndpointID, Provider: candidate.Provider, ResolvedModel: candidate.Model, ServiceClass: string(candidate.AttemptedClass), Dispatch: admission.Rejected}},
		Remaining: quoted.maximum, RemainingUSD: quoted.maximumUSD, Reservations: aggregateReservations(remaining), LeaseUntil: engine.dependencies.Clock().Add(lease), ExpiresAt: operation.ExpiresAt,
	}
	if failure != nil && failure.Provider.RequestID != "" {
		request.Outcome.ProviderRequestID = failure.Provider.RequestID
	}
	result, err := engine.dependencies.Admission.Continue(ctx, request)
	if err != nil {
		return admission.ContinueResult{}, engineError(provider.CodeStateUnavailable, provider.PhaseAdmission, provider.DispatchRejected, provider.RetrySameOperation, "fallback admission transition failed", err)
	}
	return result, nil
}

func (engine *Engine) finishFailed(ctx context.Context, operation admission.Operation, candidate routing.Candidate, failure *provider.Error, incurred pricing.MicroUSD) error {
	if failure == nil {
		failure = engineError(provider.CodeInternal, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "provider request failed", nil)
	}
	attemptNumber := operation.Attempt.AttemptNumber
	if attemptNumber <= 0 {
		attemptNumber = candidate.RouteIndex + 1
	}
	attempt := admission.AttemptFacts{RouteID: candidate.RouteID, EndpointID: candidate.EndpointID, Provider: candidate.Provider, ResolvedModel: candidate.Model, ProviderRequestID: failure.Provider.RequestID, ServiceClass: string(candidate.AttemptedClass), AttemptNumber: attemptNumber}
	attempt.Dispatch = admissionCertainty(failure.Dispatch)
	finalCtx, cancel := engine.finalizationContext(ctx)
	defer cancel()
	err := engine.dependencies.Admission.Fail(finalCtx, admission.FailRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Certainty: attempt.Dispatch, Incurred: incurred, Attempt: attempt, Reason: string(failure.Code)})
	if err != nil {
		return engineError(provider.CodeStateUnavailable, provider.PhaseFinalize, provider.DispatchAccepted, provider.RetrySameOperation, "failed to record provider outcome", err)
	}
	state := admission.StateDefiniteFailed
	if attempt.Dispatch == admission.Accepted || attempt.Dispatch == admission.Ambiguous {
		state = admission.StateAmbiguous
	}
	recordOperationState(ctx, state)
	if state == admission.StateAmbiguous {
		if metrics := observability.MetricsFromContext(ctx); metrics != nil {
			metrics.RecordAmbiguous(candidate.EndpointID)
		}
	}
	failure.OperationID = operation.ID
	return failure
}

func (engine *Engine) finishNoDispatch(ctx context.Context, operation admission.Operation, request llm.Request) error {
	if operation.State == admission.StateReserved {
		err := engine.dependencies.Admission.MarkDispatching(ctx, admission.DispatchRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Attempt: admission.AttemptFacts{ServiceClass: string(request.ServiceClass), Dispatch: admission.NotDispatched, AttemptNumber: 0}, LeaseUntil: engine.dependencies.Clock()})
		if err != nil {
			return engineError(provider.CodeStateUnavailable, provider.PhaseFinalize, provider.DispatchNotDispatched, provider.RetrySameOperation, "failed to close unused reservation", err)
		}
		recordOperationState(ctx, admission.StateDispatching)
	}
	finalCtx, cancel := engine.finalizationContext(ctx)
	defer cancel()
	if err := engine.dependencies.Admission.Fail(finalCtx, admission.FailRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Certainty: admission.NotDispatched, Incurred: 0, Attempt: admission.AttemptFacts{ServiceClass: string(request.ServiceClass), Dispatch: admission.NotDispatched}, Reason: "no eligible compiled adapter"}); err != nil {
		return engineError(provider.CodeStateUnavailable, provider.PhaseFinalize, provider.DispatchNotDispatched, provider.RetrySameOperation, "failed to close unused reservation", err)
	}
	recordOperationState(ctx, admission.StateDefiniteFailed)
	return engineError(provider.CodeNoRoute, provider.PhaseCompile, provider.DispatchNotDispatched, provider.RetryNever, "no candidate could be compiled", nil)
}

func (engine *Engine) handleCancellation(ctx context.Context, operation admission.Operation, candidate routing.Candidate, cause error) error {
	if operation.State == admission.StateReserved {
		finalCtx, cancel := engine.finalizationContext(ctx)
		defer cancel()
		if err := engine.dependencies.Admission.MarkDispatching(finalCtx, admission.DispatchRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Attempt: admission.AttemptFacts{RouteID: candidate.RouteID, EndpointID: candidate.EndpointID, Provider: candidate.Provider, Dispatch: admission.NotDispatched}, LeaseUntil: engine.dependencies.Clock()}); err == nil {
			recordOperationState(ctx, admission.StateDispatching)
		}
		if err := engine.dependencies.Admission.Fail(finalCtx, admission.FailRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Certainty: admission.NotDispatched, Incurred: 0, Attempt: admission.AttemptFacts{RouteID: candidate.RouteID, EndpointID: candidate.EndpointID, Provider: candidate.Provider, Dispatch: admission.NotDispatched}, Reason: "canceled before dispatch"}); err == nil {
			recordOperationState(ctx, admission.StateDefiniteFailed)
		}
		failure := engineError(provider.CodeCanceled, provider.PhaseAdmission, provider.DispatchNotDispatched, provider.RetryNever, "request canceled", cause)
		failure.OperationID = operation.ID
		return failure
	}
	failure := engineError(provider.CodeAmbiguousDispatch, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "request canceled after possible provider write", cause)
	failure.OperationID = operation.ID
	return failure
}

func classifyProviderError(err error, marked bool) *provider.Error {
	var mapped *provider.Error
	if errors.As(err, &mapped) {
		copy := *mapped
		if errors.Is(err, provider.ErrProviderEgressDenied) {
			// The egress transport rejects the request before it can reach a
			// provider socket. Adapters normally preserve this marker as
			// not-dispatched, but retain that fact here even though their
			// BeforePossibleWrite observer has already marked admission.
			copy.Code = provider.CodeProviderUnavailable
			copy.Phase = provider.PhaseDispatch
			copy.Dispatch = provider.DispatchNotDispatched
			copy.Retry = provider.RetryNextRoute
			return &copy
		}
		if errors.Is(err, provider.ErrProviderPreDispatch) {
			// The guarded provider transport proved that no writable connection
			// was acquired. Retain the adapter's availability or caller-context
			// result even though the observer marked admission before transport.
			return &copy
		}
		if marked && copy.Dispatch == provider.DispatchNotDispatched {
			copy.Dispatch = provider.DispatchAmbiguous
		}
		if copy.Dispatch == provider.DispatchAccepted || copy.Dispatch == provider.DispatchAmbiguous {
			copy.Code = provider.CodeAmbiguousDispatch
			copy.Retry = provider.RetryNever
		}
		return &copy
	}
	if errors.Is(err, provider.ErrProviderEgressDenied) {
		return provider.NewEgressDeniedError(err)
	}
	if errors.Is(err, provider.ErrProviderPreDispatch) {
		return provider.NewPreDispatchUnavailableError(err)
	}
	if marked {
		return engineError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "provider request outcome is ambiguous", err)
	}
	return engineError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchNotDispatched, provider.RetryNextRoute, "provider request failed before dispatch", err)
}

func admissionCertainty(value provider.DispatchCertainty) admission.DispatchCertainty {
	switch value {
	case provider.DispatchRejected:
		return admission.Rejected
	case provider.DispatchAccepted:
		return admission.Accepted
	case provider.DispatchAmbiguous:
		return admission.Ambiguous
	default:
		return admission.NotDispatched
	}
}

func mustRequestDigest(request llm.Request) [32]byte {
	digest, _ := llm.RequestDigest(request)
	return digest
}
