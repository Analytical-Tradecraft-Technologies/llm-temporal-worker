package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

const streamBufferCapacity = 16

// Stream is a deprecated residual provider-neutral event-stream API.
// Deprecated: streaming is unsupported in v1. This method remains for source
// compatibility only and MUST NOT be wired into the Temporal runtime.
func (engine *Engine) Stream(ctx context.Context, request llm.Request) (llm.EventStream, error) {
	if engine == nil {
		return nil, engineError(provider.CodeInternal, provider.PhaseNormalize, provider.DispatchNotDispatched, provider.RetryNever, "engine is nil", nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, engineError(provider.CodeCanceled, provider.PhaseNormalize, provider.DispatchNotDispatched, provider.RetryNever, "request canceled", err)
	}
	ctx, requestSpan := engine.startTrace(ctx, "llmtw.stream", requestTraceAttrs(request)...)
	setup, err := engine.prepareStream(ctx, request)
	if err != nil {
		engine.recordTraceError(ctx, requestSpan, err)
		requestSpan.End()
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(setup.ctx)
	emitter := newStreamEmitter(cancel, streamCtx.Done())
	emitter.operationID = setup.operation.ID
	go func() {
		<-streamCtx.Done()
		_ = emitter.closeActiveSource()
	}()
	go func() {
		defer requestSpan.End()
		defer emitter.close()
		defer cancel()
		if err := engine.runPreparedStream(streamCtx, setup, emitter); err != nil {
			engine.recordTraceError(streamCtx, requestSpan, err)
			emitter.fail(err)
		}
	}()
	return emitter, nil
}

type streamSetup struct {
	ctx             context.Context
	request         llm.Request
	providerRequest llm.Request
	snapshot        Snapshot
	quoted          quotedPlan
	operation       admission.Operation
	parent          *state.Continuation
	existing        bool
	now             time.Time
}

// prepareStream performs the same pre-dispatch lifecycle as Generate before
// returning an EventStream. If it fails, no stream was created and the caller
// receives the ordinary classified error directly; every returned stream has a
// durable operation identity and can therefore deliver exactly one terminal.
func (engine *Engine) prepareStream(ctx context.Context, request llm.Request) (streamSetup, error) {
	normalizeCtx, normalizeSpan := engine.startTrace(ctx, "llmtw.normalize", requestTraceAttrs(request)...)
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		engine.recordTraceError(normalizeCtx, normalizeSpan, err)
		normalizeSpan.End()
		return streamSetup{}, engineError(provider.CodeInvalidArgument, provider.PhaseNormalize, provider.DispatchNotDispatched, provider.RetryNever, "request normalization failed", err)
	}
	digest, err := llm.RequestDigest(normalized)
	if err != nil {
		engine.recordTraceError(normalizeCtx, normalizeSpan, err)
		normalizeSpan.End()
		return streamSetup{}, engineError(provider.CodeInvalidArgument, provider.PhaseNormalize, provider.DispatchNotDispatched, provider.RetryNever, "request digest failed", err)
	}
	normalizeSpan.End()
	planCtx, planSpan := engine.startTrace(normalizeCtx, "llmtw.planning", requestTraceAttrs(normalized)...)
	snapshot, err := engine.dependencies.Snapshots.Current(planCtx)
	if err != nil {
		engine.recordTraceError(planCtx, planSpan, err)
		planSpan.End()
		return streamSetup{}, engineError(provider.CodeConfiguration, provider.PhasePlan, provider.DispatchNotDispatched, provider.RetryNever, "configuration snapshot unavailable", err)
	}
	now := engine.dependencies.Clock()
	stateCtx, stateSpan := engine.startTrace(planCtx, "llmtw.state.load", requestTraceAttrs(normalized)...)
	providerRequest, constraints, parent, err := engine.loadContinuation(stateCtx, normalized, now)
	if err != nil {
		engine.recordTraceError(stateCtx, stateSpan, err)
		stateSpan.End()
		planSpan.End()
		return streamSetup{}, err
	}
	stateSpan.End()
	if err := engine.beat(planCtx, Progress{Phase: "planning", At: now}); err != nil {
		engine.recordTraceError(planCtx, planSpan, err)
		planSpan.End()
		return streamSetup{}, err
	}
	plan, err := engine.dependencies.Planner.Plan(planCtx, routingInput(normalized, snapshot, constraints, now))
	if err != nil {
		engine.recordTraceError(planCtx, planSpan, err)
		planSpan.End()
		return streamSetup{}, engineError(provider.CodeNoRoute, provider.PhasePlan, provider.DispatchNotDispatched, provider.RetryNever, "no eligible route", err)
	}
	quoted, err := engine.quotePlan(planCtx, normalized, plan, snapshot, now)
	if err != nil {
		engine.recordTraceError(planCtx, planSpan, err)
		planSpan.End()
		return streamSetup{}, err
	}
	quoted, err = engine.preflightStreamingPlan(planCtx, normalized, quoted)
	if err != nil {
		engine.recordTraceError(planCtx, planSpan, err)
		planSpan.End()
		return streamSetup{}, err
	}
	if len(quoted.candidates) == 0 {
		planSpan.End()
		return streamSetup{}, engineError(provider.CodeUnsupportedCapability, provider.PhaseStream, provider.DispatchNotDispatched, provider.RetryNever, "no eligible adapter implements provider streaming", nil)
	}
	planSpan.End()
	operationID, scopeKey := operationIdentity(normalized)
	admissionCtx, admissionSpan := engine.startTrace(planCtx, "llmtw.admission", requestTraceAttrs(normalized)...)
	operation, existing, err := engine.beginOrResume(admissionCtx, normalized, snapshot, operationID, scopeKey, digest, quoted, now)
	if err != nil {
		engine.recordTraceError(admissionCtx, admissionSpan, err)
		admissionSpan.End()
		return streamSetup{}, err
	}
	admissionSpan.End()
	return streamSetup{ctx: admissionCtx, request: normalized, providerRequest: providerRequest, snapshot: snapshot, quoted: quoted, operation: operation, parent: parent, existing: existing, now: now}, nil
}

// preflightStreamingPlan performs the non-mutating half of stream dispatch.
// It keeps only candidates with both the optional StreamingAdapter port and a
// currently supported streaming capability. The returned plan intentionally
// rebuilds maximum from retained candidates so Begin reserves, aggregates, and
// versions only the routes that Stream can actually attempt. dispatchStreamPlan
// repeats this check after admission because an adapter capability can change
// between preflight and the provider call.
func (engine *Engine) preflightStreamingPlan(ctx context.Context, request llm.Request, quoted quotedPlan) (quotedPlan, error) {
	filtered := quotedPlan{candidates: make([]quotedCandidate, 0, len(quoted.candidates))}
	strict := request.Portability != llm.PortabilityBestEffort
	for _, candidate := range quoted.candidates {
		if err := ctx.Err(); err != nil {
			return quotedPlan{}, engineError(provider.CodeCanceled, provider.PhaseStream, provider.DispatchNotDispatched, provider.RetryNever, "streaming preflight canceled", err)
		}
		adapter, err := engine.dependencies.Adapters.Adapter(ctx, candidate.candidate)
		if err != nil {
			continue
		}
		if _, ok := adapter.(provider.StreamingAdapter); !ok {
			continue
		}
		query := provider.CapabilityQuery{EndpointID: candidate.candidate.EndpointID, Family: provider.Family(candidate.candidate.Family), Model: candidate.candidate.Model, ServiceClass: candidate.candidate.AttemptedClass}
		capability, err := adapter.Capabilities(ctx, query)
		if err != nil {
			if cause := ctx.Err(); cause != nil {
				return quotedPlan{}, engineError(provider.CodeCanceled, provider.PhaseStream, provider.DispatchNotDispatched, provider.RetryNever, "streaming preflight canceled", cause)
			}
			continue
		}
		if !capability.Supports(provider.FeatureStreaming, strict) {
			continue
		}
		filtered.candidates = append(filtered.candidates, candidate)
		if candidate.estimate.MicroUSD > filtered.maximum {
			filtered.maximum = candidate.estimate.MicroUSD
		}
	}
	return filtered, nil
}

func (engine *Engine) runPreparedStream(ctx context.Context, setup streamSetup, emitter *streamEmitter) error {
	operation := setup.operation
	if setup.existing {
		response, resumed, err := engine.resolveExisting(ctx, operation, setup.now)
		if err != nil {
			return err
		}
		if response != nil {
			if err := emitter.started(ctx); err != nil {
				if cause := ctx.Err(); cause != nil {
					return engine.cancelPreparedStream(ctx, setup, operation, cause)
				}
				return err
			}
			return emitter.completed(ctx, *response)
		}
		if !resumed {
			return engineError(provider.CodeOperationConflict, provider.PhaseAdmission, provider.DispatchNotDispatched, provider.RetrySameOperation, "operation is already in progress", nil)
		}
	}
	if err := ctx.Err(); err != nil {
		return engine.cancelPreparedStream(ctx, setup, operation, err)
	}
	if err := engine.beat(ctx, Progress{OperationID: operation.ID, Phase: "admission", At: setup.now}); err != nil {
		if cause := ctx.Err(); cause != nil {
			return engine.cancelPreparedStream(ctx, setup, operation, cause)
		}
		return err
	}
	if err := emitter.started(ctx); err != nil {
		if cause := ctx.Err(); cause != nil {
			return engine.cancelPreparedStream(ctx, setup, operation, cause)
		}
		return err
	}
	return engine.dispatchStreamPlan(ctx, setup.request, setup.providerRequest, setup.snapshot, setup.quoted, operation, setup.parent, emitter)
}

// cancelPreparedStream closes only a reservation this stream owns. A replayed
// completed operation is never mutated merely because its reader canceled.
func (engine *Engine) cancelPreparedStream(ctx context.Context, setup streamSetup, operation admission.Operation, cause error) error {
	if operation.State != admission.StateReserved {
		failure := engineError(provider.CodeCanceled, provider.PhaseAdmission, provider.DispatchNotDispatched, provider.RetryNever, "request canceled", cause)
		failure.OperationID = operation.ID
		return failure
	}
	candidate := routing.Candidate{}
	if len(setup.quoted.candidates) > 0 {
		candidate = setup.quoted.candidates[0].candidate
	}
	return engine.handleCancellation(ctx, operation, candidate, cause)
}

// routingInput keeps Stream and Generate on the exact same planning input.
func routingInput(request llm.Request, snapshot Snapshot, constraints state.Constraints, now time.Time) routing.Input {
	input := routing.Input{Request: request, Catalog: snapshot.Routes, Continuation: constraints, Health: snapshot.Health, Now: now}
	if len(constraints.Affinities) == 0 {
		return input
	}
	preferences := routing.AffinityPreferences{}
	for _, affinity := range constraints.Affinities {
		if affinity.HardPinned {
			// A checkpoint can carry at most one hard pin. Affinity validation
			// rejects duplicate/ambiguous records before this conversion.
			value := affinity
			if preferences.HardPin == nil {
				preferences.HardPin = &value
				continue
			}
			// Keep the duplicate in the soft set so routing validation fails
			// closed rather than silently selecting one hard pin.
		}
		preferences.Soft = append(preferences.Soft, affinity)
	}
	input.Affinity = &preferences
	return input
}

func (engine *Engine) dispatchStreamPlan(ctx context.Context, request, providerRequest llm.Request, snapshot Snapshot, quoted quotedPlan, operation admission.Operation, parent *state.Continuation, emitter *streamEmitter) error {
	for index, candidate := range quoted.candidates {
		if index >= engine.dependencies.MaxAttempts {
			break
		}
		if err := ctx.Err(); err != nil {
			return engine.handleCancellation(ctx, operation, candidate.candidate, err)
		}
		adapter, err := engine.dependencies.Adapters.Adapter(ctx, candidate.candidate)
		if err != nil {
			continue
		}
		streaming, ok := adapter.(provider.StreamingAdapter)
		if !ok {
			continue
		}
		query := provider.CapabilityQuery{EndpointID: candidate.candidate.EndpointID, Family: provider.Family(candidate.candidate.Family), Model: candidate.candidate.Model, ServiceClass: candidate.candidate.AttemptedClass}
		capability, err := adapter.Capabilities(ctx, query)
		if err != nil || !capability.Supports(provider.FeatureStreaming, request.Portability != llm.PortabilityBestEffort) {
			continue
		}
		call, err := adapter.Compile(ctx, provider.CompileInput{
			Request: providerRequest, Query: query, Capability: capability, Strict: request.Portability != llm.PortabilityBestEffort,
			Metadata: provider.CallMetadata{SchemaDigest: mustRequestDigest(request), CapabilityVersion: candidate.candidate.CapabilityVersion, ProviderTier: candidate.candidate.ProviderTier, OpaqueStateRequired: request.Continuation != nil},
		})
		if err != nil {
			if mapped, ok := err.(*provider.Error); ok && mapped.Dispatch != provider.DispatchNotDispatched {
				return engine.finishFailed(ctx, operation, candidate.candidate, mapped, 0)
			}
			continue
		}
		lease := snapshot.ReservationLease
		if lease <= 0 {
			lease = defaultLease
		}
		observer := &dispatchObserver{engine: engine, operation: operation, candidate: candidate.candidate, attempt: index + 1, leaseUntil: engine.dependencies.Clock().Add(lease), streaming: true}
		attemptCtx, attemptSpan := engine.startTrace(ctx, "llmtw.provider_attempt", operationTraceAttrs(operation.ID, candidate.candidate)...)
		result, openErr := streaming.OpenStream(attemptCtx, call, observer)
		if openErr != nil {
			engine.recordTraceError(attemptCtx, attemptSpan, openErr)
		}
		attemptSpan.End()
		if openErr != nil {
			next, retry, err := engine.handleStreamAttemptError(ctx, operation, candidate, quoted, index, snapshot, observer, openErr)
			if err != nil {
				return err
			}
			if retry {
				operation = next
			}
			continue
		}
		if !result.Dispatch.Valid() {
			openErr = engineError(provider.CodeProviderInvalidResponse, provider.PhaseStream, provider.DispatchAmbiguous, provider.RetryNever, "provider stream returned invalid dispatch certainty", nil)
			next, retry, err := engine.handleStreamAttemptError(ctx, operation, candidate, quoted, index, snapshot, observer, openErr)
			if err != nil {
				return err
			}
			if retry {
				operation = next
			}
			continue
		}
		if result.Dispatch == provider.DispatchAccepted && !observer.marked {
			if result.Source != nil {
				_ = result.Source.Close()
			}
			failure := engineError(provider.CodeProviderInvalidResponse, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "provider stream accepted without dispatch observation", nil)
			// Treat an accepted result as a possible write even when a defective
			// adapter skipped the required observer callback. This preserves the
			// no-replay invariant and records the attempt before finalization.
			if err := observer.BeforePossibleWrite(ctx); err != nil {
				failure = engineError(provider.CodeStateUnavailable, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetrySameOperation, "stream dispatch could not be recorded", err)
			}
			return engine.finishFailed(ctx, operation, candidate.candidate, failure, candidate.estimate.MicroUSD)
		}
		if result.Source == nil {
			openErr = engineError(provider.CodeProviderInvalidResponse, provider.PhaseStream, result.Dispatch, provider.RetryNever, "provider stream source is nil", nil)
			next, retry, err := engine.handleStreamAttemptError(ctx, operation, candidate, quoted, index, snapshot, observer, openErr)
			if err != nil {
				return err
			}
			if retry {
				operation = next
			}
			continue
		}
		if result.Dispatch != provider.DispatchAccepted {
			_ = result.Source.Close()
			openErr = engineError(provider.CodeProviderInvalidResponse, provider.PhaseStream, result.Dispatch, provider.RetryNever, "provider stream opened without accepted dispatch", nil)
			next, retry, err := engine.handleStreamAttemptError(ctx, operation, candidate, quoted, index, snapshot, observer, openErr)
			if err != nil {
				return err
			}
			if retry {
				operation = next
			}
			continue
		}
		managedSource := &managedEventSource{source: result.Source}
		emitter.attachSource(managedSource)
		if err := observer.AfterResponseHeaders(ctx, result.Metadata); err != nil {
			emitter.detachSource(managedSource)
			_ = managedSource.Close()
			return engine.finishFailed(ctx, operation, candidate.candidate, engineError(provider.CodeStateUnavailable, provider.PhaseStream, provider.DispatchAccepted, provider.RetrySameOperation, "stream heartbeat failed", err), candidate.estimate.MicroUSD)
		}
		response, streamErr := engine.consumeProviderStream(ctx, managedSource, request.OperationKey, candidate.candidate, emitter, observer)
		emitter.detachSource(managedSource)
		if streamErr != nil {
			return engine.finishFailed(ctx, operation, candidate.candidate, classifyProviderError(streamErr, observer.marked), candidate.estimate.MicroUSD)
		}
		if observer.heartbeatErr != nil {
			return engine.finishFailed(ctx, operation, candidate.candidate, engineError(provider.CodeStateUnavailable, provider.PhaseFinalize, provider.DispatchAccepted, provider.RetrySameOperation, "progress heartbeat failed", observer.heartbeatErr), candidate.estimate.MicroUSD)
		}
		if response.Provider.RequestID == "" {
			response.Provider.RequestID = result.Metadata.RequestID
		}
		if response.Provider.ResponseID == "" {
			response.Provider.ResponseID = result.Metadata.ResponseID
		}
		if response.Service.ProviderValue == "" {
			response.Service.ProviderValue = result.Metadata.ProviderTier
		}
		final, err := engine.finalizeSuccess(ctx, request, snapshot, quoted, index, operation, parent, call, response)
		if err != nil {
			return err
		}
		return emitter.completed(ctx, final)
	}
	return engine.finishUnavailableStream(ctx, operation, request)
}

func (engine *Engine) handleStreamAttemptError(ctx context.Context, operation admission.Operation, candidate quotedCandidate, quoted quotedPlan, index int, snapshot Snapshot, observer *dispatchObserver, attemptErr error) (admission.Operation, bool, error) {
	mapped := classifyProviderError(attemptErr, observer.marked)
	if !observer.marked && (mapped.Dispatch == provider.DispatchNotDispatched || mapped.Dispatch == provider.DispatchRejected) {
		return operation, true, nil
	}
	certainty := admissionCertainty(mapped.Dispatch)
	if certainty == admission.Accepted || certainty == admission.Ambiguous {
		return admission.Operation{}, false, engine.finishFailed(ctx, operation, candidate.candidate, mapped, candidate.estimate.MicroUSD)
	}
	remaining := quoted.candidates[index+1:]
	if len(remaining) == 0 {
		return admission.Operation{}, false, engine.finishFailed(ctx, operation, candidate.candidate, mapped, 0)
	}
	continued, err := engine.continueAfterDefiniteFailure(ctx, operation, candidate.candidate, mapped, remaining, snapshot)
	if err != nil {
		return admission.Operation{}, false, err
	}
	if continued.Denied != nil {
		return admission.Operation{}, false, engineError(provider.CodeBudgetDenied, provider.PhaseAdmission, provider.DispatchRejected, provider.RetryAfter, "fallback budget reservation denied", nil)
	}
	return continued.Operation, true, nil
}

func (engine *Engine) consumeProviderStream(ctx context.Context, source provider.EventSource, operationKey string, candidate routing.Candidate, emitter *streamEmitter, observer *dispatchObserver) (llm.Response, error) {
	defer source.Close()
	assembler := newStreamAssembler(operationKey, candidate, emitter)
	for {
		value, err := source.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return llm.Response{}, engineError(provider.CodeProviderInvalidResponse, provider.PhaseStream, provider.DispatchAccepted, provider.RetryNever, "provider stream ended before a terminal event", nil)
			}
			return llm.Response{}, err
		}
		response, terminal, err := assembler.Add(ctx, value)
		if err != nil {
			return llm.Response{}, engineError(provider.CodeProviderInvalidResponse, provider.PhaseStream, provider.DispatchAccepted, provider.RetryNever, "provider stream event is invalid", err)
		}
		if _, ok := value.(provider.OutputFinished); ok {
			observer.OnProgress(ctx, provider.Progress{Phase: string(provider.PhaseStream), OutputItems: assembler.completedItems})
		}
		if !terminal {
			continue
		}
		// EventSource implementations must return EOF immediately after their
		// terminal event. Checking it here rejects duplicate terminals rather
		// than silently choosing the first one.
		extra, err := source.Next(ctx)
		if err == nil {
			return llm.Response{}, fmt.Errorf("provider stream emitted %T after terminal", extra)
		}
		if !errors.Is(err, io.EOF) {
			return llm.Response{}, err
		}
		return response, nil
	}
}

func (engine *Engine) finishUnavailableStream(ctx context.Context, operation admission.Operation, request llm.Request) error {
	if err := engine.finishNoDispatch(ctx, operation, request); err != nil {
		var noRoute *provider.Error
		if !errors.As(err, &noRoute) || noRoute.Code != provider.CodeNoRoute {
			return err
		}
	}
	result := engineError(provider.CodeUnsupportedCapability, provider.PhaseStream, provider.DispatchNotDispatched, provider.RetryNever, "no eligible adapter implements provider streaming", nil)
	result.OperationID = operation.ID
	return result
}

type streamEmitter struct {
	events      chan llm.Event
	contextDone <-chan struct{}
	cancel      context.CancelFunc
	closeOnce   sync.Once
	doneOnce    sync.Once
	done        chan struct{}
	closed      chan struct{}
	sourceMu    sync.Mutex
	source      *managedEventSource
	stopped     bool
	closeErr    error
	terminalMu  sync.Mutex
	pending     llm.Event
	delivered   bool
	operationID string
	sequence    uint64
	terminal    bool
}

// managedEventSource makes a provider body's Close idempotent across the
// producer's deferred cleanup, a caller's EventStream.Close, and parent
// context cancellation.
type managedEventSource struct {
	source provider.EventSource
	once   sync.Once
	err    error
}

func (source *managedEventSource) Next(ctx context.Context) (provider.Event, error) {
	if source == nil || source.source == nil {
		return nil, fmt.Errorf("provider stream source is unavailable")
	}
	return source.source.Next(ctx)
}

func (source *managedEventSource) Close() error {
	if source == nil || source.source == nil {
		return nil
	}
	source.once.Do(func() { source.err = source.source.Close() })
	return source.err
}

func newStreamEmitter(cancel context.CancelFunc, contextDone <-chan struct{}) *streamEmitter {
	return &streamEmitter{
		events:      make(chan llm.Event, streamBufferCapacity),
		contextDone: contextDone,
		cancel:      cancel,
		done:        make(chan struct{}),
		closed:      make(chan struct{}),
	}
}

func (emitter *streamEmitter) Next(ctx context.Context) (llm.Event, error) {
	if err := ctx.Err(); err != nil {
		_ = emitter.Close()
		return nil, err
	}
	select {
	case event, ok := <-emitter.events:
		if !ok {
			if terminal, ok := emitter.takePendingTerminal(); ok {
				return terminal, nil
			}
			return nil, io.EOF
		}
		return event, nil
	case <-ctx.Done():
		_ = emitter.Close()
		return nil, ctx.Err()
	}
}

func (emitter *streamEmitter) Close() error {
	emitter.cancel()
	emitter.doneOnce.Do(func() {
		emitter.sourceMu.Lock()
		emitter.stopped = true
		source := emitter.source
		emitter.source = nil
		emitter.sourceMu.Unlock()
		var closeErr error
		if source != nil {
			closeErr = source.Close()
		}
		emitter.sourceMu.Lock()
		emitter.closeErr = closeErr
		emitter.sourceMu.Unlock()
		close(emitter.done)
	})
	emitter.sourceMu.Lock()
	err := emitter.closeErr
	emitter.sourceMu.Unlock()
	return err
}

func (emitter *streamEmitter) close() {
	emitter.closeOnce.Do(func() {
		close(emitter.events)
		close(emitter.closed)
	})
}

func (emitter *streamEmitter) attachSource(source *managedEventSource) {
	if emitter == nil || source == nil {
		return
	}
	// The cancellation watcher can observe a canceled parent just before this
	// source becomes active. Check both before and after publishing it so an
	// adapter that ignores context still has its body closed.
	select {
	case <-emitter.contextDone:
		_ = source.Close()
		return
	default:
	}
	emitter.sourceMu.Lock()
	if emitter.stopped {
		emitter.sourceMu.Unlock()
		_ = source.Close()
		return
	}
	emitter.source = source
	emitter.sourceMu.Unlock()
	select {
	case <-emitter.contextDone:
		_ = emitter.closeActiveSource()
	default:
	}
}

func (emitter *streamEmitter) detachSource(source *managedEventSource) {
	if emitter == nil || source == nil {
		return
	}
	emitter.sourceMu.Lock()
	if emitter.source == source {
		emitter.source = nil
	}
	emitter.sourceMu.Unlock()
}

func (emitter *streamEmitter) closeActiveSource() error {
	if emitter == nil {
		return nil
	}
	emitter.sourceMu.Lock()
	source := emitter.source
	emitter.sourceMu.Unlock()
	if source == nil {
		return nil
	}
	return source.Close()
}

func (emitter *streamEmitter) header() llm.EventHeader {
	emitter.sequence++
	return llm.EventHeader{Sequence: emitter.sequence, OperationID: emitter.operationID}
}

func (emitter *streamEmitter) emit(ctx context.Context, outputIndex, contentIndex *int, build func(llm.EventHeader) llm.Event) error {
	if build == nil {
		return fmt.Errorf("stream event builder is nil")
	}
	header := emitter.header()
	header.OutputIndex = cloneStreamIndex(outputIndex)
	header.ContentIndex = cloneStreamIndex(contentIndex)
	if err := emitter.send(ctx, build(header)); err != nil {
		emitter.sequence--
		return err
	}
	return nil
}

func (emitter *streamEmitter) started(ctx context.Context) error {
	if err := emitter.send(ctx, llm.ResponseStarted{EventHeader: emitter.header()}); err != nil {
		emitter.sequence--
		return err
	}
	return nil
}

func (emitter *streamEmitter) completed(ctx context.Context, response llm.Response) error {
	if emitter.terminal {
		return engineError(provider.CodeProviderInvalidResponse, provider.PhaseStream, provider.DispatchAccepted, provider.RetryNever, "stream produced more than one terminal outcome", nil)
	}
	if err := emitter.send(ctx, llm.ResponseCompleted{EventHeader: emitter.header(), Response: response}); err != nil {
		emitter.sequence--
		return err
	}
	emitter.terminal = true
	return nil
}

func (emitter *streamEmitter) fail(err error) {
	if emitter.terminal || emitter.operationID == "" {
		return
	}
	emitter.terminal = true
	event := llm.StreamErrored{EventHeader: emitter.header(), Err: err}
	if validateErr := llm.ValidateStreamEvent(event); validateErr != nil {
		event.Err = engineError(provider.CodeInternal, provider.PhaseStream, provider.DispatchAmbiguous, provider.RetryNever, "stream terminal delivery failed", validateErr)
	}
	// Prefer the channel while there is capacity so the normal stream fast path
	// remains unchanged. If parent cancellation races a full buffer, preserve
	// the terminal separately and let the producer close instead of leaking
	// behind undrained deltas. Next delivers that pending terminal after the
	// channel's buffered events, exactly once, to a consumer that keeps reading.
	select {
	case emitter.events <- event:
		return
	default:
	}
	select {
	case emitter.events <- event:
	case <-emitter.done:
	case <-emitter.contextDone:
		emitter.setPendingTerminal(event)
	}
}

func (emitter *streamEmitter) setPendingTerminal(event llm.Event) {
	if emitter == nil || event == nil {
		return
	}
	emitter.terminalMu.Lock()
	if emitter.pending == nil {
		emitter.pending = event
	}
	emitter.terminalMu.Unlock()
}

func (emitter *streamEmitter) takePendingTerminal() (llm.Event, bool) {
	if emitter == nil {
		return nil, false
	}
	emitter.terminalMu.Lock()
	defer emitter.terminalMu.Unlock()
	if emitter.pending == nil || emitter.delivered {
		return nil, false
	}
	emitter.delivered = true
	return emitter.pending, true
}

func (emitter *streamEmitter) send(ctx context.Context, event llm.Event) error {
	if err := llm.ValidateStreamEvent(event); err != nil {
		return err
	}
	select {
	case emitter.events <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func cloneStreamIndex(value *int) *int {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
