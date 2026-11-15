package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
)

func TestStreamRejectsAdapterWithoutRealStreamBeforeDispatch(t *testing.T) {
	adapter := &fakeAdapter{name: "non-streaming", response: successfulResponse()}
	harness := newHarness(t, adapter)
	request := baseRequest("no-stream-port")

	stream, err := harness.engine.Stream(context.Background(), request)
	if stream != nil {
		_ = stream.Close()
		t.Fatal("Stream returned an EventStream for an adapter without provider.StreamingAdapter")
	}
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) {
		t.Fatalf("Stream error = %v, want provider error", err)
	}
	if providerErr.Code != provider.CodeUnsupportedCapability || providerErr.Dispatch != provider.DispatchNotDispatched {
		t.Fatalf("Stream error = %#v, want pre-admission streaming rejection", providerErr)
	}
	requireNoOperation(t, harness, request)
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if adapter.capabilities != 0 || adapter.compiles != 0 || adapter.invokes != 0 {
		t.Fatalf("non-streaming adapter was touched: capabilities=%d compiles=%d invokes=%d, want all zero", adapter.capabilities, adapter.compiles, adapter.invokes)
	}
}

func TestPreflightStreamingPlanRebuildsAdmissionInputsFromEligibleCandidates(t *testing.T) {
	native := &fakeAdapter{name: "native-only", response: successfulResponse()}
	streaming := newStreamingAdapter("streaming")
	harness := newHarness(t, streaming)
	harness.engine.dependencies.Adapters = AdapterMap{"native": native, "streaming": streaming}
	candidate := func(endpointID, model, priceVersion string, amount pricing.MicroUSD) quotedCandidate {
		return quotedCandidate{
			candidate: routing.Candidate{EndpointID: endpointID, Family: string(provider.FamilyOpenAIResponses), Model: model, AttemptedClass: llm.ServiceClassStandard},
			entry:     &pricing.Entry{Version: priceVersion},
			estimate:  budget.Estimate{MicroUSD: amount},
			reservations: []admission.WindowReservation{{
				PolicyID: "policy", WindowID: "window", Amount: amount, Limit: 1_000, BucketNanos: int64(time.Minute), DurationNanos: int64(time.Minute),
			}},
		}
	}
	quoted := quotedPlan{candidates: []quotedCandidate{
		candidate("native", "native-model", "native-price", 99),
		candidate("streaming", "stream-model", "stream-price", 7),
	}, maximum: 99}
	request := baseRequest("filtered-stream-admission")
	filtered, err := harness.engine.preflightStreamingPlan(context.Background(), request, quoted)
	testassert.NoError(t, err)
	if len(filtered.candidates) != 1 || filtered.candidates[0].candidate.EndpointID != "streaming" || filtered.maximum != 7 {
		t.Fatalf("filtered stream plan = %#v, want only the streaming candidate with maximum 7", filtered)
	}
	if version := priceVersion(filtered.candidates); version != "stream-price" {
		t.Fatalf("filtered price version = %q, want stream-price", version)
	}
	snapshot, err := harness.engine.dependencies.Snapshots.Current(context.Background())
	testassert.NoError(t, err)
	identity := normalizedIdentityForTest(t, request)
	operation, existing, err := harness.engine.beginOrResume(context.Background(), identity.normalized, snapshot, identity.id, identity.scopeKey, identity.digest, filtered, harness.clock)
	testassert.NoError(t, err)
	if existing || operation.ReservedMicroUSD != 7 || operation.PriceVersion != "stream-price" || len(operation.Reservations) != 1 || operation.Reservations[0].Amount != 7 {
		t.Fatalf("stream admission operation = %#v, want only the streaming candidate's reservation and price", operation)
	}
	native.mu.Lock()
	nativeCapabilities := native.capabilities
	native.mu.Unlock()
	if nativeCapabilities != 0 {
		t.Fatalf("non-streaming candidate capability calls = %d, want 0", nativeCapabilities)
	}
}

func TestStreamDeliversProviderDeltasInOrderBeforeFinalResponse(t *testing.T) {
	harness := newHarness(t, newStreamingAdapter("streaming",
		provider.OutputStarted{Index: 0},
		provider.TextDelta{Index: 0, Text: "wo"},
		provider.TextDelta{Index: 0, Text: "rld"},
		provider.OutputFinished{Index: 0},
		provider.UsageUpdated{Usage: llm.Usage{OutputTokens: 1}},
		provider.StreamCompleted{},
	))
	events := streamEvents(t, harness, baseRequest("ordered-stream"))

	gotKinds := make([]string, 0, len(events))
	for _, event := range events {
		gotKinds = append(gotKinds, reflect.TypeOf(event).Name())
	}
	wantKinds := []string{"ResponseStarted", "ContentStarted", "TextDelta", "TextDelta", "ContentCompleted", "UsageUpdated", "ResponseCompleted"}
	if !reflect.DeepEqual(gotKinds, wantKinds) {
		t.Fatalf("event order = %#v, want %#v", gotKinds, wantKinds)
	}
	completed := events[len(events)-1].(llm.ResponseCompleted)
	if completed.Response.OperationKey != "ordered-stream" || len(completed.Response.Output) != 1 {
		t.Fatalf("completed response = %#v", completed.Response)
	}
}

func TestStreamPreservesOpaqueProviderStateByteForByte(t *testing.T) {
	opaque := []byte{0x00, 0xff, 0x7f}
	harness := newHarness(t, newStreamingAdapter("opaque-stream",
		provider.OutputStarted{Index: 0},
		provider.ProviderStateDelta{Index: 0, State: llm.ProviderState{Provider: "provider-1", EndpointFamily: "openai_responses", MediaType: "application/octet-stream", Opaque: opaque}},
		provider.OutputFinished{Index: 0},
		provider.StreamCompleted{},
	))
	events := streamEvents(t, harness, baseRequest("opaque-stream"))

	var delta llm.ProviderStateDelta
	found := false
	for _, event := range events {
		if value, ok := event.(llm.ProviderStateDelta); ok {
			delta, found = value, true
			break
		}
	}
	if !found || !bytes.Equal(delta.State.Opaque, opaque) {
		t.Fatalf("provider state delta = %#v, want exact opaque bytes %#v", delta, opaque)
	}
	response := completedStreamResponse(events)
	message, ok := response.Output[0].(llm.Message)
	if !ok || len(message.Content) != 1 {
		t.Fatalf("final stream response = %#v", response)
	}
	state, ok := message.Content[0].(llm.ProviderStatePart)
	if !ok || !bytes.Equal(state.Opaque, opaque) {
		t.Fatalf("final provider state = %#v, want exact opaque bytes %#v", state, opaque)
	}
}

func TestStreamDeliversExactlyOneTerminalError(t *testing.T) {
	harness := newHarness(t, newStreamingAdapter("stream-error",
		provider.OutputStarted{Index: 0},
		provider.StreamErrored{Err: provider.NewError(provider.CodeProviderUnavailable, provider.PhaseStream, provider.DispatchAccepted, provider.RetryNever, "safe provider failure")},
	))
	events := streamEvents(t, harness, baseRequest("stream-error"))
	requireStreamErrored(t, events, "terminal event")
	for _, event := range events[:len(events)-1] {
		if llm.IsTerminalEvent(event) {
			t.Fatalf("non-final event %T was terminal", event)
		}
	}
}

func TestStreamRejectsToolFragmentWithoutIdentityAtProviderBoundary(t *testing.T) {
	harness := newHarness(t, newStreamingAdapter("deferred-tool-identity",
		provider.OutputStarted{Index: 0},
		// provider.Assembler accepts this intermediate shape, but a streaming
		// adapter must not expose it until tool identity is known.
		provider.ToolArgumentsDelta{Index: 0, Fragment: `{"argument":`},
		provider.ToolArgumentsDelta{Index: 0, CallID: "call-1", Name: "lookup", Fragment: "true}"},
		provider.OutputFinished{Index: 0},
		provider.StreamCompleted{},
	))
	events := streamEvents(t, harness, baseRequest("deferred-tool-identity"))
	requireStreamErrored(t, events, "terminal event")
	for _, event := range events {
		switch event.(type) {
		case llm.ToolCallStarted, llm.ToolArgumentsDelta:
			t.Fatalf("invalid provider fragment leaked public tool event %#v", event)
		}
	}
}

func TestStreamRejectsDuplicateProviderTerminal(t *testing.T) {
	harness := newHarness(t, newStreamingAdapter("duplicate-terminal", append(worldStreamEvents(), provider.StreamCompleted{})...))
	events := streamEvents(t, harness, baseRequest("duplicate-terminal"))
	requireStreamErrored(t, events, "terminal event")
	for _, event := range events {
		if _, ok := event.(llm.ResponseCompleted); ok {
			t.Fatalf("duplicate provider terminal leaked successful terminal %#v", event)
		}
	}
}

func TestStreamReplaysCompletedTerminalWithoutProviderCall(t *testing.T) {
	adapter := newStreamingAdapter("replay-stream", worldStreamEvents()...)
	harness := newHarness(t, adapter)
	request := baseRequest("stream-replay")

	firstResponse := completedStreamResponse(streamEvents(t, harness, request))
	secondResponse := completedStreamResponse(streamEvents(t, harness, request))
	if firstResponse.OperationID == "" || firstResponse.OperationID != secondResponse.OperationID {
		t.Fatalf("replayed response identity differs: first=%#v second=%#v", firstResponse, secondResponse)
	}
	adapter.requireInvokes(t, 1, "provider stream opens = %d, want one")
}

func TestStreamRetriesPreWriteRejectionOnFallback(t *testing.T) {
	adapter := newStreamingAdapter("retry-stream", worldStreamEvents()...)
	adapter.rejectFirst = true
	harness := newHarness(t, adapter)

	response := completedStreamResponse(streamEvents(t, harness, priorityWithStandardFallback("stream-retry")))
	if response.Service.Requested != llm.ServiceClassPriority || response.Service.Attempted != llm.ServiceClassStandard || response.Service.FallbackIndex != 1 {
		t.Fatalf("fallback response service = %#v", response.Service)
	}
	adapter.requireInvokes(t, 2, "provider stream opens = %d, want two")
}

func TestStreamRefusesAmbiguousPostWriteReplay(t *testing.T) {
	adapter := newStreamingAdapter("ambiguous-stream")
	adapter.ambiguous = true
	harness := newHarness(t, adapter)
	request := baseRequest("stream-ambiguous")

	requireStreamErrored(t, streamEvents(t, harness, request), "first terminal")
	if mapped, failure := streamFailure(t, harness, request, "second terminal"); mapped == nil || mapped.Code != provider.CodeAmbiguousDispatch {
		t.Fatalf("second terminal error = %#v, want ambiguous dispatch", failure)
	}
	adapter.requireInvokes(t, 1, "provider stream opens = %d, want one")
}

func TestStreamRejectsAcceptedResultWithoutDispatchObservation(t *testing.T) {
	adapter := newStreamingAdapter("unobserved-stream", provider.OutputStarted{Index: 0}, provider.OutputFinished{Index: 0}, provider.StreamCompleted{})
	adapter.skipObserver = true
	harness := newHarness(t, adapter)
	request := baseRequest("unobserved-stream")

	if mapped, failure := streamFailure(t, harness, request, "first terminal"); mapped == nil || mapped.Dispatch != provider.DispatchAmbiguous {
		t.Fatalf("first terminal error = %#v, want ambiguous dispatch", failure)
	}
	if mapped, failure := streamFailure(t, harness, request, "second terminal"); mapped == nil || mapped.Code != provider.CodeAmbiguousDispatch {
		t.Fatalf("second terminal error = %#v, want ambiguous replay refusal", failure)
	}
	adapter.requireInvokes(t, 1, "provider stream opens = %d, want one")
}

func TestStreamCancellationClosesProviderSourceAndTerminates(t *testing.T) {
	source := newBlockingProviderSource()
	assertCancellationClosesProviderSource(t, "cancel-stream", "stream-cancel", source, &source.providerSourceSignals, "provider source close")
}

func TestStreamParentCancellationClosesProviderSourceThatIgnoresContext(t *testing.T) {
	source := newCloseDrivenProviderSource()
	assertCancellationClosesProviderSource(t, "parent-cancel-close-driven", "stream-parent-cancel-close-driven", source, &source.providerSourceSignals, "provider source close after parent cancellation")
}

// assertCancellationClosesProviderSource cancels the parent context once the
// engine is reading source and requires an errored terminal and a closed
// provider source.
func assertCancellationClosesProviderSource(t *testing.T, adapterName, operationKey string, source provider.EventSource, signals *providerSourceSignals, closeDescription string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	harness := newHarness(t, sourceStreamingAdapter(adapterName, source))

	stream := harness.mustStream(t, ctx, baseRequest(operationKey))
	defer stream.Close()
	waitChannel(t, signals.entered, "provider source read")
	cancel()
	requireStreamErrored(t, readTerminalStream(t, stream), "terminal event")
	waitChannel(t, signals.closed, closeDescription)
}

func TestStreamCloseClosesProviderSourceThatIgnoresContext(t *testing.T) {
	source := newCloseDrivenProviderSource()
	harness := newHarness(t, sourceStreamingAdapter("explicit-close-stream", source))

	stream := harness.mustStream(t, context.Background(), baseRequest("stream-explicit-close"))
	emitter, ok := stream.(*streamEmitter)
	if !ok {
		t.Fatalf("stream type = %T, want *streamEmitter", stream)
	}
	waitChannel(t, source.entered, "provider source read")
	testassert.NoError(t, stream.Close(), "close stream")
	waitChannel(t, source.closed, "provider source close after explicit stream close")
	waitChannel(t, emitter.closed, "producer completion after explicit stream close")
}

func TestStreamCancellationBeforeDispatchClosesReservationWithoutProviderCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter := newStreamingAdapter("pre-dispatch-cancel")
	harness := newHarness(t, adapter)
	heartbeat := &cancelOnAdmissionHeartbeat{entered: make(chan struct{})}
	harness.engine.dependencies.Heartbeat = heartbeat

	stream := harness.mustStream(t, ctx, baseRequest("pre-dispatch-cancel"))
	defer stream.Close()
	waitChannel(t, heartbeat.entered, "admission heartbeat")
	cancel()
	failure := requireStreamErrored(t, readTerminalStream(t, stream), "terminal event")
	var providerErr *provider.Error
	if !errors.As(failure.Err, &providerErr) || providerErr.Code != provider.CodeCanceled || providerErr.OperationID == "" {
		t.Fatalf("cancellation terminal error = %#v, want classified operation cancellation", failure.Err)
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if adapter.capabilities != 1 || adapter.compiles != 0 || adapter.invokes != 0 {
		t.Fatalf("pre-dispatch cancellation did more than the read-only stream preflight: capabilities=%d compiles=%d invokes=%d", adapter.capabilities, adapter.compiles, adapter.invokes)
	}
}

func TestStreamAppliesBackpressureBeforePullingMoreProviderEvents(t *testing.T) {
	source := newGatedProviderSource(streamBufferCapacity)
	harness := newHarness(t, sourceStreamingAdapter("backpressure-stream", source))
	stream := harness.mustStream(t, context.Background(), baseRequest("stream-backpressure"))
	defer stream.Close()
	waitChannel(t, source.bufferFilled, "stream buffer fill")
	select {
	case <-source.gateRequested:
		t.Fatal("engine pulled a provider event beyond the full public stream buffer")
	default:
	}
	testassert.NoError(t, testassert.Err(stream.Next(context.Background())), "drain one public event")
	waitChannel(t, source.gateRequested, "provider read after drain")
}

func TestStreamParentCancellationWithFullBufferReleasesProducerAndKeepsTerminal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := newGatedProviderSource(streamBufferCapacity)
	harness := newHarness(t, sourceStreamingAdapter("cancel-full-buffer", source))
	stream := harness.mustStream(t, ctx, baseRequest("cancel-full-buffer"))
	defer stream.Close()
	emitter, ok := stream.(*streamEmitter)
	if !ok {
		t.Fatalf("stream type = %T, want *streamEmitter", stream)
	}
	waitChannel(t, source.bufferFilled, "full public stream buffer")
	cancel()
	waitChannel(t, emitter.closed, "producer completion after parent cancellation")
	emitter.terminalMu.Lock()
	pending := emitter.pending
	emitter.terminalMu.Unlock()
	if pending == nil {
		t.Fatal("full-buffer cancellation did not retain a terminal outcome")
	}
	requireStreamErrored(t, readTerminalStream(t, stream), "terminal event")
}

func TestStreamAndGenerateFinalizeEquivalentResponses(t *testing.T) {
	events := worldStreamEvents()
	events[len(events)-1] = provider.StreamCompleted{Response: successfulResponse()}
	harness := newHarness(t, newStreamingAdapter("equivalent-stream", events...))
	nonStream := harness.mustGenerate(t, baseRequest("equivalent-generate"))
	streamResponse := completedStreamResponse(streamEvents(t, harness, baseRequest("equivalent-stream")))
	nonStream.OperationKey, nonStream.OperationID = "", ""
	streamResponse.OperationKey, streamResponse.OperationID = "", ""
	if !reflect.DeepEqual(nonStream, streamResponse) {
		t.Fatalf("stream final response differs from Generate:\nstream=%#v\ngenerate=%#v", streamResponse, nonStream)
	}
}

type streamingAdapter struct {
	*fakeAdapter
	events       []provider.Event
	source       provider.EventSource
	metadata     provider.ResponseMetadata
	skipObserver bool
}

// newStreamingAdapter streams events over a successful fake adapter.
func newStreamingAdapter(name string, events ...provider.Event) *streamingAdapter {
	return &streamingAdapter{fakeAdapter: &fakeAdapter{name: name, response: successfulResponse()}, events: events}
}

// sourceStreamingAdapter streams from source over a successful fake adapter.
func sourceStreamingAdapter(name string, source provider.EventSource) *streamingAdapter {
	adapter := newStreamingAdapter(name)
	adapter.source = source
	return adapter
}

// worldStreamEvents is a complete provider stream of one "world" message.
func worldStreamEvents() []provider.Event { return textStreamEvents("world") }

// okStreamEvents is a complete provider stream of one "ok" message.
func okStreamEvents() []provider.Event { return textStreamEvents("ok") }

func textStreamEvents(text string) []provider.Event {
	return []provider.Event{
		provider.OutputStarted{Index: 0},
		provider.TextDelta{Index: 0, Text: text},
		provider.OutputFinished{Index: 0},
		provider.StreamCompleted{},
	}
}

func (adapter *streamingAdapter) OpenStream(ctx context.Context, call provider.Call, observer provider.Observer) (provider.StreamResult, error) {
	adapter.mu.Lock()
	adapter.invokes++
	index := adapter.invokes
	adapter.calls = append(adapter.calls, call)
	rejectFirst := adapter.rejectFirst
	ambiguous := adapter.ambiguous
	source := adapter.source
	events := append([]provider.Event(nil), adapter.events...)
	metadata := adapter.metadata
	skipObserver := adapter.skipObserver
	adapter.mu.Unlock()
	if rejectFirst && index == 1 {
		return provider.StreamResult{}, provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNextRoute, "provider rejected before write")
	}
	if !skipObserver {
		if err := observer.BeforePossibleWrite(ctx); err != nil {
			return provider.StreamResult{}, err
		}
	}
	if ambiguous {
		return provider.StreamResult{}, provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "provider outcome is ambiguous")
	}
	if source == nil {
		source = &providerSliceSource{events: events}
	}
	if metadata.RequestID == "" {
		metadata.RequestID = "stream-request-1"
	}
	return provider.StreamResult{
		Source:   source,
		Metadata: metadata,
		Dispatch: provider.DispatchAccepted,
	}, nil
}

type providerSliceSource struct {
	events []provider.Event
	next   int
}

func (source *providerSliceSource) Next(ctx context.Context) (provider.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source.next == len(source.events) {
		return nil, io.EOF
	}
	event := source.events[source.next]
	source.next++
	return event, nil
}

func (*providerSliceSource) Close() error { return nil }

// providerSourceSignals reports when a provider source is first read and
// when it is closed.
type providerSourceSignals struct {
	entered   chan struct{}
	closed    chan struct{}
	enterOnce sync.Once
	closeOnce sync.Once
}

func (signals *providerSourceSignals) markEntered() {
	signals.enterOnce.Do(func() { close(signals.entered) })
}

func (signals *providerSourceSignals) Close() error {
	signals.closeOnce.Do(func() { close(signals.closed) })
	return nil
}

type blockingProviderSource struct {
	providerSourceSignals
}

func newBlockingProviderSource() *blockingProviderSource {
	return &blockingProviderSource{providerSourceSignals{entered: make(chan struct{}), closed: make(chan struct{})}}
}

func (source *blockingProviderSource) Next(ctx context.Context) (provider.Event, error) {
	source.markEntered()
	<-ctx.Done()
	return nil, ctx.Err()
}

// closeDrivenProviderSource deliberately ignores the read context. Closing
// the public stream must still release this provider body.
type closeDrivenProviderSource struct {
	providerSourceSignals
}

func newCloseDrivenProviderSource() *closeDrivenProviderSource {
	return &closeDrivenProviderSource{providerSourceSignals{entered: make(chan struct{}), closed: make(chan struct{})}}
}

func (source *closeDrivenProviderSource) Next(context.Context) (provider.Event, error) {
	source.markEntered()
	<-source.closed
	return nil, errors.New("provider source was closed")
}

type cancelOnAdmissionHeartbeat struct {
	entered chan struct{}
	once    sync.Once
}

func (heartbeat *cancelOnAdmissionHeartbeat) Beat(ctx context.Context, progress Progress) error {
	if progress.Phase != "admission" {
		return nil
	}
	heartbeat.once.Do(func() { close(heartbeat.entered) })
	<-ctx.Done()
	return ctx.Err()
}

type gatedProviderSource struct {
	events        []provider.Event
	next          int
	gateAt        int
	bufferFilled  chan struct{}
	gateRequested chan struct{}
	closed        chan struct{}
	fillOnce      sync.Once
	gateOnce      sync.Once
	closeOnce     sync.Once
}

func newGatedProviderSource(gateAt int) *gatedProviderSource {
	events := []provider.Event{provider.OutputStarted{Index: 0}}
	for range streamBufferCapacity + 2 {
		events = append(events, provider.TextDelta{Index: 0, Text: "x"})
	}
	return &gatedProviderSource{
		events:        events,
		gateAt:        gateAt,
		bufferFilled:  make(chan struct{}),
		gateRequested: make(chan struct{}),
		closed:        make(chan struct{}),
	}
}

func (source *gatedProviderSource) Next(ctx context.Context) (provider.Event, error) {
	if source.next == source.gateAt {
		source.gateOnce.Do(func() { close(source.gateRequested) })
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if source.next == len(source.events) {
		return nil, io.EOF
	}
	event := source.events[source.next]
	source.next++
	if source.next == source.gateAt {
		source.fillOnce.Do(func() { close(source.bufferFilled) })
	}
	return event, nil
}

func (source *gatedProviderSource) Close() error {
	source.closeOnce.Do(func() { close(source.closed) })
	return nil
}

// streamEvents opens a stream for request, reads it to its terminal event,
// and closes it.
func streamEvents(t *testing.T, harness testHarness, request llm.Request) []llm.Event {
	t.Helper()
	stream, err := harness.engine.Stream(context.Background(), request)
	testassert.NoError(t, err, "open stream")
	defer stream.Close()
	return readTerminalStream(t, stream)
}

// requireStreamErrored requires the last event to be StreamErrored; label
// names the terminal in the failure message.
func requireStreamErrored(t *testing.T, events []llm.Event, label string) llm.StreamErrored {
	t.Helper()
	failure, ok := events[len(events)-1].(llm.StreamErrored)
	if !ok {
		t.Fatalf("%s = %T, want StreamErrored", label, events[len(events)-1])
	}
	return failure
}

// streamFailure streams request, requires an errored terminal, and returns
// the provider error it wraps (nil when it wraps none) and the raw error.
func streamFailure(t *testing.T, harness testHarness, request llm.Request, label string) (*provider.Error, error) {
	t.Helper()
	failure := requireStreamErrored(t, streamEvents(t, harness, request), label)
	var mapped *provider.Error
	if !errors.As(failure.Err, &mapped) {
		return nil, failure.Err
	}
	return mapped, failure.Err
}

// completedStreamResponse is the final response of a successful stream.
func completedStreamResponse(events []llm.Event) llm.Response {
	return events[len(events)-1].(llm.ResponseCompleted).Response
}

func readTerminalStream(t *testing.T, stream llm.EventStream) []llm.Event {
	t.Helper()
	var events []llm.Event
	for {
		event, err := stream.Next(context.Background())
		if err == io.EOF {
			t.Fatal("stream ended before a terminal event")
		}
		testassert.NoError(t, err, "read stream event")
		events = append(events, event)
		if llm.IsTerminalEvent(event) {
			if trailing, trailingErr := stream.Next(context.Background()); trailingErr != io.EOF {
				t.Fatalf("stream emitted %#v after terminal with error %v", trailing, trailingErr)
			}
			return events
		}
	}
}

func waitChannel(t *testing.T, channel <-chan struct{}, description string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case <-channel:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for %s", description)
	}
}
