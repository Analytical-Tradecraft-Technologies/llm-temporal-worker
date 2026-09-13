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

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
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
	normalized, normalizeErr := llm.NormalizeRequest(request)
	if normalizeErr != nil {
		t.Fatal(normalizeErr)
	}
	operationID, _ := operationIdentity(normalized)
	if _, operationErr := harness.admission.Get(context.Background(), operationID); !errors.Is(operationErr, admission.ErrOperationNotFound) {
		t.Fatalf("admission operation error = %v, want no operation", operationErr)
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if adapter.capabilities != 0 || adapter.compiles != 0 || adapter.invokes != 0 {
		t.Fatalf("non-streaming adapter was touched: capabilities=%d compiles=%d invokes=%d, want all zero", adapter.capabilities, adapter.compiles, adapter.invokes)
	}
}

func TestPreflightStreamingPlanRebuildsAdmissionInputsFromEligibleCandidates(t *testing.T) {
	native := &fakeAdapter{name: "native-only", response: successfulResponse()}
	streaming := &streamingAdapter{fakeAdapter: &fakeAdapter{name: "streaming", response: successfulResponse()}}
	harness := newHarness(t, streaming)
	harness.engine.dependencies.Adapters = AdapterMap{"native": native, "streaming": streaming}
	quoted := quotedPlan{candidates: []quotedCandidate{
		{
			candidate: routing.Candidate{EndpointID: "native", Family: string(provider.FamilyOpenAIResponses), Model: "native-model", AttemptedClass: llm.ServiceClassStandard},
			entry:     &pricing.Entry{Version: "native-price"},
			estimate:  budget.Estimate{MicroUSD: 99},
			reservations: []admission.WindowReservation{{
				PolicyID: "policy", WindowID: "window", Amount: 99, Limit: 1_000, BucketNanos: int64(time.Minute), DurationNanos: int64(time.Minute),
			}},
		},
		{
			candidate: routing.Candidate{EndpointID: "streaming", Family: string(provider.FamilyOpenAIResponses), Model: "stream-model", AttemptedClass: llm.ServiceClassStandard},
			entry:     &pricing.Entry{Version: "stream-price"},
			estimate:  budget.Estimate{MicroUSD: 7},
			reservations: []admission.WindowReservation{{
				PolicyID: "policy", WindowID: "window", Amount: 7, Limit: 1_000, BucketNanos: int64(time.Minute), DurationNanos: int64(time.Minute),
			}},
		},
	}, maximum: 99}
	request := baseRequest("filtered-stream-admission")
	filtered, err := harness.engine.preflightStreamingPlan(context.Background(), request, quoted)
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered.candidates) != 1 || filtered.candidates[0].candidate.EndpointID != "streaming" || filtered.maximum != 7 {
		t.Fatalf("filtered stream plan = %#v, want only the streaming candidate with maximum 7", filtered)
	}
	if version := priceVersion(filtered.candidates); version != "stream-price" {
		t.Fatalf("filtered price version = %q, want stream-price", version)
	}
	snapshot, err := harness.engine.dependencies.Snapshots.Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := llm.RequestDigest(normalized)
	if err != nil {
		t.Fatal(err)
	}
	operationID, scopeKey := operationIdentity(normalized)
	operation, existing, err := harness.engine.beginOrResume(context.Background(), normalized, snapshot, operationID, scopeKey, digest, filtered, harness.clock)
	if err != nil {
		t.Fatal(err)
	}
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
	adapter := &streamingAdapter{
		fakeAdapter: &fakeAdapter{name: "streaming", response: successfulResponse()},
		events: []provider.Event{
			provider.OutputStarted{Index: 0},
			provider.TextDelta{Index: 0, Text: "wo"},
			provider.TextDelta{Index: 0, Text: "rld"},
			provider.OutputFinished{Index: 0},
			provider.UsageUpdated{Usage: llm.Usage{OutputTokens: 1}},
			provider.StreamCompleted{},
		},
	}
	harness := newHarness(t, adapter)

	stream, err := harness.engine.Stream(context.Background(), baseRequest("ordered-stream"))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.Close()
	events := readTerminalStream(t, stream)

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
	adapter := &streamingAdapter{
		fakeAdapter: &fakeAdapter{name: "opaque-stream", response: successfulResponse()},
		events: []provider.Event{
			provider.OutputStarted{Index: 0},
			provider.ProviderStateDelta{Index: 0, State: llm.ProviderState{Provider: "provider-1", EndpointFamily: "openai_responses", MediaType: "application/octet-stream", Opaque: opaque}},
			provider.OutputFinished{Index: 0},
			provider.StreamCompleted{},
		},
	}
	harness := newHarness(t, adapter)

	stream, err := harness.engine.Stream(context.Background(), baseRequest("opaque-stream"))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	events := readTerminalStream(t, stream)

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
	response := events[len(events)-1].(llm.ResponseCompleted).Response
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
	adapter := &streamingAdapter{
		fakeAdapter: &fakeAdapter{name: "stream-error", response: successfulResponse()},
		events: []provider.Event{
			provider.OutputStarted{Index: 0},
			provider.StreamErrored{Err: provider.NewError(provider.CodeProviderUnavailable, provider.PhaseStream, provider.DispatchAccepted, provider.RetryNever, "safe provider failure")},
		},
	}
	harness := newHarness(t, adapter)

	stream, err := harness.engine.Stream(context.Background(), baseRequest("stream-error"))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.Close()
	events := readTerminalStream(t, stream)
	if _, ok := events[len(events)-1].(llm.StreamErrored); !ok {
		t.Fatalf("terminal event = %T, want StreamErrored", events[len(events)-1])
	}
	for _, event := range events[:len(events)-1] {
		if llm.IsTerminalEvent(event) {
			t.Fatalf("non-final event %T was terminal", event)
		}
	}
}

func TestStreamRejectsToolFragmentWithoutIdentityAtProviderBoundary(t *testing.T) {
	adapter := &streamingAdapter{
		fakeAdapter: &fakeAdapter{name: "deferred-tool-identity", response: successfulResponse()},
		events: []provider.Event{
			provider.OutputStarted{Index: 0},
			// provider.Assembler accepts this intermediate shape, but a streaming
			// adapter must not expose it until tool identity is known.
			provider.ToolArgumentsDelta{Index: 0, Fragment: `{"argument":`},
			provider.ToolArgumentsDelta{Index: 0, CallID: "call-1", Name: "lookup", Fragment: "true}"},
			provider.OutputFinished{Index: 0},
			provider.StreamCompleted{},
		},
	}
	harness := newHarness(t, adapter)

	stream, err := harness.engine.Stream(context.Background(), baseRequest("deferred-tool-identity"))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	events := readTerminalStream(t, stream)
	if _, ok := events[len(events)-1].(llm.StreamErrored); !ok {
		t.Fatalf("terminal event = %T, want StreamErrored", events[len(events)-1])
	}
	for _, event := range events {
		switch event.(type) {
		case llm.ToolCallStarted, llm.ToolArgumentsDelta:
			t.Fatalf("invalid provider fragment leaked public tool event %#v", event)
		}
	}
}

func TestStreamRejectsDuplicateProviderTerminal(t *testing.T) {
	adapter := &streamingAdapter{
		fakeAdapter: &fakeAdapter{name: "duplicate-terminal", response: successfulResponse()},
		events: []provider.Event{
			provider.OutputStarted{Index: 0},
			provider.TextDelta{Index: 0, Text: "world"},
			provider.OutputFinished{Index: 0},
			provider.StreamCompleted{},
			provider.StreamCompleted{},
		},
	}
	harness := newHarness(t, adapter)

	stream, err := harness.engine.Stream(context.Background(), baseRequest("duplicate-terminal"))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.Close()
	events := readTerminalStream(t, stream)
	if _, ok := events[len(events)-1].(llm.StreamErrored); !ok {
		t.Fatalf("terminal event = %T, want StreamErrored", events[len(events)-1])
	}
	for _, event := range events {
		if _, ok := event.(llm.ResponseCompleted); ok {
			t.Fatalf("duplicate provider terminal leaked successful terminal %#v", event)
		}
	}
}

func TestStreamReplaysCompletedTerminalWithoutProviderCall(t *testing.T) {
	adapter := &streamingAdapter{
		fakeAdapter: &fakeAdapter{name: "replay-stream", response: successfulResponse()},
		events: []provider.Event{
			provider.OutputStarted{Index: 0},
			provider.TextDelta{Index: 0, Text: "world"},
			provider.OutputFinished{Index: 0},
			provider.StreamCompleted{},
		},
	}
	harness := newHarness(t, adapter)
	request := baseRequest("stream-replay")

	first, err := harness.engine.Stream(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	firstEvents := readTerminalStream(t, first)
	_ = first.Close()
	second, err := harness.engine.Stream(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	secondEvents := readTerminalStream(t, second)
	_ = second.Close()
	firstResponse := firstEvents[len(firstEvents)-1].(llm.ResponseCompleted).Response
	secondResponse := secondEvents[len(secondEvents)-1].(llm.ResponseCompleted).Response
	if firstResponse.OperationID == "" || firstResponse.OperationID != secondResponse.OperationID {
		t.Fatalf("replayed response identity differs: first=%#v second=%#v", firstResponse, secondResponse)
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if adapter.invokes != 1 {
		t.Fatalf("provider stream opens = %d, want one", adapter.invokes)
	}
}

func TestStreamRetriesPreWriteRejectionOnFallback(t *testing.T) {
	adapter := &streamingAdapter{
		fakeAdapter: &fakeAdapter{name: "retry-stream", rejectFirst: true, response: successfulResponse()},
		events: []provider.Event{
			provider.OutputStarted{Index: 0},
			provider.TextDelta{Index: 0, Text: "world"},
			provider.OutputFinished{Index: 0},
			provider.StreamCompleted{},
		},
	}
	harness := newHarness(t, adapter)
	request := baseRequest("stream-retry")
	request.ServiceClass = llm.ServiceClassPriority
	request.ServiceClassFallbacks = []llm.ServiceClass{llm.ServiceClassStandard}

	stream, err := harness.engine.Stream(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	events := readTerminalStream(t, stream)
	response := events[len(events)-1].(llm.ResponseCompleted).Response
	if response.Service.Requested != llm.ServiceClassPriority || response.Service.Attempted != llm.ServiceClassStandard || response.Service.FallbackIndex != 1 {
		t.Fatalf("fallback response service = %#v", response.Service)
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if adapter.invokes != 2 {
		t.Fatalf("provider stream opens = %d, want two", adapter.invokes)
	}
}

func TestStreamRefusesAmbiguousPostWriteReplay(t *testing.T) {
	adapter := &streamingAdapter{fakeAdapter: &fakeAdapter{name: "ambiguous-stream", ambiguous: true, response: successfulResponse()}}
	harness := newHarness(t, adapter)
	request := baseRequest("stream-ambiguous")

	first, err := harness.engine.Stream(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	firstEvents := readTerminalStream(t, first)
	_ = first.Close()
	if _, ok := firstEvents[len(firstEvents)-1].(llm.StreamErrored); !ok {
		t.Fatalf("first terminal = %T, want StreamErrored", firstEvents[len(firstEvents)-1])
	}
	second, err := harness.engine.Stream(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	secondEvents := readTerminalStream(t, second)
	_ = second.Close()
	failure, ok := secondEvents[len(secondEvents)-1].(llm.StreamErrored)
	if !ok {
		t.Fatalf("second terminal = %T, want StreamErrored", secondEvents[len(secondEvents)-1])
	}
	var providerErr *provider.Error
	if !errors.As(failure.Err, &providerErr) || providerErr.Code != provider.CodeAmbiguousDispatch {
		t.Fatalf("second terminal error = %#v, want ambiguous dispatch", failure.Err)
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if adapter.invokes != 1 {
		t.Fatalf("provider stream opens = %d, want one", adapter.invokes)
	}
}

func TestStreamRejectsAcceptedResultWithoutDispatchObservation(t *testing.T) {
	adapter := &streamingAdapter{
		fakeAdapter:  &fakeAdapter{name: "unobserved-stream", response: successfulResponse()},
		skipObserver: true,
		events: []provider.Event{
			provider.OutputStarted{Index: 0},
			provider.OutputFinished{Index: 0},
			provider.StreamCompleted{},
		},
	}
	harness := newHarness(t, adapter)
	request := baseRequest("unobserved-stream")

	first, err := harness.engine.Stream(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	firstEvents := readTerminalStream(t, first)
	_ = first.Close()
	firstFailure, ok := firstEvents[len(firstEvents)-1].(llm.StreamErrored)
	if !ok {
		t.Fatalf("first terminal = %T, want StreamErrored", firstEvents[len(firstEvents)-1])
	}
	var firstProviderErr *provider.Error
	if !errors.As(firstFailure.Err, &firstProviderErr) || firstProviderErr.Dispatch != provider.DispatchAmbiguous {
		t.Fatalf("first terminal error = %#v, want ambiguous dispatch", firstFailure.Err)
	}

	second, err := harness.engine.Stream(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	secondEvents := readTerminalStream(t, second)
	_ = second.Close()
	secondFailure, ok := secondEvents[len(secondEvents)-1].(llm.StreamErrored)
	if !ok {
		t.Fatalf("second terminal = %T, want StreamErrored", secondEvents[len(secondEvents)-1])
	}
	var secondProviderErr *provider.Error
	if !errors.As(secondFailure.Err, &secondProviderErr) || secondProviderErr.Code != provider.CodeAmbiguousDispatch {
		t.Fatalf("second terminal error = %#v, want ambiguous replay refusal", secondFailure.Err)
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if adapter.invokes != 1 {
		t.Fatalf("provider stream opens = %d, want one", adapter.invokes)
	}
}

func TestStreamCancellationClosesProviderSourceAndTerminates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := newBlockingProviderSource()
	adapter := &streamingAdapter{fakeAdapter: &fakeAdapter{name: "cancel-stream", response: successfulResponse()}, source: source}
	harness := newHarness(t, adapter)

	stream, err := harness.engine.Stream(ctx, baseRequest("stream-cancel"))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	waitChannel(t, source.entered, "provider source read")
	cancel()
	events := readTerminalStream(t, stream)
	if _, ok := events[len(events)-1].(llm.StreamErrored); !ok {
		t.Fatalf("terminal event = %T, want StreamErrored", events[len(events)-1])
	}
	waitChannel(t, source.closed, "provider source close")
}

func TestStreamParentCancellationClosesProviderSourceThatIgnoresContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := newCloseDrivenProviderSource()
	adapter := &streamingAdapter{fakeAdapter: &fakeAdapter{name: "parent-cancel-close-driven", response: successfulResponse()}, source: source}
	harness := newHarness(t, adapter)

	stream, err := harness.engine.Stream(ctx, baseRequest("stream-parent-cancel-close-driven"))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	waitChannel(t, source.entered, "provider source read")
	cancel()
	events := readTerminalStream(t, stream)
	if _, ok := events[len(events)-1].(llm.StreamErrored); !ok {
		t.Fatalf("terminal event = %T, want StreamErrored", events[len(events)-1])
	}
	waitChannel(t, source.closed, "provider source close after parent cancellation")
}

func TestStreamCloseClosesProviderSourceThatIgnoresContext(t *testing.T) {
	source := newCloseDrivenProviderSource()
	adapter := &streamingAdapter{fakeAdapter: &fakeAdapter{name: "explicit-close-stream", response: successfulResponse()}, source: source}
	harness := newHarness(t, adapter)

	stream, err := harness.engine.Stream(context.Background(), baseRequest("stream-explicit-close"))
	if err != nil {
		t.Fatal(err)
	}
	emitter, ok := stream.(*streamEmitter)
	if !ok {
		t.Fatalf("stream type = %T, want *streamEmitter", stream)
	}
	waitChannel(t, source.entered, "provider source read")
	if err := stream.Close(); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	waitChannel(t, source.closed, "provider source close after explicit stream close")
	waitChannel(t, emitter.closed, "producer completion after explicit stream close")
}

func TestStreamCancellationBeforeDispatchClosesReservationWithoutProviderCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter := &streamingAdapter{fakeAdapter: &fakeAdapter{name: "pre-dispatch-cancel", response: successfulResponse()}}
	harness := newHarness(t, adapter)
	heartbeat := &cancelOnAdmissionHeartbeat{entered: make(chan struct{})}
	harness.engine.dependencies.Heartbeat = heartbeat

	stream, err := harness.engine.Stream(ctx, baseRequest("pre-dispatch-cancel"))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	waitChannel(t, heartbeat.entered, "admission heartbeat")
	cancel()
	events := readTerminalStream(t, stream)
	failure, ok := events[len(events)-1].(llm.StreamErrored)
	if !ok {
		t.Fatalf("terminal event = %T, want StreamErrored", events[len(events)-1])
	}
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
	adapter := &streamingAdapter{fakeAdapter: &fakeAdapter{name: "backpressure-stream", response: successfulResponse()}, source: source}
	harness := newHarness(t, adapter)
	stream, err := harness.engine.Stream(context.Background(), baseRequest("stream-backpressure"))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	waitChannel(t, source.bufferFilled, "stream buffer fill")
	select {
	case <-source.gateRequested:
		t.Fatal("engine pulled a provider event beyond the full public stream buffer")
	default:
	}
	if _, err := stream.Next(context.Background()); err != nil {
		t.Fatalf("drain one public event: %v", err)
	}
	waitChannel(t, source.gateRequested, "provider read after drain")
}

func TestStreamParentCancellationWithFullBufferReleasesProducerAndKeepsTerminal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := newGatedProviderSource(streamBufferCapacity)
	adapter := &streamingAdapter{fakeAdapter: &fakeAdapter{name: "cancel-full-buffer", response: successfulResponse()}, source: source}
	harness := newHarness(t, adapter)
	stream, err := harness.engine.Stream(ctx, baseRequest("cancel-full-buffer"))
	if err != nil {
		t.Fatal(err)
	}
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
	events := readTerminalStream(t, stream)
	if _, ok := events[len(events)-1].(llm.StreamErrored); !ok {
		t.Fatalf("terminal event = %T, want StreamErrored", events[len(events)-1])
	}
}

func TestStreamAndGenerateFinalizeEquivalentResponses(t *testing.T) {
	adapter := &streamingAdapter{
		fakeAdapter: &fakeAdapter{name: "equivalent-stream", response: successfulResponse()},
		events: []provider.Event{
			provider.OutputStarted{Index: 0},
			provider.TextDelta{Index: 0, Text: "world"},
			provider.OutputFinished{Index: 0},
			provider.StreamCompleted{Response: successfulResponse()},
		},
	}
	harness := newHarness(t, adapter)
	nonStream, err := harness.engine.Generate(context.Background(), baseRequest("equivalent-generate"))
	if err != nil {
		t.Fatal(err)
	}
	stream, err := harness.engine.Stream(context.Background(), baseRequest("equivalent-stream"))
	if err != nil {
		t.Fatal(err)
	}
	streamEvents := readTerminalStream(t, stream)
	_ = stream.Close()
	streamResponse := streamEvents[len(streamEvents)-1].(llm.ResponseCompleted).Response
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

type blockingProviderSource struct {
	entered   chan struct{}
	closed    chan struct{}
	enterOnce sync.Once
	closeOnce sync.Once
}

func newBlockingProviderSource() *blockingProviderSource {
	return &blockingProviderSource{entered: make(chan struct{}), closed: make(chan struct{})}
}

func (source *blockingProviderSource) Next(ctx context.Context) (provider.Event, error) {
	source.enterOnce.Do(func() { close(source.entered) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (source *blockingProviderSource) Close() error {
	source.closeOnce.Do(func() { close(source.closed) })
	return nil
}

// closeDrivenProviderSource deliberately ignores the read context. Closing
// the public stream must still release this provider body.
type closeDrivenProviderSource struct {
	entered   chan struct{}
	closed    chan struct{}
	enterOnce sync.Once
	closeOnce sync.Once
}

func newCloseDrivenProviderSource() *closeDrivenProviderSource {
	return &closeDrivenProviderSource{entered: make(chan struct{}), closed: make(chan struct{})}
}

func (source *closeDrivenProviderSource) Next(context.Context) (provider.Event, error) {
	source.enterOnce.Do(func() { close(source.entered) })
	<-source.closed
	return nil, errors.New("provider source was closed")
}

func (source *closeDrivenProviderSource) Close() error {
	source.closeOnce.Do(func() { close(source.closed) })
	return nil
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

func readTerminalStream(t *testing.T, stream llm.EventStream) []llm.Event {
	t.Helper()
	var events []llm.Event
	for {
		event, err := stream.Next(context.Background())
		if err == io.EOF {
			t.Fatal("stream ended before a terminal event")
		}
		if err != nil {
			t.Fatalf("read stream event: %v", err)
		}
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
