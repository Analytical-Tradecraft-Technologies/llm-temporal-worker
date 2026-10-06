package activity

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestGenerateActivityKeepsHeartbeatAliveDuringBlockedGenerate(t *testing.T) {
	ticker := newManualHeartbeatTicker()
	heartbeater := newRecordingHeartbeater()
	value := newBlockingGenerateEngine(testKeepaliveResponse())
	activities := Activities{
		Engine:                     value,
		Heartbeater:                heartbeater,
		HeartbeatKeepaliveInterval: time.Second,
		heartbeatTickerFactory: func(time.Duration) heartbeatTicker {
			return ticker
		},
	}

	done := make(chan generateActivityResult, 1)
	go func() {
		response, err := activities.Generate(context.Background(), testKeepalivePayload())
		done <- generateActivityResult{response: response, err: err}
	}()
	waitForKeepaliveEvent(t, value.started, "blocked Generate to start")

	// Keepalive facts bypass lifecycle de-duplication, so two identical
	// provider-wait facts remain two Temporal heartbeats while Generate blocks.
	ticker.Tick()
	heartbeater.WaitForPhase(t, heartbeatProviderWaitPhase, 1)
	ticker.Tick()
	heartbeater.WaitForPhase(t, heartbeatProviderWaitPhase, 2)

	close(value.release)
	result := waitForGenerateActivityResult(t, done)
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.response.Metadata.OperationID != "operation-id" {
		t.Fatalf("Activity response = %#v", result.response)
	}
	waitForKeepaliveEvent(t, ticker.stopped, "keepalive ticker to stop before Generate returns")

	for _, progress := range heartbeater.Snapshot() {
		if progress.Phase == "streaming" {
			t.Fatalf("one-shot Generate exposed streaming progress: %#v", heartbeater.Snapshot())
		}
		if progress.Phase == heartbeatProviderWaitPhase && (progress.OperationID != "" || progress.RouteIndex != 0 || progress.ClassIndex != 0 || progress.OutputItems != 0) {
			t.Fatalf("provider-wait keepalive contains non-lifecycle data: %#v", progress)
		}
	}
}

func TestGenerateActivityMapsKeepaliveFailureToAmbiguousAndJoins(t *testing.T) {
	ticker := newManualHeartbeatTicker()
	keepaliveErr := errors.New("heartbeat transport unavailable")
	heartbeater := newRecordingHeartbeater()
	heartbeater.failurePhase = heartbeatProviderWaitPhase
	heartbeater.failure = keepaliveErr
	value := newBlockingGenerateEngine(testKeepaliveResponse())
	activities := Activities{
		Engine:                     value,
		Heartbeater:                heartbeater,
		HeartbeatKeepaliveInterval: time.Second,
		heartbeatTickerFactory: func(time.Duration) heartbeatTicker {
			return ticker
		},
	}

	done := make(chan generateActivityResult, 1)
	go func() {
		response, err := activities.Generate(context.Background(), testKeepalivePayload())
		done <- generateActivityResult{response: response, err: err}
	}()
	waitForKeepaliveEvent(t, value.started, "blocked Generate to start")
	ticker.Tick()
	heartbeater.WaitForPhase(t, heartbeatProviderWaitPhase, 1)

	result := waitForGenerateActivityResult(t, done)
	if result.response.Metadata.OperationID != "" || result.response.Response.OperationID != "" {
		t.Fatalf("Activity response after keepalive failure = %#v, want zero response", result.response)
	}
	assertAmbiguousActivityError(t, result.err)
	if !errors.Is(value.ContextError(), context.Canceled) {
		t.Fatalf("Generate context error = %v, want context cancellation after keepalive failure", value.ContextError())
	}
	waitForKeepaliveEvent(t, ticker.stopped, "failed keepalive ticker to stop before Generate returns")
}

func TestGenerateActivityDoesNotReturnSuccessWhenInFlightKeepaliveFails(t *testing.T) {
	ticker := newManualHeartbeatTicker()
	keepaliveErr := errors.New("heartbeat transport failed after Generate completed")
	heartbeater := newGatedFailureHeartbeater(keepaliveErr)
	value := newBlockingGenerateEngine(testKeepaliveResponse())
	activities := Activities{
		Engine:                     value,
		Heartbeater:                heartbeater,
		HeartbeatKeepaliveInterval: time.Second,
		heartbeatTickerFactory: func(time.Duration) heartbeatTicker {
			return ticker
		},
	}

	done := make(chan generateActivityResult, 1)
	go func() {
		response, err := activities.Generate(context.Background(), testKeepalivePayload())
		done <- generateActivityResult{response: response, err: err}
	}()
	waitForKeepaliveEvent(t, value.started, "blocked Generate to start")
	ticker.Tick()
	waitForKeepaliveEvent(t, heartbeater.entered, "keepalive heartbeat to begin")

	// The provider can return successfully while a heartbeat transport call is
	// already in flight. Releasing the provider first makes the stop/join path
	// deterministic: a failed heartbeat must still make the Activity ambiguous.
	close(value.release)
	close(heartbeater.release)

	result := waitForGenerateActivityResult(t, done)
	if result.response.Metadata.OperationID != "" || result.response.Response.OperationID != "" {
		t.Fatalf("Activity response after in-flight keepalive failure = %#v, want zero response", result.response)
	}
	assertAmbiguousActivityError(t, result.err)
	waitForKeepaliveEvent(t, ticker.stopped, "in-flight keepalive ticker to stop before Generate returns")
}

func TestGenerateActivityIgnoresSelfStopCancellationFromInFlightKeepalive(t *testing.T) {
	ticker := newManualHeartbeatTicker()
	heartbeater := newGatedCancellationHeartbeater()
	value := newBlockingGenerateEngine(testKeepaliveResponse())
	activities := Activities{
		Engine:                     value,
		Heartbeater:                heartbeater,
		HeartbeatKeepaliveInterval: time.Second,
		heartbeatTickerFactory: func(time.Duration) heartbeatTicker {
			return ticker
		},
	}

	done := make(chan generateActivityResult, 1)
	go func() {
		response, err := activities.Generate(context.Background(), testKeepalivePayload())
		done <- generateActivityResult{response: response, err: err}
	}()
	waitForKeepaliveEvent(t, value.started, "blocked Generate to start")
	ticker.Tick()
	waitForKeepaliveEvent(t, heartbeater.entered, "keepalive heartbeat to begin")

	close(value.release)
	waitForKeepaliveEvent(t, heartbeater.canceled, "self-stop cancellation to reach in-flight heartbeat")
	close(heartbeater.release)

	result := waitForGenerateActivityResult(t, done)
	if result.err != nil {
		t.Fatalf("Generate error after self-stop heartbeat cancellation = %v", result.err)
	}
	if result.response.Metadata.OperationID != "operation-id" {
		t.Fatalf("Activity response after self-stop heartbeat cancellation = %#v", result.response)
	}
	waitForKeepaliveEvent(t, ticker.stopped, "self-stopped keepalive ticker to stop before Generate returns")
}

func TestGenerateActivityCancellationStopsKeepalive(t *testing.T) {
	ticker := newManualHeartbeatTicker()
	heartbeater := newRecordingHeartbeater()
	value := newBlockingGenerateEngine(testKeepaliveResponse())
	activities := Activities{
		Engine:                     value,
		Heartbeater:                heartbeater,
		HeartbeatKeepaliveInterval: time.Second,
		heartbeatTickerFactory: func(time.Duration) heartbeatTicker {
			return ticker
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan generateActivityResult, 1)
	go func() {
		response, err := activities.Generate(ctx, testKeepalivePayload())
		done <- generateActivityResult{response: response, err: err}
	}()
	waitForKeepaliveEvent(t, value.started, "blocked Generate to start")
	cancel()

	result := waitForGenerateActivityResult(t, done)
	if !errors.Is(result.err, context.Canceled) {
		t.Fatalf("Generate error after cancellation = %v, want context cancellation", result.err)
	}
	waitForKeepaliveEvent(t, ticker.stopped, "canceled keepalive ticker to stop before Generate returns")
}

func TestGenerateActivityStopsKeepaliveWhenGeneratePanics(t *testing.T) {
	ticker := newManualHeartbeatTicker()
	heartbeater := newRecordingHeartbeater()
	panicValue := errors.New("provider fixture panic")
	value := newPanickingGenerateEngine(panicValue)
	activities := Activities{
		Engine:                     value,
		Heartbeater:                heartbeater,
		HeartbeatKeepaliveInterval: time.Second,
		heartbeatTickerFactory: func(time.Duration) heartbeatTicker {
			return ticker
		},
	}

	panicked := make(chan any, 1)
	go func() {
		defer func() { panicked <- recover() }()
		_, _ = activities.Generate(context.Background(), testKeepalivePayload())
	}()
	waitForKeepaliveEvent(t, value.started, "panicking Generate to start")
	close(value.release)

	select {
	case got := <-panicked:
		if got != panicValue {
			t.Fatalf("Generate panic = %#v, want %#v", got, panicValue)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Generate panic")
	}
	// stop waits for the keepalive goroutine, whose deferred ticker stop runs
	// before Generate can re-panic to its caller.
	waitForKeepaliveEvent(t, ticker.stopped, "panic-path keepalive ticker to stop")
}

type generateActivityResult struct {
	response GenerateResponse
	err      error
}

type blockingGenerateEngine struct {
	response llm.Response
	started  chan struct{}
	release  chan struct{}

	mu         sync.Mutex
	contextErr error
}

func newBlockingGenerateEngine(response llm.Response) *blockingGenerateEngine {
	return &blockingGenerateEngine{response: response, started: make(chan struct{}, 1), release: make(chan struct{})}
}

func (engine *blockingGenerateEngine) Generate(ctx context.Context, _ llm.Request) (llm.Response, error) {
	engine.started <- struct{}{}
	select {
	case <-engine.release:
		return engine.response, nil
	case <-ctx.Done():
		engine.mu.Lock()
		engine.contextErr = ctx.Err()
		engine.mu.Unlock()
		return llm.Response{}, ctx.Err()
	}
}

func (engine *blockingGenerateEngine) ContextError() error {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	return engine.contextErr
}

var _ llm.Engine = (*blockingGenerateEngine)(nil)

type panickingGenerateEngine struct {
	started    chan struct{}
	release    chan struct{}
	panicValue any
}

func newPanickingGenerateEngine(panicValue any) *panickingGenerateEngine {
	return &panickingGenerateEngine{started: make(chan struct{}, 1), release: make(chan struct{}), panicValue: panicValue}
}

func (engine *panickingGenerateEngine) Generate(context.Context, llm.Request) (llm.Response, error) {
	engine.started <- struct{}{}
	<-engine.release
	panic(engine.panicValue)
}

var _ llm.Engine = (*panickingGenerateEngine)(nil)

type manualHeartbeatTicker struct {
	ticks   chan time.Time
	stopped chan struct{}
	once    sync.Once
}

func newManualHeartbeatTicker() *manualHeartbeatTicker {
	return &manualHeartbeatTicker{ticks: make(chan time.Time, 2), stopped: make(chan struct{})}
}

func (ticker *manualHeartbeatTicker) C() <-chan time.Time { return ticker.ticks }

func (ticker *manualHeartbeatTicker) Stop() {
	ticker.once.Do(func() { close(ticker.stopped) })
}

func (ticker *manualHeartbeatTicker) Tick() {
	ticker.ticks <- time.Now()
}

type recordingHeartbeater struct {
	mu           sync.Mutex
	progress     []engine.Progress
	failurePhase string
	failure      error
	notified     chan struct{}
}

func newRecordingHeartbeater() *recordingHeartbeater {
	return &recordingHeartbeater{notified: make(chan struct{}, 1)}
}

func (heartbeater *recordingHeartbeater) Beat(ctx context.Context, progress engine.Progress) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	heartbeater.mu.Lock()
	heartbeater.progress = append(heartbeater.progress, progress)
	failure := heartbeater.failure
	if progress.Phase != heartbeater.failurePhase {
		failure = nil
	}
	heartbeater.mu.Unlock()
	select {
	case heartbeater.notified <- struct{}{}:
	default:
	}
	return failure
}

func (heartbeater *recordingHeartbeater) Snapshot() []engine.Progress {
	heartbeater.mu.Lock()
	defer heartbeater.mu.Unlock()
	return append([]engine.Progress(nil), heartbeater.progress...)
}

func (heartbeater *recordingHeartbeater) WaitForPhase(t *testing.T, phase string, count int) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		matched := 0
		for _, progress := range heartbeater.Snapshot() {
			if progress.Phase == phase {
				matched++
			}
		}
		if matched >= count {
			return
		}
		select {
		case <-heartbeater.notified:
		case <-deadline.C:
			t.Fatalf("timed out waiting for %d %q heartbeat(s), got %#v", count, phase, heartbeater.Snapshot())
		}
	}
}

var _ Heartbeater = (*recordingHeartbeater)(nil)

type gatedFailureHeartbeater struct {
	entered chan struct{}
	release chan struct{}
	failure error
}

func newGatedFailureHeartbeater(failure error) *gatedFailureHeartbeater {
	return &gatedFailureHeartbeater{entered: make(chan struct{}, 1), release: make(chan struct{}), failure: failure}
}

func (heartbeater *gatedFailureHeartbeater) Beat(_ context.Context, progress engine.Progress) error {
	if progress.Phase != heartbeatProviderWaitPhase {
		return nil
	}
	heartbeater.entered <- struct{}{}
	<-heartbeater.release
	return heartbeater.failure
}

var _ Heartbeater = (*gatedFailureHeartbeater)(nil)

type gatedCancellationHeartbeater struct {
	entered  chan struct{}
	canceled chan struct{}
	release  chan struct{}
}

func newGatedCancellationHeartbeater() *gatedCancellationHeartbeater {
	return &gatedCancellationHeartbeater{entered: make(chan struct{}, 1), canceled: make(chan struct{}, 1), release: make(chan struct{})}
}

func (heartbeater *gatedCancellationHeartbeater) Beat(ctx context.Context, progress engine.Progress) error {
	if progress.Phase != heartbeatProviderWaitPhase {
		return nil
	}
	heartbeater.entered <- struct{}{}
	<-ctx.Done()
	heartbeater.canceled <- struct{}{}
	<-heartbeater.release
	return ctx.Err()
}

var _ Heartbeater = (*gatedCancellationHeartbeater)(nil)

func testKeepalivePayload() GenerateRequest {
	return GenerateRequest{APIVersion: APIVersion, Request: llm.Request{OperationKey: "operation-1", Model: "model-1", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}}}
}

func testKeepaliveResponse() llm.Response {
	return llm.Response{OperationKey: "operation-1", OperationID: "operation-id", Status: llm.ResponseStatusCompleted, Service: llm.ServiceFacts{Requested: llm.ServiceClassStandard, Attempted: llm.ServiceClassStandard}}
}

func waitForKeepaliveEvent(t *testing.T, event <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-event:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func waitForGenerateActivityResult(t *testing.T, done <-chan generateActivityResult) generateActivityResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Generate")
		return generateActivityResult{}
	}
}
