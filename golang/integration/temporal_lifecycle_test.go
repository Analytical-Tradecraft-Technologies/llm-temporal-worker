package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
	"github.com/mfow/llm-temporal-worker/golang/internal/httpserver"
	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	memory "github.com/mfow/llm-temporal-worker/golang/storage/memory"
	"github.com/prometheus/common/expfmt"
	"go.opentelemetry.io/otel/attribute"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
)

// captureRegistry is the smallest useful Temporal ActivityRegistry fake. It
// captures both the explicit name and the bound function so this test invokes
// the same Activity method that app.NewWorker installs without a live server.
type captureRegistry struct {
	mu       sync.Mutex
	name     string
	function any
}

func (registry *captureRegistry) RegisterActivity(function any) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.function = function
}

func (registry *captureRegistry) RegisterActivityWithOptions(function any, options sdkactivity.RegisterOptions) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.name = options.Name
	registry.function = function
}

func (*captureRegistry) RegisterDynamicActivity(any, sdkactivity.DynamicRegisterOptions) {}

var _ sdkworker.ActivityRegistry = (*captureRegistry)(nil)

type temporalController struct {
	startErr    error
	blockStop   bool
	started     chan struct{}
	stopEntered chan struct{}
	release     chan struct{}
	done        chan struct{}
	startOnce   sync.Once
	stopOnce    sync.Once
}

func newTemporalController(blockStop bool) *temporalController {
	controller := &temporalController{
		blockStop:   blockStop,
		started:     make(chan struct{}),
		stopEntered: make(chan struct{}),
		done:        make(chan struct{}),
	}
	if blockStop {
		controller.release = make(chan struct{})
	}
	return controller
}

func (controller *temporalController) Start() error {
	controller.startOnce.Do(func() { close(controller.started) })
	return controller.startErr
}

func (controller *temporalController) Stop() {
	controller.stopOnce.Do(func() {
		close(controller.stopEntered)
		if controller.release != nil {
			<-controller.release
		}
		close(controller.done)
	})
}

var _ app.WorkerController = (*temporalController)(nil)

type recordingAdapter struct {
	mu       sync.Mutex
	response llm.Response
	calls    int
	invokes  int
}

func (adapter *recordingAdapter) Name() string { return "offline-integration-adapter" }

func (*recordingAdapter) Capabilities(context.Context, provider.CapabilityQuery) (provider.CapabilitySet, error) {
	return provider.CapabilitySet{
		Version: "capabilities-1",
		Features: map[provider.Feature]provider.Capability{
			provider.FeatureText:      {State: provider.CapabilityNative},
			provider.FeatureStreaming: {State: provider.CapabilityUnsupported},
		},
	}, nil
}

func (*recordingAdapter) Compile(_ context.Context, input provider.CompileInput) (provider.Call, error) {
	return provider.Call{
		EndpointID:   input.Query.EndpointID,
		Family:       input.Query.Family,
		Model:        input.Query.Model,
		OperationKey: input.Request.OperationKey,
		ServiceClass: input.Query.ServiceClass,
		Metadata:     input.Metadata,
	}, nil
}

func (adapter *recordingAdapter) Invoke(ctx context.Context, _ provider.Call, observer provider.Observer) (provider.Result, error) {
	if err := observer.BeforePossibleWrite(ctx); err != nil {
		return provider.Result{}, err
	}
	adapter.mu.Lock()
	adapter.calls++
	adapter.invokes++
	response := adapter.response
	adapter.mu.Unlock()
	if err := observer.AfterResponseHeaders(ctx, provider.ResponseMetadata{RequestID: response.Provider.RequestID}); err != nil {
		return provider.Result{}, err
	}
	observer.OnProgress(ctx, provider.Progress{Phase: string(provider.PhaseLift), OutputItems: len(response.Output)})
	return provider.Result{Response: response}, nil
}

func (adapter *recordingAdapter) Calls() int {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	return adapter.calls
}

func (adapter *recordingAdapter) Invokes() int {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	return adapter.invokes
}

type recordingResultStore struct {
	mu     sync.Mutex
	values map[string]llm.Response
	puts   int
}

func (store *recordingResultStore) Get(_ context.Context, operationID string) (llm.Response, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	response, ok := store.values[operationID]
	if !ok {
		return llm.Response{}, engine.ErrResultNotFound
	}
	return response, nil
}

func (store *recordingResultStore) Put(_ context.Context, operationID string, response llm.Response) (state.BlobRef, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.puts++
	if _, exists := store.values[operationID]; !exists {
		store.values[operationID] = response
	}
	digest := sha256.Sum256([]byte(operationID))
	return state.BlobRef{Digest: digest, Size: int64(len(operationID)), Media: "application/json"}, nil
}

func (store *recordingResultStore) Puts() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.puts
}

type integrationEngine struct {
	engine  *engine.Engine
	adapter *recordingAdapter
	results *recordingResultStore
}

func newIntegrationEngine(t *testing.T) integrationEngine {
	t.Helper()
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	classes := []llm.ServiceClass{llm.ServiceClassEconomy, llm.ServiceClassStandard, llm.ServiceClassPriority}
	tiers := map[llm.ServiceClass]string{
		llm.ServiceClassEconomy:  "economy-tier",
		llm.ServiceClassStandard: "standard-tier",
		llm.ServiceClassPriority: "priority-tier",
	}
	routes, err := routing.CompileCatalog("routes-1", map[string]routing.Model{
		"logical-model": {Routes: []routing.Route{{
			ID: "route-1", EndpointID: "endpoint-1", Provider: "provider-1",
			Family: string(provider.FamilyOpenAIResponses), Region: "us-east-1", AccountRegion: "us-east-1",
			Model: "provider-model", ModelLineage: "provider-lineage", Classes: classes, ProviderTiers: tiers,
			PriceVersion: "prices-1", PriceAvailable: true,
			Capabilities: routing.CapabilitySet{Version: "route-cap-1", Features: map[routing.Feature]routing.Capability{
				routing.FeatureText: {State: routing.CapabilityNative},
			}},
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]pricing.Entry, 0, len(classes))
	for _, class := range classes {
		entries = append(entries, pricing.Entry{
			Provider: "provider-1", Family: string(provider.FamilyOpenAIResponses), EndpointID: "endpoint-1",
			Region: "us-east-1", Model: "provider-model", ProviderTier: tiers[class], Version: "prices-1",
			Prices: pricing.UnitPrices{PerRequest: pricing.MustDecimalUSD("0.000001"), OutputPerMillion: pricing.MustDecimalUSD("1")},
		})
	}
	priceCatalog, err := pricing.CompileUSD("prices-1", entries)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &recordingAdapter{response: llm.Response{
		Status:   llm.ResponseStatusCompleted,
		Output:   []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "offline response"}}}},
		Usage:    llm.Usage{OutputTokens: 1},
		Provider: llm.ProviderFacts{RequestID: "provider-request-1"},
	}}
	results := &recordingResultStore{values: make(map[string]llm.Response)}
	admissionStore := memory.NewAdmissionStore(memory.AdmissionOptions{Clock: func() time.Time { return now }})
	value, err := engine.New(engine.Dependencies{
		Snapshots: engine.StaticSnapshot{Value: engine.Snapshot{
			Version: "snapshot-1", Routes: routes, Prices: pricing.NewResolver(priceCatalog),
			ReservationLease: time.Minute, OperationRetention: time.Hour,
		}},
		Planner: routing.DeterministicPlanner{}, Adapters: engine.AdapterMap{"endpoint-1": adapter},
		Admission: admissionStore, Results: results, Clock: func() time.Time { return now },
		Estimator: budget.Estimator{MaxOutput: 1}, MaxAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	return integrationEngine{engine: value, adapter: adapter, results: results}
}

func integrationRequest(operationKey string) llm.Request {
	return llm.Request{
		OperationKey: operationKey,
		Context:      llm.RequestContext{Tenant: "tenant-1"},
		Model:        "logical-model",
		Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
			llm.TextPart{Text: "hello"},
		}}},
	}
}

func newWorker(t *testing.T, activities *activity.Activities, health *httpserver.HealthState, metrics *observability.Metrics, controller *temporalController, registry *captureRegistry) *app.TemporalWorker {
	t.Helper()
	worker, err := app.NewWorker(app.WorkerOptions{
		TaskQueue: "llmtw-integration", Identity: "integration-worker",
		MaxConcurrentActivities: 1, MaxConcurrentActivityTaskPolls: 1,
		GracefulStopTimeout: 50 * time.Millisecond, Activities: activities,
		Health: health, Metrics: metrics,
		Factory: func(_ client.Client, queue string, options sdkworker.Options) (app.WorkerController, sdkworker.ActivityRegistry, error) {
			if queue != "llmtw-integration" || options.MaxConcurrentActivityExecutionSize != 1 || options.MaxConcurrentActivityTaskPollers != 1 || options.WorkerStopTimeout != 50*time.Millisecond {
				return nil, nil, fmt.Errorf("unexpected Temporal worker options")
			}
			return controller, registry, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func TestTemporalWorkerLifecycleRegistrationReplayAndReadiness(t *testing.T) {
	engineValue := newIntegrationEngine(t)
	activities := &activity.Activities{Engine: engineValue.engine}
	health := httpserver.NewHealthState()
	metrics, err := observability.NewMetrics(observability.AllowedValues{
		Endpoints: []string{"endpoint-1"}, Models: []string{"provider-model"},
		Outcomes: []string{"success"}, Methods: []string{"provider"},
	})
	if err != nil {
		t.Fatal(err)
	}
	controller := newTemporalController(false)
	registry := &captureRegistry{}
	worker := newWorker(t, activities, health, metrics, controller, registry)
	if registry.name != activity.GenerateActivityName {
		t.Fatalf("registered Activity = %q, want %q", registry.name, activity.GenerateActivityName)
	}
	generate, ok := registry.function.(func(context.Context, activity.GenerateRequest) (*activity.GenerateResponse, error))
	if !ok {
		t.Fatalf("captured Activity has type %T, want pointer response handler", registry.function)
	}
	if health.Ready() || metricsPolling(t, metrics) {
		t.Fatal("worker became ready or started polling before Start")
	}
	if err := worker.Start(); err != nil {
		t.Fatal(err)
	}
	if !health.Ready() || !worker.Ready() || !metricsPolling(t, metrics) {
		t.Fatal("worker did not publish readiness and polling after Start")
	}

	payload := activity.GenerateRequest{APIVersion: activity.APIVersion, Request: integrationRequest("replay-operation")}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var decoded activity.GenerateRequest
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	first, err := generate(context.Background(), decoded)
	if err != nil {
		t.Fatal(err)
	}
	second, err := generate(context.Background(), decoded)
	if err != nil {
		t.Fatal(err)
	}
	if first == nil || second == nil {
		t.Fatalf("registered Activity responses = %#v and %#v, want success responses", first, second)
	}
	if first.Response.OperationID == "" || first.Response.OperationID != second.Response.OperationID {
		t.Fatalf("replay operation IDs = %q and %q", first.Response.OperationID, second.Response.OperationID)
	}
	if first.Response.Service.Requested != llm.ServiceClassStandard || first.Response.Service.Attempted != llm.ServiceClassStandard {
		t.Fatalf("omitted service class facts = %#v, want standard/standard", first.Response.Service)
	}
	if calls := engineValue.adapter.Calls(); calls != 1 {
		t.Fatalf("provider dispatches = %d, want one for replay", calls)
	}
	if invokes := engineValue.adapter.Invokes(); invokes != 1 {
		t.Fatalf("Activity invokes = %d, want one one-shot provider dispatch", invokes)
	}
	if puts := engineValue.results.Puts(); puts != 1 {
		t.Fatalf("result writes = %d, want one for replay", puts)
	}

	worker.Stop()
	if health.Ready() || metricsPolling(t, metrics) || !controllerWasStopped(controller) {
		t.Fatal("worker did not fail readiness and polling before Stop returned")
	}
}

func controllerWasStopped(controller *temporalController) bool {
	select {
	case <-controller.done:
		return true
	default:
		return false
	}
}

func metricsPolling(t *testing.T, metrics *observability.Metrics) bool {
	t.Helper()
	families, err := metrics.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	encoder := expfmt.NewEncoder(&output, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, family := range families {
		if err := encoder.Encode(family); err != nil {
			t.Fatal(err)
		}
	}
	return strings.Contains(output.String(), "llmtw_worker_polling 1") || strings.Contains(output.String(), "llmtw_worker_polling 1.0")
}

func TestTemporalShutdownBoundsGracefulStopAndFlushesTelemetry(t *testing.T) {
	engineValue := newIntegrationEngine(t)
	health := httpserver.NewHealthState()
	controller := newTemporalController(true)
	registry := &captureRegistry{}
	worker := newWorker(t, &activity.Activities{Engine: engineValue.engine}, health, nil, controller, registry)
	if err := worker.Start(); err != nil {
		t.Fatal(err)
	}
	if !health.Ready() {
		t.Fatal("worker did not become ready")
	}
	var eventsMu sync.Mutex
	var events []string
	appendEvent := func(event string) {
		eventsMu.Lock()
		defer eventsMu.Unlock()
		events = append(events, event)
	}
	coordinator, err := app.NewShutdownCoordinator(app.ShutdownOptions{
		Worker: worker, Health: health, Timeout: 25 * time.Millisecond,
		CloseApp:  func(context.Context) error { appendEvent("app.close"); return nil },
		Telemetry: []app.TelemetryFlusher{app.FlushFunc(func(context.Context) error { appendEvent("telemetry.flush"); return nil })},
	})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	shutdownErr := coordinator.Shutdown(context.Background())
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("bounded shutdown took %s", elapsed)
	}
	if shutdownErr == nil || !errors.Is(shutdownErr, context.DeadlineExceeded) {
		t.Fatalf("shutdown error = %v, want worker timeout", shutdownErr)
	}
	if health.Ready() {
		t.Fatal("readiness remained true after shutdown began")
	}
	eventsMu.Lock()
	gotEvents := append([]string(nil), events...)
	eventsMu.Unlock()
	if len(gotEvents) != 2 || gotEvents[0] != "app.close" || gotEvents[1] != "telemetry.flush" {
		t.Fatalf("shutdown follow-up events = %#v", gotEvents)
	}
	close(controller.release)
	select {
	case <-controller.done:
	case <-time.After(time.Second):
		t.Fatal("blocked Temporal worker did not finish after release")
	}
}

func TestObservabilityRejectsContentAndTenantMarkersAcrossSinks(t *testing.T) {
	const marker = "integration-secret-marker"
	var logs bytes.Buffer
	logger, err := observability.NewLogger(observability.LogOptions{Format: "json", Level: "debug", Output: &logs})
	if err != nil {
		t.Fatal(err)
	}
	providerErr := provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNextRoute, "safe failure")
	providerErr.Cause = errors.New(marker)
	logger.Info(context.Background(), "provider request accepted", slog.String("tenant", marker), slog.String("prompt", marker), slog.String("operation_id", "op-1"))
	logger.Error(context.Background(), "provider failure", providerErr)
	if strings.Contains(logs.String(), marker) || strings.Contains(logs.String(), `"prompt"`) || !strings.Contains(logs.String(), `"tenant_hash"`) {
		t.Fatalf("unsafe log output: %s", logs.String())
	}

	exporter := &observability.MemoryExporter{}
	tracer := observability.NewTracer(observability.TraceOptions{Enabled: true, Exporter: exporter})
	ctx, span := tracer.Start(context.Background(), "provider.attempt", attribute.String("tenant", marker), attribute.String("prompt", marker), attribute.String("operation_id", "op-1"))
	tracer.RecordError(span, providerErr)
	span.End()
	if err := tracer.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if spans := exporter.Spans(); len(spans) != 1 {
		t.Fatalf("trace span count = %d, want one", len(spans))
	} else {
		foundTenantHash := false
		for _, attr := range spans[0].Attributes() {
			if attr.Key == "tenant_hash" {
				foundTenantHash = true
			}
			if strings.Contains(attr.Value.AsString(), marker) || attr.Key == "prompt" {
				t.Fatalf("unsafe trace attribute: %#v", attr)
			}
		}
		if !foundTenantHash {
			t.Fatal("trace did not hash tenant")
		}
	}

	metrics, err := observability.NewMetrics(observability.AllowedValues{Endpoints: []string{"endpoint-1"}, Models: []string{"model-1"}, Outcomes: []string{"success"}, Methods: []string{"provider"}})
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordProviderAttempt(marker, marker, marker, marker, time.Second)
	metrics.RecordExactCost("endpoint-1", "model-1", "standard", marker)
	families, err := metrics.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var encoded strings.Builder
	textEncoder := expfmt.NewEncoder(&encoded, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, family := range families {
		if err := textEncoder.Encode(family); err != nil {
			t.Fatal(err)
		}
	}
	if strings.Contains(encoded.String(), marker) || !strings.Contains(encoded.String(), `endpoint="other"`) {
		t.Fatalf("unsafe metrics output: %s", encoded.String())
	}
}
