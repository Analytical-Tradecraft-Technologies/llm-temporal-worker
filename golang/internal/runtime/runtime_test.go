package runtime

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appactivity "github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func runtimeConfig(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	value := string(data)
	value = strings.Replace(value, "health_address: 0.0.0.0:8080", "health_address: 127.0.0.1:0", 1)
	value = strings.Replace(value, "metrics_address: 0.0.0.0:9090", "metrics_address: 127.0.0.1:0", 1)
	return []byte(value)
}

type testEngine struct{}

func (testEngine) Generate(context.Context, llm.Request) (llm.Response, error) {
	return llm.Response{}, errors.New("test engine")
}

var _ llm.Engine = testEngine{}

// testV1Runtime represents a composed durable implementation for lifecycle
// tests. The production runtime deliberately fails closed when this seam is
// omitted; tests that exercise worker startup must opt into the seam.
type testV1Runtime struct{}

func (testV1Runtime) GenerateV1(context.Context, llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
	return llm.GenerateResponseV1{}, nil
}

func (testV1Runtime) CompactV1(context.Context, llm.CompactRequestV1) (llm.CompactResponseV1, error) {
	return llm.CompactResponseV1{}, nil
}

func (testV1Runtime) QueryV1(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	return llm.QueryResponseV1{}, nil
}

var _ appactivity.V1Runtime = testV1Runtime{}

type testQueryService struct {
	response llm.QueryResponseV1
	called   atomic.Int32
}

func (service *testQueryService) Execute(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	service.called.Add(1)
	return service.response, nil
}

type testQueryClientSet struct {
	service appactivity.QueryService
	closed  atomic.Int32
}

func (clients *testQueryClientSet) Close(context.Context) error {
	clients.closed.Add(1)
	return nil
}

func (clients *testQueryClientSet) QueryService() appactivity.QueryService {
	return clients.service
}

var _ QueryServiceSource = (*testQueryClientSet)(nil)

type testV1ClientSet struct {
	runtime appactivity.V1Runtime
	closed  atomic.Int32
}

func (clients *testV1ClientSet) Close(context.Context) error {
	clients.closed.Add(1)
	return nil
}

func (clients *testV1ClientSet) V1Runtime() appactivity.V1Runtime {
	return clients.runtime
}

var _ V1RuntimeSource = (*testV1ClientSet)(nil)

type testQueryEngineFactory struct {
	service appactivity.QueryService
}

func (factory testQueryEngineFactory) Build(context.Context, *config.Snapshot) (llm.Engine, app.ClientSet, error) {
	return testEngine{}, app.ClientSetFunc(func(context.Context) error { return nil }), nil
}

func (factory testQueryEngineFactory) BuildQueryService(context.Context, *config.Snapshot) (appactivity.QueryService, error) {
	return factory.service, nil
}

var _ EngineFactory = testQueryEngineFactory{}
var _ QueryServiceFactory = testQueryEngineFactory{}

type testRegistry struct {
	names []string
}

func (testRegistry) RegisterActivity(interface{}) {}
func (registry *testRegistry) RegisterActivityWithOptions(_ interface{}, options activity.RegisterOptions) {
	registry.names = append(registry.names, options.Name)
}
func (testRegistry) RegisterDynamicActivity(interface{}, activity.DynamicRegisterOptions) {}

var _ worker.ActivityRegistry = (*testRegistry)(nil)

type testWorker struct {
	started atomic.Bool
	stopped atomic.Bool
}

func (worker *testWorker) Start() error {
	worker.started.Store(true)
	return nil
}

func (worker *testWorker) Stop() { worker.stopped.Store(true) }

func testRuntimeOptions(t *testing.T, workerController *testWorker, closed *atomic.Bool) Options {
	t.Helper()
	return Options{
		Resolver: config.ReferenceResolverFunc(func(context.Context, *config.Config) error { return nil }),
		TemporalFactory: TemporalClientFactoryFunc(func(_ context.Context, value config.Config) (client.Client, error) {
			return client.NewLazyClient(client.Options{HostPort: "127.0.0.1:1", Namespace: value.Temporal.Namespace})
		}),
		EngineFactory: EngineFactoryFunc(func(context.Context, *config.Snapshot) (llm.Engine, app.ClientSet, error) {
			return testEngine{}, app.ClientSetFunc(func(context.Context) error {
				closed.Store(true)
				return nil
			}), nil
		}),
		V1Runtime: testV1Runtime{},
		WorkerFactory: func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, worker.ActivityRegistry, error) {
			return workerController, &testRegistry{}, nil
		},
	}
}

func TestRuntimeMetricsSurfaceMatchesListenerConfiguration(t *testing.T) {
	tests := []struct {
		name              string
		configuration     func(*testing.T) []byte
		wantMetricsServer bool
		wantHealthStatus  int
	}{
		{
			name:             "shared listener exposes metrics",
			configuration:    runtimeConfig,
			wantHealthStatus: http.StatusOK,
		},
		{
			name: "distinct listener isolates metrics",
			configuration: func(t *testing.T) []byte {
				return []byte(strings.Replace(
					string(runtimeConfig(t)),
					"metrics_address: 127.0.0.1:0",
					"metrics_address: localhost:0",
					1,
				))
			},
			wantMetricsServer: true,
			wantHealthStatus:  http.StatusNotFound,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			controller := &testWorker{}
			var closed atomic.Bool
			runtime, err := New(context.Background(), test.configuration(t), testRuntimeOptions(t, controller, &closed))
			if err != nil {
				t.Fatal(err)
			}
			if err := runtime.Start(); err != nil {
				if strings.Contains(err.Error(), "operation not permitted") {
					t.Skipf("sandbox does not permit loopback listeners: %v", err)
				}
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })

			if got := runtime.MetricsServer != nil; got != test.wantMetricsServer {
				t.Fatalf("metrics server presence = %v, want %v", got, test.wantMetricsServer)
			}
			if got := runtimeHealthStatus(t, runtime.HealthServer.Addr(), "/metrics"); got != test.wantHealthStatus {
				t.Fatalf("health listener /metrics status = %d, want %d", got, test.wantHealthStatus)
			}
			if runtime.MetricsServer != nil {
				if got := runtimeHealthStatus(t, runtime.MetricsServer.Addr(), "/metrics"); got != http.StatusOK {
					t.Fatalf("metrics listener /metrics status = %d, want %d", got, http.StatusOK)
				}
			}
		})
	}
}

func TestRuntimeStartFailsClosedWithoutV1Runtime(t *testing.T) {
	controller := &testWorker{}
	var closed atomic.Bool
	options := testRuntimeOptions(t, controller, &closed)
	options.V1Runtime = nil
	runtime, err := New(context.Background(), runtimeConfig(t), options)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Run(context.Background()); !errors.Is(err, ErrV1RuntimeUnavailable) {
		t.Fatalf("Run error = %v, want ErrV1RuntimeUnavailable", err)
	}
	if controller.started.Load() || !closed.Load() || runtime.Health.Ready() {
		t.Fatalf("unconfigured runtime lifecycle started=%v closed=%v ready=%v", controller.started.Load(), closed.Load(), runtime.Health.Ready())
	}
}

func TestDevelopmentRuntimeAllowsFailClosedV1Runtime(t *testing.T) {
	controller := &testWorker{}
	var closed atomic.Bool
	options := testRuntimeOptions(t, controller, &closed)
	options.V1Runtime = nil
	data := []byte(strings.Replace(string(runtimeConfig(t)), "environment: production", "environment: development", 1))
	runtime, err := New(context.Background(), data, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(); err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("sandbox does not permit loopback listeners: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	if !controller.started.Load() || !runtime.Health.Ready() {
		t.Fatalf("development runtime did not start: started=%v ready=%v", controller.started.Load(), runtime.Health.Ready())
	}
}

func TestConcurrentRuntimeStartHasExactlyOneOwner(t *testing.T) {
	controller := &testWorker{}
	var closed atomic.Bool
	runtime, err := New(context.Background(), runtimeConfig(t), testRuntimeOptions(t, controller, &closed))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })

	const callers = 32
	start := make(chan struct{})
	errors := make(chan error, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	var attempting sync.WaitGroup
	attempting.Add(callers)

	// Queue every caller behind the existing lifecycle check. Once released,
	// a correct implementation records one starting owner before any network
	// or worker startup can run.
	runtime.mu.Lock()
	for range callers {
		go func() {
			ready.Done()
			<-start
			attempting.Done()
			errors <- runtime.Start()
		}()
	}
	ready.Wait()
	close(start)
	attempting.Wait()
	time.Sleep(20 * time.Millisecond)
	runtime.mu.Unlock()

	successes := 0
	for range callers {
		err := <-errors
		if err == nil {
			successes++
			continue
		}
		if !strings.Contains(err.Error(), "already started") {
			t.Fatalf("concurrent Start() error = %v, want already-started error", err)
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent Start() successes = %d, want exactly 1", successes)
	}
}

func TestRuntimeStartCanRetryAfterInitialBindFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("sandbox does not permit loopback listeners: %v", err)
		}
		t.Fatal(err)
	}
	data := strings.Replace(
		string(runtimeConfig(t)),
		"health_address: 127.0.0.1:0",
		"health_address: "+listener.Addr().String(),
		1,
	)
	controller := &testWorker{}
	var closed atomic.Bool
	runtime, err := New(context.Background(), []byte(data), testRuntimeOptions(t, controller, &closed))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })

	if err := runtime.Start(); err == nil || !strings.Contains(err.Error(), "listen for health server") {
		t.Fatalf("Start() error = %v, want wrapped listen error", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(); err != nil {
		t.Fatalf("Start() after failed bind error = %v, want retry to succeed", err)
	}
}

func TestDevelopmentRuntimeOmitsV1ActivitiesWithoutRuntime(t *testing.T) {
	activities := composeRuntimeActivities(config.Config{Environment: "development"}, testEngine{}, nil, nil, nil, &testQueryService{})
	if activities.V1Runtime != nil || activities.QueryService != nil {
		t.Fatalf("development fixture retained v1 seams: runtime=%T query=%T", activities.V1Runtime, activities.QueryService)
	}
	registry := &testRegistry{}
	activities.Register(registry)
	if len(registry.names) != 1 || registry.names[0] != appactivity.GenerateActivityName {
		t.Fatalf("development fixture registered activities = %v, want only %q", registry.names, appactivity.GenerateActivityName)
	}
}

func TestProductionCompositionDoesNotAdaptLegacyEngineToV1(t *testing.T) {
	// The legacy engine has one Generate(llm.Request) entry point. It cannot
	// safely implement the closed Generate/Compact/Query v1 contract: Compact
	// and Query have different durable state and accounting phases, while
	// checkpoint-aware Generate requires materialization before dispatch.
	// Keep this guard close to composition so a future refactor cannot silently
	// turn the legacy engine into a partial production runtime.
	activities := composeRuntimeActivities(config.Config{Environment: "production"}, testEngine{}, nil, nil, nil, nil)
	if activities == nil || activities.Engine == nil {
		t.Fatal("production composition lost the legacy engine used for direct helper tests")
	}
	if _, ok := activities.V1Runtime.(appactivity.UnconfiguredV1Runtime); !ok {
		t.Fatalf("production composition adapted legacy engine to V1Runtime: %T", activities.V1Runtime)
	}
	if isV1RuntimeConfigured(activities.V1Runtime) {
		t.Fatal("legacy engine was treated as a configured durable v1 runtime")
	}
}

func TestPhaseOnlyV1RuntimesNeverAdvertiseProductionReadiness(t *testing.T) {
	for _, runtime := range []appactivity.V1Runtime{
		&appactivity.GenerateOnlyV1Runtime{},
		&appactivity.CompactOnlyV1Runtime{},
	} {
		if isV1RuntimeConfigured(runtime) {
			t.Fatalf("phase-only runtime %T was treated as production configured", runtime)
		}
	}
}

func TestUnknownEnvironmentStillRequiresV1Runtime(t *testing.T) {
	if !v1RuntimeRequired(config.Config{Environment: "staging"}) {
		t.Fatal("staging environment must require durable v1 runtime")
	}
}

func TestNewFailsClosedWithoutProviderFactory(t *testing.T) {
	marker := "runtime-secret-marker"
	data := strings.Replace(string(runtimeConfig(t)), "OPENAI_API_KEY", marker, 1)
	_, err := New(context.Background(), []byte(data), Options{Resolver: config.ReferenceResolverFunc(func(context.Context, *config.Config) error { return nil })})
	if !errors.Is(err, ErrEngineFactoryUnavailable) {
		t.Fatalf("error = %v, want ErrEngineFactoryUnavailable", err)
	}
	if strings.Contains(err.Error(), marker) {
		t.Fatalf("error leaked configuration marker: %v", err)
	}
}

func TestMetricsAllowOneShotResponsePhaseButNotStreaming(t *testing.T) {
	metrics, err := newMetrics(config.Config{Telemetry: config.TelemetryConfig{Metrics: config.MetricsConfig{Enabled: true}}, Models: map[string]config.ModelConfig{
		"logical-model": {Routes: []config.RouteConfig{{Model: "provider-model"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordActivity("success", "none", time.Millisecond, "response_received")
	metrics.RecordActivity("completed", "none", time.Millisecond, "total")
	metrics.RecordActivity("success", "none", time.Millisecond, "streaming")
	metrics.RecordExactCost("endpoint", "model", "standard", "catalog_usage")
	metrics.RecordProviderAttempt("endpoint", "provider-model", "standard", "success", time.Millisecond)
	families, err := metrics.Gather()
	if err != nil {
		t.Fatal(err)
	}
	phases := make(map[string]struct{})
	for _, family := range families {
		if family.GetName() != "llmtw_activity_duration_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "phase" {
					phases[label.GetValue()] = struct{}{}
				}
			}
		}
	}
	if _, ok := phases["response_received"]; !ok {
		t.Fatalf("activity metric phases = %v, want response_received", phases)
	}
	if _, ok := phases["total"]; !ok {
		t.Fatalf("activity metric phases = %v, want total", phases)
	}
	if _, ok := phases["streaming"]; ok {
		t.Fatalf("activity metric phases = %v, must not allow streaming for the Temporal runtime", phases)
	}
	if _, ok := phases["other"]; !ok {
		t.Fatalf("activity metric phases = %v, want disallowed streaming to map to other", phases)
	}
	methods := make(map[string]struct{})
	for _, family := range families {
		if family.GetName() != "llmtw_cost_usd_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "method" {
					methods[label.GetValue()] = struct{}{}
				}
			}
		}
	}
	if _, ok := methods["catalog_usage"]; !ok {
		t.Fatalf("cost metric methods = %v, want catalog_usage", methods)
	}
	providerModels := make(map[string]struct{})
	for _, family := range families {
		if family.GetName() != "llmtw_provider_attempt_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "model" {
					providerModels[label.GetValue()] = struct{}{}
				}
			}
		}
	}
	if _, ok := providerModels["provider-model"]; !ok {
		t.Fatalf("provider metric models = %v, want provider-model", providerModels)
	}
}

func TestRuntimeActivitiesUseConfiguredHeartbeatKeepaliveInterval(t *testing.T) {
	configuration, err := config.Load(runtimeConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	configuration.Temporal.Worker.HeartbeatKeepaliveInterval = config.Duration(250 * time.Millisecond)
	activities := newRuntimeActivities(configuration, testEngine{}, nil, nil, nil)
	if got, want := activities.HeartbeatKeepaliveInterval, 250*time.Millisecond; got != want {
		t.Fatalf("Activity heartbeat keepalive interval = %s, want %s", got, want)
	}
}

func TestQueryServiceFromClientsPreservesSnapshotComposition(t *testing.T) {
	service := &testQueryService{}
	clients := &testQueryClientSet{service: service}
	got := queryServiceFromClients(clients)
	if got == nil {
		t.Fatal("query service source returned nil")
	}
	if _, err := got.Execute(context.Background(), llm.QueryRequestV1{}); err != nil {
		t.Fatalf("query service execution error = %v", err)
	}
	if service.called.Load() != 1 {
		t.Fatalf("query service calls = %d, want 1", service.called.Load())
	}
}

func TestComposeQueryServicePrefersSnapshotFactory(t *testing.T) {
	fromClients := &testQueryService{response: llm.QueryResponseV1{Source: "client"}}
	fromFactory := &testQueryService{response: llm.QueryResponseV1{Source: "factory"}}
	clients := &testQueryClientSet{service: fromClients}
	got, err := composeQueryService(context.Background(), nil, testQueryEngineFactory{service: fromFactory}, clients)
	if err != nil {
		t.Fatal(err)
	}
	response, err := got.Execute(context.Background(), llm.QueryRequestV1{})
	if err != nil || response.Source != "factory" {
		t.Fatalf("composed query response = %#v, error = %v", response, err)
	}
	if fromClients.called.Load() != 0 || fromFactory.called.Load() != 1 {
		t.Fatalf("query calls = client:%d factory:%d, want client:0 factory:1", fromClients.called.Load(), fromFactory.called.Load())
	}
}

func TestSnapshotQueryServiceFollowsAppReload(t *testing.T) {
	first := &testQueryService{response: llm.QueryResponseV1{Source: "first"}}
	second := &testQueryService{response: llm.QueryResponseV1{Source: "second"}}
	var builds atomic.Int32
	appInstance, err := app.New(context.Background(), app.Options{
		InitialConfig: runtimeConfig(t),
		Builder:       app.SnapshotBuilder{References: config.ReferenceResolverFunc(func(context.Context, *config.Config) error { return nil })},
		Clients: func(context.Context, *config.Snapshot) (app.ClientSet, error) {
			if builds.Add(1) == 1 {
				return &snapshotClients{clients: &testQueryClientSet{service: first}, queryService: first}, nil
			}
			return &snapshotClients{clients: &testQueryClientSet{service: second}, queryService: second}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = appInstance.Close(context.Background()) }()
	service := &snapshotQueryService{application: appInstance}
	response, err := service.Execute(context.Background(), llm.QueryRequestV1{})
	if err != nil || response.Source != "first" {
		t.Fatalf("initial query response = %#v, error = %v", response, err)
	}
	if err := appInstance.Reload(context.Background(), runtimeConfig(t)); err != nil {
		t.Fatal(err)
	}
	response, err = service.Execute(context.Background(), llm.QueryRequestV1{})
	if err != nil || response.Source != "second" {
		t.Fatalf("reloaded query response = %#v, error = %v", response, err)
	}
	if first.called.Load() != 1 || second.called.Load() != 1 {
		t.Fatalf("query calls = first:%d second:%d, want one each", first.called.Load(), second.called.Load())
	}
}

func TestSnapshotQueryServiceFailsClosedWithoutComposition(t *testing.T) {
	appInstance, err := app.New(context.Background(), app.Options{
		InitialConfig: runtimeConfig(t),
		Builder:       app.SnapshotBuilder{References: config.ReferenceResolverFunc(func(context.Context, *config.Config) error { return nil })},
		Clients: func(context.Context, *config.Snapshot) (app.ClientSet, error) {
			return &snapshotClients{clients: app.ClientSetFunc(func(context.Context) error { return nil })}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = appInstance.Close(context.Background()) }()
	_, err = (&snapshotQueryService{application: appInstance}).Execute(context.Background(), llm.QueryRequestV1{})
	if err == nil || !strings.Contains(err.Error(), "runtime query service is not configured") {
		t.Fatalf("unconfigured query error = %v", err)
	}
}

func TestFactoryErrorsDoNotLeakSecretText(t *testing.T) {
	marker := "provider-secret-marker"
	_, err := New(context.Background(), runtimeConfig(t), Options{
		Resolver: config.ReferenceResolverFunc(func(context.Context, *config.Config) error { return nil }),
		EngineFactory: EngineFactoryFunc(func(context.Context, *config.Snapshot) (llm.Engine, app.ClientSet, error) {
			return nil, nil, errors.New(marker)
		}),
	})
	if err == nil || strings.Contains(err.Error(), marker) {
		t.Fatalf("engine factory error = %v", err)
	}
	_, err = New(context.Background(), runtimeConfig(t), Options{
		Resolver: config.ReferenceResolverFunc(func(context.Context, *config.Config) error { return nil }),
		EngineFactory: EngineFactoryFunc(func(context.Context, *config.Snapshot) (llm.Engine, app.ClientSet, error) {
			return testEngine{}, nil, nil
		}),
		TemporalFactory: TemporalClientFactoryFunc(func(context.Context, config.Config) (client.Client, error) {
			return nil, errors.New(marker)
		}),
	})
	if err == nil || strings.Contains(err.Error(), marker) {
		t.Fatalf("Temporal factory error = %v", err)
	}
}

func TestRuntimeInitialDependencyFailureClosesUnpublishedSnapshot(t *testing.T) {
	probe := &mutableRuntimeProbe{}
	var closed atomic.Bool
	options := testRuntimeOptions(t, &testWorker{}, &closed)
	var temporalCalls atomic.Int32
	options.TemporalFactory = TemporalClientFactoryFunc(func(context.Context, config.Config) (client.Client, error) {
		temporalCalls.Add(1)
		return nil, errors.New("Temporal must not start before required dependencies")
	})
	options.DependencyProbes = []DependencyProbe{probe}
	runtime, err := New(context.Background(), runtimeConfig(t), options)
	if !errors.Is(err, errRequiredDependencyUnavailable) {
		t.Fatalf("initial dependency error = %v", err)
	}
	if runtime != nil || temporalCalls.Load() != 0 || !closed.Load() {
		t.Fatalf("initial dependency failure runtime=%#v temporal-calls=%d clients-closed=%v", runtime, temporalCalls.Load(), closed.Load())
	}
}

func TestRuntimeReloadDependencyFailureKeepsPriorSnapshot(t *testing.T) {
	probe := &mutableRuntimeProbe{}
	probe.healthy.Store(true)
	var created atomic.Int32
	var closed atomic.Int32
	options := testRuntimeOptions(t, &testWorker{}, &atomic.Bool{})
	options.EngineFactory = EngineFactoryFunc(func(context.Context, *config.Snapshot) (llm.Engine, app.ClientSet, error) {
		created.Add(1)
		return testEngine{}, app.ClientSetFunc(func(context.Context) error {
			closed.Add(1)
			return nil
		}), nil
	})
	options.DependencyProbes = []DependencyProbe{probe}
	runtime, err := New(context.Background(), runtimeConfig(t), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	old := runtime.App.Current()
	probe.healthy.Store(false)
	if err := runtime.App.Reload(context.Background(), runtimeConfig(t)); !errors.Is(err, errRequiredDependencyUnavailable) {
		t.Fatalf("reload dependency error = %v", err)
	}
	if runtime.App.Current() != old {
		t.Fatal("failed reload replaced the active runtime snapshot")
	}
	if created.Load() != 2 || closed.Load() != 1 {
		t.Fatalf("reload client lifecycle created=%d closed=%d", created.Load(), closed.Load())
	}
}

func TestRuntimeMonitorPausesPollingAndRestoresReadyHealth(t *testing.T) {
	probe := &mutableRuntimeProbe{}
	probe.healthy.Store(true)
	controller := &monitoringWorker{}
	var closed atomic.Bool
	options := testRuntimeOptions(t, &testWorker{}, &closed)
	options.WorkerFactory = func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, worker.ActivityRegistry, error) {
		return controller, &testRegistry{}, nil
	}
	options.DependencyProbes = []DependencyProbe{probe}
	runtime, err := New(context.Background(), runtimeMonitorConfig(t), options)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(); err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("sandbox does not permit loopback listeners: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	if !runtime.Health.Ready() || controller.starts.Load() != 1 {
		t.Fatalf("initial monitor state ready=%v starts=%d", runtime.Health.Ready(), controller.starts.Load())
	}
	probe.healthy.Store(false)
	waitForRuntime(t, func() bool { return !runtime.Health.Ready() && controller.stops.Load() >= 1 })
	if runtime.Health.Live() != true {
		t.Fatal("dependency failure changed liveness")
	}
	if got := runtimeHealthStatus(t, runtime.HealthServer.Addr(), "/health/live"); got != http.StatusOK {
		t.Fatalf("live status during dependency failure = %d", got)
	}
	if got := runtimeHealthStatus(t, runtime.HealthServer.Addr(), "/health/ready"); got != http.StatusServiceUnavailable {
		t.Fatalf("ready status during dependency failure = %d", got)
	}
	probe.healthy.Store(true)
	waitForRuntime(t, func() bool { return runtime.Health.Ready() && controller.starts.Load() >= 2 })
	if got := runtimeHealthStatus(t, runtime.HealthServer.Addr(), "/health/ready"); got != http.StatusOK {
		t.Fatalf("ready status after dependency recovery = %d", got)
	}
}

func TestStopDependencyMonitorSignalsCancellationBeforeReadinessHandoff(t *testing.T) {
	runtime := &Runtime{}
	runtime.readinessMu.Lock()
	done := make(chan struct{})
	canceled := make(chan struct{})
	runtime.mu.Lock()
	runtime.monitorCancel = func() { close(canceled) }
	runtime.monitorDone = done
	runtime.mu.Unlock()

	stopped := make(chan struct{})
	go func() {
		runtime.stopDependencyMonitor(context.Background())
		close(stopped)
	}()

	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("monitor cancellation was blocked by readiness handoff")
	}
	close(done)
	runtime.readinessMu.Unlock()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stopDependencyMonitor did not finish after readiness handoff")
	}
}

func TestRuntimeMonitorTracksReloadedV1RuntimeReadiness(t *testing.T) {
	probe := &mutableRuntimeProbe{}
	probe.healthy.Store(true)
	initial := &monitoringWorker{}
	replacement := &monitoringWorker{}
	var workerBuilds atomic.Int32
	var clientBuilds atomic.Int32
	options := testRuntimeOptions(t, &testWorker{}, &atomic.Bool{})
	options.V1Runtime = nil
	options.WorkerFactory = func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, worker.ActivityRegistry, error) {
		if workerBuilds.Add(1) == 1 {
			return initial, &testRegistry{}, nil
		}
		return replacement, &testRegistry{}, nil
	}
	options.EngineFactory = EngineFactoryFunc(func(context.Context, *config.Snapshot) (llm.Engine, app.ClientSet, error) {
		var v1Runtime appactivity.V1Runtime
		if clientBuilds.Add(1) != 2 {
			v1Runtime = testV1Runtime{}
		}
		return testEngine{}, &testV1ClientSet{runtime: v1Runtime}, nil
	})
	options.DependencyProbes = []DependencyProbe{probe}
	runtime, err := New(context.Background(), runtimeMonitorConfig(t), options)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(); err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("sandbox does not permit loopback listeners: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	if !runtime.Health.Ready() || initial.starts.Load() != 1 {
		t.Fatalf("initial runtime state ready=%v starts=%d", runtime.Health.Ready(), initial.starts.Load())
	}

	if err := runtime.App.Reload(context.Background(), runtimeMonitorConfig(t)); err != nil {
		t.Fatal(err)
	}
	waitForRuntime(t, func() bool { return !runtime.Health.Ready() && initial.stops.Load() >= 1 })
	if runtime.Health.Live() != true {
		t.Fatal("unconfigured v1 reload changed liveness")
	}
	if got := runtimeHealthStatus(t, runtime.HealthServer.Addr(), "/health/ready"); got != http.StatusServiceUnavailable {
		t.Fatalf("ready status after unconfigured v1 reload = %d", got)
	}

	if err := runtime.App.Reload(context.Background(), runtimeMonitorConfig(t)); err != nil {
		t.Fatal(err)
	}
	waitForRuntime(t, func() bool { return runtime.Health.Ready() && replacement.starts.Load() >= 1 })
	if got := runtimeHealthStatus(t, runtime.HealthServer.Addr(), "/health/ready"); got != http.StatusOK {
		t.Fatalf("ready status after configured v1 reload = %d", got)
	}
}

func TestRuntimeMonitorTracksReloadedV1ReadinessWithoutDependencyProbes(t *testing.T) {
	initial := &monitoringWorker{}
	var workerBuilds atomic.Int32
	var clientBuilds atomic.Int32
	options := testRuntimeOptions(t, &testWorker{}, &atomic.Bool{})
	options.V1Runtime = nil
	options.WorkerFactory = func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, worker.ActivityRegistry, error) {
		if workerBuilds.Add(1) == 1 {
			return initial, &testRegistry{}, nil
		}
		return &monitoringWorker{}, &testRegistry{}, nil
	}
	options.EngineFactory = EngineFactoryFunc(func(context.Context, *config.Snapshot) (llm.Engine, app.ClientSet, error) {
		var v1Runtime appactivity.V1Runtime
		if clientBuilds.Add(1) != 2 {
			v1Runtime = testV1Runtime{}
		}
		return testEngine{}, &testV1ClientSet{runtime: v1Runtime}, nil
	})
	runtime, err := New(context.Background(), runtimeMonitorConfig(t), options)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(); err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("sandbox does not permit loopback listeners: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	if !runtime.Health.Ready() || initial.starts.Load() != 1 {
		t.Fatalf("initial runtime state ready=%v starts=%d", runtime.Health.Ready(), initial.starts.Load())
	}

	if err := runtime.App.Reload(context.Background(), runtimeMonitorConfig(t)); err != nil {
		t.Fatal(err)
	}
	waitForRuntime(t, func() bool { return !runtime.Health.Ready() && initial.stops.Load() >= 1 })
	if runtime.Health.Live() != true {
		t.Fatal("unconfigured v1 reload changed liveness")
	}
	if got := runtimeHealthStatus(t, runtime.HealthServer.Addr(), "/health/ready"); got != http.StatusServiceUnavailable {
		t.Fatalf("ready status after unconfigured v1 reload without probes = %d", got)
	}
}

func TestRuntimeMonitorContinuesCheckingWhilePausedWorkerDrains(t *testing.T) {
	probe := &countingRuntimeProbe{}
	probe.healthy.Store(true)
	releaseStop := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseStop) }) }
	first := &drainingMonitoringWorker{
		stopEntered: make(chan struct{}, 1),
		releaseStop: releaseStop,
		stopExited:  make(chan struct{}, 1),
	}
	replacement := &monitoringWorker{}
	var built atomic.Int32
	var closed atomic.Bool
	options := testRuntimeOptions(t, &testWorker{}, &closed)
	options.WorkerFactory = func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, worker.ActivityRegistry, error) {
		if built.Add(1) == 1 {
			return first, &testRegistry{}, nil
		}
		return replacement, &testRegistry{}, nil
	}
	options.DependencyProbes = []DependencyProbe{probe}
	runtime, err := New(context.Background(), runtimeMonitorConfig(t), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	t.Cleanup(release)
	if err := runtime.Start(); err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("sandbox does not permit loopback listeners: %v", err)
		}
		t.Fatal(err)
	}
	if !runtime.Health.Ready() || first.starts.Load() != 1 {
		t.Fatalf("initial monitor state ready=%v starts=%d", runtime.Health.Ready(), first.starts.Load())
	}

	probe.healthy.Store(false)
	waitForRuntimeEvent(t, first.stopEntered, "first worker pause drain")
	waitForRuntime(t, func() bool { return !runtime.Health.Ready() && first.stops.Load() == 1 })

	healthyChecks := probe.healthyChecks.Load()
	probe.healthy.Store(true)
	waitForRuntime(t, func() bool { return probe.healthyChecks.Load() > healthyChecks })
	if runtime.Health.Ready() || replacement.starts.Load() != 0 || built.Load() != 1 {
		t.Fatalf("replacement started before pause drain completed: ready=%v replacement-starts=%d built=%d", runtime.Health.Ready(), replacement.starts.Load(), built.Load())
	}

	release()
	waitForRuntimeEvent(t, first.stopExited, "first worker pause drain completion")
	waitForRuntime(t, func() bool { return runtime.Health.Ready() && replacement.starts.Load() == 1 && built.Load() == 2 })
}

type mutableRuntimeProbe struct{ healthy atomic.Bool }

func (probe *mutableRuntimeProbe) Probe(context.Context) ProbeResult {
	if probe.healthy.Load() {
		return ProbeResult{Dependency: DependencyRedis, Status: ProbeStatusReady, Reason: ProbeReasonReady}
	}
	return ProbeResult{Dependency: DependencyRedis, Status: ProbeStatusUnavailable, Reason: ProbeReasonUnavailable}
}

type countingRuntimeProbe struct {
	healthy       atomic.Bool
	healthyChecks atomic.Int32
}

func (probe *countingRuntimeProbe) Probe(context.Context) ProbeResult {
	if probe.healthy.Load() {
		probe.healthyChecks.Add(1)
		return ProbeResult{Dependency: DependencyRedis, Status: ProbeStatusReady, Reason: ProbeReasonReady}
	}
	return ProbeResult{Dependency: DependencyRedis, Status: ProbeStatusUnavailable, Reason: ProbeReasonUnavailable}
}

type monitoringWorker struct {
	starts atomic.Int32
	stops  atomic.Int32
}

func (worker *monitoringWorker) Start() error {
	worker.starts.Add(1)
	return nil
}

func (worker *monitoringWorker) Stop() { worker.stops.Add(1) }

type drainingMonitoringWorker struct {
	starts      atomic.Int32
	stops       atomic.Int32
	stopEntered chan struct{}
	releaseStop <-chan struct{}
	stopExited  chan struct{}
}

func (worker *drainingMonitoringWorker) Start() error {
	worker.starts.Add(1)
	return nil
}

func (worker *drainingMonitoringWorker) Stop() {
	worker.stops.Add(1)
	select {
	case worker.stopEntered <- struct{}{}:
	default:
	}
	<-worker.releaseStop
	select {
	case worker.stopExited <- struct{}{}:
	default:
	}
}

func waitForRuntimeEvent(t *testing.T, event <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-event:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func runtimeMonitorConfig(t *testing.T) []byte {
	t.Helper()
	value := string(runtimeConfig(t))
	value = strings.Replace(value, "readiness_probe_interval: 5s", "readiness_probe_interval: 10ms", 1)
	value = strings.Replace(value, "readiness_probe_timeout: 2s", "readiness_probe_timeout: 5ms", 1)
	return []byte(value)
}

func waitForRuntime(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for runtime dependency transition")
}

func runtimeHealthStatus(t *testing.T, address, path string) int {
	t.Helper()
	response, err := (&http.Client{Timeout: time.Second}).Get("http://" + address + path)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	return response.StatusCode
}

func TestRuntimeStartsAndShutsDownInOrder(t *testing.T) {
	controller := &testWorker{}
	var closed atomic.Bool
	runtime, err := New(context.Background(), runtimeConfig(t), testRuntimeOptions(t, controller, &closed))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Health.Ready() {
		t.Fatal("runtime became ready before Start")
	}
	if err := runtime.Start(); err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("sandbox does not permit loopback listeners: %v", err)
		}
		t.Fatal(err)
	}
	if !controller.started.Load() || !runtime.Health.Ready() {
		t.Fatal("worker did not start or readiness did not transition")
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.Shutdown(shutdownContext); err != nil {
		t.Fatal(err)
	}
	if runtime.Health.Ready() || runtime.Health.Live() || !controller.stopped.Load() || !closed.Load() {
		t.Fatalf("shutdown state = ready %v live %v stopped %v closed %v", runtime.Health.Ready(), runtime.Health.Live(), controller.stopped.Load(), closed.Load())
	}
}

func TestRunTreatsCancellationAsGracefulExit(t *testing.T) {
	controller := &testWorker{}
	var closed atomic.Bool
	runtime, err := New(context.Background(), runtimeConfig(t), testRuntimeOptions(t, controller, &closed))
	if err != nil {
		t.Fatal(err)
	}
	// Probe listeners are intentionally real sockets. Some restricted test
	// sandboxes deny loopback binds; skip this lifecycle test there while the
	// listener behavior remains covered by internal/httpserver tests.
	if err := runtime.Start(); err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("sandbox does not permit loopback listeners: %v", err)
		}
		t.Fatal(err)
	}
	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	controller = &testWorker{}
	closed = atomic.Bool{}
	runtime, err = New(context.Background(), runtimeConfig(t), testRuntimeOptions(t, controller, &closed))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	deadline := time.After(2 * time.Second)
	for !runtime.Health.Ready() {
		select {
		case <-deadline:
			t.Fatal("worker did not become ready")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not shut down")
	}
	if runtime.Health.Live() || runtime.Health.Ready() || !controller.stopped.Load() || !closed.Load() {
		t.Fatal("cancellation did not complete graceful shutdown")
	}
}

func TestLoadTLSConfigPinsConfiguredCA(t *testing.T) {
	caPEM := testCertificateAuthorityPEM(t)
	tlsConfig, err := loadTLSConfig(config.TLSConfig{Enabled: true, CAFile: "/temporal-ca.pem", ServerName: "temporal.example.test"}, func(path string) ([]byte, error) {
		if path != "/temporal-ca.pem" {
			t.Fatalf("path = %q, want configured CA file", path)
		}
		return caPEM, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if tlsConfig.RootCAs == nil {
		t.Fatal("RootCAs is nil")
	}
	if subjects := tlsConfig.RootCAs.Subjects(); len(subjects) != 1 {
		t.Fatalf("RootCAs subjects = %d, want only configured CA", len(subjects))
	}
}

func testCertificateAuthorityPEM(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Temporal Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestLoadTLSConfigDoesNotExposeCertificateBytes(t *testing.T) {
	marker := "PRIVATE-CERT-MARKER"
	_, err := loadTLSConfig(config.TLSConfig{Enabled: true, CAFile: "/redacted"}, func(string) ([]byte, error) {
		return []byte(marker), nil
	})
	if err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("error = %v, want invalid certificate", err)
	}
	if strings.Contains(err.Error(), marker) {
		t.Fatalf("TLS error leaked certificate bytes: %v", err)
	}
}
