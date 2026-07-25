// Package runtime composes the validated configuration, Temporal worker,
// probe servers, and reloadable application clients. It deliberately keeps
// provider construction behind EngineFactory: a process must not start with a
// misleading ready state when its provider/state implementation is absent.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
	"github.com/mfow/llm-temporal-worker/golang/internal/httpserver"
	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"go.temporal.io/sdk/client"
)

var (
	// ErrEngineFactoryUnavailable is returned when the process has not been
	// given a provider/state-backed engine factory. Failing before worker start
	// is safer than accepting Activities that can never perform inference.
	ErrEngineFactoryUnavailable = errors.New("provider-backed engine factory is not configured")
	// ErrV1RuntimeUnavailable is returned before listeners or Temporal polling
	// start when the process has not been given the durable v1 implementation.
	// The Activity adapter remains fail-closed as a second line of defense, but
	// a process that advertises the v1 Activity names must not report readiness
	// while every invocation would return a permanent configuration error.
	ErrV1RuntimeUnavailable = errors.New("durable v1 runtime is not configured")
	// ErrTemporalClientUnavailable is returned when the Temporal factory gives
	// runtime an unusable client.
	ErrTemporalClientUnavailable = errors.New("Temporal client factory returned no client")
)

// EngineFactory constructs the provider-neutral engine and the clients owned
// by one immutable configuration snapshot. The returned ClientSet must not
// retain secret values in errors or logs.
type EngineFactory interface {
	Build(context.Context, *config.Snapshot) (llm.Engine, app.ClientSet, error)
}

// QueryServiceFactory is an optional companion to EngineFactory. A factory
// that owns the durable control-plane repositories can implement this seam to
// publish a query service alongside the engine for each immutable snapshot.
// Keeping it optional preserves the fail-closed behaviour while allowing
// query state and authorization clients to be replaced atomically on reload.
// The returned service must not retain references to a previous snapshot.
type QueryServiceFactory interface {
	BuildQueryService(context.Context, *config.Snapshot) (activity.QueryService, error)
}

// QueryServiceSource lets a ClientSet expose a query service it constructed
// with the same snapshot as its engine. It is intentionally small so custom
// composition can opt in without coupling the app package to control-plane
// storage types.
type QueryServiceSource interface {
	QueryService() activity.QueryService
}

// V1RuntimeSource exposes the durable one-shot implementation built for one
// immutable configuration snapshot. A source is intentionally attached to
// ClientSet rather than EngineFactory: reload constructs a new client set,
// and the runtime must never dispatch a request through a previous snapshot's
// closed state clients.
type V1RuntimeSource interface {
	V1Runtime() activity.V1Runtime
}

// EngineFactoryFunc adapts a function to EngineFactory.
type EngineFactoryFunc func(context.Context, *config.Snapshot) (llm.Engine, app.ClientSet, error)

func (function EngineFactoryFunc) Build(ctx context.Context, snapshot *config.Snapshot) (llm.Engine, app.ClientSet, error) {
	return function(ctx, snapshot)
}

// UnavailableEngineFactory deliberately models missing provider/state wiring
// for tests and custom embeddings. The CLI uses ProductionEngineFactory;
// retaining this implementation keeps the injectable fail-closed seam small.
type UnavailableEngineFactory struct{}

func (UnavailableEngineFactory) Build(context.Context, *config.Snapshot) (llm.Engine, app.ClientSet, error) {
	return nil, nil, ErrEngineFactoryUnavailable
}

// Options controls process composition. TemporalFactory and EngineFactory are
// injectable so package tests never require credentials or a Temporal cluster.
type Options struct {
	Resolver       config.ReferenceResolver
	SecretResolver secrets.Resolver

	TemporalFactory TemporalClientFactory
	EngineFactory   EngineFactory
	// V1Runtime supplies the durable implementation behind the one-shot v1
	// Activity boundary. Production must provide this before Start; leaving it
	// unset keeps construction inspectable but fails closed at startup.
	V1Runtime     activity.V1Runtime
	WorkerFactory app.WorkerFactory
	// DependencyProbes supplements production snapshot probes for embeddings
	// and tests. ProductionEngineFactory contributes Redis and bucket probes
	// through its ClientSet; provider endpoints are never probes.
	DependencyProbes []DependencyProbe

	Health  *httpserver.HealthState
	Metrics *observability.Metrics
	Tracer  *observability.Tracer
	Logger  *observability.Logger

	TraceExporterFactory TraceExporterFactory
	LogOutput            io.Writer

	// Identity overrides the generated Temporal identity.
	Identity string
}

// Runtime owns all process resources created from one initial configuration.
// Reloading the App swaps engines while the worker's dynamic engine lease
// keeps in-flight calls on their captured snapshot.
type Runtime struct {
	App      *app.App
	Temporal client.Client
	Worker   *app.TemporalWorker
	Health   *httpserver.HealthState
	Metrics  *observability.Metrics
	Tracer   *observability.Tracer
	Logger   *observability.Logger

	HealthServer  *httpserver.Server
	MetricsServer *httpserver.Server

	shutdown *app.ShutdownCoordinator
	timeout  time.Duration

	readinessProbeInterval time.Duration
	readinessProbeTimeout  time.Duration
	v1RuntimeConfigured    bool
	monitorCancel          context.CancelFunc
	monitorDone            chan struct{}

	mu      sync.Mutex
	started bool
}

// New validates and composes a runtime. It performs no provider calls when
// EngineFactory is unavailable; the returned error is safe to show in CLI
// output and contains neither config contents nor resolved secret values.
func New(ctx context.Context, data []byte, options Options) (*Runtime, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(data) == 0 {
		return nil, errors.New("runtime configuration is required")
	}
	if options.EngineFactory == nil {
		return nil, ErrEngineFactoryUnavailable
	}
	references := options.Resolver
	if references == nil {
		secretResolver := options.SecretResolver
		if secretResolver == nil {
			secretResolver = secrets.New(secrets.Options{})
		}
		references = secrets.ConfigResolver{Resolver: secretResolver}
	}
	builder := app.SnapshotBuilder{References: references}
	var engineFactory = options.EngineFactory
	configuredProbes := append([]DependencyProbe(nil), options.DependencyProbes...)
	application, err := app.New(ctx, app.Options{
		InitialConfig: data,
		Builder:       builder,
		Clients: func(buildContext context.Context, snapshot *config.Snapshot) (app.ClientSet, error) {
			engine, clients, err := engineFactory.Build(buildContext, snapshot)
			if err != nil {
				return nil, &engineFactoryError{cause: err}
			}
			if engine == nil {
				return nil, errors.New("engine factory returned no engine")
			}
			probes := append([]DependencyProbe(nil), configuredProbes...)
			if source, ok := clients.(dependencyProbeSource); ok {
				probes = append(probes, source.DependencyProbes()...)
			}
			v1Runtime := options.V1Runtime
			if source, ok := clients.(V1RuntimeSource); ok {
				// A source is authoritative even when it returns nil. This keeps a
				// reload from falling back to a runtime that is bound to a previous
				// snapshot and may already be draining.
				v1Runtime = source.V1Runtime()
			}
			queryService, err := composeQueryService(buildContext, snapshot, engineFactory, clients)
			if err != nil {
				if clients != nil {
					_ = clients.Close(context.Background())
				}
				return nil, &engineFactoryError{cause: err}
			}
			return &snapshotClients{engine: engine, clients: clients, probes: probes, queryService: queryService, v1Runtime: v1Runtime}, nil
		},
		Verify: verifySnapshotDependencies,
	})
	if err != nil {
		var factoryError *engineFactoryError
		if errors.As(err, &factoryError) {
			return nil, fmt.Errorf("construct provider-backed engine: %w", unwrapEngineFactoryError(err))
		}
		return nil, err
	}
	configuration := application.Current().Config.Config()
	temporalFactory := options.TemporalFactory
	if temporalFactory == nil {
		temporalFactory = DefaultTemporalClientFactory{Identity: options.Identity}
	}
	temporalClient, err := temporalFactory.New(ctx, configuration)
	if err != nil {
		_ = application.Close(context.Background())
		return nil, safeTemporalFactoryError(err)
	}
	if temporalClient == nil {
		_ = application.Close(context.Background())
		return nil, ErrTemporalClientUnavailable
	}
	health := options.Health
	if health == nil {
		health = httpserver.NewHealthState()
	}
	metrics, tracer, logger, err := newRuntimeTelemetry(ctx, configuration, options)
	if err != nil {
		temporalClient.Close()
		_ = application.Close(context.Background())
		return nil, err
	}
	dynamic := &snapshotEngine{application: application}
	identity := options.Identity
	if identity == "" {
		identity = (DefaultTemporalClientFactory{}).identity(configuration.Temporal.IdentityPrefix)
	}
	initialV1Runtime := v1RuntimeForSnapshot(application.Current(), options.V1Runtime)
	var workerV1Runtime activity.V1Runtime
	if isV1RuntimeConfigured(initialV1Runtime) {
		workerV1Runtime = &snapshotV1Runtime{application: application, fallback: options.V1Runtime}
	}
	activities := composeRuntimeActivities(configuration, dynamic, metrics, tracer, workerV1Runtime, &snapshotQueryService{application: application, fallback: workerV1Runtime})
	worker, err := app.NewWorker(app.WorkerOptions{
		Client:                         temporalClient,
		TaskQueue:                      configuration.Temporal.TaskQueue,
		Identity:                       identity,
		MaxConcurrentActivities:        configuration.Temporal.Worker.MaxConcurrentActivities,
		MaxConcurrentActivityTaskPolls: configuration.Temporal.Worker.MaxConcurrentActivityTaskPolls,
		GracefulStopTimeout:            time.Duration(configuration.Temporal.Worker.GracefulStopTimeout),
		Activities:                     activities,
		Health:                         health,
		Metrics:                        metrics,
		Factory:                        options.WorkerFactory,
	})
	if err != nil {
		_ = tracer.Shutdown(context.Background())
		temporalClient.Close()
		_ = application.Close(context.Background())
		return nil, err
	}
	healthServer, err := httpserver.New(httpserver.Options{
		Address: configuration.Server.HealthAddress,
		Health:  health,
		Metrics: metrics.Handler(),
	})
	if err != nil {
		_ = tracer.Shutdown(context.Background())
		temporalClient.Close()
		_ = application.Close(context.Background())
		return nil, fmt.Errorf("construct health server: %w", err)
	}
	var metricsServer *httpserver.Server
	if configuration.Server.MetricsAddress != configuration.Server.HealthAddress {
		metricsServer, err = httpserver.New(httpserver.Options{
			Address: configuration.Server.MetricsAddress,
			Health:  health,
			Metrics: metrics.Handler(),
		})
		if err != nil {
			_ = tracer.Shutdown(context.Background())
			temporalClient.Close()
			_ = application.Close(context.Background())
			return nil, fmt.Errorf("construct metrics server: %w", err)
		}
	}
	timeout := time.Duration(configuration.Server.ShutdownTimeout)
	coordinator, err := app.NewShutdownCoordinator(app.ShutdownOptions{
		Worker: worker,
		Health: health,
		CloseApp: func(closeContext context.Context) error {
			return closeResources(closeContext, application, temporalClient, healthServer, metricsServer, health)
		},
		Telemetry: []app.TelemetryFlusher{
			app.FlushFunc(tracer.Shutdown),
			logger,
		},
		Timeout: timeout,
	})
	if err != nil {
		_ = tracer.Shutdown(context.Background())
		temporalClient.Close()
		_ = application.Close(context.Background())
		return nil, err
	}
	return &Runtime{
		App: application, Temporal: temporalClient, Worker: worker,
		Health: health, Metrics: metrics, Tracer: tracer, Logger: logger,
		HealthServer: healthServer, MetricsServer: metricsServer,
		shutdown: coordinator, timeout: timeout,
		readinessProbeInterval: time.Duration(configuration.Server.ReadinessProbeInterval),
		readinessProbeTimeout:  time.Duration(configuration.Server.ReadinessProbeTimeout),
		v1RuntimeConfigured:    !v1RuntimeRequired(configuration) || isV1RuntimeConfigured(initialV1Runtime),
	}, nil
}

func v1RuntimeRequired(configuration config.Config) bool {
	// Only the checked-in development composition is allowed to start without
	// the durable v1 seam. Every other environment, including production and
	// unknown values, must fail closed before advertising readiness.
	return configuration.Environment != "development"
}

func composeRuntimeActivities(configuration config.Config, engine llm.Engine, metrics *observability.Metrics, tracer *observability.Tracer, v1Runtime activity.V1Runtime, queryService activity.QueryService) *activity.Activities {
	activities := newRuntimeActivities(configuration, engine, metrics, tracer, queryService)
	if v1Runtime != nil {
		activities.V1Runtime = v1Runtime
	}
	if !v1RuntimeRequired(configuration) && !isV1RuntimeConfigured(activities.V1Runtime) {
		// The local Compose profile is a parser/configuration/readiness fixture,
		// not a v1 worker. Omit all v1 seams so it cannot advertise the versioned
		// Activity names while no durable implementation is present.
		activities.V1Runtime = nil
		activities.QueryService = nil
	}
	return activities
}

func isV1RuntimeConfigured(runtime activity.V1Runtime) bool {
	switch runtime.(type) {
	case nil, activity.UnconfiguredV1Runtime, *activity.UnconfiguredV1Runtime:
		return false
	default:
		return true
	}
}

// newRuntimeActivities keeps the worker's Activity-bound configuration in one
// composition point. In particular, the periodic provider-wait cadence is a
// worker setting, never a provider SDK timeout.
func newRuntimeActivities(configuration config.Config, dynamic llm.Engine, metrics *observability.Metrics, tracer *observability.Tracer, queryService activity.QueryService) *activity.Activities {
	return &activity.Activities{
		Engine:                     dynamic,
		HeartbeatKeepaliveInterval: time.Duration(configuration.Temporal.Worker.HeartbeatKeepaliveInterval),
		HeartbeaterFactory: func() activity.Heartbeater {
			return activity.NewTemporalHeartbeater(activity.TemporalHeartbeaterOptions{Metrics: metrics})
		},
		Metrics:       metrics,
		Tracer:        tracer,
		PayloadLimits: activity.PayloadLimits{MaxInlineBytes: configuration.Server.InlinePayloadBytes},
		// The v1 Activity names are always registered by production workers,
		// but durable checkpoint/cache/control state is supplied by the future
		// provider-backed V1Runtime. Until that composition is present, fail
		// closed before any provider dispatch rather than registering the
		// pre-release envelope or silently bypassing durable state.
		V1Runtime:    activity.UnconfiguredV1Runtime{},
		QueryService: queryService,
	}
}

func newMetrics(configuration config.Config) (*observability.Metrics, error) {
	if !configuration.Telemetry.Metrics.Enabled {
		// A nil Metrics is the intentional disabled implementation: its Handler
		// supplies a 404 no-op and every recording consumer treats nil as no-op.
		// Do not replace it with an empty Metrics value, whose collectors are not
		// initialized and therefore cannot safely record.
		return nil, nil
	}
	endpoints := make([]string, 0, len(configuration.Endpoints))
	models := make([]string, 0, len(configuration.Models))
	policies := make([]string, 0, len(configuration.Budgets.Policies))
	for endpoint := range configuration.Endpoints {
		endpoints = append(endpoints, endpoint)
	}
	for modelID, model := range configuration.Models {
		models = append(models, modelID)
		for _, route := range model.Routes {
			models = append(models, route.Model)
		}
	}
	for _, policy := range configuration.Budgets.Policies {
		policies = append(policies, policy.ID)
	}
	return observability.NewMetrics(observability.AllowedValues{
		Endpoints: endpoints, Models: models, Policies: policies,
		Outcomes:              []string{"success", "failure", "accepted", "rejected", "denied"},
		Phases:                []string{"planning", "admission", "pre_write", "response_received", "lift", "finalization", "continuation_write", "total"},
		Statuses:              []string{"completed", "failed", "canceled"},
		ErrorClasses:          []string{"none", "internal", "provider_unavailable", "budget_denied"},
		Methods:               []string{"provider_reported", "catalog_usage", "reconstructed_usage", "retained_reservation"},
		OperationStates:       []string{"reserved", "dispatching", "completed", "failed", "ambiguous"},
		ContinuationDecisions: []string{"created", "reused", "dropped"},
	})
}

// Start starts probe listeners before Temporal polling. Required dependency
// probes run before a controller may poll; a transient failure keeps liveness
// up and lets the bounded monitor restore polling after recovery.
func (runtime *Runtime) Start() error {
	if runtime == nil || runtime.Worker == nil {
		return errors.New("runtime is not initialized")
	}
	if !runtime.v1RuntimeConfigured {
		if runtime.Health != nil {
			runtime.Health.SetReady(false)
		}
		return ErrV1RuntimeUnavailable
	}
	runtime.mu.Lock()
	if runtime.started {
		runtime.mu.Unlock()
		return errors.New("runtime is already started")
	}
	runtime.mu.Unlock()
	if err := runtime.HealthServer.Start(); err != nil {
		return err
	}
	if runtime.MetricsServer != nil {
		if err := runtime.MetricsServer.Start(); err != nil {
			_ = runtime.HealthServer.Shutdown(context.Background())
			return err
		}
	}
	if err := runtime.syncDependencyReadiness(context.Background()); err != nil {
		_ = runtime.HealthServer.Shutdown(context.Background())
		if runtime.MetricsServer != nil {
			_ = runtime.MetricsServer.Shutdown(context.Background())
		}
		return err
	}
	runtime.mu.Lock()
	runtime.started = true
	runtime.mu.Unlock()
	runtime.startDependencyMonitor()
	return nil
}

// Shutdown applies the readiness-first, worker-stop, client-drain, telemetry
// flush ordering and is safe to call more than once.
func (runtime *Runtime) Shutdown(ctx context.Context) error {
	if runtime == nil || runtime.shutdown == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runtime.stopDependencyMonitor(ctx)
	err := runtime.shutdown.Shutdown(ctx)
	runtime.Health.SetLive(false)
	return err
}

// Run starts the process and blocks until cancellation or a probe listener
// fails. Cancellation is a successful graceful exit when shutdown succeeds.
func (runtime *Runtime) Run(ctx context.Context) error {
	return runtime.run(ctx, "", nil)
}

// RunWithReload adds an explicit, already-sanitized reload trigger to the
// normal worker lifecycle. A rejected replacement is recorded and leaves the
// worker running on its current immutable snapshot.
func (runtime *Runtime) RunWithReload(ctx context.Context, path string, reloads <-chan struct{}) error {
	if path == "" {
		return errors.New("configuration path is required")
	}
	return runtime.run(ctx, path, reloads)
}

func (runtime *Runtime) run(ctx context.Context, reloadPath string, reloads <-chan struct{}) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := runtime.Start(); err != nil {
		// Start may fail before listeners are bound (for example when the
		// durable v1 runtime is absent), but New has already allocated the
		// Temporal client, worker, and snapshot resources. Close those resources
		// before returning the startup error so a fail-closed process does not
		// leak credentials, connections, or telemetry goroutines.
		return errors.Join(err, runtime.Shutdown(context.Background()))
	}
	var healthErrors <-chan error
	var metricsErrors <-chan error
	if runtime.HealthServer != nil {
		healthErrors = runtime.HealthServer.Errors()
	}
	if runtime.MetricsServer != nil {
		metricsErrors = runtime.MetricsServer.Errors()
	}
	for healthErrors != nil || metricsErrors != nil || reloads != nil {
		select {
		case <-ctx.Done():
			return runtime.gracefulShutdown()
		case _, ok := <-reloads:
			if !ok {
				reloads = nil
				continue
			}
			runtime.reloadFromTrigger(reloadPath)
		case err, ok := <-healthErrors:
			if !ok {
				healthErrors = nil
				continue
			}
			if err != nil {
				shutdownErr := runtime.gracefulShutdown()
				return errors.Join(err, shutdownErr)
			}
		case err, ok := <-metricsErrors:
			if !ok {
				metricsErrors = nil
				continue
			}
			if err != nil {
				shutdownErr := runtime.gracefulShutdown()
				return errors.Join(err, shutdownErr)
			}
		}
	}
	return runtime.gracefulShutdown()
}

func (runtime *Runtime) reloadFromTrigger(path string) {
	if runtime == nil || path == "" {
		return
	}
	timeout := runtime.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = runtime.ReloadFile(ctx, path)
}

func (runtime *Runtime) gracefulShutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), runtime.timeout)
	defer cancel()
	return runtime.Shutdown(ctx)
}

func closeResources(ctx context.Context, application *app.App, temporalClient client.Client, healthServer, metricsServer *httpserver.Server, health *httpserver.HealthState) error {
	if health != nil {
		health.SetReady(false)
	}
	var errs []error
	if healthServer != nil {
		if err := healthServer.Shutdown(ctx); err != nil {
			errs = append(errs, errors.New("shutdown health server failed"))
		}
	}
	if metricsServer != nil {
		if err := metricsServer.Shutdown(ctx); err != nil {
			errs = append(errs, errors.New("shutdown metrics server failed"))
		}
	}
	if application != nil {
		if err := application.Close(ctx); err != nil {
			errs = append(errs, errors.New("close runtime app failed"))
		}
	}
	if temporalClient != nil {
		temporalClient.Close()
	}
	return errors.Join(errs...)
}

// RunWorker is the CLI-facing entry point for embeddings that provide their
// own lifecycle trigger. It does not install a process-wide signal handler.
func RunWorker(ctx context.Context, data []byte, _ io.Writer) error {
	runtime, err := newProductionRuntime(ctx, data)
	if err != nil {
		return err
	}
	return runtime.Run(ctx)
}

// RunWorkerFile runs the production worker with both SIGHUP and safe file
// replacement triggers. It begins from the already validated bytes supplied by
// the command boundary; subsequent reloads always read the same file path.
func RunWorkerFile(ctx context.Context, path string, data []byte, _ io.Writer) error {
	if ctx == nil {
		ctx = context.Background()
	}
	runtime, err := newProductionRuntime(ctx, data)
	if err != nil {
		return err
	}
	watcher, err := newConfigFileWatcher(path, defaultConfigWatchInterval)
	if err != nil {
		_ = runtime.gracefulShutdown()
		return errors.New("watch configuration file failed")
	}
	defer watcher.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP)
	defer signal.Stop(signals)
	reloadContext, cancelReloads := context.WithCancel(ctx)
	defer cancelReloads()
	reloads := combineReloadTriggers(reloadContext, watcher.Changes(), signals)
	return runtime.RunWithReload(ctx, path, reloads)
}

func newProductionRuntime(ctx context.Context, data []byte) (*Runtime, error) {
	secretResolver := secrets.New(secrets.Options{})
	references := secrets.ConfigResolver{Resolver: secretResolver}
	factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
		Resolver:       secretResolver,
		SnapshotLoader: CatalogSnapshotLoader{},
	})
	if err != nil {
		return nil, err
	}
	runtime, err := New(ctx, data, Options{
		Resolver:       references,
		SecretResolver: secretResolver,
		EngineFactory:  factory,
	})
	if err != nil {
		return nil, err
	}
	return runtime, nil
}

type snapshotClients struct {
	engine       llm.Engine
	clients      app.ClientSet
	probes       []DependencyProbe
	queryService activity.QueryService
	v1Runtime    activity.V1Runtime
}

var _ V1RuntimeSource = (*snapshotClients)(nil)

func (clients *snapshotClients) Engine() llm.Engine {
	if clients == nil {
		return nil
	}
	return clients.engine
}

func (clients *snapshotClients) Close(ctx context.Context) error {
	if clients == nil || clients.clients == nil {
		return nil
	}
	return clients.clients.Close(ctx)
}

type dependencyProbeSource interface {
	DependencyProbes() []DependencyProbe
}

func (clients *snapshotClients) DependencyProbes() []DependencyProbe {
	if clients == nil {
		return nil
	}
	return append([]DependencyProbe(nil), clients.probes...)
}

func (clients *snapshotClients) QueryService() activity.QueryService {
	if clients == nil {
		return nil
	}
	return clients.queryService
}

func (clients *snapshotClients) V1Runtime() activity.V1Runtime {
	if clients == nil {
		return nil
	}
	return clients.v1Runtime
}

func v1RuntimeForSnapshot(snapshot *app.RuntimeSnapshot, fallback activity.V1Runtime) activity.V1Runtime {
	if snapshot == nil {
		return fallback
	}
	if source, ok := snapshot.Clients.(V1RuntimeSource); ok {
		return source.V1Runtime()
	}
	return fallback
}

func queryServiceFromClients(clients app.ClientSet) activity.QueryService {
	if source, ok := clients.(QueryServiceSource); ok {
		return source.QueryService()
	}
	return nil
}

func composeQueryService(ctx context.Context, snapshot *config.Snapshot, engineFactory EngineFactory, clients app.ClientSet) (activity.QueryService, error) {
	queryService := queryServiceFromClients(clients)
	if source, ok := engineFactory.(QueryServiceFactory); ok {
		return source.BuildQueryService(ctx, snapshot)
	}
	return queryService, nil
}

func verifySnapshotDependencies(ctx context.Context, snapshot *config.Snapshot, clients app.ClientSet) error {
	if snapshot == nil {
		return errRequiredDependencyUnavailable
	}
	source, ok := clients.(dependencyProbeSource)
	if !ok {
		return nil
	}
	return CheckDependencyProbes(ctx, source.DependencyProbes(), time.Duration(snapshot.Config().Server.ReadinessProbeTimeout))
}

func (runtime *Runtime) dependencyProbes() []DependencyProbe {
	if runtime == nil || runtime.App == nil {
		return nil
	}
	current := runtime.App.Current()
	if current == nil {
		return nil
	}
	source, ok := current.Clients.(dependencyProbeSource)
	if !ok {
		return nil
	}
	return source.DependencyProbes()
}

// currentV1RuntimeConfigured resolves the durable capability from the active
// configuration snapshot. V1 composition is snapshot-scoped, just like the
// Redis, PostgreSQL, and provider clients it owns; caching the initial value
// would let a reload publish an unconfigured runtime while readiness remained
// true.
func (runtime *Runtime) currentV1RuntimeConfigured() bool {
	if runtime == nil || runtime.App == nil {
		return false
	}
	current := runtime.App.Current()
	if current == nil || current.Config == nil {
		return false
	}
	configuration := current.Config.Config()
	if !v1RuntimeRequired(configuration) {
		return true
	}
	return isV1RuntimeConfigured(v1RuntimeForSnapshot(current, nil))
}

func (runtime *Runtime) currentV1RuntimeRequired() bool {
	if runtime == nil || runtime.App == nil {
		return false
	}
	current := runtime.App.Current()
	if current == nil || current.Config == nil {
		return false
	}
	return v1RuntimeRequired(current.Config.Config())
}

// syncDependencyReadiness applies the fail-closed state transition in one
// place. A failed probe pauses polling and leaves liveness alone; a successful
// probe resumes only after every required probe passed.
func (runtime *Runtime) syncDependencyReadiness(ctx context.Context) error {
	if runtime == nil || runtime.Worker == nil {
		return errors.New("runtime is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !runtime.currentV1RuntimeConfigured() {
		runtime.Health.SetReady(false)
		runtime.Worker.Pause()
		return nil
	}
	probes := runtime.dependencyProbes()
	if len(probes) > 0 {
		if err := CheckDependencyProbes(ctx, probes, runtime.readinessProbeTimeout); err != nil {
			runtime.Health.SetReady(false)
			runtime.Worker.Pause()
			return nil
		}
	}
	if runtime.Worker.Started() {
		return nil
	}
	if err := runtime.Worker.Resume(); err != nil {
		runtime.Health.SetReady(false)
		if errors.Is(err, app.ErrWorkerDraining) {
			return nil
		}
		return err
	}
	return nil
}

func (runtime *Runtime) startDependencyMonitor() {
	if runtime == nil || runtime.readinessProbeInterval <= 0 {
		return
	}
	// A production snapshot may have no explicit dependency probes when the
	// ClientSet is supplied by an embedding. Still monitor reloads so an
	// authoritative V1 source disappearing from a later snapshot cannot leave
	// readiness stuck true indefinitely.
	if len(runtime.dependencyProbes()) == 0 && !runtime.currentV1RuntimeRequired() {
		return
	}
	runtime.mu.Lock()
	if runtime.monitorCancel != nil {
		runtime.mu.Unlock()
		return
	}
	monitorContext, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	runtime.monitorCancel = cancel
	runtime.monitorDone = done
	interval := runtime.readinessProbeInterval
	runtime.mu.Unlock()
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-monitorContext.Done():
				return
			case <-ticker.C:
				_ = runtime.syncDependencyReadiness(monitorContext)
			}
		}
	}()
}

func (runtime *Runtime) stopDependencyMonitor(ctx context.Context) {
	if runtime == nil {
		return
	}
	runtime.mu.Lock()
	cancel := runtime.monitorCancel
	done := runtime.monitorDone
	runtime.monitorCancel = nil
	runtime.monitorDone = nil
	runtime.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	if done == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-done:
	case <-ctx.Done():
	}
}

type snapshotEngine struct{ application *app.App }

func (engine *snapshotEngine) Generate(ctx context.Context, request llm.Request) (llm.Response, error) {
	if engine == nil || engine.application == nil {
		return llm.Response{}, provider.NewError(provider.CodeInternal, provider.PhaseFinalize, provider.DispatchNotDispatched, provider.RetryNever, "runtime engine is unavailable")
	}
	lease, err := engine.application.Acquire()
	if err != nil {
		return llm.Response{}, provider.NewError(provider.CodeInternal, provider.PhasePlan, provider.DispatchNotDispatched, provider.RetryNever, "runtime snapshot is unavailable")
	}
	defer lease.Release()
	clients, ok := lease.Snapshot().Clients.(*snapshotClients)
	if !ok || clients.Engine() == nil {
		return llm.Response{}, provider.NewError(provider.CodeInternal, provider.PhasePlan, provider.DispatchNotDispatched, provider.RetryNever, "runtime engine is unavailable")
	}
	return clients.Engine().Generate(ctx, request)
}

var _ llm.Engine = (*snapshotEngine)(nil)

// engineFactoryError deliberately hides an injectable factory's Error text.
// Factory implementations are required to return safe errors, but the
// process boundary still fails closed if one accidentally contains payload or
// credential material.
type engineFactoryError struct{ cause error }

func (err *engineFactoryError) Error() string { return "engine construction failed" }

func (err *engineFactoryError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.cause
}

func unwrapEngineFactoryError(err error) error {
	var factoryError *engineFactoryError
	if !errors.As(err, &factoryError) || factoryError == nil {
		return errors.New("engine construction failed")
	}
	if errors.Is(factoryError.cause, ErrEngineFactoryUnavailable) {
		return ErrEngineFactoryUnavailable
	}
	if errors.Is(factoryError.cause, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(factoryError.cause, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return errors.New("engine construction failed")
}

func safeTemporalFactoryError(err error) error {
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("construct Temporal client: %w", context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("construct Temporal client: %w", context.DeadlineExceeded)
	}
	return errors.New("construct Temporal client failed")
}
