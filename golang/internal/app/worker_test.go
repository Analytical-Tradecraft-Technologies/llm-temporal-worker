package app_test

import (
	"context"
	"errors"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/workflows"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	domainactivity "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/httpserver"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type fakeWorker struct {
	startErr error
	started  atomic.Bool
	stopped  atomic.Bool
}

func (worker *fakeWorker) Start() error {
	worker.started.Store(worker.startErr == nil)
	return worker.startErr
}
func (worker *fakeWorker) Stop() { worker.stopped.Store(true) }

// nonComparableWorker deliberately has a slice dynamic type. WorkerController
// implementations are not required to be comparable.
type nonComparableWorker []int

func (nonComparableWorker) Start() error { return errors.New("start failed") }
func (nonComparableWorker) Stop()        {}

type blockingWorker struct {
	startErr     error
	startEntered chan struct{}
	releaseStart <-chan struct{}
	stopEntered  chan struct{}
	releaseStop  <-chan struct{}
	stopExited   chan struct{}

	startCalls atomic.Int32
	stopCalls  atomic.Int32
}

func (worker *blockingWorker) Start() error {
	worker.startCalls.Add(1)
	signalWorkerEvent(worker.startEntered)
	if worker.releaseStart != nil {
		<-worker.releaseStart
	}
	return worker.startErr
}

func (worker *blockingWorker) Stop() {
	worker.stopCalls.Add(1)
	signalWorkerEvent(worker.stopEntered)
	if worker.releaseStop != nil {
		<-worker.releaseStop
	}
	signalWorkerEvent(worker.stopExited)
}

func signalWorkerEvent(event chan struct{}) {
	if event == nil {
		return
	}
	select {
	case event <- struct{}{}:
	default:
	}
}

func waitForWorkerEvent(t *testing.T, event <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-event:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func waitForWorkerCondition(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

// resumeWorkerAfterDrain waits for TemporalWorker to observe that its detached
// controller has completed Stop. A controller-side completion signal can be
// published just before TemporalWorker clears its own drain state, so callers
// must retry ErrWorkerDraining rather than treating that signal as the worker's
// linearization point.
func resumeWorkerAfterDrain(t *testing.T, temporalWorker *app.TemporalWorker, description string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		err := temporalWorker.Resume()
		if err == nil {
			return
		}
		if !errors.Is(err, app.ErrWorkerDraining) {
			t.Fatalf("Resume while waiting for %s = %v, want nil or ErrWorkerDraining", description, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		time.Sleep(time.Millisecond)
	}
}

// mustStartWorker starts temporalWorker, failing the test on an error.
func mustStartWorker(t *testing.T, temporalWorker *app.TemporalWorker) {
	t.Helper()
	testassert.NoError(t, temporalWorker.Start())
}

// runInBackground runs call on a new goroutine and returns a channel closed
// once it returns.
func runInBackground(call func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		call()
		close(done)
	}()
	return done
}

// requireStillRunning fails with message when done closes within 25ms.
func requireStillRunning(t *testing.T, done <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-done:
		t.Fatal(message)
	case <-time.After(25 * time.Millisecond):
	}
}

// requireDrainGatesResume checks that Resume reports ErrWorkerDraining while
// first's gated drain is in progress, then that releasing the drain lets
// Resume build and start a second controller.
func requireDrainGatesResume(t *testing.T, temporalWorker *app.TemporalWorker, health *httpserver.HealthState, first *blockingWorker, controllers *[]app.WorkerController, release func(), drain string) {
	t.Helper()
	if err := temporalWorker.Resume(); !errors.Is(err, app.ErrWorkerDraining) {
		t.Fatalf("Resume during %s = %v, want ErrWorkerDraining", drain, err)
	}
	if len(*controllers) != 1 || first.stopCalls.Load() != 1 {
		t.Fatalf("controllers=%d stop-calls=%d during %s", len(*controllers), first.stopCalls.Load(), drain)
	}

	release()
	waitForWorkerEvent(t, first.stopExited, drain+" completion")
	resumeWorkerAfterDrain(t, temporalWorker, "the "+drain)
	if len(*controllers) != 2 || !health.Ready() || !temporalWorker.Started() {
		t.Fatalf("resume after %s controllers=%d ready=%v started=%v", drain, len(*controllers), health.Ready(), temporalWorker.Started())
	}
}

func closeWorkerGate(gate chan struct{}) func() {
	var once sync.Once
	return func() {
		once.Do(func() { close(gate) })
	}
}

// gatedBlockingWorker returns a blockingWorker whose Stop waits for
// releaseStop and, when gateStart is set, whose Start waits for releaseStart.
// Both release functions may be called more than once.
func gatedBlockingWorker(gateStart bool) (controller *blockingWorker, releaseStart, releaseStop func()) {
	stopGate := make(chan struct{})
	controller = &blockingWorker{
		stopEntered: make(chan struct{}, 1),
		releaseStop: stopGate,
		stopExited:  make(chan struct{}, 1),
	}
	releaseStart = func() {}
	if gateStart {
		startGate := make(chan struct{})
		controller.startEntered, controller.releaseStart = make(chan struct{}, 1), startGate
		releaseStart = closeWorkerGate(startGate)
	}
	return controller, releaseStart, closeWorkerGate(stopGate)
}

// firstThenFakeWorkers returns first from its first call and a fresh
// fakeWorker from every later call, recording each in controllers.
func firstThenFakeWorkers(first app.WorkerController, controllers *[]app.WorkerController) func(worker.Options) app.WorkerController {
	return func(worker.Options) app.WorkerController {
		controller := first
		if len(*controllers) > 0 {
			controller = &fakeWorker{}
		}
		*controllers = append(*controllers, controller)
		return controller
	}
}

// queueAWorkerOptions returns single-slot "queue-a" options with a one-second
// graceful stop whose factory pairs each controller from newController with a
// fresh fakeRegistry. health may be nil.
func queueAWorkerOptions(health *httpserver.HealthState, newController func(worker.Options) app.WorkerController) app.WorkerOptions {
	return app.WorkerOptions{
		TaskQueue: "queue-a", MaxConcurrentActivities: 1, MaxConcurrentActivityTaskPolls: 1,
		GracefulStopTimeout: time.Second, Activities: &domainactivity.Activities{}, Health: health,
		Factory: func(_ client.Client, _ string, options worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
			return newController(options), &fakeRegistry{}, nil
		},
	}
}

func mustNewWorker(t *testing.T, options app.WorkerOptions) *app.TemporalWorker {
	t.Helper()
	temporalWorker, err := app.NewWorker(options)
	testassert.NoError(t, err)
	return temporalWorker
}

// newQueueAWorker builds a worker from queueAWorkerOptions.
func newQueueAWorker(t *testing.T, health *httpserver.HealthState, newController func(worker.Options) app.WorkerController) *app.TemporalWorker {
	t.Helper()
	return mustNewWorker(t, queueAWorkerOptions(health, newController))
}

type fakeRegistry struct {
	name       string
	workflows  []string
	activities []string
}

func (registry *fakeRegistry) RegisterActivity(value interface{}) {}
func (registry *fakeRegistry) RegisterActivityWithOptions(_ interface{}, options activity.RegisterOptions) {
	registry.name = options.Name
	registry.activities = append(registry.activities, options.Name)
}
func (registry *fakeRegistry) RegisterDynamicActivity(interface{}, activity.DynamicRegisterOptions) {}

func (*fakeRegistry) RegisterWorkflow(interface{}) {}
func (registry *fakeRegistry) RegisterWorkflowWithOptions(_ interface{}, options workflow.RegisterOptions) {
	registry.workflows = append(registry.workflows, options.Name)
}
func (*fakeRegistry) RegisterDynamicWorkflow(interface{}, workflow.DynamicRegisterOptions) {}

var _ app.WorkerRegistry = (*fakeRegistry)(nil)

func TestWorkerRegistersExactActivityAndTransitionsReadiness(t *testing.T) {
	health := httpserver.NewHealthState()
	controller := &fakeWorker{}
	registry := &fakeRegistry{}
	var gotQueue string
	var gotOptions worker.Options
	temporalWorker := mustNewWorker(t, app.WorkerOptions{
		TaskQueue: "queue-a", Identity: "identity-a", MaxConcurrentActivities: 3,
		MaxConcurrentActivityTaskPolls: 2, GracefulStopTimeout: time.Second,
		Activities: &domainactivity.Activities{}, Health: health,
		Factory: func(_ client.Client, queue string, options worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
			gotQueue, gotOptions = queue, options
			return controller, registry, nil
		},
	})
	if gotQueue != "queue-a" || gotOptions.Identity != "identity-a" || gotOptions.MaxConcurrentActivityExecutionSize != 3 || gotOptions.MaxConcurrentActivityTaskPollers != 2 {
		t.Fatalf("worker options = %#v queue=%q", gotOptions, gotQueue)
	}
	// A workflow panic must fail the workflow, not block it forever (#1324).
	if gotOptions.WorkflowPanicPolicy != worker.FailWorkflow {
		t.Fatalf("workflow panic policy = %v, want FailWorkflow", gotOptions.WorkflowPanicPolicy)
	}
	// Without a v1 runtime the v1 workflows could only stall, so none are
	// registered, and the legacy Activity keeps its own name (#1102).
	if len(registry.workflows) != 0 {
		t.Fatalf("registered workflows = %v, want none without a v1 runtime", registry.workflows)
	}
	if registry.name != domainactivity.LegacyGenerateActivityName {
		t.Fatalf("registered activity = %q", registry.name)
	}
	if health.Ready() {
		t.Fatal("worker was ready before Start")
	}
	mustStartWorker(t, temporalWorker)
	if !health.Ready() || !temporalWorker.Started() {
		t.Fatal("worker did not become ready")
	}
	temporalWorker.Stop()
	if health.Ready() || !controller.stopped.Load() {
		t.Fatal("worker did not stop polling")
	}
}

func TestWorkerRejectsUnsafeTaskQueueBeforeConstructingTemporalWorker(t *testing.T) {
	var factoryCalls atomic.Int32
	for _, taskQueue := range []string{" queue-a", "queue-a\n", "queue-a\x00"} {
		t.Run(taskQueue, func(t *testing.T) {
			options := queueAWorkerOptions(nil, func(worker.Options) app.WorkerController {
				factoryCalls.Add(1)
				return &fakeWorker{}
			})
			options.TaskQueue = taskQueue
			if _, err := app.NewWorker(options); err == nil {
				t.Fatal("NewWorker unexpectedly accepted unsafe task queue")
			}
		})
	}
	if got := factoryCalls.Load(); got != 0 {
		t.Fatalf("Temporal worker factory calls = %d, want 0 for rejected queues", got)
	}
}

func TestWorkerStartErrorLeavesReadinessFalse(t *testing.T) {
	health := httpserver.NewHealthState()
	controller := &fakeWorker{startErr: errors.New("start failed")}
	temporalWorker := newQueueAWorker(t, health, func(_ worker.Options) app.WorkerController {
		return controller
	})
	if err := temporalWorker.Start(); err == nil || health.Ready() {
		t.Fatal("start error did not fail closed")
	}
}

func TestWorkerResumeRebuildsControllerAfterStartFailure(t *testing.T) {
	startErr := errors.New("start failed")
	failed := &blockingWorker{startErr: startErr}
	replacement := &fakeWorker{}
	var factoryCalls atomic.Int32
	options := queueAWorkerOptions(nil, nil)
	options.Factory = func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
		switch factoryCalls.Add(1) {
		case 1:
			return failed, &fakeRegistry{}, nil
		case 2:
			return replacement, &fakeRegistry{}, nil
		default:
			return nil, nil, errors.New("unexpected worker construction")
		}
	}
	temporalWorker := mustNewWorker(t, options)
	t.Cleanup(func() { temporalWorker.Stop() })

	testassert.ErrorIs(t, temporalWorker.Start(), startErr, "Start()")
	if failed.stopCalls.Load() != 1 {
		t.Fatalf("failed controller stop calls = %d, want 1", failed.stopCalls.Load())
	}
	if err := temporalWorker.Resume(); err != nil {
		t.Fatalf("Resume() error = %v, want nil", err)
	}
	if got := factoryCalls.Load(); got != 2 {
		t.Fatalf("worker factory calls = %d, want 2", got)
	}
	if !replacement.started.Load() || !temporalWorker.Started() {
		t.Fatal("Resume did not start the replacement controller")
	}
}

func TestWorkerStartFailureSupportsNonComparableController(t *testing.T) {
	replacement := &fakeWorker{}
	var factoryCalls atomic.Int32
	temporalWorker := newQueueAWorker(t, nil, func(_ worker.Options) app.WorkerController {
		if factoryCalls.Add(1) == 1 {
			return nonComparableWorker{1}
		}
		return replacement
	})
	t.Cleanup(func() { temporalWorker.Stop() })

	if err := temporalWorker.Start(); err == nil {
		t.Fatal("Start() error = nil, want failure")
	}
	if err := temporalWorker.Resume(); err != nil {
		t.Fatalf("Resume() error = %v, want nil", err)
	}
	if got := factoryCalls.Load(); got != 2 || !replacement.started.Load() {
		t.Fatalf("factory calls=%d replacement-started=%v, want 2 and true", got, replacement.started.Load())
	}
}

func TestWorkerResumeWaitsForFailedStartCleanup(t *testing.T) {
	startErr := errors.New("start failed")
	failed, _, release := gatedBlockingWorker(false)
	failed.startErr = startErr
	replacement := &fakeWorker{}
	var factoryCalls atomic.Int32
	temporalWorker := newQueueAWorker(t, nil, func(_ worker.Options) app.WorkerController {
		if factoryCalls.Add(1) == 1 {
			return failed
		}
		return replacement
	})
	t.Cleanup(func() { temporalWorker.Stop() })
	t.Cleanup(release)

	started := make(chan error, 1)
	go func() { started <- temporalWorker.Start() }()
	waitForWorkerEvent(t, failed.stopEntered, "failed controller cleanup")
	if err := temporalWorker.Resume(); err == nil {
		t.Fatal("Resume during failed controller cleanup = nil, want an error")
	}
	if got := factoryCalls.Load(); got != 1 {
		t.Fatalf("worker factory calls during cleanup = %d, want 1", got)
	}

	release()
	testassert.ErrorIs(t, <-started, startErr, "Start()")
	if err := temporalWorker.Resume(); err != nil {
		t.Fatalf("Resume() after cleanup error = %v, want nil", err)
	}
	if got := factoryCalls.Load(); got != 2 || !replacement.started.Load() {
		t.Fatalf("factory calls=%d replacement-started=%v, want 2 and true", got, replacement.started.Load())
	}
}

func TestWorkerStopWaitsForFailedStartCleanup(t *testing.T) {
	startErr := errors.New("start failed")
	failed, _, release := gatedBlockingWorker(false)
	failed.startErr = startErr
	temporalWorker := newQueueAWorker(t, nil, func(_ worker.Options) app.WorkerController {
		return failed
	})
	t.Cleanup(release)

	started := make(chan error, 1)
	go func() { started <- temporalWorker.Start() }()
	waitForWorkerEvent(t, failed.stopEntered, "failed controller cleanup")
	stopped := runInBackground(temporalWorker.Stop)
	requireStillRunning(t, stopped, "Stop returned before failed controller cleanup completed")

	release()
	testassert.ErrorIs(t, <-started, startErr, "Start()")
	waitForWorkerEvent(t, stopped, "Stop after failed controller cleanup")
	if failed.stopCalls.Load() != 1 {
		t.Fatalf("failed controller stop calls = %d, want 1", failed.stopCalls.Load())
	}
}

func TestWorkerPauseStopsPollingAndResumeBuildsFreshController(t *testing.T) {
	health := httpserver.NewHealthState()
	controllers := make([]*fakeWorker, 0, 2)
	temporalWorker := newQueueAWorker(t, health, func(_ worker.Options) app.WorkerController {
		controller := &fakeWorker{}
		controllers = append(controllers, controller)
		return controller
	})
	mustStartWorker(t, temporalWorker)
	temporalWorker.Pause()
	waitForWorkerCondition(t, func() bool { return controllers[0].stopped.Load() }, "paused controller stop")
	if health.Ready() || !controllers[0].stopped.Load() || temporalWorker.Started() {
		t.Fatal("pause did not turn readiness off and stop polling")
	}
	resumeWorkerAfterDrain(t, temporalWorker, "paused controller drain")
	if len(controllers) != 2 || !controllers[1].started.Load() || !health.Ready() || !temporalWorker.Started() {
		t.Fatalf("resume controllers=%d started=%v ready=%v", len(controllers), len(controllers) == 2 && controllers[1].started.Load(), health.Ready())
	}
	temporalWorker.Stop()
	if !controllers[1].stopped.Load() || health.Ready() {
		t.Fatal("permanent stop did not stop the resumed controller")
	}
}

func workerPollingGauge(t *testing.T, metrics *observability.Metrics) float64 {
	t.Helper()
	families, err := metrics.Gather()
	testassert.NoError(t, err)
	for _, family := range families {
		if family.GetName() == "llmtw_worker_polling" && len(family.GetMetric()) == 1 {
			return family.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatal("llmtw_worker_polling gauge not found")
	return 0
}

// fatalWorker stands in for an SDK worker that stops itself after a fatal poll
// error: the SDK calls OnFatalError and then, once it returns, Stop.
type fatalWorker struct {
	fakeWorker
	onFatal func(error)
}

func (worker *fatalWorker) fail(err error) {
	worker.onFatal(err)
	if !worker.stopped.Load() {
		worker.Stop()
	}
}

func TestWorkerFatalSDKStopClearsReadinessAndResumeRebuildsController(t *testing.T) {
	health := httpserver.NewHealthState()
	metrics, err := observability.NewMetrics(observability.AllowedValues{})
	testassert.NoError(t, err)
	var mu sync.Mutex
	controllers := make([]*fatalWorker, 0, 2)
	options := queueAWorkerOptions(health, func(options worker.Options) app.WorkerController {
		if options.OnFatalError == nil {
			t.Fatal("worker options have no OnFatalError callback")
		}
		controller := &fatalWorker{onFatal: options.OnFatalError}
		mu.Lock()
		controllers = append(controllers, controller)
		mu.Unlock()
		return controller
	})
	options.Metrics = metrics
	temporalWorker := mustNewWorker(t, options)
	mustStartWorker(t, temporalWorker)
	if !health.Ready() || workerPollingGauge(t, metrics) != 1 {
		t.Fatal("worker did not report polling after Start")
	}
	controllers[0].fail(errors.New("namespace not found"))
	if health.Ready() || temporalWorker.Started() || workerPollingGauge(t, metrics) != 0 {
		t.Fatalf("after fatal stop ready=%v started=%v polling=%v, want all false", health.Ready(), temporalWorker.Started(), workerPollingGauge(t, metrics))
	}
	if !controllers[0].stopped.Load() {
		t.Fatal("fatally stopped controller was not stopped")
	}
	resumeWorkerAfterDrain(t, temporalWorker, "fatally stopped controller drain")
	mu.Lock()
	count := len(controllers)
	mu.Unlock()
	if count != 2 || !controllers[1].started.Load() || !health.Ready() || !temporalWorker.Started() || workerPollingGauge(t, metrics) != 1 {
		t.Fatalf("resume after fatal stop controllers=%d ready=%v started=%v", count, health.Ready(), temporalWorker.Started())
	}
	// A late callback from the replaced controller must not touch the new one.
	controllers[0].onFatal(errors.New("late"))
	if !health.Ready() || !temporalWorker.Started() || controllers[1].stopped.Load() {
		t.Fatal("stale fatal callback affected the replacement controller")
	}
	temporalWorker.Stop()
	if !controllers[1].stopped.Load() || health.Ready() {
		t.Fatal("permanent stop did not stop the rebuilt controller")
	}
}

func TestWorkerFatalSDKStopAfterPauseIsIgnored(t *testing.T) {
	health := httpserver.NewHealthState()
	var controller *fatalWorker
	temporalWorker := newQueueAWorker(t, health, func(options worker.Options) app.WorkerController {
		controller = &fatalWorker{onFatal: options.OnFatalError}
		return controller
	})
	mustStartWorker(t, temporalWorker)
	paused := controller
	temporalWorker.Pause()
	waitForWorkerCondition(t, func() bool { return paused.stopped.Load() }, "paused controller stop")
	paused.onFatal(errors.New("namespace not found"))
	resumeWorkerAfterDrain(t, temporalWorker, "paused controller drain")
	if controller == paused || !health.Ready() || !temporalWorker.Started() {
		t.Fatal("fatal callback from a paused controller blocked Resume")
	}
	temporalWorker.Stop()
}

func TestWorkerPauseReturnsBeforeDrainAndResumeWaitsForCompletion(t *testing.T) {
	health := httpserver.NewHealthState()
	first, _, release := gatedBlockingWorker(false)
	controllers := make([]app.WorkerController, 0, 2)
	temporalWorker := newQueueAWorker(t, health, firstThenFakeWorkers(first, &controllers))
	t.Cleanup(func() { temporalWorker.Stop() })
	t.Cleanup(release)
	mustStartWorker(t, temporalWorker)

	paused := runInBackground(temporalWorker.Pause)
	waitForWorkerEvent(t, first.stopEntered, "first controller drain")
	waitForWorkerEvent(t, paused, "Pause to return before the graceful drain completes")
	if health.Ready() || temporalWorker.Started() {
		t.Fatal("Pause did not fail readiness closed while draining")
	}
	requireDrainGatesResume(t, temporalWorker, health, first, &controllers, release, "first controller drain")
}

func TestWorkerStopWaitsForOwnedPauseDrainWithoutDoubleStopping(t *testing.T) {
	health := httpserver.NewHealthState()
	first, _, release := gatedBlockingWorker(false)
	temporalWorker := newQueueAWorker(t, health, func(_ worker.Options) app.WorkerController {
		return first
	})
	t.Cleanup(release)
	mustStartWorker(t, temporalWorker)

	paused := runInBackground(temporalWorker.Pause)
	waitForWorkerEvent(t, first.stopEntered, "first controller drain")
	waitForWorkerEvent(t, paused, "Pause to return before the graceful drain completes")

	stopped := runInBackground(temporalWorker.Stop)
	requireStillRunning(t, stopped, "permanent Stop returned before the owned pause drain completed")
	testassert.ErrorIs(t, temporalWorker.Resume(), app.ErrWorkerStopping, "Resume while stopping")
	if first.stopCalls.Load() != 1 {
		t.Fatalf("pause drain was stopped %d times", first.stopCalls.Load())
	}

	release()
	waitForWorkerEvent(t, first.stopExited, "first controller drain completion")
	waitForWorkerEvent(t, stopped, "permanent Stop after the owned pause drain")
	if first.stopCalls.Load() != 1 {
		t.Fatalf("permanent Stop double-stopped the pause drain: %d calls", first.stopCalls.Load())
	}
}

func TestWorkerPauseDuringStartDrainsWithoutBlockingStartOrReplacingController(t *testing.T) {
	health := httpserver.NewHealthState()
	first, releaseStartGate, releaseStopGate := gatedBlockingWorker(true)
	controllers := make([]app.WorkerController, 0, 2)
	temporalWorker := newQueueAWorker(t, health, firstThenFakeWorkers(first, &controllers))
	t.Cleanup(func() { temporalWorker.Stop() })
	t.Cleanup(releaseStopGate)
	t.Cleanup(releaseStartGate)

	started := make(chan error, 1)
	go func() { started <- temporalWorker.Start() }()
	waitForWorkerEvent(t, first.startEntered, "first controller start")
	// Start has passed its authorization point, so Pause may return promptly;
	// startController must drain this controller once Start returns.
	temporalWorker.Pause()
	if health.Ready() || temporalWorker.Started() {
		t.Fatal("Pause during Start did not fail readiness closed")
	}

	releaseStartGate()
	select {
	case err := <-started:
		testassert.NoError(t, err, "Start after Pause")
	case <-time.After(time.Second):
		t.Fatal("Start waited for the paused controller's graceful drain")
	}
	waitForWorkerEvent(t, first.stopEntered, "paused controller drain")
	requireDrainGatesResume(t, temporalWorker, health, first, &controllers, releaseStopGate, "paused controller drain")
}

func TestWorkerPauseDuringBuildPreventsUncommittedStart(t *testing.T) {
	health := httpserver.NewHealthState()
	releaseBuild := make(chan struct{})
	releaseBuildGate := closeWorkerGate(releaseBuild)
	initial := &fakeWorker{}
	builtWhilePaused := &fakeWorker{}
	replacement := &fakeWorker{}
	buildEntered := make(chan struct{}, 1)
	var factoryCalls atomic.Int32
	temporalWorker := newQueueAWorker(t, health, func(_ worker.Options) app.WorkerController {
		switch factoryCalls.Add(1) {
		case 1:
			return initial
		case 2:
			signalWorkerEvent(buildEntered)
			<-releaseBuild
			return builtWhilePaused
		default:
			return replacement
		}
	})
	t.Cleanup(func() { temporalWorker.Stop() })
	t.Cleanup(releaseBuildGate)
	mustStartWorker(t, temporalWorker)
	temporalWorker.Pause()
	waitForWorkerCondition(t, func() bool { return initial.stopped.Load() }, "initial controller pause drain")

	var resumeDone <-chan error
	for resumeDone == nil {
		attempt := make(chan error, 1)
		go func() { attempt <- temporalWorker.Resume() }()
		select {
		case <-buildEntered:
			resumeDone = attempt
		case err := <-attempt:
			testassert.ErrorIs(t, err, app.ErrWorkerDraining, "Resume before build")
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for replacement build")
		}
	}

	// Pause wins before the starter commits permission to call Start. The
	// completed Resume must leave the newly built controller detached.
	temporalWorker.Pause()
	releaseBuildGate()
	testassert.NoError(t, <-resumeDone, "Resume after Pause during build")
	if builtWhilePaused.started.Load() || health.Ready() || temporalWorker.Started() {
		t.Fatalf("uncommitted build started=%v ready=%v worker-started=%v", builtWhilePaused.started.Load(), health.Ready(), temporalWorker.Started())
	}

	testassert.NoError(t, temporalWorker.Resume())
	if !replacement.started.Load() || !health.Ready() || !temporalWorker.Started() {
		t.Fatalf("replacement after cancelled build started=%v ready=%v worker-started=%v", replacement.started.Load(), health.Ready(), temporalWorker.Started())
	}
}

func TestWorkerStopDuringStartWaitsForControllerStop(t *testing.T) {
	health := httpserver.NewHealthState()
	first, releaseStartGate, releaseStopGate := gatedBlockingWorker(true)
	temporalWorker := newQueueAWorker(t, health, func(_ worker.Options) app.WorkerController {
		return first
	})
	t.Cleanup(releaseStartGate)
	t.Cleanup(releaseStopGate)

	started := make(chan error, 1)
	go func() { started <- temporalWorker.Start() }()
	waitForWorkerEvent(t, first.startEntered, "first controller start")

	stopped := runInBackground(temporalWorker.Stop)
	waitForWorkerCondition(t, func() bool {
		return errors.Is(temporalWorker.Resume(), app.ErrWorkerStopping)
	}, "Stop to own the in-progress start")

	releaseStartGate()
	waitForWorkerEvent(t, first.stopEntered, "controller stop after start completes")
	requireStillRunning(t, stopped, "Stop returned before the in-progress controller completed its graceful stop")

	releaseStopGate()
	waitForWorkerEvent(t, first.stopExited, "controller stop completion")
	testassert.ErrorIs(t, <-started, app.ErrWorkerStopping, "Start after Stop")
	waitForWorkerEvent(t, stopped, "Stop after the in-progress controller completed")
	if first.stopCalls.Load() != 1 || health.Ready() || temporalWorker.Started() {
		t.Fatalf("stop-during-start calls=%d ready=%v started=%v", first.stopCalls.Load(), health.Ready(), temporalWorker.Started())
	}
}

// Registration does not invoke provider code. Embedding the runtime interface
// lets this fixture prove the production v1 selection without fake paid work.
type registrationRuntime struct{ domainactivity.V1Runtime }

func TestWorkerRegistersV1WorkflowsAndActivitiesAgainAfterDrain(t *testing.T) {
	var registries []*fakeRegistry
	var controllers []*fakeWorker
	value := mustNewWorker(t, app.WorkerOptions{
		TaskQueue: "v1-queue", MaxConcurrentActivities: 1, MaxConcurrentActivityTaskPolls: 1,
		GracefulStopTimeout: time.Second,
		Activities:          &domainactivity.Activities{V1Runtime: registrationRuntime{}},
		Factory: func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
			registry, controller := &fakeRegistry{}, &fakeWorker{}
			registries = append(registries, registry)
			controllers = append(controllers, controller)
			return controller, registry, nil
		},
	})
	t.Cleanup(value.Stop)
	mustStartWorker(t, value)
	value.Pause()
	resumeWorkerAfterDrain(t, value, "new workflow worker")
	if len(registries) != 2 || !controllers[0].stopped.Load() || !controllers[1].started.Load() {
		t.Fatal("worker was not replaced")
	}
	descriptors, err := domainactivity.V1ActivityDescriptors("v1-queue")
	testassert.NoError(t, err)
	expectedActivities := make([]string, 0, len(descriptors))
	for _, descriptor := range descriptors {
		expectedActivities = append(expectedActivities, descriptor.Name)
	}
	expectedWorkflows := []string{workflows.RequestWorkflowName, workflows.BudgetWorkflowName, workflows.LangfuseExportWorkflowName, workflows.GenerateWorkflowName, workflows.CompactWorkflowName, workflows.QueryWorkflowName}
	for _, registry := range registries {
		if !reflect.DeepEqual(registry.workflows, expectedWorkflows) {
			t.Fatalf("missing/duplicate workflows: %v", registry.workflows)
		}
		if !reflect.DeepEqual(registry.activities, expectedActivities) {
			t.Fatalf("missing/duplicate activities: %v", registry.activities)
		}
	}
}

// sdkLikeWorker models the Temporal SDK worker's Stop: it waits for the one
// in-flight Activity up to WorkerStopTimeout and otherwise gives up on it, the
// point at which the SDK cancels every Activity context. The Activity itself
// runs under BackgroundActivityContext and returns when that context is
// cancelled or when the test completes it.
type sdkLikeWorker struct {
	options      worker.Options
	activityDone chan struct{}
	stopEntered  chan struct{}
	stopCalls    atomic.Int32
	// abandoned records that Stop gave up on the in-flight Activity.
	abandoned atomic.Bool
	// completedLive records that the Activity completed while its context
	// was still live.
	completedLive atomic.Bool
}

func newSDKLikeWorker(options worker.Options) *sdkLikeWorker {
	return &sdkLikeWorker{options: options, activityDone: make(chan struct{}), stopEntered: make(chan struct{}, 1)}
}

func (worker *sdkLikeWorker) Start() error { return nil }

func (worker *sdkLikeWorker) Stop() {
	worker.stopCalls.Add(1)
	signalWorkerEvent(worker.stopEntered)
	ctx := worker.options.BackgroundActivityContext
	if ctx == nil {
		ctx = context.Background()
	}
	timer := time.NewTimer(worker.options.WorkerStopTimeout)
	defer timer.Stop()
	select {
	case <-worker.activityDone:
		worker.completedLive.Store(ctx.Err() == nil)
	case <-ctx.Done():
	case <-timer.C:
		worker.abandoned.Store(true)
	}
}

func (worker *sdkLikeWorker) activityContextCause() error {
	if worker.options.BackgroundActivityContext == nil {
		return nil
	}
	return context.Cause(worker.options.BackgroundActivityContext)
}

func newSDKLikeTemporalWorker(t *testing.T, graceful, pauseDrain time.Duration) (*app.TemporalWorker, func() *sdkLikeWorker) {
	t.Helper()
	var mu sync.Mutex
	var controllers []*sdkLikeWorker
	options := queueAWorkerOptions(httpserver.NewHealthState(), func(options worker.Options) app.WorkerController {
		controller := newSDKLikeWorker(options)
		mu.Lock()
		controllers = append(controllers, controller)
		mu.Unlock()
		return controller
	})
	options.GracefulStopTimeout, options.PauseDrainTimeout = graceful, pauseDrain
	temporalWorker := mustNewWorker(t, options)
	latest := func() *sdkLikeWorker {
		mu.Lock()
		defer mu.Unlock()
		return controllers[len(controllers)-1]
	}
	return temporalWorker, latest
}

func TestWorkerPauseLetsInFlightActivitiesOutliveGracefulStopTimeout(t *testing.T) {
	const graceful = 20 * time.Millisecond
	temporalWorker, latest := newSDKLikeTemporalWorker(t, graceful, time.Second)
	t.Cleanup(temporalWorker.Stop)
	mustStartWorker(t, temporalWorker)
	first := latest()
	temporalWorker.Pause()
	waitForWorkerEvent(t, first.stopEntered, "paused controller drain")

	// The Activity needs several graceful stop windows to finish.
	time.Sleep(5 * graceful)
	if first.abandoned.Load() || first.activityContextCause() != nil {
		t.Fatalf("pause drain gave up on the in-flight Activity after the graceful stop timeout: abandoned=%v cause=%v", first.abandoned.Load(), first.activityContextCause())
	}
	close(first.activityDone)
	resumeWorkerAfterDrain(t, temporalWorker, "pause drain to finish after the Activity completed")
	if !first.completedLive.Load() {
		t.Fatal("in-flight Activity did not complete under a live context")
	}
}

func TestWorkerStopCancelsInFlightActivitiesAfterGracefulStopTimeout(t *testing.T) {
	const graceful = 20 * time.Millisecond
	temporalWorker, latest := newSDKLikeTemporalWorker(t, graceful, time.Minute)
	mustStartWorker(t, temporalWorker)
	controller := latest()
	started := time.Now()
	temporalWorker.Stop()
	if elapsed := time.Since(started); elapsed < graceful || elapsed > 20*graceful {
		t.Fatalf("permanent Stop returned after %s, want the graceful stop timeout %s", elapsed, graceful)
	}
	if controller.abandoned.Load() || !errors.Is(controller.activityContextCause(), worker.ErrWorkerShutdown) {
		t.Fatalf("permanent Stop did not cancel the in-flight Activity: abandoned=%v cause=%v", controller.abandoned.Load(), controller.activityContextCause())
	}
}

func TestWorkerStopDuringPauseDrainCancelsInFlightActivitiesAfterGracefulStopTimeout(t *testing.T) {
	const graceful = 20 * time.Millisecond
	temporalWorker, latest := newSDKLikeTemporalWorker(t, graceful, time.Minute)
	mustStartWorker(t, temporalWorker)
	controller := latest()
	temporalWorker.Pause()
	waitForWorkerEvent(t, controller.stopEntered, "paused controller drain")
	time.Sleep(5 * graceful)
	if controller.activityContextCause() != nil {
		t.Fatalf("pause drain cancelled the in-flight Activity: %v", controller.activityContextCause())
	}

	started := time.Now()
	temporalWorker.Stop()
	if elapsed := time.Since(started); elapsed < graceful || elapsed > 20*graceful {
		t.Fatalf("permanent Stop returned after %s, want the graceful stop timeout %s", elapsed, graceful)
	}
	if controller.stopCalls.Load() != 1 || controller.abandoned.Load() || !errors.Is(controller.activityContextCause(), worker.ErrWorkerShutdown) {
		t.Fatalf("permanent Stop during the pause drain: stop-calls=%d abandoned=%v cause=%v", controller.stopCalls.Load(), controller.abandoned.Load(), controller.activityContextCause())
	}
}
