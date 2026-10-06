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

func closeWorkerGate(gate chan struct{}) func() {
	var once sync.Once
	return func() {
		once.Do(func() { close(gate) })
	}
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
	temporalWorker, err := app.NewWorker(app.WorkerOptions{
		TaskQueue: "queue-a", Identity: "identity-a", MaxConcurrentActivities: 3,
		MaxConcurrentActivityTaskPolls: 2, GracefulStopTimeout: time.Second,
		Activities: &domainactivity.Activities{}, Health: health,
		Factory: func(_ client.Client, queue string, options worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
			gotQueue, gotOptions = queue, options
			return controller, registry, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotQueue != "queue-a" || gotOptions.Identity != "identity-a" || gotOptions.MaxConcurrentActivityExecutionSize != 3 || gotOptions.MaxConcurrentActivityTaskPollers != 2 {
		t.Fatalf("worker options = %#v queue=%q", gotOptions, gotQueue)
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
	if err := temporalWorker.Start(); err != nil {
		t.Fatal(err)
	}
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
			_, err := app.NewWorker(app.WorkerOptions{
				TaskQueue: taskQueue, MaxConcurrentActivities: 1, MaxConcurrentActivityTaskPolls: 1,
				GracefulStopTimeout: time.Second, Activities: &domainactivity.Activities{},
				Factory: func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
					factoryCalls.Add(1)
					return &fakeWorker{}, &fakeRegistry{}, nil
				},
			})
			if err == nil {
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
	registry := &fakeRegistry{}
	temporalWorker, err := app.NewWorker(app.WorkerOptions{
		TaskQueue: "queue-a", MaxConcurrentActivities: 1, MaxConcurrentActivityTaskPolls: 1,
		GracefulStopTimeout: time.Second, Activities: &domainactivity.Activities{}, Health: health,
		Factory: func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
			return controller, registry, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := temporalWorker.Start(); err == nil || health.Ready() {
		t.Fatal("start error did not fail closed")
	}
}

func TestWorkerResumeRebuildsControllerAfterStartFailure(t *testing.T) {
	startErr := errors.New("start failed")
	failed := &blockingWorker{startErr: startErr}
	replacement := &fakeWorker{}
	var factoryCalls atomic.Int32
	temporalWorker, err := app.NewWorker(app.WorkerOptions{
		TaskQueue: "queue-a", MaxConcurrentActivities: 1, MaxConcurrentActivityTaskPolls: 1,
		GracefulStopTimeout: time.Second, Activities: &domainactivity.Activities{},
		Factory: func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
			switch factoryCalls.Add(1) {
			case 1:
				return failed, &fakeRegistry{}, nil
			case 2:
				return replacement, &fakeRegistry{}, nil
			default:
				return nil, nil, errors.New("unexpected worker construction")
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { temporalWorker.Stop() })

	if err := temporalWorker.Start(); !errors.Is(err, startErr) {
		t.Fatalf("Start() error = %v, want %v", err, startErr)
	}
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
	temporalWorker, err := app.NewWorker(app.WorkerOptions{
		TaskQueue: "queue-a", MaxConcurrentActivities: 1, MaxConcurrentActivityTaskPolls: 1,
		GracefulStopTimeout: time.Second, Activities: &domainactivity.Activities{},
		Factory: func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
			if factoryCalls.Add(1) == 1 {
				return nonComparableWorker{1}, &fakeRegistry{}, nil
			}
			return replacement, &fakeRegistry{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
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
	releaseStop := make(chan struct{})
	release := closeWorkerGate(releaseStop)
	failed := &blockingWorker{
		startErr:    startErr,
		stopEntered: make(chan struct{}, 1),
		releaseStop: releaseStop,
	}
	replacement := &fakeWorker{}
	var factoryCalls atomic.Int32
	temporalWorker, err := app.NewWorker(app.WorkerOptions{
		TaskQueue: "queue-a", MaxConcurrentActivities: 1, MaxConcurrentActivityTaskPolls: 1,
		GracefulStopTimeout: time.Second, Activities: &domainactivity.Activities{},
		Factory: func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
			if factoryCalls.Add(1) == 1 {
				return failed, &fakeRegistry{}, nil
			}
			return replacement, &fakeRegistry{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
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
	if err := <-started; !errors.Is(err, startErr) {
		t.Fatalf("Start() error = %v, want %v", err, startErr)
	}
	if err := temporalWorker.Resume(); err != nil {
		t.Fatalf("Resume() after cleanup error = %v, want nil", err)
	}
	if got := factoryCalls.Load(); got != 2 || !replacement.started.Load() {
		t.Fatalf("factory calls=%d replacement-started=%v, want 2 and true", got, replacement.started.Load())
	}
}

func TestWorkerStopWaitsForFailedStartCleanup(t *testing.T) {
	startErr := errors.New("start failed")
	releaseStop := make(chan struct{})
	release := closeWorkerGate(releaseStop)
	failed := &blockingWorker{
		startErr:    startErr,
		stopEntered: make(chan struct{}, 1),
		releaseStop: releaseStop,
	}
	temporalWorker, err := app.NewWorker(app.WorkerOptions{
		TaskQueue: "queue-a", MaxConcurrentActivities: 1, MaxConcurrentActivityTaskPolls: 1,
		GracefulStopTimeout: time.Second, Activities: &domainactivity.Activities{},
		Factory: func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
			return failed, &fakeRegistry{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)

	started := make(chan error, 1)
	go func() { started <- temporalWorker.Start() }()
	waitForWorkerEvent(t, failed.stopEntered, "failed controller cleanup")
	stopped := make(chan struct{})
	go func() {
		temporalWorker.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("Stop returned before failed controller cleanup completed")
	case <-time.After(25 * time.Millisecond):
	}

	release()
	if err := <-started; !errors.Is(err, startErr) {
		t.Fatalf("Start() error = %v, want %v", err, startErr)
	}
	waitForWorkerEvent(t, stopped, "Stop after failed controller cleanup")
	if failed.stopCalls.Load() != 1 {
		t.Fatalf("failed controller stop calls = %d, want 1", failed.stopCalls.Load())
	}
}

func TestWorkerPauseStopsPollingAndResumeBuildsFreshController(t *testing.T) {
	health := httpserver.NewHealthState()
	controllers := make([]*fakeWorker, 0, 2)
	temporalWorker, err := app.NewWorker(app.WorkerOptions{
		TaskQueue: "queue-a", MaxConcurrentActivities: 1, MaxConcurrentActivityTaskPolls: 1,
		GracefulStopTimeout: time.Second, Activities: &domainactivity.Activities{}, Health: health,
		Factory: func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
			controller := &fakeWorker{}
			controllers = append(controllers, controller)
			return controller, &fakeRegistry{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := temporalWorker.Start(); err != nil {
		t.Fatal(err)
	}
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

func TestWorkerPauseReturnsBeforeDrainAndResumeWaitsForCompletion(t *testing.T) {
	health := httpserver.NewHealthState()
	releaseStop := make(chan struct{})
	release := closeWorkerGate(releaseStop)
	first := &blockingWorker{
		stopEntered: make(chan struct{}, 1),
		releaseStop: releaseStop,
		stopExited:  make(chan struct{}, 1),
	}
	controllers := make([]app.WorkerController, 0, 2)
	temporalWorker, err := app.NewWorker(app.WorkerOptions{
		TaskQueue: "queue-a", MaxConcurrentActivities: 1, MaxConcurrentActivityTaskPolls: 1,
		GracefulStopTimeout: time.Second, Activities: &domainactivity.Activities{}, Health: health,
		Factory: func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
			if len(controllers) == 0 {
				controllers = append(controllers, first)
				return first, &fakeRegistry{}, nil
			}
			controller := &fakeWorker{}
			controllers = append(controllers, controller)
			return controller, &fakeRegistry{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { temporalWorker.Stop() })
	t.Cleanup(release)
	if err := temporalWorker.Start(); err != nil {
		t.Fatal(err)
	}

	paused := make(chan struct{})
	go func() {
		temporalWorker.Pause()
		close(paused)
	}()
	waitForWorkerEvent(t, first.stopEntered, "first controller drain")
	waitForWorkerEvent(t, paused, "Pause to return before the graceful drain completes")
	if health.Ready() || temporalWorker.Started() {
		t.Fatal("Pause did not fail readiness closed while draining")
	}
	if err := temporalWorker.Resume(); !errors.Is(err, app.ErrWorkerDraining) {
		t.Fatalf("Resume while draining = %v, want ErrWorkerDraining", err)
	}
	if len(controllers) != 1 || first.stopCalls.Load() != 1 {
		t.Fatalf("controllers=%d stop-calls=%d while draining", len(controllers), first.stopCalls.Load())
	}

	release()
	waitForWorkerEvent(t, first.stopExited, "first controller drain completion")
	resumeWorkerAfterDrain(t, temporalWorker, "the first controller drain")
	if len(controllers) != 2 || !health.Ready() || !temporalWorker.Started() {
		t.Fatalf("resume after drain controllers=%d ready=%v started=%v", len(controllers), health.Ready(), temporalWorker.Started())
	}
}

func TestWorkerStopWaitsForOwnedPauseDrainWithoutDoubleStopping(t *testing.T) {
	health := httpserver.NewHealthState()
	releaseStop := make(chan struct{})
	release := closeWorkerGate(releaseStop)
	first := &blockingWorker{
		stopEntered: make(chan struct{}, 1),
		releaseStop: releaseStop,
		stopExited:  make(chan struct{}, 1),
	}
	temporalWorker, err := app.NewWorker(app.WorkerOptions{
		TaskQueue: "queue-a", MaxConcurrentActivities: 1, MaxConcurrentActivityTaskPolls: 1,
		GracefulStopTimeout: time.Second, Activities: &domainactivity.Activities{}, Health: health,
		Factory: func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
			return first, &fakeRegistry{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	if err := temporalWorker.Start(); err != nil {
		t.Fatal(err)
	}

	paused := make(chan struct{})
	go func() {
		temporalWorker.Pause()
		close(paused)
	}()
	waitForWorkerEvent(t, first.stopEntered, "first controller drain")
	waitForWorkerEvent(t, paused, "Pause to return before the graceful drain completes")

	stopped := make(chan struct{})
	go func() {
		temporalWorker.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("permanent Stop returned before the owned pause drain completed")
	case <-time.After(25 * time.Millisecond):
	}
	if err := temporalWorker.Resume(); !errors.Is(err, app.ErrWorkerStopping) {
		t.Fatalf("Resume while stopping = %v, want ErrWorkerStopping", err)
	}
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
	releaseStart := make(chan struct{})
	releaseStop := make(chan struct{})
	releaseStartGate := closeWorkerGate(releaseStart)
	releaseStopGate := closeWorkerGate(releaseStop)
	first := &blockingWorker{
		startEntered: make(chan struct{}, 1),
		releaseStart: releaseStart,
		stopEntered:  make(chan struct{}, 1),
		releaseStop:  releaseStop,
		stopExited:   make(chan struct{}, 1),
	}
	controllers := make([]app.WorkerController, 0, 2)
	temporalWorker, err := app.NewWorker(app.WorkerOptions{
		TaskQueue: "queue-a", MaxConcurrentActivities: 1, MaxConcurrentActivityTaskPolls: 1,
		GracefulStopTimeout: time.Second, Activities: &domainactivity.Activities{}, Health: health,
		Factory: func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
			if len(controllers) == 0 {
				controllers = append(controllers, first)
				return first, &fakeRegistry{}, nil
			}
			controller := &fakeWorker{}
			controllers = append(controllers, controller)
			return controller, &fakeRegistry{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
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
		if err != nil {
			t.Fatalf("Start after Pause = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start waited for the paused controller's graceful drain")
	}
	waitForWorkerEvent(t, first.stopEntered, "paused controller drain")
	if err := temporalWorker.Resume(); !errors.Is(err, app.ErrWorkerDraining) {
		t.Fatalf("Resume while draining = %v, want ErrWorkerDraining", err)
	}
	if len(controllers) != 1 || first.stopCalls.Load() != 1 {
		t.Fatalf("controllers=%d stop-calls=%d after pause during start", len(controllers), first.stopCalls.Load())
	}

	releaseStopGate()
	waitForWorkerEvent(t, first.stopExited, "paused controller drain completion")
	resumeWorkerAfterDrain(t, temporalWorker, "the paused controller drain")
	if len(controllers) != 2 || !health.Ready() || !temporalWorker.Started() {
		t.Fatalf("resume after paused start controllers=%d ready=%v started=%v", len(controllers), health.Ready(), temporalWorker.Started())
	}
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
	temporalWorker, err := app.NewWorker(app.WorkerOptions{
		TaskQueue: "queue-a", MaxConcurrentActivities: 1, MaxConcurrentActivityTaskPolls: 1,
		GracefulStopTimeout: time.Second, Activities: &domainactivity.Activities{}, Health: health,
		Factory: func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
			switch factoryCalls.Add(1) {
			case 1:
				return initial, &fakeRegistry{}, nil
			case 2:
				signalWorkerEvent(buildEntered)
				<-releaseBuild
				return builtWhilePaused, &fakeRegistry{}, nil
			default:
				return replacement, &fakeRegistry{}, nil
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { temporalWorker.Stop() })
	t.Cleanup(releaseBuildGate)
	if err := temporalWorker.Start(); err != nil {
		t.Fatal(err)
	}
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
			if !errors.Is(err, app.ErrWorkerDraining) {
				t.Fatalf("Resume before build = %v, want ErrWorkerDraining", err)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for replacement build")
		}
	}

	// Pause wins before the starter commits permission to call Start. The
	// completed Resume must leave the newly built controller detached.
	temporalWorker.Pause()
	releaseBuildGate()
	if err := <-resumeDone; err != nil {
		t.Fatalf("Resume after Pause during build = %v", err)
	}
	if builtWhilePaused.started.Load() || health.Ready() || temporalWorker.Started() {
		t.Fatalf("uncommitted build started=%v ready=%v worker-started=%v", builtWhilePaused.started.Load(), health.Ready(), temporalWorker.Started())
	}

	if err := temporalWorker.Resume(); err != nil {
		t.Fatal(err)
	}
	if !replacement.started.Load() || !health.Ready() || !temporalWorker.Started() {
		t.Fatalf("replacement after cancelled build started=%v ready=%v worker-started=%v", replacement.started.Load(), health.Ready(), temporalWorker.Started())
	}
}

func TestWorkerStopDuringStartWaitsForControllerStop(t *testing.T) {
	health := httpserver.NewHealthState()
	releaseStart := make(chan struct{})
	releaseStop := make(chan struct{})
	releaseStartGate := closeWorkerGate(releaseStart)
	releaseStopGate := closeWorkerGate(releaseStop)
	first := &blockingWorker{
		startEntered: make(chan struct{}, 1),
		releaseStart: releaseStart,
		stopEntered:  make(chan struct{}, 1),
		releaseStop:  releaseStop,
		stopExited:   make(chan struct{}, 1),
	}
	temporalWorker, err := app.NewWorker(app.WorkerOptions{
		TaskQueue: "queue-a", MaxConcurrentActivities: 1, MaxConcurrentActivityTaskPolls: 1,
		GracefulStopTimeout: time.Second, Activities: &domainactivity.Activities{}, Health: health,
		Factory: func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
			return first, &fakeRegistry{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(releaseStartGate)
	t.Cleanup(releaseStopGate)

	started := make(chan error, 1)
	go func() { started <- temporalWorker.Start() }()
	waitForWorkerEvent(t, first.startEntered, "first controller start")

	stopped := make(chan struct{})
	go func() {
		temporalWorker.Stop()
		close(stopped)
	}()
	waitForWorkerCondition(t, func() bool {
		return errors.Is(temporalWorker.Resume(), app.ErrWorkerStopping)
	}, "Stop to own the in-progress start")

	releaseStartGate()
	waitForWorkerEvent(t, first.stopEntered, "controller stop after start completes")
	select {
	case <-stopped:
		t.Fatal("Stop returned before the in-progress controller completed its graceful stop")
	case <-time.After(25 * time.Millisecond):
	}

	releaseStopGate()
	waitForWorkerEvent(t, first.stopExited, "controller stop completion")
	if err := <-started; !errors.Is(err, app.ErrWorkerStopping) {
		t.Fatalf("Start after Stop = %v, want ErrWorkerStopping", err)
	}
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
	value, err := app.NewWorker(app.WorkerOptions{
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
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(value.Stop)
	if err := value.Start(); err != nil {
		t.Fatal(err)
	}
	value.Pause()
	resumeWorkerAfterDrain(t, value, "new workflow worker")
	if len(registries) != 2 || !controllers[0].stopped.Load() || !controllers[1].started.Load() {
		t.Fatal("worker was not replaced")
	}
	descriptors, err := domainactivity.V1ActivityDescriptors("v1-queue")
	if err != nil {
		t.Fatal(err)
	}
	expectedActivities := make([]string, 0, len(descriptors))
	for _, descriptor := range descriptors {
		expectedActivities = append(expectedActivities, descriptor.Name)
	}
	expectedWorkflows := []string{workflows.RequestWorkflowName, workflows.BudgetWorkflowName, workflows.GenerateWorkflowName, workflows.CompactWorkflowName, workflows.QueryWorkflowName}
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
	temporalWorker, err := app.NewWorker(app.WorkerOptions{
		TaskQueue: "queue-a", MaxConcurrentActivities: 1, MaxConcurrentActivityTaskPolls: 1,
		GracefulStopTimeout: graceful, PauseDrainTimeout: pauseDrain,
		Activities: &domainactivity.Activities{}, Health: httpserver.NewHealthState(),
		Factory: func(_ client.Client, _ string, options worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
			controller := newSDKLikeWorker(options)
			mu.Lock()
			controllers = append(controllers, controller)
			mu.Unlock()
			return controller, &fakeRegistry{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
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
	if err := temporalWorker.Start(); err != nil {
		t.Fatal(err)
	}
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
	if err := temporalWorker.Start(); err != nil {
		t.Fatal(err)
	}
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
	if err := temporalWorker.Start(); err != nil {
		t.Fatal(err)
	}
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
