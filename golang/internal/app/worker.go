package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/httpserver"
	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	"github.com/mfow/llm-temporal-worker/golang/workflows"
	"go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"
)

var (
	ErrWorkerStopping = errors.New("worker is stopping")
	// ErrWorkerDraining means a paused controller is still completing its
	// graceful stop. Callers can retry after the dependency monitor's next
	// successful check; a replacement controller must not overlap it.
	ErrWorkerDraining = errors.New("worker is draining")
)

type WorkerController interface {
	Start() error
	Stop()
}

// WorkerRegistry requires both activity and workflow registration so a factory
// cannot silently construct a worker unable to execute its public entry points.
type WorkerRegistry interface {
	sdkworker.ActivityRegistry
	sdkworker.WorkflowRegistry
}

type WorkerFactory func(client.Client, string, sdkworker.Options) (WorkerController, WorkerRegistry, error)

type WorkerOptions struct {
	Versioning                     config.WorkerVersioningConfig
	Client                         client.Client
	TaskQueue                      string
	Identity                       string
	MaxConcurrentActivities        int
	MaxConcurrentActivityTaskPolls int
	GracefulStopTimeout            time.Duration
	// PauseDrainTimeout bounds how long a paused controller lets in-flight
	// Activities finish before the Temporal SDK cancels them. A pause is not a
	// process exit, so it should cover the longest Activity rather than the
	// graceful stop timeout. Zero falls back to GracefulStopTimeout.
	PauseDrainTimeout time.Duration
	Activities        *activity.Activities
	Health            *httpserver.HealthState
	Metrics           *observability.Metrics
	Factory           WorkerFactory
}

// boundController pairs a Temporal worker controller with the cancel function
// of the BackgroundActivityContext it was built with. The SDK worker's own
// stop timeout is the pause drain window, so a Pause lets in-flight Activities
// finish; a permanent Stop cancels them itself once the graceful stop timeout
// elapses, which is what the SDK did when that was its stop timeout.
type boundController struct {
	WorkerController
	cancelActivities context.CancelCauseFunc
}

// drain stops the controller and lets in-flight Activities run to completion
// within the pause drain window.
func (controller *boundController) drain() {
	controller.WorkerController.Stop()
	controller.cancelActivities(nil)
}

// stopGracefully stops the controller and cancels in-flight Activities when
// the graceful stop timeout elapses before they complete.
func (controller *boundController) stopGracefully(graceful time.Duration) {
	stopped := make(chan struct{})
	go func() {
		controller.WorkerController.Stop()
		close(stopped)
	}()
	controller.awaitStop(stopped, graceful)
}

// awaitStop waits for a stop already in progress on the controller, cancelling
// its in-flight Activities when the graceful stop timeout elapses first.
func (controller *boundController) awaitStop(stopped <-chan struct{}, graceful time.Duration) {
	timer := time.NewTimer(graceful)
	defer timer.Stop()
	select {
	case <-stopped:
	case <-timer.C:
		controller.cancelActivities(sdkworker.ErrWorkerShutdown)
		<-stopped
	}
	controller.cancelActivities(nil)
}

type TemporalWorker struct {
	controller   *boundController
	build        func() (*boundController, error)
	gracefulStop time.Duration
	health       *httpserver.HealthState
	metrics      *observability.Metrics

	mu       sync.Mutex
	started  bool
	starting bool
	// startAuthorized is the start-call linearization point. A Pause that
	// observes it false prevents controller.Start from being called; once true,
	// Pause returns promptly and startController drains the controller on return.
	startAuthorized bool
	paused          bool
	stopping        bool

	startDone chan struct{}
	drainDone chan struct{}
	draining  *boundController
	stopDone  chan struct{}
}

func NewWorker(options WorkerOptions) (*TemporalWorker, error) {
	if err := options.Versioning.Validate(); err != nil {
		return nil, err
	}
	if options.TaskQueue == "" {
		return nil, fmt.Errorf("Temporal task queue is required")
	}
	if _, err := activity.V1ActivityDescriptors(options.TaskQueue); err != nil {
		return nil, fmt.Errorf("invalid Temporal task queue: %w", err)
	}
	if options.Activities == nil {
		return nil, fmt.Errorf("Activity implementation is required")
	}
	if options.MaxConcurrentActivities <= 0 || options.MaxConcurrentActivityTaskPolls <= 0 {
		return nil, fmt.Errorf("Temporal worker concurrency must be positive")
	}
	if options.GracefulStopTimeout <= 0 {
		return nil, fmt.Errorf("Temporal graceful stop timeout must be positive")
	}
	if options.PauseDrainTimeout < 0 {
		return nil, fmt.Errorf("Temporal pause drain timeout must not be negative")
	}
	if options.PauseDrainTimeout < options.GracefulStopTimeout {
		options.PauseDrainTimeout = options.GracefulStopTimeout
	}
	if options.Health == nil {
		options.Health = httpserver.NewHealthState()
	}
	if options.Factory == nil {
		options.Factory = defaultWorkerFactory
	}
	var deployment sdkworker.DeploymentOptions
	if options.Versioning.Enabled {
		deployment = sdkworker.DeploymentOptions{
			UseVersioning:             true,
			Version:                   sdkworker.WorkerDeploymentVersion{DeploymentName: options.Versioning.DeploymentName, BuildID: options.Versioning.BuildID},
			DefaultVersioningBehavior: sdkworkflow.VersioningBehaviorPinned,
		}
	}
	build := func() (*boundController, error) {
		activityContext, cancelActivities := context.WithCancelCause(context.Background())
		controller, registry, err := options.Factory(options.Client, options.TaskQueue, sdkworker.Options{
			Identity:                           options.Identity,
			DeploymentOptions:                  deployment,
			MaxConcurrentActivityExecutionSize: options.MaxConcurrentActivities,
			MaxConcurrentActivityTaskPollers:   options.MaxConcurrentActivityTaskPolls,
			WorkerStopTimeout:                  options.PauseDrainTimeout,
			BackgroundActivityContext:          activityContext,
		})
		if err != nil {
			cancelActivities(nil)
			return nil, fmt.Errorf("construct Temporal worker: %w", err)
		}
		if controller == nil || registry == nil {
			cancelActivities(nil)
			return nil, fmt.Errorf("Temporal worker factory returned incomplete worker")
		}
		if err := options.Activities.RegisterForTaskQueue(registry, options.TaskQueue); err != nil {
			cancelActivities(nil)
			return nil, fmt.Errorf("register Temporal Activities: %w", err)
		}
		// The v1 workflows schedule the v1 Activities; without a v1 runtime
		// (development memory mode) they could only stall, so a worker that
		// has none does not offer them.
		if options.Activities.V1Runtime != nil {
			workflows.Register(registry, options.Activities.PayloadLimits)
		}
		return &boundController{WorkerController: controller, cancelActivities: cancelActivities}, nil
	}
	controller, err := build()
	if err != nil {
		return nil, err
	}
	return &TemporalWorker{controller: controller, build: build, gracefulStop: options.GracefulStopTimeout, health: options.Health, metrics: options.Metrics}, nil
}

func defaultWorkerFactory(workflowClient client.Client, taskQueue string, options sdkworker.Options) (WorkerController, WorkerRegistry, error) {
	if workflowClient == nil {
		return nil, nil, fmt.Errorf("Temporal client is required")
	}
	worker := sdkworker.New(workflowClient, taskQueue, options)
	return worker, worker, nil
}

func (worker *TemporalWorker) Start() error {
	if worker == nil {
		return fmt.Errorf("Temporal worker is not initialized")
	}
	worker.mu.Lock()
	if worker.stopping {
		worker.mu.Unlock()
		return ErrWorkerStopping
	}
	if worker.drainDone != nil {
		worker.mu.Unlock()
		return ErrWorkerDraining
	}
	if worker.started || worker.starting {
		worker.mu.Unlock()
		return fmt.Errorf("Temporal worker is already started")
	}
	worker.paused = false
	worker.starting = true
	worker.startAuthorized = false
	worker.startDone = make(chan struct{})
	controller := worker.controller
	worker.mu.Unlock()
	return worker.startController(controller)
}

// Pause stops Temporal polling without making the worker permanently
// unavailable. The detached controller drains asynchronously so dependency
// checks can continue; Resume creates a fresh controller only after that drain
// completes because a stopped Temporal SDK worker is not reusable.
func (worker *TemporalWorker) Pause() {
	if worker == nil {
		return
	}
	worker.mu.Lock()
	worker.setNotReadyLocked()
	if worker.stopping {
		worker.mu.Unlock()
		return
	}
	worker.paused = true
	if !worker.started {
		if worker.starting && !worker.startAuthorized {
			worker.controller = nil
		}
		worker.mu.Unlock()
		return
	}
	controller := worker.controller
	worker.controller = nil
	worker.started = false
	drainDone, draining := worker.beginDrainLocked(controller)
	worker.mu.Unlock()
	if draining {
		worker.drainController(controller, drainDone)
	}
}

// Resume restarts Temporal polling after a dependency monitor has proved that
// every required dependency is healthy again.
func (worker *TemporalWorker) Resume() error {
	if worker == nil {
		return fmt.Errorf("Temporal worker is not initialized")
	}
	worker.mu.Lock()
	if worker.stopping {
		worker.mu.Unlock()
		return ErrWorkerStopping
	}
	if worker.drainDone != nil {
		worker.mu.Unlock()
		return ErrWorkerDraining
	}
	if worker.started || worker.starting {
		worker.mu.Unlock()
		return fmt.Errorf("Temporal worker is already started")
	}
	worker.paused = false
	worker.starting = true
	worker.startAuthorized = false
	worker.startDone = make(chan struct{})
	controller := worker.controller
	worker.mu.Unlock()
	return worker.startController(controller)
}

func (worker *TemporalWorker) startController(controller *boundController) error {
	if controller == nil {
		if worker.build == nil {
			worker.finishStartFailure(nil)
			return fmt.Errorf("Temporal worker is not initialized")
		}
		var err error
		controller, err = worker.build()
		if err != nil {
			worker.finishStartFailure(nil)
			return err
		}
	}
	startAllowed, err := worker.authorizeStart(controller)
	if err != nil || !startAllowed {
		return err
	}
	if err := controller.Start(); err != nil {
		worker.finishStartFailure(controller)
		return fmt.Errorf("start Temporal worker: %w", err)
	}
	worker.mu.Lock()
	if worker.stopping {
		worker.controller = nil
		worker.mu.Unlock()
		controller.stopGracefully(worker.gracefulStop)
		worker.mu.Lock()
		worker.finishStartLocked()
		worker.mu.Unlock()
		return ErrWorkerStopping
	}
	if worker.paused {
		worker.controller = nil
		drainDone, draining := worker.beginDrainLocked(controller)
		worker.finishStartLocked()
		worker.mu.Unlock()
		if draining {
			worker.drainController(controller, drainDone)
		}
		return nil
	}
	worker.started = true
	worker.setReadyLocked()
	worker.finishStartLocked()
	worker.mu.Unlock()
	return nil
}

// authorizeStart is the sole permission handoff to controller.Start. Keeping
// the paused check and permission commit under one lock makes a Pause that
// wins before this point cancel the pending start without blocking on a slow
// controller.Start call.
func (worker *TemporalWorker) authorizeStart(controller *boundController) (bool, error) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.stopping {
		worker.finishStartLocked()
		return false, ErrWorkerStopping
	}
	if worker.paused {
		worker.controller = nil
		controller.cancelActivities(nil)
		worker.finishStartLocked()
		return false, nil
	}
	worker.controller = controller
	worker.startAuthorized = true
	return true, nil
}

// finishStartFailure releases an authorized controller after Start fails.
// WorkerController ownership transfers at authorizeStart, and remains with the
// start attempt until cleanup returns so Resume cannot overlap a replacement
// and Stop waits for that cleanup.
func (worker *TemporalWorker) finishStartFailure(controller *boundController) {
	worker.mu.Lock()
	worker.setNotReadyLocked()
	worker.mu.Unlock()
	if controller != nil {
		controller.stopGracefully(worker.gracefulStop)
	}
	worker.mu.Lock()
	worker.controller = nil
	worker.finishStartLocked()
	worker.mu.Unlock()
}

func (worker *TemporalWorker) setReadyLocked() {
	worker.health.SetReady(true)
	if worker.metrics != nil {
		worker.metrics.SetWorkerPolling(true)
	}
}

func (worker *TemporalWorker) setNotReadyLocked() {
	worker.health.SetReady(false)
	if worker.metrics != nil {
		worker.metrics.SetWorkerPolling(false)
	}
}

func (worker *TemporalWorker) finishStartLocked() {
	worker.starting = false
	worker.startAuthorized = false
	if worker.startDone == nil {
		return
	}
	close(worker.startDone)
	worker.startDone = nil
}

func (worker *TemporalWorker) beginDrainLocked(controller *boundController) (chan struct{}, bool) {
	if controller == nil || worker.drainDone != nil {
		return worker.drainDone, false
	}
	done := make(chan struct{})
	worker.drainDone = done
	worker.draining = controller
	return done, true
}

func (worker *TemporalWorker) drainController(controller *boundController, done chan struct{}) {
	if controller == nil || done == nil {
		return
	}
	go func() {
		controller.drain()
		worker.mu.Lock()
		if worker.drainDone == done {
			worker.drainDone = nil
			worker.draining = nil
		}
		worker.mu.Unlock()
		close(done)
	}()
}

// Stop turns readiness off before stopping pollers. A permanent stop waits for
// an owned pause drain before allowing client teardown to continue. In-flight
// Activity calls get the configured graceful wait and are then cancelled.
func (worker *TemporalWorker) Stop() {
	if worker == nil {
		return
	}
	worker.mu.Lock()
	worker.setNotReadyLocked()
	if worker.stopping {
		done := worker.stopDone
		worker.mu.Unlock()
		if done != nil {
			<-done
		}
		return
	}
	worker.stopping = true
	stopDone := make(chan struct{})
	worker.stopDone = stopDone
	starting := worker.starting
	startDone := worker.startDone
	drainDone := worker.drainDone
	draining := worker.draining
	started := worker.started
	controller := worker.controller
	if !starting && drainDone == nil {
		worker.started = false
		worker.controller = nil
	}
	worker.mu.Unlock()
	defer close(stopDone)
	if starting {
		if startDone != nil {
			<-startDone
		}
		return
	}
	if drainDone != nil {
		draining.awaitStop(drainDone, worker.gracefulStop)
		return
	}
	if started && controller != nil {
		controller.stopGracefully(worker.gracefulStop)
	}
}

func (worker *TemporalWorker) Started() bool {
	if worker == nil {
		return false
	}
	worker.mu.Lock()
	defer worker.mu.Unlock()
	return worker.started
}

func (worker *TemporalWorker) Ready() bool {
	return worker != nil && worker.health.Ready()
}

// Run starts the worker and waits for cancellation. It is useful for the CLI,
// while tests can use Start and Stop directly.
func (worker *TemporalWorker) Run(ctx context.Context) error {
	if err := worker.Start(); err != nil {
		return err
	}
	<-ctx.Done()
	worker.Stop()
	return ctx.Err()
}
