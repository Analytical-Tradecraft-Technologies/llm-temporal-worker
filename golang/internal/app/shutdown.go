package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/httpserver"
)

type FlushFunc func(context.Context) error

func (function FlushFunc) Flush(ctx context.Context) error {
	if function == nil {
		return nil
	}
	return function(ctx)
}

type TelemetryFlusher interface {
	Flush(context.Context) error
}

type ShutdownOptions struct {
	Worker    interface{ Stop() }
	Health    *httpserver.HealthState
	CloseApp  func(context.Context) error
	Telemetry []TelemetryFlusher
	Timeout   time.Duration
}

type ShutdownCoordinator struct {
	worker    interface{ Stop() }
	health    *httpserver.HealthState
	closeApp  func(context.Context) error
	telemetry []TelemetryFlusher
	timeout   time.Duration

	once sync.Once
	err  error
}

func NewShutdownCoordinator(options ShutdownOptions) (*ShutdownCoordinator, error) {
	if options.Timeout <= 0 {
		return nil, fmt.Errorf("shutdown timeout must be positive")
	}
	return &ShutdownCoordinator{
		worker: options.Worker, health: options.Health, closeApp: options.CloseApp,
		telemetry: append([]TelemetryFlusher(nil), options.Telemetry...), timeout: options.Timeout,
	}, nil
}

// Shutdown enforces the documented ordering: fail readiness, stop Temporal
// polling, drain the published snapshot, then flush telemetry.
func (coordinator *ShutdownCoordinator) Shutdown(ctx context.Context) error {
	if coordinator == nil {
		return nil
	}
	coordinator.once.Do(func() {
		coordinator.err = coordinator.shutdown(ctx)
	})
	return coordinator.err
}

func (coordinator *ShutdownCoordinator) shutdown(parent context.Context) error {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, coordinator.timeout)
	defer cancel()
	if coordinator.health != nil {
		coordinator.health.SetReady(false)
	}

	var errs []error
	if coordinator.worker != nil {
		stopped := make(chan struct{})
		go func() {
			coordinator.worker.Stop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-ctx.Done():
			errs = append(errs, fmt.Errorf("stop Temporal worker: %w", ctx.Err()))
		}
	}
	if coordinator.closeApp != nil {
		if err := coordinator.closeApp(ctx); err != nil {
			errs = append(errs, fmt.Errorf("close runtime snapshot: %w", err))
		}
	}
	for index, telemetry := range coordinator.telemetry {
		if telemetry == nil {
			continue
		}
		if err := telemetry.Flush(ctx); err != nil {
			errs = append(errs, fmt.Errorf("flush telemetry %d: %w", index, err))
		}
	}
	return errors.Join(errs...)
}
