package app_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/httpserver"
)

type shutdownWorker struct{ events *[]string }

func (worker shutdownWorker) Stop() { *worker.events = append(*worker.events, "worker.stop") }

type shutdownTelemetry struct {
	events *[]string
	err    error
}

func (telemetry shutdownTelemetry) Flush(context.Context) error {
	if telemetry.events != nil {
		*telemetry.events = append(*telemetry.events, "telemetry.flush")
	}
	return telemetry.err
}

func TestShutdownOrderingAndReadiness(t *testing.T) {
	events := []string{}
	health := httpserver.NewHealthState()
	health.SetReady(true)
	coordinator, err := app.NewShutdownCoordinator(app.ShutdownOptions{
		Worker: shutdownWorker{events: &events}, Health: health, Timeout: time.Second,
		CloseApp:  func(context.Context) error { events = append(events, "app.close"); return nil },
		Telemetry: []app.TelemetryFlusher{shutdownTelemetry{events: &events}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if health.Ready() {
		t.Fatal("readiness remained true")
	}
	if want := []string{"worker.stop", "app.close", "telemetry.flush"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("shutdown order = %#v, want %#v", events, want)
	}
	if err := coordinator.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownReturnsTelemetryError(t *testing.T) {
	expected := errors.New("flush failed")
	coordinator, err := app.NewShutdownCoordinator(app.ShutdownOptions{
		Timeout:   time.Second,
		Telemetry: []app.TelemetryFlusher{shutdownTelemetry{err: expected}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Shutdown(context.Background()); !errors.Is(err, expected) {
		t.Fatalf("shutdown error = %v", err)
	}
}

func TestShutdownTimeoutStillClosesSnapshot(t *testing.T) {
	stopStarted := make(chan struct{})
	release := make(chan struct{})
	worker := blockingShutdownWorker{started: stopStarted, release: release}
	closed := false
	coordinator, err := app.NewShutdownCoordinator(app.ShutdownOptions{
		Worker: worker, Timeout: 20 * time.Millisecond,
		CloseApp: func(ctx context.Context) error { closed = true; return ctx.Err() },
	})
	if err != nil {
		t.Fatal(err)
	}
	err = coordinator.Shutdown(context.Background())
	if err == nil || !errors.Is(err, context.DeadlineExceeded) || !closed {
		t.Fatalf("timeout shutdown error=%v closed=%v", err, closed)
	}
	close(release)
	<-stopStarted
}

type blockingShutdownWorker struct {
	started chan struct{}
	release chan struct{}
}

func (worker blockingShutdownWorker) Stop() {
	close(worker.started)
	<-worker.release
}
