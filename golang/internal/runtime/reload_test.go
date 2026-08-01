package runtime

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
)

func TestRuntimeReloadFileRejectsBadReplacementAndRecordsBoundedFailure(t *testing.T) {
	controller := &testWorker{}
	var closed atomic.Bool
	options := testRuntimeOptions(t, controller, &closed)
	var logs bytes.Buffer
	options.LogOutput = &logs
	runtime, err := New(context.Background(), runtimeConfig(t), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	old := runtime.App.Current()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("not-a-valid-config: true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	err = runtime.ReloadFile(context.Background(), path)
	if err == nil || !strings.Contains(err.Error(), "reload configuration failed") {
		t.Fatalf("reload error = %v", err)
	}
	if runtime.App.Current() != old {
		t.Fatal("bad replacement published a new runtime snapshot")
	}
	if got := reloadOutcome(t, runtime.Metrics, "failure"); got != 1 {
		t.Fatalf("reload failure metric = %v, want 1", got)
	}
	if strings.Contains(logs.String(), "not-a-valid-config") || strings.Contains(logs.String(), path) {
		t.Fatalf("reload log leaked replacement data or path: %q", logs.String())
	}
}

func TestRuntimeReloadFileRejectsProcessLifetimeReplacementAndRecordsBoundedFailure(t *testing.T) {
	controller := &testWorker{}
	var closed atomic.Bool
	options := testRuntimeOptions(t, controller, &closed)
	var logs bytes.Buffer
	options.LogOutput = &logs
	data := runtimeConfig(t)
	runtime, err := New(context.Background(), data, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	old := runtime.App.Current()
	oldVersion := old.Config.ConfigVersion()
	path := filepath.Join(t.TempDir(), "config.yaml")
	const replacementQueue = "llm-inference-restart-required"
	replacement := strings.Replace(string(data), "task_queue: llm-inference", "task_queue: "+replacementQueue, 1)
	if replacement == string(data) {
		t.Fatal("test replacement did not change the task queue")
	}
	if err := os.WriteFile(path, []byte(replacement), 0600); err != nil {
		t.Fatal(err)
	}

	err = runtime.ReloadFile(context.Background(), path)
	if err == nil || err.Error() != "reload configuration failed" {
		t.Fatalf("reload error = %v, want sanitized rejection", err)
	}
	if runtime.App.Current() != old || runtime.App.Current().Config.ConfigVersion() != oldVersion {
		t.Fatal("process-lifetime replacement changed the active snapshot or version")
	}
	if got := reloadOutcome(t, runtime.Metrics, "failure"); got != 1 {
		t.Fatalf("reload failure metric = %v, want 1", got)
	}
	if strings.Contains(logs.String(), replacementQueue) || strings.Contains(logs.String(), path) {
		t.Fatalf("reload log leaked replacement data or path: %q", logs.String())
	}
}

func TestRuntimeReloadFileCountsPublishedDrainCancellationAsSuccess(t *testing.T) {
	controller := &testWorker{}
	var closed atomic.Bool
	data := runtimeConfig(t)
	runtime, err := New(context.Background(), data, testRuntimeOptions(t, controller, &closed))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })

	old := runtime.App.Current()
	lease, err := runtime.App.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()

	path := filepath.Join(t.TempDir(), "config.yaml")
	replacement := strings.Replace(string(data), "provider_timeout: 120s", "provider_timeout: 121s", 1)
	if err := os.WriteFile(path, []byte(replacement), 0600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reloaded := make(chan error, 1)
	go func() { reloaded <- runtime.ReloadFile(ctx, path) }()
	waitForRuntime(t, func() bool { return runtime.App.Current() != old })
	cancel()
	select {
	case err := <-reloaded:
		if err != nil {
			t.Fatalf("published reload returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("published reload did not finish after drain cancellation")
	}

	current := runtime.App.Current()
	if current == nil || current == old || current.Config.ConfigVersion() == old.Config.ConfigVersion() {
		t.Fatal("published reload did not retain the replacement snapshot")
	}
	if got := reloadOutcome(t, runtime.Metrics, "success"); got != 1 {
		t.Fatalf("reload success metric = %v, want 1", got)
	}
	if got := reloadOutcome(t, runtime.Metrics, "failure"); got != 0 {
		t.Fatalf("reload failure metric = %v, want 0", got)
	}

	lease.Release()
	if err := old.WaitClosed(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeRunReloadsFromSIGHUPTrigger(t *testing.T) {
	controller := &testWorker{}
	var closed atomic.Bool
	data := runtimeConfig(t)
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(context.Background(), data, testRuntimeOptions(t, controller, &closed))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 1)
	reloads := combineReloadTriggers(ctx, nil, signals)
	done := make(chan error, 1)
	go func() { done <- runtime.RunWithReload(ctx, path, reloads) }()
	waitForReloadRuntimeStart(t, runtime, done)
	oldVersion := runtime.App.Current().Config.ConfigVersion()
	replacement := strings.Replace(string(data), "provider_timeout: 120s", "provider_timeout: 121s", 1)
	if err := os.WriteFile(path, []byte(replacement), 0600); err != nil {
		t.Fatal(err)
	}
	signals <- syscall.SIGHUP
	waitForRuntime(t, func() bool {
		current := runtime.App.Current()
		if current == nil || current.Config.ConfigVersion() == oldVersion {
			return false
		}
		// ReloadFile publishes the replacement snapshot before recording the
		// outcome metric. Wait for both externally visible effects so this test
		// does not race RunWithReload after observing the swap.
		return reloadOutcome(t, runtime.Metrics, "success") == 1
	})
	if got := reloadOutcome(t, runtime.Metrics, "success"); got != 1 {
		t.Fatalf("reload success metric = %v, want 1", got)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunWithReload returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunWithReload did not shut down")
	}
}

func waitForReloadRuntimeStart(t *testing.T, runtime *Runtime, done <-chan error) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for !runtime.Health.Ready() {
		select {
		case err := <-done:
			if err != nil && strings.Contains(err.Error(), "operation not permitted") {
				t.Skipf("sandbox does not permit loopback listeners: %v", err)
			}
			t.Fatalf("RunWithReload started unsuccessfully: %v", err)
		case <-deadline:
			t.Fatal("RunWithReload did not become ready")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestConfigFileWatcherDetectsAtomicReplacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("first"), 0600); err != nil {
		t.Fatal(err)
	}
	watcher, err := newConfigFileWatcher(path, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = watcher.Close() })
	replacement := filepath.Join(filepath.Dir(path), "replacement.yaml")
	if err := os.WriteFile(replacement, []byte("second"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	select {
	case <-watcher.Changes():
	case <-time.After(time.Second):
		t.Fatal("watcher did not observe atomic configuration replacement")
	}
}

func TestConfigFileWatcherDetectsReplacementBeforeWatcherInitialization(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	initial := []byte("version: initial\n")
	replacement := []byte("version: replacement\n")
	if err := os.WriteFile(path, replacement, 0600); err != nil {
		t.Fatal(err)
	}
	watcher, err := newConfigFileWatcherWithBaseline(path, time.Hour, initial)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = watcher.Close() })
	select {
	case <-watcher.Changes():
		// A metadata-only watcher would have incorrectly treated the
		// replacement as its initial state and emitted nothing.
	case <-time.After(time.Second):
		t.Fatal("watcher did not emit startup replacement notification")
	}
}

func TestConfigFileWatcherDoesNotEmitForMatchingStartupBaseline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	initial := []byte("version: initial\n")
	if err := os.WriteFile(path, initial, 0600); err != nil {
		t.Fatal(err)
	}
	watcher, err := newConfigFileWatcherWithBaseline(path, time.Millisecond, initial)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = watcher.Close() })
	select {
	case <-watcher.Changes():
		t.Fatal("watcher emitted a false startup replacement notification")
	case <-time.After(20 * time.Millisecond):
	}
}

func reloadOutcome(t *testing.T, metrics *observability.Metrics, outcome string) float64 {
	t.Helper()
	if metrics == nil {
		t.Fatal("metrics are not configured")
	}
	families, err := metrics.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "llmtw_config_reload_total" {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "outcome" && label.GetValue() == outcome {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}
