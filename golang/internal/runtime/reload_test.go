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

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
)

func TestRuntimeReloadFileRejectsBadReplacementAndRecordsBoundedFailure(t *testing.T) {
	controller := &testWorker{}
	var closed atomic.Bool
	options := testRuntimeOptions(t, controller, &closed)
	var logs bytes.Buffer
	options.LogOutput = &logs
	runtime := newTestRuntime(t, context.Background(), runtimeConfig(t), options)
	old := runtime.App.Current()
	path := tempConfigFile(t, "not-a-valid-config: true\n")
	err := runtime.ReloadFile(context.Background(), path)
	testassert.ErrorContains(t, err, "reload configuration failed")
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
	runtime := newTestRuntime(t, context.Background(), data, options)
	old := runtime.App.Current()
	oldVersion := old.Config.ConfigVersion()
	path := filepath.Join(t.TempDir(), "config.yaml")
	const replacementQueue = "llm-inference-restart-required"
	writeReplacedConfig(t, path, data, "task_queue: llm-inference", "task_queue: "+replacementQueue)

	err := runtime.ReloadFile(context.Background(), path)
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
	runtime := newTestRuntime(t, context.Background(), data, testRuntimeOptions(t, controller, &closed))

	old := runtime.App.Current()
	lease, err := runtime.App.Acquire()
	testassert.NoError(t, err)
	defer lease.Release()

	path := filepath.Join(t.TempDir(), "config.yaml")
	writeReplacedConfig(t, path, data, "provider_timeout: 120s", "provider_timeout: 121s")

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
	testassert.NoError(t, old.WaitClosed(context.Background()))
}

func TestRuntimeRunReloadsFromSIGHUPTrigger(t *testing.T) {
	controller := &testWorker{}
	var closed atomic.Bool
	data := runtimeConfig(t)
	path := tempConfigFile(t, string(data))
	runtime := mustNewRuntime(t, data, testRuntimeOptions(t, controller, &closed))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 1)
	reloads := combineReloadTriggers(ctx, nil, signals)
	done := make(chan error, 1)
	go func() { done <- runtime.RunWithReload(ctx, path, reloads) }()
	waitForReloadRuntimeStart(t, runtime, done)
	oldVersion := runtime.App.Current().Config.ConfigVersion()
	writeReplacedConfig(t, path, data, "provider_timeout: 120s", "provider_timeout: 121s")
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
	requireCleanReturn(t, done, "RunWithReload")
}

func TestRuntimeRunCancellationInterruptsPublishedReloadDrain(t *testing.T) {
	controller := &testWorker{}
	var closed atomic.Bool
	data := runtimeConfig(t)
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeReplacedConfig(t, path, data, "provider_timeout: 120s", "provider_timeout: 121s")
	runtime := mustNewRuntime(t, data, testRuntimeOptions(t, controller, &closed))

	ctx, cancel := context.WithCancel(context.Background())
	reloads := make(chan struct{}, 1)
	done := make(chan error, 1)
	var lease *app.Lease
	runFinished := false
	defer func() {
		cancel()
		if lease != nil {
			lease.Release()
		}
		if !runFinished {
			select {
			case <-done:
			case <-time.After(2 * time.Second):
			}
		}
	}()
	go func() { done <- runtime.RunWithReload(ctx, path, reloads) }()
	waitForReloadRuntimeStart(t, runtime, done)
	old := runtime.App.Current()
	lease, err := runtime.App.Acquire()
	testassert.NoError(t, err)

	reloads <- struct{}{}
	waitForRuntime(t, func() bool {
		current := runtime.App.Current()
		return current != nil && current != old
	})
	if controller.stopped.Load() {
		t.Fatal("worker stopped before run cancellation")
	}
	cancel()

	deadline := time.After(time.Second)
	for !controller.stopped.Load() {
		select {
		case err := <-done:
			runFinished = true
			if err != nil {
				t.Fatalf("RunWithReload returned %v", err)
			}
			if !controller.stopped.Load() {
				t.Fatal("RunWithReload returned before the worker stopped")
			}
		case <-deadline:
			t.Fatal("cancellation did not begin shutdown while the published reload was draining")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if runtime.Health.Ready() {
		t.Fatal("worker stop began before readiness dropped")
	}

	// The old snapshot lease is deliberately held through the shutdown
	// assertions. Releasing it here must only finish its already-cancelled
	// cleanup; it must not be what allows shutdown to start.
	lease.Release()
	if !runFinished {
		select {
		case err := <-done:
			runFinished = true
			if err != nil {
				t.Fatalf("RunWithReload returned %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("RunWithReload did not finish after cancellation")
		}
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
	path := tempConfigFile(t, "first")
	watcher, err := newConfigFileWatcher(path, time.Millisecond)
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = watcher.Close() })
	replacement := filepath.Join(filepath.Dir(path), "replacement.yaml")
	testassert.NoError(t, os.WriteFile(replacement, []byte("second"), 0600))
	testassert.NoError(t, os.Rename(replacement, path))
	select {
	case <-watcher.Changes():
	case <-time.After(time.Second):
		t.Fatal("watcher did not observe atomic configuration replacement")
	}
}

func TestConfigFileWatcherDetectsReplacementBeforeWatcherInitialization(t *testing.T) {
	path := tempConfigFile(t, "version: replacement\n")
	watcher := pollingConfigFileWatcher(t, path, time.Millisecond, []byte("version: initial\n"))
	select {
	case <-watcher.Changes():
		// A metadata-only watcher would have incorrectly treated the
		// replacement as its initial state and emitted nothing.
	case <-time.After(time.Second):
		t.Fatal("watcher did not emit startup replacement notification")
	}
}

func TestConfigFileWatcherDoesNotEmitForMatchingStartupBaseline(t *testing.T) {
	const initial = "version: initial\n"
	path := tempConfigFile(t, initial)
	watcher := pollingConfigFileWatcher(t, path, time.Millisecond, []byte(initial))
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
	testassert.NoError(t, err)
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

func TestRuntimeReloadFileLabelsIdentifiersIntroducedByTheReload(t *testing.T) {
	controller := &testWorker{}
	var closed atomic.Bool
	options := testRuntimeOptions(t, controller, &closed)
	data := runtimeConfig(t)
	runtime := newTestRuntime(t, context.Background(), data, options)
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeReplacedConfig(t, path, data, "model: gpt-example-2026-07-01", "model: gpt-replacement-2026-09-20")
	// Hold the old snapshot so the reload publishes and then blocks draining:
	// requests can already run on the replacement during that interval.
	old := runtime.App.Current()
	lease, err := runtime.App.Acquire()
	testassert.NoError(t, err)
	reloaded := make(chan error, 1)
	go func() { reloaded <- runtime.ReloadFile(context.Background(), path) }()
	waitForRuntime(t, func() bool { return runtime.App.Current() != old })
	runtime.Metrics.RecordProviderAttempt("openai-prod", "gpt-replacement-2026-09-20", "standard", "success", time.Millisecond)
	lease.Release()
	testassert.NoError(t, <-reloaded)
	families, err := runtime.Metrics.Gather()
	testassert.NoError(t, err)
	found := false
	for _, family := range families {
		if family.GetName() != "llmtw_provider_attempt_total" {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "model" && label.GetValue() == "gpt-replacement-2026-09-20" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("model introduced by reload was not labelled after the reload")
	}
}

// writeReplacedConfig writes data to path with the first occurrence of old
// replaced by replacement, failing the test if data does not contain old.
func writeReplacedConfig(t *testing.T, path string, data []byte, old, replacement string) {
	t.Helper()
	if !bytes.Contains(data, []byte(old)) {
		t.Fatalf("test configuration does not contain %q", old)
	}
	testassert.NoError(t, os.WriteFile(path, bytes.Replace(data, []byte(old), []byte(replacement), 1), 0600))
}
