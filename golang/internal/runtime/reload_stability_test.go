package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
)

// manualConfigFileWatcher returns a watcher whose ticker never fires in a
// test, so each observation is an explicit poll call.
func manualConfigFileWatcher(t *testing.T, path string, baseline []byte) *configFileWatcher {
	t.Helper()
	watcher, err := newConfigFileWatcherWithBaseline(path, time.Hour, baseline)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = watcher.Close() })
	return watcher
}

func watcherNotified(watcher *configFileWatcher) bool {
	select {
	case <-watcher.changes:
		return true
	default:
		return false
	}
}

func writeConfigFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestConfigFileWatcherNotifiesOnlyAfterTwoEqualPolls(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeConfigFile(t, path, "first")
	watcher := manualConfigFileWatcher(t, path, []byte("first"))

	watcher.poll()
	if watcherNotified(watcher) {
		t.Fatal("unchanged file was notified")
	}
	// Each poll sees the file at a different point of an in-place write.
	for _, partial := range []string{"sec", "second ver", "second version"} {
		writeConfigFile(t, path, partial)
		watcher.poll()
		if watcherNotified(watcher) {
			t.Fatalf("file still changing between polls was notified at %q", partial)
		}
	}
	watcher.poll()
	if !watcherNotified(watcher) {
		t.Fatal("file unchanged across two polls was not notified")
	}
	if !watcher.settled([]byte("second version")) {
		t.Fatal("the notified content is not the settled content")
	}
	watcher.poll()
	if watcherNotified(watcher) {
		t.Fatal("stable file was notified twice")
	}
}

func TestConfigFileWatcherReobservesAfterALateReadSawOtherContent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeConfigFile(t, path, "stable")
	watcher := manualConfigFileWatcher(t, path, []byte("stable"))

	if watcher.settled([]byte("stab")) {
		t.Fatal("content the watcher never saw stable was accepted")
	}
	// The mismatch must not be lost even when the metadata is unchanged again.
	watcher.poll()
	if watcherNotified(watcher) || watcher.settled([]byte("stable")) {
		t.Fatal("watcher settled after a single poll")
	}
	watcher.poll()
	if !watcherNotified(watcher) || !watcher.settled([]byte("stable")) {
		t.Fatal("watcher did not notify the content once it was stable again")
	}
}

func TestConfigFileWatcherDefersStartupReplacementUntilStable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeConfigFile(t, path, "version: replacement\n")
	watcher := manualConfigFileWatcher(t, path, []byte("version: initial\n"))
	if watcherNotified(watcher) {
		t.Fatal("startup replacement was notified before it was seen stable")
	}
	watcher.poll()
	watcher.poll()
	if !watcherNotified(watcher) {
		t.Fatal("stable startup replacement was not notified")
	}
}

func TestConfigFileWatcherDetectsConfigMapSymlinkSwap(t *testing.T) {
	directory := t.TempDir()
	for _, version := range []string{"first", "second"} {
		if err := os.Mkdir(filepath.Join(directory, version), 0700); err != nil {
			t.Fatal(err)
		}
		// Equal sizes: only the file identity distinguishes the two versions.
		writeConfigFile(t, filepath.Join(directory, version, "config.yaml"), "version: "+version[:5]+"\n")
	}
	link := filepath.Join(directory, "data")
	if err := os.Symlink("first", link); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(link, "config.yaml")
	watcher := manualConfigFileWatcher(t, path, []byte("version: first\n"))

	next := filepath.Join(directory, "next")
	if err := os.Symlink("second", next); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(next, link); err != nil {
		t.Fatal(err)
	}
	watcher.poll()
	watcher.poll()
	if !watcherNotified(watcher) {
		t.Fatal("watcher did not observe the symlink swap")
	}
}

// budgetsLastConfig moves the budgets section to the end of the file, where a
// prefix that stops before it is still a complete, valid configuration.
func budgetsLastConfig(t *testing.T, providerTimeout string) (complete, withoutBudgets string) {
	t.Helper()
	value := string(runtimeConfig(t))
	start := strings.Index(value, "\nbudgets:\n")
	end := strings.Index(value, "\ncontinuation:\n")
	if start < 0 || end < start || !strings.Contains(value, "provider_timeout: 120s") {
		t.Fatal("example configuration layout changed")
	}
	withoutBudgets = strings.Replace(value[:start]+value[end:], "provider_timeout: 120s", "provider_timeout: "+providerTimeout, 1)
	return withoutBudgets + value[start:end] + "\n", withoutBudgets
}

func TestWatchedReloadNeverPublishesAFileCaughtMidWrite(t *testing.T) {
	controller := &testWorker{}
	var closed atomic.Bool
	data := runtimeConfig(t)
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeConfigFile(t, path, string(data))
	runtime, err := New(context.Background(), data, testRuntimeOptions(t, controller, &closed))
	if err != nil {
		t.Fatal(err)
	}
	watcher := manualConfigFileWatcher(t, path, data)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 1)
	done := make(chan error, 1)
	go func() { done <- runtime.runWatchedFile(ctx, path, watcher, signals) }()
	waitForReloadRuntimeStart(t, runtime, done)

	published := func() config.Config { return runtime.App.Current().Config.Config() }
	requirePublished := func(successes float64, providerTimeout time.Duration) {
		t.Helper()
		waitForRuntime(t, func() bool { return reloadOutcome(t, runtime.Metrics, "success") == successes })
		current := published()
		if time.Duration(current.Limits.ProviderTimeout) != providerTimeout {
			t.Fatalf("published provider timeout = %s, want %s", current.Limits.ProviderTimeout, providerTimeout)
		}
		if len(current.Budgets.Policies) != 1 || len(current.Budgets.Policies[0].Windows) != 3 || !current.Budgets.RequireMatch {
			t.Fatalf("published budgets = %#v, want the complete policy", current.Budgets)
		}
		if failures := reloadOutcome(t, runtime.Metrics, "failure"); failures != 0 {
			t.Fatalf("reload failures = %v, want none", failures)
		}
	}

	// An in-place write observed halfway: the first poll sees a valid prefix
	// without budgets, the next one the complete file.
	complete, prefix := budgetsLastConfig(t, "121s")
	if _, err := config.Load([]byte(prefix)); err != nil {
		t.Fatalf("the truncated prefix must be a valid configuration for this test: %v", err)
	}
	writeConfigFile(t, path, prefix)
	watcher.poll()
	writeConfigFile(t, path, complete)
	watcher.poll()
	if got := reloadOutcome(t, runtime.Metrics, "success"); got != 0 {
		t.Fatalf("reload ran %v times while the file was still changing", got)
	}
	watcher.poll()
	requirePublished(1, 121*time.Second)

	// A new write begins after the notification but before the reload reads
	// the file. The reload must decline the prefix and wait for the watcher.
	complete, prefix = budgetsLastConfig(t, "122s")
	writeConfigFile(t, path, prefix)
	watcher.changes <- struct{}{}
	waitForRuntime(t, func() bool {
		watcher.mu.Lock()
		defer watcher.mu.Unlock()
		return watcher.unsettled
	})
	requirePublished(1, 121*time.Second)
	writeConfigFile(t, path, complete)
	watcher.poll()
	watcher.poll()
	requirePublished(2, 122*time.Second)

	// SIGHUP is explicit operator intent and does not wait for stability.
	complete, _ = budgetsLastConfig(t, "123s")
	writeConfigFile(t, path, complete)
	signals <- syscall.SIGHUP
	requirePublished(3, 123*time.Second)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runWatchedFile returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runWatchedFile did not shut down")
	}
}

// config.Validate must reject exactly the budget windows that budget
// compilation rejects at worker start, so validate-config is authoritative.
func TestConfigValidationAgreesWithBudgetCompilationBounds(t *testing.T) {
	const window = "duration: 30d\n          bucket: 1h\n          limit_usd: \"3000.000000000000000000\""
	tests := []struct {
		name, replacement string
		valid             bool
	}{
		{"most buckets", "duration: 2046m\n          bucket: 1m\n          limit_usd: \"3000\"", true},
		{"one bucket too many", "duration: 2047m\n          bucket: 1m\n          limit_usd: \"3000\"", false},
		{"largest limit", "duration: 30d\n          bucket: 1h\n          limit_usd: \"9007199.254740991\"", true},
		{"limit above the Redis range", "duration: 30d\n          bucket: 1h\n          limit_usd: \"9007199.254740992\"", false},
		{"smallest limit", "duration: 30d\n          bucket: 1h\n          limit_usd: \"0.000000001\"", true},
		{"limit below one nano-USD", "duration: 30d\n          bucket: 1h\n          limit_usd: \"0.0000000009\"", false},
	}
	base := string(runtimeConfig(t))
	if !strings.Contains(base, window) {
		t.Fatal("example budget window changed")
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, err := config.Load([]byte(strings.Replace(base, window, test.replacement, 1)))
			if !test.valid {
				if err == nil || !strings.Contains(err.Error(), "budgets.policies[0].windows[2]") {
					t.Fatalf("validation error = %v, want one naming the window", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := compileBudgetPolicies(value); err != nil {
				t.Fatalf("validated configuration fails budget compilation: %v", err)
			}
		})
	}
}

// A SIGHUP that arrives while a watcher-triggered reload is already in flight
// must not exempt the bytes that reload read before the signal; the bypass
// belongs to the reload the signal queues.
func TestWatchedReloadGateBindsSIGHUPToTheReloadItQueues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeConfigFile(t, path, "stable")
	gate := &watchedReloadGate{watcher: manualConfigFileWatcher(t, path, []byte("stable"))}

	watcherReload := gate.begin()
	gate.signal()
	if watcherReload == nil || watcherReload([]byte("stab")) {
		t.Fatal("a later SIGHUP exempted bytes a watcher-triggered reload had already read")
	}
	if gate.begin() != nil {
		t.Fatal("the SIGHUP reload lost its bypass to the reload already in flight")
	}
	if gate.begin() == nil {
		t.Fatal("one SIGHUP exempted more than one reload")
	}
}
