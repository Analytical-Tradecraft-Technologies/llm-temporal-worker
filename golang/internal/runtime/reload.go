package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/internal/app"
)

const defaultConfigWatchInterval = time.Second

// maxConfigWatchBaselineBytes matches the application reload limit. The
// watcher reads this bounded amount once at startup to close the race between
// validating startup bytes and beginning metadata polling; subsequent polls
// remain metadata-only.
const maxConfigWatchBaselineBytes = 4 << 20

// ReloadFile compiles and verifies a complete file replacement before the app
// publishes it. Reload failures are deliberately reduced to a safe process
// boundary error: configuration contents, paths, and resolved references never
// become log fields or command output.
func (runtime *Runtime) ReloadFile(ctx context.Context, path string) error {
	if runtime == nil || runtime.App == nil {
		return errors.New("runtime is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	err := runtime.App.ReloadFile(ctx, path)
	if err != nil && !app.IsPublishedReloadError(err) {
		runtime.recordReloadFailure(ctx, err)
		return safeReloadError(err)
	}
	cleanupIncomplete := err != nil
	runtime.Metrics.RecordConfigReload("success")
	version := ""
	if current := runtime.App.Current(); current != nil && current.Config != nil {
		version = current.Config.ConfigVersion()
	}
	message := "configuration reloaded"
	if cleanupIncomplete {
		message = "configuration reloaded; previous snapshot cleanup did not finish before reload returned"
	}
	runtime.Logger.Info(ctx, message, slog.String("outcome", "success"), slog.String("config_version", version))
	return nil
}

func (runtime *Runtime) recordReloadFailure(ctx context.Context, err error) {
	if runtime == nil {
		return
	}
	runtime.Metrics.RecordConfigReload("failure")
	runtime.Logger.Error(ctx, "configuration reload failed", err, slog.String("outcome", "failure"))
}

func safeReloadError(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	default:
		return errors.New("reload configuration failed")
	}
}

// configFileWatcher intentionally uses metadata polling rather than a native
// watcher dependency. It observes atomic rename replacements as well as
// in-place updates, gives reload a complete file to compile, and keeps the
// portable process image small. It only emits a bounded notification; reload
// owns reading and validation.
type configFileWatcher struct {
	path     string
	interval time.Duration

	mu      sync.Mutex
	state   configFileState
	changes chan struct{}
	done    chan struct{}
	stopped chan struct{}
	once    sync.Once
}

type configFileState struct {
	info os.FileInfo
}

func newConfigFileWatcher(path string, interval time.Duration) (*configFileWatcher, error) {
	return newConfigFileWatcherWithBaseline(path, interval, nil)
}

// newConfigFileWatcherWithBaseline compares the current file once with the
// bytes already validated by the command boundary. A mismatch emits an
// immediate notification, covering an atomic replacement that occurred after
// startup validation but before watcher initialization. A nil baseline keeps
// the metadata-only behavior for callers without validated startup bytes.
func newConfigFileWatcherWithBaseline(path string, interval time.Duration, baseline []byte) (*configFileWatcher, error) {
	if path == "" {
		return nil, errors.New("configuration path is required")
	}
	if interval <= 0 {
		interval = defaultConfigWatchInterval
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	watcher := &configFileWatcher{
		path: path, interval: interval, state: configFileState{info: info},
		changes: make(chan struct{}, 1), done: make(chan struct{}), stopped: make(chan struct{}),
	}
	if baseline != nil {
		current, readErr := readConfigFileBytes(path)
		if readErr != nil || !bytes.Equal(current, baseline) {
			// Comparison failure is conservative: the runtime attempts a bounded
			// reload and retains the last valid snapshot if the replacement is
			// incomplete or unreadable.
			watcher.changes <- struct{}{}
		}
	}
	go watcher.watch()
	return watcher, nil
}

func readConfigFileBytes(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxConfigWatchBaselineBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxConfigWatchBaselineBytes {
		return nil, fmt.Errorf("configuration file exceeds safe size")
	}
	return data, nil
}

func (watcher *configFileWatcher) Changes() <-chan struct{} {
	if watcher == nil {
		return nil
	}
	return watcher.changes
}

func (watcher *configFileWatcher) Close() error {
	if watcher == nil {
		return nil
	}
	watcher.once.Do(func() {
		close(watcher.done)
		<-watcher.stopped
		close(watcher.changes)
	})
	return nil
}

func (watcher *configFileWatcher) watch() {
	defer close(watcher.stopped)
	ticker := time.NewTicker(watcher.interval)
	defer ticker.Stop()
	for {
		select {
		case <-watcher.done:
			return
		case <-ticker.C:
			next := readConfigFileState(watcher.path)
			watcher.mu.Lock()
			changed := !watcher.state.equal(next)
			if changed {
				watcher.state = next
			}
			watcher.mu.Unlock()
			if changed {
				select {
				case watcher.changes <- struct{}{}:
				default:
				}
			}
		}
	}
}

func readConfigFileState(path string) configFileState {
	info, err := os.Stat(path)
	if err != nil {
		return configFileState{}
	}
	return configFileState{info: info}
}

func (left configFileState) equal(right configFileState) bool {
	if left.info == nil || right.info == nil {
		return left.info == nil && right.info == nil
	}
	return os.SameFile(left.info, right.info) && left.info.Size() == right.info.Size() && left.info.Mode() == right.info.Mode() && left.info.ModTime().Equal(right.info.ModTime())
}

// combineReloadTriggers turns signal and watcher notifications into one
// nonblocking input for Runtime.RunWithReload. The caller owns cancellation;
// closing it never writes to an already closed channel.
func combineReloadTriggers(ctx context.Context, changes <-chan struct{}, signals <-chan os.Signal) <-chan struct{} {
	if ctx == nil {
		ctx = context.Background()
	}
	triggers := make(chan struct{}, 1)
	go func() {
		defer close(triggers)
		for changes != nil || signals != nil {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-changes:
				if !ok {
					changes = nil
					continue
				}
				notifyReload(triggers)
			case _, ok := <-signals:
				if !ok {
					signals = nil
					continue
				}
				notifyReload(triggers)
			}
		}
	}()
	return triggers
}

func notifyReload(triggers chan<- struct{}) {
	select {
	case triggers <- struct{}{}:
	default:
	}
}
