package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
)

const defaultConfigWatchInterval = time.Second

// maxConfigWatchBaselineBytes matches the application reload limit. The
// watcher reads this bounded amount once at startup to close the race between
// validating startup bytes and beginning metadata polling. Later polls stay
// metadata-only while the file is unchanged and read the same bounded amount
// only while a change is settling.
const maxConfigWatchBaselineBytes = 4 << 20

// ReloadFile compiles and verifies a complete file replacement before the app
// publishes it. Reload failures are deliberately reduced to a safe process
// boundary error: configuration contents, paths, and resolved references never
// become log fields or command output.
func (runtime *Runtime) ReloadFile(ctx context.Context, path string) error {
	return runtime.reloadFile(ctx, path, nil)
}

// reloadFile is ReloadFile with an optional gate over the bytes just read. A
// declined read is not a failure: the file is still being written, nothing is
// compiled or published, and the watcher notifies again once it settles.
func (runtime *Runtime) reloadFile(ctx context.Context, path string, accept func([]byte) bool) error {
	if runtime == nil || runtime.App == nil {
		return errors.New("runtime is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	err := runtime.App.ReloadFileIf(ctx, path, accept)
	if errors.Is(err, app.ErrReloadDeferred) {
		runtime.Logger.Info(ctx, "configuration reload deferred until the file is stable", slog.String("outcome", "deferred"))
		return nil
	}
	if err != nil && !app.IsPublishedReloadError(err) {
		runtime.recordReloadFailure(ctx, err)
		return safeReloadError(err)
	}
	cleanupIncomplete := err != nil
	version := ""
	if current := runtime.App.Current(); current != nil && current.Config != nil {
		version = current.Config.ConfigVersion()
	}
	runtime.Metrics.RecordConfigReload("success")
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
	// Every rejection used to log the same line. Name a bounded cause and,
	// where one is known, the schema path of the offending field.
	cause, field := classifyReloadFailure(err)
	attrs := []slog.Attr{slog.String("outcome", "failure"), slog.String("cause", cause)}
	if field != "" {
		attrs = append(attrs, slog.String("config_field", field))
	}
	runtime.Logger.Error(ctx, "configuration reload failed", err, attrs...)
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
// in-place updates and keeps the portable process image small. It only emits
// a bounded notification; reload owns reading and validation.
//
// A change is notified only once it is stable: two consecutive polls must see
// the same file identity, size, mode, modification time and content digest.
// A prefix of a YAML file is usually valid YAML and every budget setting is
// optional, so a file caught halfway through an in-place write could
// otherwise validate and reload with its budgets missing. An atomic rename or
// ConfigMap symlink swap is stable at once and is notified one poll later
// than it is first seen.
type configFileWatcher struct {
	path     string
	interval time.Duration

	mu sync.Mutex
	// state is the last stable observation: the startup file or the one most
	// recently notified. unsettled forces the next polls to re-observe the
	// file even when its metadata still equals state.
	state     configFileState
	unsettled bool
	// candidate is a changed observation waiting for a second, equal poll. It
	// is only touched by the polling goroutine.
	candidate *configFileState
	changes   chan struct{}
	done      chan struct{}
	stopped   chan struct{}
	once      sync.Once
}

type configFileState struct {
	info os.FileInfo
	// digest is the SHA-256 of the content when hashed is true. The startup
	// state of a watcher without a baseline is metadata-only.
	digest [sha256.Size]byte
	hashed bool
}

func newConfigFileWatcher(path string, interval time.Duration) (*configFileWatcher, error) {
	return newConfigFileWatcherWithBaseline(path, interval, nil)
}

// newConfigFileWatcherWithBaseline compares the current file once with the
// bytes already validated by the command boundary. A mismatch leaves the
// watcher unsettled, so the replacement is notified as soon as two polls agree
// on it; this covers a replacement that occurred after startup validation but
// before watcher initialization. A nil baseline keeps the metadata-only
// startup state for callers without validated startup bytes.
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
			// Comparison failure is conservative: once the file settles the
			// runtime attempts a bounded reload and retains the last valid
			// snapshot if the replacement is invalid or unreadable.
			watcher.unsettled = true
		} else {
			watcher.state.digest, watcher.state.hashed = sha256.Sum256(baseline), true
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
			watcher.poll()
		}
	}
}

// poll performs one observation. It notifies only when this observation
// differs from the last stable state and equals the previous poll's.
func (watcher *configFileWatcher) poll() {
	next := readConfigFileState(watcher.path)
	watcher.mu.Lock()
	unchanged := !watcher.unsettled && watcher.state.sameMetadata(next)
	watcher.mu.Unlock()
	if unchanged {
		watcher.candidate = nil
		return
	}
	if next.info != nil {
		if data, err := readConfigFileBytes(watcher.path); err == nil {
			next.digest, next.hashed = sha256.Sum256(data), true
		}
	}
	if watcher.candidate == nil || !watcher.candidate.equal(next) {
		watcher.candidate = &next
		return
	}
	watcher.candidate = nil
	watcher.mu.Lock()
	watcher.state, watcher.unsettled = next, false
	watcher.mu.Unlock()
	select {
	case watcher.changes <- struct{}{}:
	default:
	}
}

// settled reports whether data, as just read by a reload, is the content the
// watcher last observed as stable. A mismatch means the file changed again
// after the notification; the reload must not publish it, and the watcher is
// marked unsettled so the new content is notified once it is stable.
func (watcher *configFileWatcher) settled(data []byte) bool {
	if watcher == nil {
		return true
	}
	watcher.mu.Lock()
	defer watcher.mu.Unlock()
	if watcher.unsettled {
		return false
	}
	if watcher.state.hashed && watcher.state.digest != sha256.Sum256(data) {
		watcher.unsettled = true
		return false
	}
	return true
}

func readConfigFileState(path string) configFileState {
	info, err := os.Stat(path)
	if err != nil {
		return configFileState{}
	}
	return configFileState{info: info}
}

func (left configFileState) sameMetadata(right configFileState) bool {
	if left.info == nil || right.info == nil {
		return left.info == nil && right.info == nil
	}
	return os.SameFile(left.info, right.info) && left.info.Size() == right.info.Size() && left.info.Mode() == right.info.Mode() && left.info.ModTime().Equal(right.info.ModTime())
}

func (left configFileState) equal(right configFileState) bool {
	return left.sameMetadata(right) && left.hashed == right.hashed && left.digest == right.digest
}

// watchedReloadGate decides, per triggered reload, whether the bytes it reads
// must be the content the watcher saw stable. Triggers are coalesced into one
// channel, so the source of a reload is recorded here instead.
type watchedReloadGate struct {
	watcher  *configFileWatcher
	explicit atomic.Bool
}

// signal records a SIGHUP before its trigger is forwarded.
func (gate *watchedReloadGate) signal() { gate.explicit.Store(true) }

// begin is called before a reload reads the file. A pending SIGHUP is
// consumed here, never after the read: a signal that arrives while a
// watcher-triggered reload is already in flight must not exempt the bytes
// that reload read earlier, and keeps its bypass for the reload it queued.
func (gate *watchedReloadGate) begin() func([]byte) bool {
	if gate.explicit.Swap(false) {
		return nil
	}
	return gate.watcher.settled
}

// combineReloadTriggers turns signal and watcher notifications into one
// nonblocking input for Runtime.RunWithReload. The caller owns cancellation;
// closing it never writes to an already closed channel.
func combineReloadTriggers(ctx context.Context, changes <-chan struct{}, signals <-chan os.Signal) <-chan struct{} {
	return combineReloadTriggersNotify(ctx, changes, signals, nil)
}

// combineReloadTriggersNotify additionally calls onSignal, when set, before a
// signal is forwarded, so the reload it causes can be told apart from a
// watcher notification.
func combineReloadTriggersNotify(ctx context.Context, changes <-chan struct{}, signals <-chan os.Signal, onSignal func()) <-chan struct{} {
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
				if onSignal != nil {
					onSignal()
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
