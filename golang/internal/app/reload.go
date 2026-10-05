package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
)

const maxReloadFileBytes = 4 << 20

// PublishedReloadError reports cleanup that did not finish before a reload
// returned, after the replacement snapshot was atomically published. Callers
// must not report this as a rejected configuration: the active snapshot has
// already changed and the underlying cleanup outcome is separate from the
// reload outcome.
type PublishedReloadError struct {
	cause error
}

func (err *PublishedReloadError) Error() string {
	return fmt.Sprintf("replacement published but previous snapshot cleanup did not finish: %v", err.cause)
}

func (err *PublishedReloadError) Unwrap() error { return err.cause }

// IsPublishedReloadError reports whether err occurred after a replacement
// snapshot was successfully published.
func IsPublishedReloadError(err error) bool {
	var published *PublishedReloadError
	return errors.As(err, &published)
}

// ErrReloadDeferred reports that ReloadFileIf read the file but its caller
// declined the bytes, so nothing was compiled or published.
var ErrReloadDeferred = errors.New("configuration reload deferred")

// ReloadFile reads a complete replacement before compiling it. A read or
// validation error leaves the currently published snapshot untouched.
func (app *App) ReloadFile(ctx context.Context, path string) error {
	return app.ReloadFileIf(ctx, path, nil)
}

// ReloadFileIf is ReloadFile with an optional gate over the bytes read. The
// file watcher uses it to refuse a file that changed again after it was last
// seen stable; accept sees exactly the bytes that would be compiled.
func (app *App) ReloadFileIf(ctx context.Context, path string, accept func([]byte) bool) error {
	if path == "" {
		return fmt.Errorf("configuration path is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("read configuration file: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxReloadFileBytes+1))
	if err != nil {
		return fmt.Errorf("read configuration file: %w", err)
	}
	if len(data) > maxReloadFileBytes {
		return fmt.Errorf("configuration file exceeds safe size")
	}
	if accept != nil && !accept(data) {
		return ErrReloadDeferred
	}
	return app.Reload(ctx, data)
}
