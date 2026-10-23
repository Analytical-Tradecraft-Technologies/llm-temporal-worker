package runtime

import (
	"context"
	"errors"
	"os"
	"syscall"
	"testing"
	"time"
)

// A SIGHUP delivered while the runtime is still being built must not take
// the default disposition, which terminates the process (#1532). Without
// the early handler this test binary itself would exit.
func TestRunWorkerFileSurvivesSIGHUPDuringStartup(t *testing.T) {
	stop := errors.New("startup stopped by the test")
	err := runWorkerFile(context.Background(), "unused.yaml", nil, func(context.Context, []byte) (*Runtime, error) {
		if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
			t.Fatal(err)
		}
		// Give the runtime time to deliver the signal before startup ends.
		time.Sleep(100 * time.Millisecond)
		return nil, stop
	})
	if !errors.Is(err, stop) {
		t.Fatalf("runWorkerFile = %v, want the startup error", err)
	}
}
