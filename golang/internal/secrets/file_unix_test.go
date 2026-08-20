//go:build darwin || linux

package secrets

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestReadSecretFileRejectsUnixSpecialTargets(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T) string
	}{
		{
			name: "fifo",
			setup: func(t *testing.T) string {
				path := filepath.Join(t.TempDir(), "fifo")
				if err := syscall.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
				return path
			},
		},
		{
			name: "device through symlink",
			setup: func(t *testing.T) string {
				if _, err := os.Stat("/dev/null"); err != nil {
					t.Skipf("device unavailable: %v", err)
				}
				path := filepath.Join(t.TempDir(), "device")
				if err := os.Symlink("/dev/null", path); err != nil {
					t.Skipf("symlinks unavailable: %v", err)
				}
				return path
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ReadSecretFile(context.Background(), test.setup(t), 64)
			if err == nil || !strings.Contains(err.Error(), "secret file must be regular") {
				t.Fatalf("error = %v, want non-regular-file rejection", err)
			}
		})
	}
}
