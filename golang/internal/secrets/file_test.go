package secrets

import (
	"context"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"os"
	"path/filepath"
	"testing"
)

func TestReadSecretFileAcceptsProjectedVolumeSymlink(t *testing.T) {
	volume := t.TempDir()
	dataDirectory := filepath.Join(volume, "..2026_08_20_13_00_00.0000000000")
	testassert.NoError(t, os.Mkdir(dataDirectory, 0700))
	testassert.NoError(t, os.WriteFile(filepath.Join(dataDirectory, "api-key"), []byte("projected-secret"), 0400))
	if err := os.Symlink(filepath.Base(dataDirectory), filepath.Join(volume, "..data")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	path := filepath.Join(volume, "api-key")
	testassert.NoError(t, os.Symlink(filepath.Join("..data", "api-key"), path))

	value, err := ReadSecretFile(context.Background(), path, 64)
	testassert.NoError(t, err)
	if string(value) != "projected-secret" {
		t.Fatalf("secret = %q, want projected-secret", value)
	}
}

func TestReadSecretFileRejectsNonRegularTargets(t *testing.T) {
	_, err := ReadSecretFile(context.Background(), t.TempDir(), 64)
	testassert.ErrorContains(t, err, "secret file must be regular")
}

func TestReadSecretFileBoundsSymlinkTarget(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target")
	testassert.NoError(t, os.WriteFile(target, []byte("oversized"), 0400))
	path := filepath.Join(directory, "secret")
	if err := os.Symlink(filepath.Base(target), path); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	_, err := ReadSecretFile(context.Background(), path, 4)
	testassert.ErrorContains(t, err, "exceeds the configured size limit")
}
