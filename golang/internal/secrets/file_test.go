package secrets

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadSecretFileAcceptsProjectedVolumeSymlink(t *testing.T) {
	volume := t.TempDir()
	dataDirectory := filepath.Join(volume, "..2026_08_20_13_00_00.0000000000")
	if err := os.Mkdir(dataDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDirectory, "api-key"), []byte("projected-secret"), 0400); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Base(dataDirectory), filepath.Join(volume, "..data")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	path := filepath.Join(volume, "api-key")
	if err := os.Symlink(filepath.Join("..data", "api-key"), path); err != nil {
		t.Fatal(err)
	}

	value, err := ReadSecretFile(context.Background(), path, 64)
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "projected-secret" {
		t.Fatalf("secret = %q, want projected-secret", value)
	}
}

func TestReadSecretFileRejectsNonRegularTargets(t *testing.T) {
	_, err := ReadSecretFile(context.Background(), t.TempDir(), 64)
	if err == nil || !strings.Contains(err.Error(), "secret file must be regular") {
		t.Fatalf("error = %v, want non-regular-file rejection", err)
	}
}

func TestReadSecretFileBoundsSymlinkTarget(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target")
	if err := os.WriteFile(target, []byte("oversized"), 0400); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "secret")
	if err := os.Symlink(filepath.Base(target), path); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	_, err := ReadSecretFile(context.Background(), path, 4)
	if err == nil || !strings.Contains(err.Error(), "exceeds the configured size limit") {
		t.Fatalf("error = %v, want size-limit rejection", err)
	}
}
