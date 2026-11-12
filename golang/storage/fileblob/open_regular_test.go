package fileblob

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/blob"
)

func TestOpenRegularMapsEachFailure(t *testing.T) {
	root := t.TempDir()
	notRegular := errors.New("not regular")
	if _, _, err := openRegular(filepath.Join(root, "missing"), "blob", notRegular); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("missing file error = %v", err)
	}
	if _, _, err := openRegular(root, "blob", notRegular); !errors.Is(err, notRegular) {
		t.Fatalf("directory error = %v", err)
	}
	path := filepath.Join(root, "object")
	if err := os.WriteFile(path, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	file, info, err := openRegular(path, "blob", notRegular)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if info.Size() != 4 || !info.Mode().IsRegular() {
		t.Fatalf("info = %v", info)
	}
}
