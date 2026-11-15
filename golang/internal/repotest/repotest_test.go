package repotest

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFindRepositoryRootReturnsNearestAncestorWithGit(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(root, "golang", "internal", "repotest")
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, start := range []string{root, nested} {
		got, ok := findRepositoryRoot(start)
		if !ok || got != root {
			t.Fatalf("findRepositoryRoot(%q) = %q, %v; want %q, true", start, got, ok, root)
		}
	}
}

func TestReadJSONObjectDecodesAnObject(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object.json")
	if err := os.WriteFile(path, []byte(`{"name":"value","count":2}`), 0o600); err != nil {
		t.Fatal(err)
	}
	got := ReadJSONObject(t, path)
	if len(got) != 2 || got["name"] != "value" || got["count"] != float64(2) {
		t.Fatalf("ReadJSONObject = %#v", got)
	}
}

func TestModuleRootContainsThisModule(t *testing.T) {
	if _, err := os.Stat(filepath.Join(ModuleRoot(t), "go.mod")); err != nil {
		t.Fatalf("module root has no go.mod: %v", err)
	}
	if _, err := os.Stat(filepath.Join(RepositoryRoot(t), ".git")); err != nil {
		t.Fatalf("repository root has no .git: %v", err)
	}
}
