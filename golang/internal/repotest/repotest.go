// Package repotest locates the repository checkout and reads its files for
// tests that verify repository content, such as documentation, workflows and
// release scripts.
package repotest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// RepositoryRoot returns the nearest ancestor of the working directory that
// contains .git, failing the test when there is none.
func RepositoryRoot(t testing.TB) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, ok := findRepositoryRoot(directory)
	if !ok {
		t.Fatal("repository checkout root with .git not found")
	}
	return root
}

// ModuleRoot returns the golang/ directory of the repository checkout.
func ModuleRoot(t testing.TB) string {
	t.Helper()
	return filepath.Join(RepositoryRoot(t), "golang")
}

// ReadJSONObject decodes the JSON object stored at path, failing the test when
// the file cannot be read or is not a JSON object.
func ReadJSONObject(t testing.TB, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err := json.Unmarshal(data, &object); err != nil {
		t.Fatal(err)
	}
	return object
}

func findRepositoryRoot(directory string) (string, bool) {
	for {
		if _, err := os.Stat(filepath.Join(directory, ".git")); err == nil {
			return directory, true
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return "", false
		}
		directory = parent
	}
}
