// Package repotest locates the repository checkout, reads and writes its files
// and checks their text for tests that verify repository content, such as
// documentation, workflows and release scripts.
package repotest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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

// ReadText returns the contents of the file at the joined path elements,
// failing the test when it cannot be read.
func ReadText(t testing.TB, elem ...string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(elem...))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// WriteFile writes data to path with mode, failing the test on error.
func WriteFile(t testing.TB, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatal(err)
	}
}

// ReadJSONObject decodes the JSON object stored at path, failing the test when
// the file cannot be read or is not a JSON object.
func ReadJSONObject(t testing.TB, path string) map[string]any {
	t.Helper()
	var object map[string]any
	if err := json.Unmarshal([]byte(ReadText(t, path)), &object); err != nil {
		t.Fatal(err)
	}
	return object
}

// AssertContains stops the test with failure unless text contains want.
func AssertContains(t testing.TB, text, want, failure string) {
	t.Helper()
	if !strings.Contains(text, want) {
		t.Fatal(failure)
	}
}

// AssertOmits stops the test with failure if text contains unwanted.
func AssertOmits(t testing.TB, text, unwanted, failure string) {
	t.Helper()
	if strings.Contains(text, unwanted) {
		t.Fatal(failure)
	}
}

// AssertContainsEach stops the test with failure, formatted with the missing
// value, unless text contains every value.
func AssertContainsEach(t testing.TB, text, failure string, values ...string) {
	t.Helper()
	for _, value := range values {
		if !strings.Contains(text, value) {
			t.Fatalf(failure, value)
		}
	}
}

// AssertOmitsEach stops the test with failure, formatted with the value, if
// text contains any of values.
func AssertOmitsEach(t testing.TB, text, failure string, values ...string) {
	t.Helper()
	for _, value := range values {
		if strings.Contains(text, value) {
			t.Fatalf(failure, value)
		}
	}
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
