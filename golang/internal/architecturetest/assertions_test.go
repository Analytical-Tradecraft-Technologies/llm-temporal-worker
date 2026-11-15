package architecturetest

import (
	"encoding/json"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// assertCommandRejected fails unless a command failed and its combined output
// names the expected reason. accepted is the failure message for a command
// that unexpectedly succeeded; failure labels unexpected rejection output.
func assertCommandRejected(t *testing.T, output []byte, err error, accepted, want, failure string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s:\n%s", accepted, output)
	}
	if !strings.Contains(string(output), want) {
		t.Fatalf("%s failure = %q, want it to contain %q", failure, output, want)
	}
}

// assertExactKeys fails unless document has exactly the named top-level keys;
// label names the document in failures.
func assertExactKeys(t *testing.T, label string, document map[string]any, keys ...string) {
	t.Helper()
	if len(document) != len(keys) {
		t.Fatalf("%s keys = %#v, want only %#v", label, document, keys)
	}
	for _, key := range keys {
		if _, ok := document[key]; !ok {
			t.Fatalf("%s does not contain %q: %#v", label, key, document)
		}
	}
}

// assertOutputOmitsSentinel fails if rejection output discloses sentinel;
// failure labels the rejection.
func assertOutputOmitsSentinel(t *testing.T, output []byte, sentinel, failure string) {
	t.Helper()
	if strings.Contains(string(output), sentinel) {
		t.Fatalf("%s failure disclosed sentinel: %q", failure, output)
	}
}

// expectContainsEach reports failure, formatted with the value, for every
// value text does not contain, and lets the test continue.
func expectContainsEach(t *testing.T, text, failure string, values ...string) {
	t.Helper()
	for _, value := range values {
		if !strings.Contains(text, value) {
			t.Errorf(failure, value)
		}
	}
}

// assertPathAbsent fails with failure and the Stat error unless path does not
// exist, for example because a rejected command must not leave output behind.
func assertPathAbsent(t *testing.T, path, failure string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("%s: %v", failure, err)
	}
}

// expectOmitsEach reports failure, formatted with the value, for every value
// text contains, and lets the test continue.
func expectOmitsEach(t *testing.T, text, failure string, values ...string) {
	t.Helper()
	for _, value := range values {
		if strings.Contains(text, value) {
			t.Errorf(failure, value)
		}
	}
}

// decodeJSON decodes data into value; label names the document in a decode
// failure.
func decodeJSON(t *testing.T, data []byte, label string, value any) {
	t.Helper()
	if err := json.Unmarshal(data, value); err != nil {
		t.Fatalf("decode %s: %v", label, err)
	}
}

// readJSONFile decodes the JSON file at path into value; label names the
// document in a decode failure.
func readJSONFile(t *testing.T, path, label string, value any) {
	t.Helper()
	decodeJSON(t, []byte(readText(t, path)), label, value)
}

// makeDirectory creates the directory at the joined path elements, and any
// missing parents, with mode and returns its path.
func makeDirectory(t *testing.T, mode os.FileMode, elem ...string) string {
	t.Helper()
	path := filepath.Join(elem...)
	testassert.NoError(t, os.MkdirAll(path, mode))
	return path
}

// mappingField returns document[key] as a JSON object, failing with label and
// the value when it is missing or not an object.
func mappingField(t *testing.T, label string, document map[string]any, key string) map[string]any {
	t.Helper()
	mapping, ok := document[key].(map[string]any)
	if !ok {
		t.Fatalf("%s = %#v, want mapping", label, document[key])
	}
	return mapping
}

// combinedOutputIn runs the named program with arguments in directory and
// returns its combined output.
func combinedOutputIn(directory, name string, arguments ...string) ([]byte, error) {
	command := exec.Command(name, arguments...)
	command.Dir = directory
	return command.CombinedOutput()
}

// tempFilePaths returns the paths of two named files in a fresh temporary
// directory, typically a command's input and output.
func tempFilePaths(t *testing.T, first, second string) (string, string) {
	t.Helper()
	directory := t.TempDir()
	return filepath.Join(directory, first), filepath.Join(directory, second)
}
