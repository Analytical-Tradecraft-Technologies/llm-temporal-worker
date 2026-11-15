package architecturetest

import (
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

// assertContainsEach stops the test with failure, formatted with the missing
// value, unless text contains every value.
func assertContainsEach(t *testing.T, text, failure string, values ...string) {
	t.Helper()
	for _, value := range values {
		if !strings.Contains(text, value) {
			t.Fatalf(failure, value)
		}
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
