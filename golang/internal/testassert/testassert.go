// Package testassert holds assertions shared by tests across the module, and
// the must-style JSON and file helpers those tests build fixtures with.
//
// Every assertion fails the test immediately, as t.Fatal does. NoError,
// ErrorIs, ErrorIsNot and ErrorContains take an optional context naming the step under test;
// when given, the failure message starts with "<context>: ", so
//
//	testassert.NoError(t, err, "Build")
//
// reports "Build: <err>", exactly as t.Fatalf("Build: %v", err) would.
package testassert

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

// NoError fails the test immediately if err is non-nil. Without a context it
// reports err alone, as t.Fatal(err) would.
func NoError(t testing.TB, err error, context ...string) {
	t.Helper()
	if err != nil {
		fatal(t, context, "%v", err)
	}
}

// ErrorIs fails the test immediately unless errors.Is(err, target).
func ErrorIs(t testing.TB, err, target error, context ...string) {
	t.Helper()
	if !errors.Is(err, target) {
		fatal(t, context, "error = %v, want an error matching %v", err, target)
	}
}

// ErrorIsNot fails the test immediately if errors.Is(err, target).
func ErrorIsNot(t testing.TB, err, target error, context ...string) {
	t.Helper()
	if errors.Is(err, target) {
		fatal(t, context, "error = %v, want an error not matching %v", err, target)
	}
}

// ErrorContains fails the test immediately unless err is non-nil and its
// message contains want.
func ErrorContains(t testing.TB, err error, want string, context ...string) {
	t.Helper()
	if err == nil {
		fatal(t, context, "error = nil, want an error containing %q", want)
		return
	}
	if !strings.Contains(err.Error(), want) {
		fatal(t, context, "error = %v, want an error containing %q", err, want)
	}
}

// MarshalJSON returns value encoded with json.Marshal, failing the test
// immediately if it cannot be encoded.
func MarshalJSON(t testing.TB, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	NoError(t, err)
	return data
}

// UnmarshalJSON decodes data into a new T with json.Unmarshal, failing the
// test immediately if it cannot be decoded.
func UnmarshalJSON[T any](t testing.TB, data []byte, context ...string) T {
	t.Helper()
	var value T
	NoError(t, json.Unmarshal(data, &value), context...)
	return value
}

// ReadFile returns the contents of the file at path, failing the test
// immediately if it cannot be read.
func ReadFile(t testing.TB, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	NoError(t, err)
	return data
}

// Err returns the error from a call that also returns a value, so the call
// can be passed straight to an assertion:
//
//	testassert.ErrorContains(t, testassert.Err(NewProfile(profile)), "unsupported")
func Err[T any](_ T, err error) error {
	return err
}

// fatal fails t with the formatted message, prefixed by the context parts
// joined with ": ".
func fatal(t testing.TB, context []string, format string, args ...any) {
	t.Helper()
	message := fmt.Sprintf(format, args...)
	if len(context) > 0 {
		message = strings.Join(context, ": ") + ": " + message
	}
	t.Fatalf("%s", message)
}
