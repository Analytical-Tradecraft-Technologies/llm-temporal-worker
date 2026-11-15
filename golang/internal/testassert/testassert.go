// Package testassert holds assertions shared by tests across the module.
//
// Every assertion fails the test immediately, as t.Fatal does. NoError,
// ErrorIs and ErrorIsNot take an optional context naming the step under test;
// when given, the failure message starts with "<context>: ", so
//
//	testassert.NoError(t, err, "Build")
//
// reports "Build: <err>", exactly as t.Fatalf("Build: %v", err) would.
package testassert

import (
	"errors"
	"fmt"
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
func ErrorContains(t testing.TB, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want an error containing %q", want)
		return
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want an error containing %q", err, want)
	}
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
