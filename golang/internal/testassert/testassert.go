// Package testassert holds assertions shared by tests across the module.
package testassert

import (
	"strings"
	"testing"
)

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
// can be passed straight to ErrorContains:
//
//	testassert.ErrorContains(t, testassert.Err(NewProfile(profile)), "unsupported")
func Err[T any](_ T, err error) error {
	return err
}
