package testassert

import (
	"errors"
	"fmt"
	"testing"
)

type recorder struct {
	testing.TB
	failed  bool
	message string
}

func (r *recorder) Helper() {}

func (r *recorder) Fatalf(format string, args ...any) {
	r.failed = true
	r.message = fmt.Sprintf(format, args...)
}

// assertion is one call of a helper under test against a recorder.
type assertion struct {
	name        string
	assert      func(testing.TB)
	wantMessage string // "" when the assertion must pass
}

func runAssertions(t *testing.T, assertions []assertion) {
	t.Helper()
	for _, test := range assertions {
		t.Run(test.name, func(t *testing.T) {
			r := &recorder{}
			test.assert(r)
			if wantFail := test.wantMessage != ""; r.failed != wantFail {
				t.Fatalf("failed = %v, want %v (message %q)", r.failed, wantFail, r.message)
			}
			if r.message != test.wantMessage {
				t.Fatalf("message = %q, want %q", r.message, test.wantMessage)
			}
		})
	}
}

var (
	errBoom    = errors.New("boom")
	errWrapped = fmt.Errorf("load: %w", errBoom)
	errOther   = errors.New("timeout")
)

func TestNoError(t *testing.T) {
	runAssertions(t, []assertion{
		{name: "nil error", assert: func(t testing.TB) { NoError(t, nil) }},
		{name: "nil error with context", assert: func(t testing.TB) { NoError(t, nil, "Build") }},
		{name: "error", assert: func(t testing.TB) { NoError(t, errBoom) }, wantMessage: "boom"},
		{name: "error with context", assert: func(t testing.TB) { NoError(t, errBoom, "Build") }, wantMessage: "Build: boom"},
		{name: "nested context", assert: func(t testing.TB) { NoError(t, errBoom, "case", "Build") }, wantMessage: "case: Build: boom"},
		{name: "context with a verb", assert: func(t testing.TB) { NoError(t, errBoom, "100%") }, wantMessage: "100%: boom"},
	})
}

func TestErrorIs(t *testing.T) {
	runAssertions(t, []assertion{
		{name: "same error", assert: func(t testing.TB) { ErrorIs(t, errBoom, errBoom) }},
		{name: "wrapped error", assert: func(t testing.TB) { ErrorIs(t, errWrapped, errBoom) }},
		{name: "nil target and error", assert: func(t testing.TB) { ErrorIs(t, nil, nil) }},
		{name: "nil error", assert: func(t testing.TB) { ErrorIs(t, nil, errBoom) }, wantMessage: "error = <nil>, want an error matching boom"},
		{name: "other error", assert: func(t testing.TB) { ErrorIs(t, errOther, errBoom, "Load") }, wantMessage: "Load: error = timeout, want an error matching boom"},
	})
}

func TestErrorIsNot(t *testing.T) {
	runAssertions(t, []assertion{
		{name: "nil error", assert: func(t testing.TB) { ErrorIsNot(t, nil, errBoom) }},
		{name: "other error", assert: func(t testing.TB) { ErrorIsNot(t, errOther, errBoom) }},
		{name: "wrapped error", assert: func(t testing.TB) { ErrorIsNot(t, errWrapped, errBoom, "Load") }, wantMessage: "Load: error = load: boom, want an error not matching boom"},
	})
}

func TestErrorContains(t *testing.T) {
	runAssertions(t, []assertion{
		{name: "matching error", assert: func(t testing.TB) { ErrorContains(t, errors.New("profile is unsupported here"), "unsupported") }},
		{name: "nil error", assert: func(t testing.TB) { ErrorContains(t, nil, "unsupported") }, wantMessage: `error = nil, want an error containing "unsupported"`},
		{name: "other error", assert: func(t testing.TB) { ErrorContains(t, errOther, "unsupported") }, wantMessage: `error = timeout, want an error containing "unsupported"`},
	})
}

func TestErr(t *testing.T) {
	if got := Err(42, errBoom); got != errBoom {
		t.Fatalf("Err() = %v, want %v", got, errBoom)
	}
}
