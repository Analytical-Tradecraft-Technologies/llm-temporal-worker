package testassert

import (
	"errors"
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
	r.message = format
}

func TestErrorContains(t *testing.T) {
	for _, test := range []struct {
		name     string
		err      error
		wantFail bool
	}{
		{name: "matching error", err: errors.New("profile is unsupported here")},
		{name: "nil error", err: nil, wantFail: true},
		{name: "other error", err: errors.New("timeout"), wantFail: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &recorder{}
			ErrorContains(r, test.err, "unsupported")
			if r.failed != test.wantFail {
				t.Fatalf("failed = %v, want %v (message %q)", r.failed, test.wantFail, r.message)
			}
		})
	}
}

func TestErr(t *testing.T) {
	want := errors.New("boom")
	if got := Err(42, want); got != want {
		t.Fatalf("Err() = %v, want %v", got, want)
	}
}
