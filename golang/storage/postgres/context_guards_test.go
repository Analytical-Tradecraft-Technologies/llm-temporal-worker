package postgres

import (
	"strings"
	"testing"
)

func TestPostgresEntryPointsRejectNilContextBeforeDependencies(t *testing.T) {
	checks := []struct {
		name string
		call func() error
		want string
	}{
		{
			name: "health",
			call: func() error { return Health(nil, nil, Namespace{}) },
			want: "PostgreSQL health context is nil",
		},
		{
			name: "verify",
			call: func() error { return Verify(nil, nil, Namespace{}) },
			want: "PostgreSQL verification context is nil",
		},
		{
			name: "install",
			call: func() error { return Install(nil, nil, Namespace{}) },
			want: "PostgreSQL install context is nil",
		},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			err := check.call()
			if err == nil || !strings.Contains(err.Error(), check.want) {
				t.Fatalf("error = %v, want %q", err, check.want)
			}
		})
	}
}
