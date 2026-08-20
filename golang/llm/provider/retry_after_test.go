package provider

import (
	"testing"
	"time"
)

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name  string
		value string
		want  time.Duration
		ok    bool
	}{
		{name: "delta seconds", value: "2", want: 2 * time.Second, ok: true},
		{name: "zero delta seconds", value: "0", ok: true},
		{name: "http date", value: "Thu, 20 Aug 2026 12:00:02 GMT", want: 2 * time.Second, ok: true},
		{name: "negative", value: "-1", ok: false},
		{name: "malformed", value: "soon", ok: false},
		{name: "past http date", value: "Thu, 20 Aug 2026 11:59:59 GMT", ok: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := ParseRetryAfter(test.value, now)
			if got != test.want || ok != test.ok {
				t.Fatalf("ParseRetryAfter(%q) = (%s, %v), want (%s, %v)", test.value, got, ok, test.want, test.ok)
			}
		})
	}
}
