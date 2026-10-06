package llm_test

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestAPIVersion(t *testing.T) {
	if got, want := llm.APIVersion, "llm.temporal/v1"; got != want {
		t.Fatalf("APIVersion = %q, want %q", got, want)
	}
}
