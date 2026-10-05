package state

import (
	"errors"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

// Regression for #776: a checkpoint larger than the low-level canonicalizer's
// default 8 MiB envelope but inside the graph's 16 MiB materialization bound
// must still receive a real request digest, so a different checkpoint written
// under the same handle and operation key is rejected as a conflict instead of
// being accepted as an identical retry.
func TestCheckpointGraphLargeCheckpointConflictIsDetected(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{})
	first := rootCheckpoint("root", "tenant-a", "operation-root")
	first.Delta = []llm.Item{message(strings.Repeat("a", 9<<20))}
	if err := graph.PutRoot(first); err != nil {
		t.Fatalf("put large root: %v", err)
	}
	if _, err := graph.Materialize("tenant-a", "root"); err != nil {
		t.Fatalf("materialize large root: %v", err)
	}
	stored, err := graph.Get("root")
	if err != nil {
		t.Fatal(err)
	}
	if stored.RequestDigest == ([32]byte{}) {
		t.Fatal("large checkpoint stored a zero request digest")
	}

	if err := graph.PutRoot(first); err != nil {
		t.Fatalf("identical large retry: %v", err)
	}

	conflicting := rootCheckpoint("root", "tenant-a", "operation-root")
	conflicting.Delta = []llm.Item{message(strings.Repeat("b", 9<<20))}
	if err := graph.PutRoot(conflicting); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting large checkpoint returned %v, want %v", err, ErrConflict)
	}
	materialized, err := graph.Materialize("tenant-a", "root")
	if err != nil {
		t.Fatal(err)
	}
	if got := materialized.Items[0].(llm.Message).Content[0].(llm.TextPart).Text[0]; got != 'a' {
		t.Fatalf("retained content first byte = %q, want 'a'", got)
	}
}

// Requests whose digest envelope cannot be computed within the graph's byte
// limit are rejected explicitly rather than sharing a sentinel digest.
func TestCheckpointGraphRejectsRequestBeyondByteLimit(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{MaxBytes: 1 << 10})
	checkpoint := rootCheckpoint("root", "tenant-a", "operation-root")
	checkpoint.Delta = []llm.Item{message(strings.Repeat("a", 2<<10))}
	if err := graph.PutRoot(checkpoint); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("oversized checkpoint returned %v, want %v", err, ErrLimitExceeded)
	}
	if _, err := graph.Get("root"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("oversized checkpoint was stored: %v", err)
	}
}
