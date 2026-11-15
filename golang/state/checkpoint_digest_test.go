package state

import (
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// Regression for #776: a checkpoint larger than the low-level canonicalizer's
// default 8 MiB envelope but inside the graph's 16 MiB materialization bound
// must still receive a real request digest, so a different checkpoint written
// under the same handle and operation key is rejected as a conflict instead of
// being accepted as an identical retry.
func TestCheckpointGraphLargeCheckpointConflictIsDetected(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{})
	first := rootCheckpoint("root", "tenant-a", "operation-root")
	first.Delta = []llm.Item{llm.HumanText(strings.Repeat("a", 9<<20))}
	assertErrIs(t, graph.PutRoot(first), nil, "put large root")
	assertMaterializeErr(t, graph, "tenant-a", "root", nil, "materialize large root")
	stored, err := graph.Get("root")
	if err != nil {
		t.Fatal(err)
	}
	if stored.RequestDigest == ([32]byte{}) {
		t.Fatal("large checkpoint stored a zero request digest")
	}

	assertErrIs(t, graph.PutRoot(first), nil, "identical large retry")

	conflicting := rootCheckpoint("root", "tenant-a", "operation-root")
	conflicting.Delta = []llm.Item{llm.HumanText(strings.Repeat("b", 9<<20))}
	assertErrIs(t, graph.PutRoot(conflicting), ErrConflict, "conflicting large checkpoint")
	materialized := mustMaterialize(t, graph, "tenant-a", "root")
	if got := materialized.Items[0].(llm.Message).Content[0].(llm.TextPart).Text[0]; got != 'a' {
		t.Fatalf("retained content first byte = %q, want 'a'", got)
	}
}

// A checkpoint whose transcript and settings patch each fit the byte limit
// must be accepted even when their combined digest envelope does not: durable
// publication bounds those blobs separately and replays them through PutRoot.
// Conflicts must still be detected for such checkpoints.
func TestCheckpointGraphAcceptsSeparatelyBoundedPartsBeyondCombinedLimit(t *testing.T) {
	const limit = 4 << 10
	withParts := func(text, instructions string) Checkpoint {
		checkpoint := rootCheckpoint("root", "tenant-a", "operation-root")
		checkpoint.Delta = []llm.Item{llm.HumanText(text)}
		checkpoint.SettingsPatch.Instructions = SetPatch([]llm.Instruction{{
			Kind: llm.InstructionKindParts, Level: llm.InstructionLevelApplication,
			Content: []llm.Part{llm.TextPart{Text: instructions}},
		}})
		return checkpoint
	}
	graph := NewCheckpointGraph(MaterializeLimits{MaxBytes: limit})
	first := withParts(strings.Repeat("a", 3<<10), strings.Repeat("i", 3<<10))
	assertErrIs(t, graph.PutRoot(first), nil, "put separately bounded checkpoint")
	assertMaterializeErr(t, graph, "tenant-a", "root", nil, "materialize separately bounded checkpoint")
	assertErrIs(t, graph.PutRoot(first), nil, "identical retry")
	for name, conflicting := range map[string]Checkpoint{
		"delta":    withParts(strings.Repeat("b", 3<<10), strings.Repeat("i", 3<<10)),
		"settings": withParts(strings.Repeat("a", 3<<10), strings.Repeat("j", 3<<10)),
	} {
		assertErrIs(t, graph.PutRoot(conflicting), ErrConflict, "conflicting "+name)
	}
}

// A request part that cannot be digested within the graph's byte limit is
// rejected explicitly rather than sharing a sentinel digest.
func TestCheckpointGraphRejectsRequestBeyondByteLimit(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{MaxBytes: 1 << 10})
	checkpoint := rootCheckpoint("root", "tenant-a", "operation-root")
	checkpoint.Delta = []llm.Item{llm.HumanText(strings.Repeat("a", 2<<10))}
	assertErrIs(t, graph.PutRoot(checkpoint), ErrLimitExceeded, "oversized checkpoint")
	_, err := graph.Get("root")
	assertErrIs(t, err, ErrNotFound, "get oversized checkpoint")
}
