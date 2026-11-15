package state

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/blob"
)

func TestMaterializationLimitViolationsAreTyped(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{MaxItems: 1})
	root := rootCheckpoint("root", "tenant-a", "op-root")
	root.Delta = []llm.Item{
		llm.HumanText("one"),
		llm.ModelText("two"),
	}
	err := graph.PutRoot(root)
	if err == nil {
		_, err = graph.Materialize("tenant-a", "root")
	}
	testassert.ErrorIs(t, err, ErrLimitExceeded, "item limit")
}

// Each checkpoint fits MaxBytes but their lineage does not. The canonicalizer
// rejects the aggregate before its length can be compared, and that rejection
// must still be typed.
func TestAggregateLineageByteLimitIsTyped(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{MaxBytes: 1024})
	root := rootCheckpoint("root", "tenant-a", "op-root")
	root.Delta = []llm.Item{llm.HumanText(strings.Repeat("r", 600))}
	testassert.NoError(t, graph.PutRoot(root), "root within the byte limit rejected")
	testassert.NoError(t, graph.PutChild(childCheckpoint("child", "root", "tenant-a", "op-child", strings.Repeat("c", 600))), "child within the byte limit rejected")
	testassert.ErrorIs(t, testassert.Err(graph.Materialize("tenant-a", "child")), ErrLimitExceeded, "aggregate byte limit")
}

func TestCheckpointBlobSizeViolationsAreTyped(t *testing.T) {
	items := []llm.Item{llm.HumanText(strings.Repeat("x", 600))}
	encoded, err := CheckpointBlobCodec{}.EncodeDelta(items)
	testassert.NoError(t, err)
	small := CheckpointBlobCodec{MaxBytes: 256}
	testassert.ErrorIs(t, testassert.Err(small.EncodeDelta(items)), ErrLimitExceeded, "encode over the byte limit")
	testassert.ErrorIs(t, testassert.Err(small.DecodeDelta(encoded)), ErrLimitExceeded, "decode over the byte limit")
	// A malformed blob is not a limit violation.
	if _, err := small.DecodeDelta([]byte(`{"version":`)); err == nil || errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("malformed blob error = %v, want an untyped decode failure", err)
	}

	reader := ScopedBlobReader{Store: &testCheckpointBlobStore{}, MaxBytes: 8, Resolve: func(context.Context, string, BlobID) (blob.Ref, error) {
		t.Fatal("oversized blob reached the locator")
		return blob.Ref{}, nil
	}}
	reference := CheckpointBlobReference{ID: "blob-1", Digest: [32]byte{1}, ByteLength: 9, MediaType: "application/json"}
	testassert.ErrorIs(t, testassert.Err(reader.Read(context.Background(), "scope-a", reference)), ErrLimitExceeded, "reader over the byte limit")
}
