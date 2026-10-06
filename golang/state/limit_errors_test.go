package state

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/blob"
)

func TestMaterializationLimitViolationsAreTyped(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{MaxItems: 1})
	root := rootCheckpoint("root", "tenant-a", "op-root")
	root.Delta = []llm.Item{
		llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "one"}}},
		llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "two"}}},
	}
	err := graph.PutRoot(root)
	if err == nil {
		_, err = graph.Materialize("tenant-a", "root")
	}
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("item limit error = %v, want ErrLimitExceeded", err)
	}
}

// Each checkpoint fits MaxBytes but their lineage does not. The canonicalizer
// rejects the aggregate before its length can be compared, and that rejection
// must still be typed.
func TestAggregateLineageByteLimitIsTyped(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{MaxBytes: 1024})
	root := rootCheckpoint("root", "tenant-a", "op-root")
	root.Delta = []llm.Item{message(strings.Repeat("r", 600))}
	if err := graph.PutRoot(root); err != nil {
		t.Fatalf("root within the byte limit rejected: %v", err)
	}
	if err := graph.PutChild(childCheckpoint("child", "root", "tenant-a", "op-child", strings.Repeat("c", 600))); err != nil {
		t.Fatalf("child within the byte limit rejected: %v", err)
	}
	if _, err := graph.Materialize("tenant-a", "child"); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("aggregate byte limit error = %v, want ErrLimitExceeded", err)
	}
}

func TestCheckpointBlobSizeViolationsAreTyped(t *testing.T) {
	items := []llm.Item{message(strings.Repeat("x", 600))}
	encoded, err := CheckpointBlobCodec{}.EncodeDelta(items)
	if err != nil {
		t.Fatal(err)
	}
	small := CheckpointBlobCodec{MaxBytes: 256}
	if _, err := small.EncodeDelta(items); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("encode over the byte limit = %v, want ErrLimitExceeded", err)
	}
	if _, err := small.DecodeDelta(encoded); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("decode over the byte limit = %v, want ErrLimitExceeded", err)
	}
	// A malformed blob is not a limit violation.
	if _, err := small.DecodeDelta([]byte(`{"version":`)); err == nil || errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("malformed blob error = %v, want an untyped decode failure", err)
	}

	reader := ScopedBlobReader{Store: &testCheckpointBlobStore{}, MaxBytes: 8, Resolve: func(context.Context, string, BlobID) (blob.Ref, error) {
		t.Fatal("oversized blob reached the locator")
		return blob.Ref{}, nil
	}}
	reference := CheckpointBlobReference{ID: "blob-1", Digest: [32]byte{1}, ByteLength: 9, MediaType: "application/json"}
	if _, err := reader.Read(context.Background(), "scope-a", reference); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("reader over the byte limit = %v, want ErrLimitExceeded", err)
	}
}
