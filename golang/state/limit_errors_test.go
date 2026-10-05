package state

import (
	"errors"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
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
