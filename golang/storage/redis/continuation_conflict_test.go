package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

func TestContinuationChildRetryWithDifferentHistoryConflicts(t *testing.T) {
	now := time.Unix(100, 0)
	harness := newContinuationHarness()
	store, err := NewContinuationStore(ContinuationOptions{Invoker: harness, Reader: harness, Keys: testKeyOptions(), Keyring: testKeyring(t), Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	parent, err := store.CreateRoot(context.Background(), testContinuation(t, now))
	if err != nil {
		t.Fatal(err)
	}
	child := func(text string) state.Continuation {
		value := testContinuation(t, now)
		value.ParentID, value.Depth = parent.String(), 1
		value.Transcript = []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: text}}}}
		_, value.TranscriptDigest, err = state.CanonicalTranscript(value.Transcript)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	first, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: parent, Child: child("first result"), OperationKey: "op-1"})
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: parent, Child: child("first result"), OperationKey: "op-1"}); err != nil || replay != first {
		t.Fatalf("identical retry = %q, %v", replay, err)
	}
	if _, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: parent, Child: child("conflicting result"), OperationKey: "op-1"}); !errors.Is(err, state.ErrConflict) {
		t.Fatalf("conflicting retry error = %v, want ErrConflict", err)
	}
}
