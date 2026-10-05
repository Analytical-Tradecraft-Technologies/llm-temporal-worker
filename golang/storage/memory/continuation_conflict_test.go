package memory

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

func TestContinuationChildRetryWithDifferentHistoryConflicts(t *testing.T) {
	keyring, err := state.NewKeyring([]state.Key{{ID: "k1", Secret: bytes.Repeat([]byte{3}, 32), Primary: true}}, bytes.NewReader(append(bytes.Repeat([]byte{4}, 16), bytes.Repeat([]byte{5}, 16)...)))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	store, err := NewContinuationStore(ContinuationOptions{Keyring: keyring, Clock: func() time.Time { return now }, MaxDepth: 4})
	if err != nil {
		t.Fatal(err)
	}
	transcript := func(text string) ([]llm.Item, [32]byte) {
		items := []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}}}
		_, digest, err := state.CanonicalTranscript(items)
		if err != nil {
			t.Fatal(err)
		}
		return items, digest
	}
	rootItems, rootDigest := transcript("hello")
	parent, err := store.CreateRoot(context.Background(), state.Continuation{Tenant: "tenant", Transcript: rootItems, TranscriptDigest: rootDigest, TranscriptComplete: true, ExpiresAt: now.Add(time.Hour), LastOperationID: "root"})
	if err != nil {
		t.Fatal(err)
	}
	child := func(text string) state.Continuation {
		items, digest := transcript(text)
		return state.Continuation{Tenant: "tenant", ParentID: parent.String(), Depth: 1, Transcript: items, TranscriptDigest: digest, TranscriptComplete: true, ExpiresAt: now.Add(time.Hour), LastOperationID: "op-1"}
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
	stored, err := store.Get(context.Background(), first)
	if err != nil || stored.Transcript[0].(llm.Message).Content[0].(llm.TextPart).Text != "first result" {
		t.Fatalf("stored child = %#v, %v", stored, err)
	}
}
