package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

func TestContinuationChildRetryWithDifferentHistoryConflicts(t *testing.T) {
	now := time.Unix(100, 0)
	store := newTestContinuationStore(t, &now, deterministicKeyringRandom())
	rootItems, rootDigest := textTranscript(t, llm.ActorHuman, "hello")
	parent, err := store.CreateRoot(context.Background(), state.Continuation{Tenant: "tenant", Transcript: rootItems, TranscriptDigest: rootDigest, TranscriptComplete: true, ExpiresAt: now.Add(time.Hour), LastOperationID: "root"})
	if err != nil {
		t.Fatal(err)
	}
	child := func(text string) state.Continuation {
		items, digest := textTranscript(t, llm.ActorHuman, text)
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
