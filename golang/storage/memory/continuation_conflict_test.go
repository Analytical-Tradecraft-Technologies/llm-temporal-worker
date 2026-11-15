package memory

import (
	"context"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

func TestContinuationChildRetryWithDifferentHistoryConflicts(t *testing.T) {
	now := time.Unix(100, 0)
	store := newTestContinuationStore(t, &now, deterministicKeyringRandom())
	parent, _ := createTextRoot(t, store, "hello", now.Add(time.Hour))
	child := func(text string) state.Continuation {
		items, digest := textTranscript(t, llm.ActorHuman, text)
		return state.Continuation{Tenant: "tenant", ParentID: parent.String(), Depth: 1, Transcript: items, TranscriptDigest: digest, TranscriptComplete: true, ExpiresAt: now.Add(time.Hour), LastOperationID: "op-1"}
	}
	first, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: parent, Child: child("first result"), OperationKey: "op-1"})
	testassert.NoError(t, err)
	if replay, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: parent, Child: child("first result"), OperationKey: "op-1"}); err != nil || replay != first {
		t.Fatalf("identical retry = %q, %v", replay, err)
	}
	testassert.ErrorIs(t, testassert.Err(store.PutChild(context.Background(), state.PutChildRequest{Parent: parent, Child: child("conflicting result"), OperationKey: "op-1"})), state.ErrConflict, "conflicting retry")
	stored, err := store.Get(context.Background(), first)
	if err != nil || stored.Transcript[0].(llm.Message).Content[0].(llm.TextPart).Text != "first result" {
		t.Fatalf("stored child = %#v, %v", stored, err)
	}
}
