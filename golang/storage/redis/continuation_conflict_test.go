package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/redis/go-redis/v9"
)

func TestContinuationChildRetryWithDifferentHistoryConflicts(t *testing.T) {
	now := time.Unix(100, 0)
	harness := newContinuationHarness()
	store := newTestContinuationStore(t, harness, harness, now)
	parent := createTestRoot(t, store, now)
	assertChildRetryConflicts(t, store, parent, now)
}

// staleOperationIndexReader simulates a concurrent retry whose preliminary
// operation-index read missed the winning child's mapping.
type staleOperationIndexReader struct {
	*continuationHarness
	hidden string
}

func (r staleOperationIndexReader) Get(ctx context.Context, key string) (string, error) {
	if key == r.hidden {
		return "", redis.Nil
	}
	return r.continuationHarness.Get(ctx, key)
}

func TestContinuationChildConflictDetectedWhenAtomicWriteFindsExistingOperation(t *testing.T) {
	now := time.Unix(100, 0)
	harness := newContinuationHarness()
	reader := &staleOperationIndexReader{continuationHarness: harness}
	store := newTestContinuationStore(t, harness, reader, now)
	parent := createTestRoot(t, store, now)
	reader.hidden = store.space.continuationOperationKey("tenant-a", parent.String(), "op-1")
	assertChildRetryConflicts(t, store, parent, now)
}

// assertChildRetryConflicts puts a child of parent under one operation key,
// then checks that an identical retry returns the same handle and a retry
// with a different transcript conflicts.
func assertChildRetryConflicts(t *testing.T, store *ContinuationStore, parent state.Handle, now time.Time) {
	t.Helper()
	child := func(text string) state.Continuation {
		value := testChildContinuation(t, now, parent)
		value.Transcript = []llm.Item{llm.ModelText(text)}
		var err error
		_, value.TranscriptDigest, err = state.CanonicalTranscript(value.Transcript)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	first := mustPutChild(t, store, state.PutChildRequest{Parent: parent, Child: child("first result"), OperationKey: "op-1"})
	if replay, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: parent, Child: child("first result"), OperationKey: "op-1"}); err != nil || replay != first {
		t.Fatalf("identical retry = %q, %v", replay, err)
	}
	if _, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: parent, Child: child("conflicting result"), OperationKey: "op-1"}); !errors.Is(err, state.ErrConflict) {
		t.Fatalf("conflicting retry error = %v, want ErrConflict", err)
	}
}

// newTestContinuationStore builds a store over test doubles with the test
// key space and keyring and a clock fixed at now.
func newTestContinuationStore(t *testing.T, invoker ContinuationInvoker, reader StringReader, now time.Time) *ContinuationStore {
	t.Helper()
	store := mustCall(t, NewContinuationStore, ContinuationOptions{Invoker: invoker, Reader: reader, Keys: testKeyOptions(), Keyring: testKeyring(t), Clock: func() time.Time { return now }})
	return store
}
