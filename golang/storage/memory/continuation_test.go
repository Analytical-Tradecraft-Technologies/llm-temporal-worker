package memory

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

func TestContinuationStoreBranchesImmutably(t *testing.T) {
	keyring, err := state.NewKeyring([]state.Key{{ID: "k1", Secret: bytes.Repeat([]byte{3}, 32), Primary: true}}, bytes.NewReader(append(bytes.Repeat([]byte{4}, 16), bytes.Repeat([]byte{5}, 16)...)))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	store, err := NewContinuationStore(ContinuationOptions{Keyring: keyring, Clock: func() time.Time { return now }, MaxDepth: 4})
	if err != nil {
		t.Fatal(err)
	}
	items := []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}
	_, digest, err := state.CanonicalTranscript(items)
	if err != nil {
		t.Fatal(err)
	}
	root := state.Continuation{Tenant: "tenant", Transcript: items, TranscriptDigest: digest, TranscriptComplete: true, ExpiresAt: now.Add(time.Hour), LastOperationID: "root"}
	rootHandle, err := store.CreateRoot(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.GetForTenant(context.Background(), "tenant", rootHandle)
	if err != nil {
		t.Fatal(err)
	}
	got.Transcript[0] = llm.Message{Actor: llm.ActorHuman}
	again, err := store.Get(context.Background(), rootHandle)
	if err != nil || len(again.Transcript) != 1 {
		t.Fatalf("immutable read failed: %#v %v", again, err)
	}
	child := again
	child.ParentID = rootHandle.String()
	child.Depth = 1
	child.LastOperationID = "op-1"
	child.Transcript = append(child.Transcript, llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "world"}}})
	_, child.TranscriptDigest, err = state.CanonicalTranscript(child.Transcript)
	if err != nil {
		t.Fatal(err)
	}
	childHandle, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: rootHandle, Child: child, OperationKey: "op-1"})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: rootHandle, Child: child, OperationKey: "op-1"})
	if err != nil || replay != childHandle {
		t.Fatalf("idempotent child = %q %v", replay, err)
	}
}

func TestContinuationStoreHundredWaySameKeyReplay(t *testing.T) {
	keyring, err := state.NewKeyring([]state.Key{{ID: "k1", Secret: bytes.Repeat([]byte{3}, 32), Primary: true}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	store, err := NewContinuationStore(ContinuationOptions{Keyring: keyring, Clock: func() time.Time { return now }, MaxDepth: 4})
	if err != nil {
		t.Fatal(err)
	}
	items := []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "same-key"}}}}
	_, digest, err := state.CanonicalTranscript(items)
	if err != nil {
		t.Fatal(err)
	}
	root, err := store.CreateRoot(context.Background(), state.Continuation{
		Tenant: "tenant", Transcript: items, TranscriptDigest: digest, TranscriptComplete: true,
		ExpiresAt: now.Add(time.Hour), LastOperationID: "root",
	})
	if err != nil {
		t.Fatal(err)
	}
	childItems := append(append([]llm.Item(nil), items...), llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "child"}}})
	_, childDigest, err := state.CanonicalTranscript(childItems)
	if err != nil {
		t.Fatal(err)
	}
	child := state.Continuation{
		Tenant: "tenant", ParentID: root.String(), Transcript: childItems, TranscriptDigest: childDigest,
		TranscriptComplete: true, ExpiresAt: now.Add(time.Hour), LastOperationID: "same-operation", Depth: 1,
	}

	const callers = 100
	start := make(chan struct{})
	handles := make(chan state.Handle, callers)
	errs := make(chan error, callers)
	var group sync.WaitGroup
	group.Add(callers)
	for index := 0; index < callers; index++ {
		go func(index int) {
			defer group.Done()
			<-start
			handle, putErr := store.PutChild(context.Background(), state.PutChildRequest{
				Parent: root, Child: child, OperationKey: fmt.Sprintf("same-operation-%d", index),
			})
			// Each distinct key should produce a distinct sibling; the same-key
			// replay is exercised below after this concurrent fan-out.
			handles <- handle
			errs <- putErr
		}(index)
	}
	close(start)
	group.Wait()
	close(handles)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	unique := make(map[state.Handle]struct{}, callers)
	for handle := range handles {
		unique[handle] = struct{}{}
	}
	if len(unique) != callers {
		t.Fatalf("distinct sibling handles=%d, want %d", len(unique), callers)
	}

	// A retry storm for one operation key must elect one immutable child and
	// return that handle to every caller, never minting duplicate branches.
	const replays = 100
	replayStart := make(chan struct{})
	replayHandles := make(chan state.Handle, replays)
	replayErrs := make(chan error, replays)
	group = sync.WaitGroup{}
	group.Add(replays)
	for index := 0; index < replays; index++ {
		go func() {
			defer group.Done()
			<-replayStart
			handle, putErr := store.PutChild(context.Background(), state.PutChildRequest{Parent: root, Child: child, OperationKey: "same-operation"})
			replayHandles <- handle
			replayErrs <- putErr
		}()
	}
	close(replayStart)
	group.Wait()
	close(replayHandles)
	close(replayErrs)
	for err := range replayErrs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var elected state.Handle
	for handle := range replayHandles {
		if elected == "" {
			elected = handle
		}
		if handle != elected {
			t.Fatalf("same-key replay returned %q after electing %q", handle, elected)
		}
	}
	if elected == "" {
		t.Fatal("same-key replay returned no child handle")
	}
	if got, err := store.Get(context.Background(), elected); err != nil || got.LastOperationID != child.LastOperationID {
		t.Fatalf("elected child=%#v err=%v", got, err)
	}
}
