package memory

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

func TestContinuationStoreBranchesImmutably(t *testing.T) {
	now := time.Unix(100, 0)
	store := newTestContinuationStore(t, &now, deterministicKeyringRandom())
	items, digest := textTranscript(t, llm.ActorHuman, "hello")
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
	child.Transcript = append(child.Transcript, llm.ModelText("world"))
	child.TranscriptDigest = canonicalDigest(t, child.Transcript)
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
	now := time.Unix(100, 0)
	store := newTestContinuationStore(t, &now, nil)
	items, digest := textTranscript(t, llm.ActorHuman, "same-key")
	root, err := store.CreateRoot(context.Background(), state.Continuation{
		Tenant: "tenant", Transcript: items, TranscriptDigest: digest, TranscriptComplete: true,
		ExpiresAt: now.Add(time.Hour), LastOperationID: "root",
	})
	if err != nil {
		t.Fatal(err)
	}
	childItems := append(append([]llm.Item(nil), items...), llm.ModelText("child"))
	childDigest := canonicalDigest(t, childItems)
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

func TestContinuationStoreDetachesNestedTranscriptData(t *testing.T) {
	now := time.Unix(100, 0)
	store := newTestContinuationStore(t, &now, deterministicKeyringRandom())
	content := []llm.Part{llm.TextPart{Text: "hello"}}
	arguments := []byte(`{"q":"x"}`)
	items := []llm.Item{
		llm.Message{Actor: llm.ActorHuman, Content: content},
		llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: arguments},
	}
	digest := canonicalDigest(t, items)
	handle, err := store.CreateRoot(context.Background(), state.Continuation{Tenant: "tenant", Transcript: items, TranscriptDigest: digest, TranscriptComplete: true, ExpiresAt: now.Add(time.Hour), LastOperationID: "root"})
	if err != nil {
		t.Fatal(err)
	}
	// Mutate the caller's input after the write.
	content[0] = llm.TextPart{Text: "mutated input"}
	arguments[2] = 'Q'
	got, err := store.Get(context.Background(), handle)
	if err != nil {
		t.Fatal(err)
	}
	// Mutate a returned read.
	got.Transcript[0].(llm.Message).Content[0] = llm.TextPart{Text: "mutated read"}
	got.Transcript[1].(llm.ToolCall).Arguments[2] = 'R'

	again, err := store.Get(context.Background(), handle)
	if err != nil {
		t.Fatal(err)
	}
	if text := again.Transcript[0].(llm.Message).Content[0].(llm.TextPart).Text; text != "hello" {
		t.Fatalf("stored message text = %q", text)
	}
	if args := string(again.Transcript[1].(llm.ToolCall).Arguments); args != `{"q":"x"}` {
		t.Fatalf("stored tool arguments = %s", args)
	}
	if err := again.Validate(now); err != nil {
		t.Fatalf("stored continuation no longer matches its digest: %v", err)
	}
}
