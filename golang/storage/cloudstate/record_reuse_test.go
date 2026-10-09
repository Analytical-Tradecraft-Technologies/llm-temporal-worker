package cloudstate

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
)

func TestRecordReuseTracksAuthoritativeRevisionAndOwnsCopies(t *testing.T) {
	r, table, blobs, request := fixture(t)
	want, err := r.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	opens := 0
	blobs.open = func(blob.BlobKey) error { opens++; return nil }
	ctx := WithRecordReuse(context.Background())
	first, err := r.Read(ctx, request.Scope, request.ID)
	if err != nil {
		t.Fatal(err)
	}
	first.Request.Manifest[0] = '!'
	first.Progress[0] = '!'
	second, err := r.Read(ctx, request.Scope, request.ID)
	if err != nil || !reflect.DeepEqual(second, want) || opens != 1 {
		t.Fatalf("repeated read lost isolation or reread the blob: opens=%d err=%v", opens, err)
	}
	// A different repository commits a new revision while this invocation
	// holds the old one. Resolving events must observe that revision.
	want, err = reopen(t, table, blobs).TryUpdate(context.Background(), request.Scope, request.ID, change(want, StatusRunning, "other-writer"))
	if err != nil {
		t.Fatal(err)
	}
	opens = 0
	for range 2 {
		got, err := r.Read(ctx, request.Scope, request.ID)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("reused stale revision: got=%+v err=%v", got, err)
		}
	}
	if opens != 1 {
		t.Fatalf("new revision opened %d times", opens)
	}
	if _, err := r.Read(ctx, Scope{Tenant: request.Scope.Tenant, Project: "another"}, request.ID); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("scope bypass: %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := r.Read(cancelled, request.Scope, request.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation bypass: %v", err)
	}
	// Removing the event stream must not make the reused record authority.
	table.mu.Lock()
	for key := range table.rows {
		if key.PartitionKey == r.stream(request.ID) {
			delete(table.rows, key)
		}
	}
	table.mu.Unlock()
	if _, err := r.Read(ctx, request.Scope, request.ID); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("removed authority bypass: %v", err)
	}
}

func TestRecordReuseDoesNotCrossInvocationOrRepository(t *testing.T) {
	r, table, blobs, request := fixture(t)
	if _, err := r.Create(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	ctx := WithRecordReuse(context.Background())
	if _, err := r.Read(ctx, request.Scope, request.ID); err != nil {
		t.Fatal(err)
	}
	// A blob lost after its successful read may be reused only within that
	// invocation. Another repository or Activity must detect the loss.
	blobs.mu.Lock()
	clear(blobs.values)
	blobs.mu.Unlock()
	if _, err := r.Read(ctx, request.Scope, request.ID); err != nil {
		t.Fatalf("validated immutable record was not reused: %v", err)
	}
	for name, read := range map[string]func() error{
		"new invocation":     func() error { _, err := r.Read(WithRecordReuse(ctx), request.Scope, request.ID); return err },
		"plain context":      func() error { _, err := r.Read(context.Background(), request.Scope, request.ID); return err },
		"another repository": func() error { _, err := reopen(t, table, blobs).Read(ctx, request.Scope, request.ID); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := read(); !errors.Is(err, ErrCorrupt) || errors.Is(err, contracts.ErrNotFound) {
				t.Fatalf("lost blob was hidden: %v", err)
			}
		})
	}
}

func TestRecordReuseConcurrentCopies(t *testing.T) {
	r, _, _, request := fixture(t)
	if _, err := r.Create(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	ctx := WithRecordReuse(context.Background())
	if _, err := r.Read(ctx, request.Scope, request.ID); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 24 {
		wg.Go(func() {
			record, err := r.Read(ctx, request.Scope, request.ID)
			if err != nil {
				t.Error(err)
				return
			}
			record.Request.Manifest[0] = '!'
			record.Progress[0] = '!'
		})
	}
	wg.Wait()
	if _, err := r.Read(ctx, request.Scope, request.ID); err != nil {
		t.Fatal(err)
	}
}

func TestRecordReuseDoesNotTreatPublicationAsReadableStorage(t *testing.T) {
	for _, update := range []bool{false, true} {
		name := "create"
		if update {
			name = "update"
		}
		t.Run(name, func(t *testing.T) {
			r, _, blobs, request := fixture(t)
			ctx := WithRecordReuse(context.Background())
			record, err := r.Create(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if update {
				if _, err := r.Read(ctx, request.Scope, request.ID); err != nil {
					t.Fatal(err)
				}
				if _, err := r.TryUpdate(ctx, request.Scope, request.ID, change(record, StatusRunning, "published")); err != nil {
					t.Fatal(err)
				}
			}
			// An acknowledged publication does not prove that its bytes can
			// still be read. The old revision must not hide this new loss.
			blobs.mu.Lock()
			clear(blobs.values)
			blobs.mu.Unlock()
			if _, err := r.Read(ctx, request.Scope, request.ID); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("published revision bypassed storage validation: %v", err)
			}
		})
	}
}

func TestRecordReuseRequiresEntireValidatedPointer(t *testing.T) {
	r, _, _, request := fixture(t)
	if _, err := r.Create(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	ctx := WithRecordReuse(context.Background())
	if _, err := r.Read(ctx, request.Scope, request.ID); err != nil {
		t.Fatal(err)
	}
	state, err := r.events.ReadState(ctx, r.stream(request.ID))
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*recordPointer){
		"revision": func(p *recordPointer) { p.Revision++ },
		"scope":    func(p *recordPointer) { p.ScopeTag = r.scopeTag(Scope{Tenant: "other", Project: "other"}) },
		"binding":  func(p *recordPointer) { p.Binding = r.digest("binding", nil) },
		"status":   func(p *recordPointer) { p.Status = StatusRunning },
		"blob":     func(p *recordPointer) { p.Blob = r.blobKey(r.stream(request.ID), []byte("missing")) },
	} {
		t.Run(name, func(t *testing.T) {
			pointer := state.Value
			mutate(&pointer)
			if _, err := r.loadRecord(ctx, pointer); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("changed pointer used a validated record: %v", err)
			}
		})
	}
}

func TestRecordReuseBoundsRetainedPayloads(t *testing.T) {
	ctx := WithRecordReuse(context.Background())
	reuse := recordReuseFrom(ctx)
	r := &Repository{}
	// Three individually bounded records exceed the invocation's retained
	// payload allowance. A rejected entry must remain a normal storage miss.
	for range 3 {
		id, err := NewRequestID()
		if err != nil {
			t.Fatal(err)
		}
		pointer := recordPointer{ID: id}
		record := Record{Request: CreateRequest{Manifest: make([]byte, maxPayloadBytes)}}
		reuse.remember(r, pointer, record)
	}
	if reuse.bytes > recordReuseMaxBytes || len(reuse.records) != 2 {
		t.Fatalf("unbounded retained payloads: %d bytes in %d records", reuse.bytes, len(reuse.records))
	}
}
