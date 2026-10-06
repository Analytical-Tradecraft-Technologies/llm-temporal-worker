package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/blob"
)

func TestBlobStoreCopiesDataAndBindsTenant(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	store, err := NewBlobStore(BlobOptions{MaxBytes: 32, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("hello")
	ref, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: data, ExpiresAt: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	data[0] = 'X'
	got, err := store.Get(context.Background(), "tenant", ref)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Fatalf("Get() = %q, want copied original", got)
	}
	got[0] = 'Y'
	again, err := store.Get(context.Background(), "tenant", ref)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != "hello" {
		t.Fatalf("Get() returned mutable backing bytes: %q", again)
	}
	if _, err := store.Get(context.Background(), "other", ref); !errors.Is(err, blob.ErrTenantMismatch) {
		t.Fatalf("wrong tenant error = %v, want tenant mismatch", err)
	}
}

func TestBlobStoreIsIdempotentAndRejectsConflictingMetadata(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	store, err := NewBlobStore(BlobOptions{MaxBytes: 32, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	request := blob.PutRequest{Tenant: "tenant", MediaType: "application/json", Data: []byte(`{"ok":true}`), ExpiresAt: now.Add(time.Hour)}
	first, err := store.Put(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Put(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("idempotent refs differ: %#v vs %#v", first, second)
	}
	request.MediaType = "text/plain"
	if _, err := store.Put(context.Background(), request); !errors.Is(err, blob.ErrConflict) {
		t.Fatalf("conflicting metadata error = %v, want conflict", err)
	}
}

func TestBlobStoreAllowsDifferentExpiryForSameContent(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	clock := now
	store, err := NewBlobStore(BlobOptions{MaxBytes: 32, Clock: func() time.Time { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("hello"), ExpiresAt: now.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("hello"), ExpiresAt: now.Add(time.Hour)})
	if err != nil {
		t.Fatalf("same content with a different expiry was rejected: %v", err)
	}
	if first == second {
		t.Fatal("operation-specific expiry was not preserved in the returned reference")
	}
	if _, err := store.Get(context.Background(), "tenant", second); err != nil {
		t.Fatalf("long-lived reference Get() error = %v", err)
	}
	clock = now.Add(2 * time.Minute)
	if _, err := store.Get(context.Background(), "tenant", first); !errors.Is(err, blob.ErrExpired) {
		t.Fatalf("short-lived reference error = %v, want expired", err)
	}
	if _, err := store.Get(context.Background(), "tenant", second); err != nil {
		t.Fatalf("long-lived reference expired with short-lived reference: %v", err)
	}
}

func TestBlobStoreSweepsExpiredEntries(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	clock := now
	store, err := NewBlobStore(BlobOptions{MaxBytes: 32, Clock: func() time.Time { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("hello"), ExpiresAt: now.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	clock = now.Add(time.Minute)
	if removed := store.Sweep(clock); removed != 1 {
		t.Fatalf("Sweep() removed %d entries, want 1", removed)
	}
	if _, err := store.Get(context.Background(), "tenant", ref); !errors.Is(err, blob.ErrExpired) {
		t.Fatalf("expired Get() error = %v, want expired", err)
	}
}

func TestBlobStoreEnforcesConfiguredPerBlobLimit(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	store, err := NewBlobStore(BlobOptions{MaxBytes: 4, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Put(context.Background(), blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("12345"), ExpiresAt: now.Add(time.Hour)})
	if err == nil {
		t.Fatal("blob larger than configured limit was accepted")
	}
}
