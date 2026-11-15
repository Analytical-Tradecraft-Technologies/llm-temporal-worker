package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/blob"
)

// newTestBlobStore builds a blob store whose clock reads *clock.
func newTestBlobStore(t *testing.T, maxBytes int64, clock *time.Time) *BlobStore {
	t.Helper()
	store, err := NewBlobStore(BlobOptions{MaxBytes: maxBytes, Clock: func() time.Time { return *clock }})
	testassert.NoError(t, err)
	return store
}

func TestBlobStoreCopiesDataAndBindsTenant(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	store := newTestBlobStore(t, 32, &now)
	data := []byte("hello")
	ref, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: data, ExpiresAt: now.Add(time.Hour)})
	testassert.NoError(t, err)
	data[0] = 'X'
	got, err := store.Get(context.Background(), "tenant", ref)
	testassert.NoError(t, err)
	if string(got) != "hello" {
		t.Fatalf("Get() = %q, want copied original", got)
	}
	got[0] = 'Y'
	again, err := store.Get(context.Background(), "tenant", ref)
	testassert.NoError(t, err)
	if string(again) != "hello" {
		t.Fatalf("Get() returned mutable backing bytes: %q", again)
	}
	if _, err := store.Get(context.Background(), "other", ref); !errors.Is(err, blob.ErrTenantMismatch) {
		t.Fatalf("wrong tenant error = %v, want tenant mismatch", err)
	}
}

func TestBlobStoreIsIdempotentAndRejectsConflictingMetadata(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	store := newTestBlobStore(t, 32, &now)
	request := blob.PutRequest{Tenant: "tenant", MediaType: "application/json", Data: []byte(`{"ok":true}`), ExpiresAt: now.Add(time.Hour)}
	first, err := store.Put(context.Background(), request)
	testassert.NoError(t, err)
	second, err := store.Put(context.Background(), request)
	testassert.NoError(t, err)
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
	store := newTestBlobStore(t, 32, &clock)
	first, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("hello"), ExpiresAt: now.Add(time.Minute)})
	testassert.NoError(t, err)
	second, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("hello"), ExpiresAt: now.Add(time.Hour)})
	testassert.NoError(t, err, "same content with a different expiry was rejected")
	if first == second {
		t.Fatal("operation-specific expiry was not preserved in the returned reference")
	}
	testassert.NoError(t, testassert.Err(store.Get(context.Background(), "tenant", second)), "long-lived reference Get() error")
	clock = now.Add(2 * time.Minute)
	if _, err := store.Get(context.Background(), "tenant", first); !errors.Is(err, blob.ErrExpired) {
		t.Fatalf("short-lived reference error = %v, want expired", err)
	}
	testassert.NoError(t, testassert.Err(store.Get(context.Background(), "tenant", second)), "long-lived reference expired with short-lived reference")
}

func TestBlobStoreSweepsExpiredEntries(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	clock := now
	store := newTestBlobStore(t, 32, &clock)
	ref, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("hello"), ExpiresAt: now.Add(time.Minute)})
	testassert.NoError(t, err)
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
	store := newTestBlobStore(t, 4, &now)
	_, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("12345"), ExpiresAt: now.Add(time.Hour)})
	if err == nil {
		t.Fatal("blob larger than configured limit was accepted")
	}
}
