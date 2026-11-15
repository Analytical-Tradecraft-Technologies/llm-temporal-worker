package fileblob

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/blob"
)

// newTestStore opens a store under root whose clock is fixed at now.
func newTestStore(t *testing.T, root string, maxBytes int64, now time.Time) *Store {
	t.Helper()
	store, err := New(Options{Root: root, MaxBytes: maxBytes, Clock: func() time.Time { return now }})
	testassert.NoError(t, err)
	return store
}

// putTestText stores data as a text/plain blob for tenant.
func putTestText(t *testing.T, store *Store, tenant, data string, expiresAt time.Time) blob.Ref {
	t.Helper()
	ref, err := store.Put(context.Background(), blob.PutRequest{Tenant: tenant, MediaType: "text/plain", Data: []byte(data), ExpiresAt: expiresAt})
	testassert.NoError(t, err)
	return ref
}

func TestStorePutGetIsImmutableAndTenantBound(t *testing.T) {
	now := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	store := newTestStore(t, t.TempDir(), 1024, now)
	ref, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant-a", MediaType: "application/json", Data: []byte(`{"ok":true}`), ExpiresAt: now.Add(time.Hour)})
	testassert.NoError(t, err)
	got, err := store.Get(context.Background(), "tenant-a", ref)
	testassert.NoError(t, err)
	if string(got) != `{"ok":true}` {
		t.Fatalf("blob = %q", got)
	}
	got[0] = 'X'
	again, err := store.Get(context.Background(), "tenant-a", ref)
	testassert.NoError(t, err)
	if string(again) != `{"ok":true}` {
		t.Fatalf("store returned mutable bytes %q", again)
	}
	testassert.ErrorIs(t, testassert.Err(store.Get(context.Background(), "tenant-b", ref)), blob.ErrTenantMismatch, "cross-tenant get")
	duplicate, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant-a", MediaType: "application/json", Data: []byte(`{"ok":true}`), ExpiresAt: now.Add(time.Hour)})
	if err != nil || duplicate.Locator != ref.Locator {
		t.Fatalf("idempotent put = %#v, %v", duplicate, err)
	}
}

func TestStoreDetectsTamperExpiryAndLimit(t *testing.T) {
	now := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	root := t.TempDir()
	store := newTestStore(t, root, 5, now)
	ref := putTestText(t, store, "tenant-a", "hello", now.Add(time.Hour))
	prefix, _ := blob.TenantPrefix("tenant-a")
	testassert.NoError(t, os.WriteFile(filepath.Join(root, prefix, ref.Digest), []byte("tampered"), 0600))
	testassert.ErrorIs(t, testassert.Err(store.Get(context.Background(), "tenant-a", ref)), blob.ErrDigestMismatch, "tamper")
	if _, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant-a", MediaType: "text/plain", Data: []byte("123456"), ExpiresAt: now.Add(time.Hour)}); err == nil {
		t.Fatal("oversized put unexpectedly succeeded")
	}
	ref.ExpiresAt = now
	testassert.ErrorIs(t, testassert.Err(store.Get(context.Background(), "tenant-a", ref)), blob.ErrExpired, "expired")
}

func TestStoreHonorsCancellation(t *testing.T) {
	store, err := New(Options{Root: t.TempDir(), MaxBytes: 32})
	testassert.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Put(ctx, blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("value"), ExpiresAt: time.Now().Add(time.Hour)}); err != context.Canceled {
		t.Fatalf("canceled put = %v", err)
	}
}

func TestStoreRejectsSymlinkObjects(t *testing.T) {
	now := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	root := t.TempDir()
	store := newTestStore(t, root, 1024, now)
	ref := putTestText(t, store, "tenant-a", "safe", now.Add(time.Hour))
	prefix, _ := blob.TenantPrefix("tenant-a")
	path := filepath.Join(root, prefix, ref.Digest)
	testassert.NoError(t, os.Remove(path))
	target := filepath.Join(root, "outside")
	testassert.NoError(t, os.WriteFile(target, []byte("unsafe"), 0600))
	if err := os.Symlink(target, path); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	testassert.ErrorIs(t, testassert.Err(store.Get(context.Background(), "tenant-a", ref)), blob.ErrNotFound, "symlink get")
}

func TestStoreProbeBucketChecksWritableRoot(t *testing.T) {
	root := t.TempDir()
	store, err := New(Options{Root: root, MaxBytes: 1024})
	testassert.NoError(t, err)
	prober, ok := any(store).(interface {
		ProbeBucket(context.Context) error
	})
	if !ok {
		t.Fatal("file blob store does not expose a readiness probe")
	}
	testassert.NoError(t, prober.ProbeBucket(context.Background()), "ProbeBucket() error")
	entries, err := os.ReadDir(root)
	testassert.NoError(t, err)
	if len(entries) != 0 {
		t.Fatalf("readiness probe left files behind: %#v", entries)
	}
	testassert.NoError(t, os.RemoveAll(root))
	testassert.NoError(t, os.WriteFile(root, []byte("not a directory"), 0600))
	if err := prober.ProbeBucket(context.Background()); err == nil {
		t.Fatal("ProbeBucket() accepted an unusable root")
	}
}
