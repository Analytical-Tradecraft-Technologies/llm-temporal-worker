package fileblob

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/blob"
)

func TestStorePutGetIsImmutableAndTenantBound(t *testing.T) {
	now := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	store, err := New(Options{Root: t.TempDir(), MaxBytes: 1024, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant-a", MediaType: "application/json", Data: []byte(`{"ok":true}`), ExpiresAt: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(context.Background(), "tenant-a", ref)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"ok":true}` {
		t.Fatalf("blob = %q", got)
	}
	got[0] = 'X'
	again, err := store.Get(context.Background(), "tenant-a", ref)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != `{"ok":true}` {
		t.Fatalf("store returned mutable bytes %q", again)
	}
	if _, err := store.Get(context.Background(), "tenant-b", ref); !errors.Is(err, blob.ErrTenantMismatch) {
		t.Fatalf("cross-tenant get error = %v", err)
	}
	duplicate, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant-a", MediaType: "application/json", Data: []byte(`{"ok":true}`), ExpiresAt: now.Add(time.Hour)})
	if err != nil || duplicate.Locator != ref.Locator {
		t.Fatalf("idempotent put = %#v, %v", duplicate, err)
	}
}

func TestStoreDetectsTamperExpiryAndLimit(t *testing.T) {
	now := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	root := t.TempDir()
	store, err := New(Options{Root: root, MaxBytes: 5, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant-a", MediaType: "text/plain", Data: []byte("hello"), ExpiresAt: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	prefix, _ := blob.TenantPrefix("tenant-a")
	if err := os.WriteFile(filepath.Join(root, prefix, ref.Digest), []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), "tenant-a", ref); !errors.Is(err, blob.ErrDigestMismatch) {
		t.Fatalf("tamper error = %v", err)
	}
	if _, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant-a", MediaType: "text/plain", Data: []byte("123456"), ExpiresAt: now.Add(time.Hour)}); err == nil {
		t.Fatal("oversized put unexpectedly succeeded")
	}
	ref.ExpiresAt = now
	if _, err := store.Get(context.Background(), "tenant-a", ref); !errors.Is(err, blob.ErrExpired) {
		t.Fatalf("expired error = %v", err)
	}
}

func TestStoreHonorsCancellation(t *testing.T) {
	store, err := New(Options{Root: t.TempDir(), MaxBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Put(ctx, blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("value"), ExpiresAt: time.Now().Add(time.Hour)}); err != context.Canceled {
		t.Fatalf("canceled put = %v", err)
	}
}

func TestStoreRejectsSymlinkObjects(t *testing.T) {
	now := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	root := t.TempDir()
	store, err := New(Options{Root: root, MaxBytes: 1024, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant-a", MediaType: "text/plain", Data: []byte("safe"), ExpiresAt: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	prefix, _ := blob.TenantPrefix("tenant-a")
	path := filepath.Join(root, prefix, ref.Digest)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "outside")
	if err := os.WriteFile(target, []byte("unsafe"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := store.Get(context.Background(), "tenant-a", ref); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("symlink get error = %v", err)
	}
}

func TestStoreProbeBucketChecksWritableRoot(t *testing.T) {
	root := t.TempDir()
	store, err := New(Options{Root: root, MaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	prober, ok := any(store).(interface {
		ProbeBucket(context.Context) error
	})
	if !ok {
		t.Fatal("file blob store does not expose a readiness probe")
	}
	if err := prober.ProbeBucket(context.Background()); err != nil {
		t.Fatalf("ProbeBucket() error = %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("readiness probe left files behind: %#v", entries)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(root, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := prober.ProbeBucket(context.Background()); err == nil {
		t.Fatal("ProbeBucket() accepted an unusable root")
	}
}
