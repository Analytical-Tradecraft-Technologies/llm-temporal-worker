package fileblob

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/blob"
)

func TestStoreConstructorAndPutValidation(t *testing.T) {
	for name, options := range map[string]Options{
		"empty root":         {Root: "", MaxBytes: 1},
		"whitespace root":    {Root: "  ", MaxBytes: 1},
		"zero max bytes":     {Root: t.TempDir(), MaxBytes: 0},
		"negative max bytes": {Root: t.TempDir(), MaxBytes: -1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := New(options); err == nil {
				t.Fatal("invalid options unexpectedly accepted")
			}
		})
	}

	fileRoot := filepath.Join(t.TempDir(), "root-file")
	if err := os.WriteFile(fileRoot, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Options{Root: fileRoot, MaxBytes: 1}); err == nil {
		t.Fatal("file path unexpectedly accepted as blob root")
	}

	now := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	store, err := New(Options{Root: t.TempDir(), MaxBytes: 16, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	for name, request := range map[string]blob.PutRequest{
		"missing tenant":     {MediaType: "text/plain", Data: []byte("value"), ExpiresAt: now.Add(time.Hour)},
		"missing media type": {Tenant: "tenant", Data: []byte("value"), ExpiresAt: now.Add(time.Hour)},
		"whitespace tenant":  {Tenant: " ", MediaType: "text/plain", Data: []byte("value"), ExpiresAt: now.Add(time.Hour)},
		"zero expiry":        {Tenant: "tenant", MediaType: "text/plain", Data: []byte("value")},
		"expired":            {Tenant: "tenant", MediaType: "text/plain", Data: []byte("value"), ExpiresAt: now},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := store.Put(context.Background(), request); err == nil {
				t.Fatal("invalid put unexpectedly succeeded")
			}
		})
	}

	var nilStore *Store
	if _, err := nilStore.Put(context.Background(), blob.PutRequest{}); err == nil {
		t.Fatal("nil store accepted a put")
	}
}

func TestStorePutConflictAndSecureObjectLayout(t *testing.T) {
	now := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	root := t.TempDir()
	store, err := New(Options{Root: root, MaxBytes: 64, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	request := blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("wanted"), ExpiresAt: now.Add(time.Hour)}
	prefix, err := blob.TenantPrefix(request.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	digest := blob.Digest(request.Data)
	path := filepath.Join(root, prefix, digest)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("different"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), request); !errors.Is(err, blob.ErrConflict) {
		t.Fatalf("conflicting object error = %v", err)
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), request); !errors.Is(err, blob.ErrConflict) {
		t.Fatalf("directory object error = %v", err)
	}
	if _, err := store.Get(context.Background(), request.Tenant, blob.Ref{Store: "file", Locator: prefix + "/" + digest, Digest: digest, ByteLength: int64(len(request.Data)), MediaType: request.MediaType, ExpiresAt: request.ExpiresAt}); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("directory get error = %v", err)
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	ref, err := store.Put(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	fileInfo, err := os.Stat(filepath.Join(root, ref.Locator))
	if err != nil {
		t.Fatal(err)
	}
	if fileInfo.Mode().Perm() != 0600 {
		t.Fatalf("blob mode = %o, want 600", fileInfo.Mode().Perm())
	}
	tenantInfo, err := os.Stat(filepath.Dir(filepath.Join(root, ref.Locator)))
	if err != nil {
		t.Fatal(err)
	}
	if tenantInfo.Mode().Perm() != 0700 {
		t.Fatalf("tenant directory mode = %o, want 700", tenantInfo.Mode().Perm())
	}
}

func TestStoreGetAndProbeHonorCancellationAndValidatePaths(t *testing.T) {
	now := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	store, err := New(Options{Root: t.TempDir(), MaxBytes: 64, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("value"), ExpiresAt: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Get(canceled, "tenant", ref); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled get = %v", err)
	}
	if _, err := store.Put(canceled, blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("other"), ExpiresAt: now.Add(time.Hour)}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled put = %v", err)
	}
	if _, err := store.Get(context.Background(), "tenant", blob.Ref{Store: "file", Locator: "tenant/not-a-digest", Digest: "short", ByteLength: 1, MediaType: "text/plain", ExpiresAt: now.Add(time.Hour)}); err == nil {
		t.Fatal("invalid digest unexpectedly accepted")
	}
	if _, err := store.Get(context.Background(), "tenant", ref); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(store.root, ref.Locator)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), "tenant", ref); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("missing object error = %v", err)
	}

	if err := store.ProbeBucket(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled probe = %v", err)
	}
	if err := store.ProbeBucket(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStoreObjectPathCannotEscapeRoot(t *testing.T) {
	store, err := New(Options{Root: t.TempDir(), MaxBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	if _, err := store.objectPath("..", digest); err == nil {
		t.Fatal("parent prefix escaped configured root")
	}
	if _, err := store.objectPath("tenant", "short"); err == nil {
		t.Fatal("short digest accepted in object path")
	}
}
