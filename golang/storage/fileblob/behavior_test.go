package fileblob

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
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
	testassert.NoError(t, os.WriteFile(fileRoot, []byte("not a directory"), 0600))
	if _, err := New(Options{Root: fileRoot, MaxBytes: 1}); err == nil {
		t.Fatal("file path unexpectedly accepted as blob root")
	}

	now := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	store := newTestStore(t, t.TempDir(), 16, now)
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
	store := newTestStore(t, root, 64, now)
	request := blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("wanted"), ExpiresAt: now.Add(time.Hour)}
	prefix, err := blob.TenantPrefix(request.Tenant)
	testassert.NoError(t, err)
	digest := blob.Digest(request.Data)
	path := filepath.Join(root, prefix, digest)
	testassert.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
	testassert.NoError(t, os.WriteFile(path, []byte("different"), 0600))
	testassert.ErrorIs(t, testassert.Err(store.Put(context.Background(), request)), blob.ErrConflict, "conflicting object")

	testassert.NoError(t, os.Remove(path))
	testassert.NoError(t, os.Mkdir(path, 0700))
	testassert.ErrorIs(t, testassert.Err(store.Put(context.Background(), request)), blob.ErrConflict, "directory object")
	testassert.ErrorIs(t, testassert.Err(store.Get(context.Background(), request.Tenant, blob.Ref{Store: "file", Locator: prefix + "/" + digest, Digest: digest, ByteLength: int64(len(request.Data)), MediaType: request.MediaType, ExpiresAt: request.ExpiresAt})), blob.ErrNotFound, "directory get")

	testassert.NoError(t, os.Remove(path))
	ref, err := store.Put(context.Background(), request)
	testassert.NoError(t, err)
	fileInfo, err := os.Stat(filepath.Join(root, ref.Locator))
	testassert.NoError(t, err)
	if fileInfo.Mode().Perm() != 0600 {
		t.Fatalf("blob mode = %o, want 600", fileInfo.Mode().Perm())
	}
	tenantInfo, err := os.Stat(filepath.Dir(filepath.Join(root, ref.Locator)))
	testassert.NoError(t, err)
	if tenantInfo.Mode().Perm() != 0700 {
		t.Fatalf("tenant directory mode = %o, want 700", tenantInfo.Mode().Perm())
	}
}

func TestStoreGetAndProbeHonorCancellationAndValidatePaths(t *testing.T) {
	now := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	store := newTestStore(t, t.TempDir(), 64, now)
	ref := putTestText(t, store, "tenant", "value", now.Add(time.Hour))
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	testassert.ErrorIs(t, testassert.Err(store.Get(canceled, "tenant", ref)), context.Canceled, "canceled get")
	testassert.ErrorIs(t, testassert.Err(store.Put(canceled, blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("other"), ExpiresAt: now.Add(time.Hour)})), context.Canceled, "canceled put")
	if _, err := store.Get(context.Background(), "tenant", blob.Ref{Store: "file", Locator: "tenant/not-a-digest", Digest: "short", ByteLength: 1, MediaType: "text/plain", ExpiresAt: now.Add(time.Hour)}); err == nil {
		t.Fatal("invalid digest unexpectedly accepted")
	}
	testassert.NoError(t, testassert.Err(store.Get(context.Background(), "tenant", ref)))
	testassert.NoError(t, os.Remove(filepath.Join(store.root, ref.Locator)))
	testassert.ErrorIs(t, testassert.Err(store.Get(context.Background(), "tenant", ref)), blob.ErrNotFound, "missing object")

	testassert.ErrorIs(t, store.ProbeBucket(canceled), context.Canceled, "canceled probe")
	testassert.NoError(t, store.ProbeBucket(context.Background()))
}

func TestStoreObjectPathCannotEscapeRoot(t *testing.T) {
	store, err := New(Options{Root: t.TempDir(), MaxBytes: 1})
	testassert.NoError(t, err)
	digest := strings.Repeat("a", 64)
	if _, err := store.objectPath("..", digest); err == nil {
		t.Fatal("parent prefix escaped configured root")
	}
	if _, err := store.objectPath("tenant", "short"); err == nil {
		t.Fatal("short digest accepted in object path")
	}
}
