// Package fileblob implements the development-only content-addressed blob
// store. It accepts no caller-controlled path and writes atomically under one
// configured root.
package fileblob

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/blob"
)

type Options struct {
	Root     string
	MaxBytes int64
	Clock    func() time.Time
}

type Store struct {
	root     string
	maxBytes int64
	clock    func() time.Time
	mu       sync.Mutex
}

func New(options Options) (*Store, error) {
	if strings.TrimSpace(options.Root) == "" {
		return nil, fmt.Errorf("file blob root is required")
	}
	if options.MaxBytes <= 0 {
		return nil, fmt.Errorf("file blob max bytes must be positive")
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	root, err := filepath.Abs(options.Root)
	if err != nil {
		return nil, fmt.Errorf("resolve file blob root: %w", err)
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, fmt.Errorf("create file blob root: %w", err)
	}
	return &Store{root: root, maxBytes: options.MaxBytes, clock: options.Clock}, nil
}

// ProbeBucket implements the runtime's common blob readiness contract. The
// file store has no bucket API, so it verifies that the pre-provisioned root
// remains a writable directory without creating a tenant-visible object.
func (store *Store) ProbeBucket(ctx context.Context) error {
	if store == nil || store.root == "" {
		return fmt.Errorf("file blob store is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	info, err := os.Stat(store.root)
	if err != nil {
		return fmt.Errorf("inspect file blob root: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("file blob root is not a directory")
	}
	temporary, err := os.CreateTemp(store.root, ".probe-*")
	if err != nil {
		return fmt.Errorf("create file blob probe: %w", err)
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("secure file blob probe: %w", err)
	}
	if _, err := temporary.Write([]byte{0}); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write file blob probe: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync file blob probe: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close file blob probe: %w", err)
	}
	if err := os.Remove(name); err != nil {
		return fmt.Errorf("remove file blob probe: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (store *Store) Put(ctx context.Context, request blob.PutRequest) (blob.Ref, error) {
	if store == nil {
		return blob.Ref{}, fmt.Errorf("file blob store is nil")
	}
	if err := ctx.Err(); err != nil {
		return blob.Ref{}, err
	}
	if request.Tenant == "" || request.MediaType == "" {
		return blob.Ref{}, fmt.Errorf("blob tenant and media type are required")
	}
	if int64(len(request.Data)) > store.maxBytes {
		return blob.Ref{}, fmt.Errorf("blob exceeds the configured size limit")
	}
	if request.ExpiresAt.IsZero() || !store.clock().Before(request.ExpiresAt) {
		return blob.Ref{}, blob.ErrExpired
	}
	tenantPrefix, err := blob.TenantPrefix(request.Tenant)
	if err != nil {
		return blob.Ref{}, err
	}
	digest := blob.Digest(request.Data)
	locator := tenantPrefix + "/" + digest
	ref := blob.Ref{Store: "file", Locator: locator, Digest: digest, ByteLength: int64(len(request.Data)), MediaType: request.MediaType, ExpiresAt: request.ExpiresAt}
	if err := ref.Validate(store.clock()); err != nil {
		return blob.Ref{}, err
	}
	path, err := store.objectPath(tenantPrefix, digest)
	if err != nil {
		return blob.Ref{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if existing, err := store.readExisting(path, ref); err == nil {
		_ = existing
		return ref, nil
	} else if !errors.Is(err, blob.ErrNotFound) {
		return blob.Ref{}, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return blob.Ref{}, fmt.Errorf("create blob tenant directory: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".blob-*")
	if err != nil {
		return blob.Ref{}, fmt.Errorf("create blob temporary file: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0600); err != nil {
		_ = temporary.Close()
		return blob.Ref{}, fmt.Errorf("secure blob temporary file: %w", err)
	}
	if _, err := temporary.Write(request.Data); err != nil {
		_ = temporary.Close()
		return blob.Ref{}, fmt.Errorf("write blob: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return blob.Ref{}, fmt.Errorf("sync blob: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return blob.Ref{}, fmt.Errorf("close blob: %w", err)
	}
	if err := os.Rename(temporaryName, path); err != nil {
		if os.IsExist(err) {
			if _, readErr := store.readExisting(path, ref); readErr == nil {
				return ref, nil
			}
		}
		return blob.Ref{}, fmt.Errorf("publish blob: %w", err)
	}
	return ref, nil
}

func (store *Store) Get(ctx context.Context, tenant string, ref blob.Ref) ([]byte, error) {
	if store == nil {
		return nil, fmt.Errorf("file blob store is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ref.Validate(store.clock()); err != nil {
		return nil, err
	}
	prefix, err := blob.TenantPrefix(tenant)
	if err != nil {
		return nil, err
	}
	if ref.Store != "file" || ref.Locator != prefix+"/"+ref.Digest {
		return nil, blob.ErrTenantMismatch
	}
	path, err := store.objectPath(prefix, ref.Digest)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, blob.ErrNotFound
		}
		return nil, fmt.Errorf("inspect blob: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, blob.ErrNotFound
	}
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, blob.ErrNotFound
		}
		return nil, fmt.Errorf("open blob: %w", err)
	}
	defer file.Close()
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, blob.ErrNotFound
	}
	if info.Size() != ref.ByteLength || info.Size() > store.maxBytes {
		return nil, blob.ErrDigestMismatch
	}
	data, err := io.ReadAll(io.LimitReader(file, store.maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read blob: %w", err)
	}
	if int64(len(data)) != ref.ByteLength || int64(len(data)) > store.maxBytes || blob.Digest(data) != ref.Digest {
		return nil, blob.ErrDigestMismatch
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append([]byte(nil), data...), nil
}

func (store *Store) objectPath(prefix, digest string) (string, error) {
	if store == nil || store.root == "" || len(digest) != sha256.Size*2 {
		return "", fmt.Errorf("invalid file blob path")
	}
	path := filepath.Join(store.root, prefix, digest)
	cleanRoot := filepath.Clean(store.root)
	cleanPath := filepath.Clean(path)
	if cleanPath != cleanRoot && !strings.HasPrefix(cleanPath, cleanRoot+string(filepath.Separator)) {
		return "", fmt.Errorf("blob path escapes configured root")
	}
	return cleanPath, nil
}

func (store *Store) readExisting(path string, ref blob.Ref) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, blob.ErrNotFound
		}
		return nil, fmt.Errorf("inspect existing blob: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, blob.ErrConflict
	}
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, blob.ErrNotFound
		}
		return nil, fmt.Errorf("open existing blob: %w", err)
	}
	defer file.Close()
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, blob.ErrConflict
	}
	data, err := io.ReadAll(io.LimitReader(file, store.maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read existing blob: %w", err)
	}
	if int64(len(data)) != ref.ByteLength || blob.Digest(data) != ref.Digest || info.Size() != ref.ByteLength {
		return nil, blob.ErrConflict
	}
	return data, nil
}
