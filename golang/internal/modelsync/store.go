package modelsync

import (
	"context"
	"sync"
	"time"
)

// Store shares the latest Document between workers and elects the one
// worker that refreshes it. Bodies are opaque canonical Document bytes; the
// digest is Digest(body).
type Store interface {
	// LatestDigest returns the published digest, or "" when none exists. It
	// is the cheap poll every worker runs to notice a new Document.
	LatestDigest(ctx context.Context) (string, error)
	// Latest returns the published Document atomically with its digest and
	// fetch time, or found=false when none exists.
	Latest(ctx context.Context) (digest string, fetchedAt time.Time, body []byte, found bool, err error)
	// Publish replaces the published Document only when fetchedAt is later
	// than the published one, so a slow fetch can never overwrite a newer
	// one. It reports whether the Document was published.
	Publish(ctx context.Context, digest string, fetchedAt time.Time, body []byte) (bool, error)
	// AcquireRefresh takes the refresh lease for ttl under token, reporting
	// false when another worker holds it.
	AcquireRefresh(ctx context.Context, token string, ttl time.Duration) (bool, error)
	// ReleaseRefresh releases the lease only when token still holds it.
	ReleaseRefresh(ctx context.Context, token string) error
}

// MemoryStore is the in-process Store for memory state: each process
// fetches and holds its own Document.
type MemoryStore struct {
	mu         sync.Mutex
	digest     string
	fetchedAt  time.Time
	body       []byte
	leaseOwner string
	leaseUntil time.Time
	clock      func() time.Time
}

// NewMemoryStore returns an empty MemoryStore. A nil clock uses time.Now.
func NewMemoryStore(clock func() time.Time) *MemoryStore {
	if clock == nil {
		clock = time.Now
	}
	return &MemoryStore{clock: clock}
}

func (store *MemoryStore) LatestDigest(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.digest, nil
}

func (store *MemoryStore) Latest(ctx context.Context) (string, time.Time, []byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", time.Time{}, nil, false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.digest == "" {
		return "", time.Time{}, nil, false, nil
	}
	return store.digest, store.fetchedAt, append([]byte(nil), store.body...), true, nil
}

func (store *MemoryStore) Publish(ctx context.Context, digest string, fetchedAt time.Time, body []byte) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.digest != "" && !fetchedAt.After(store.fetchedAt) {
		return false, nil
	}
	store.digest, store.fetchedAt, store.body = digest, fetchedAt, append([]byte(nil), body...)
	return true, nil
}

func (store *MemoryStore) AcquireRefresh(ctx context.Context, token string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	now := store.clock()
	if store.leaseOwner != "" && now.Before(store.leaseUntil) {
		return false, nil
	}
	store.leaseOwner, store.leaseUntil = token, now.Add(ttl)
	return true, nil
}

func (store *MemoryStore) ReleaseRefresh(ctx context.Context, token string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.leaseOwner == token {
		store.leaseOwner, store.leaseUntil = "", time.Time{}
	}
	return nil
}
