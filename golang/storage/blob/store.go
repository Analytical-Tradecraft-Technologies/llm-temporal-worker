// Package blob defines the small content-addressed object-store port used by
// runtime storage implementations. The port deliberately contains no
// provider, Temporal, or configuration types.
package blob

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrNotFound       = errors.New("blob not found")
	ErrConflict       = errors.New("blob already exists with different metadata")
	ErrExpired        = errors.New("blob expired")
	ErrDigestMismatch = errors.New("blob digest mismatch")
	ErrTenantMismatch = errors.New("blob tenant mismatch")
)

// Ref is safe to put in a durable record or Activity payload. Locator values
// are opaque to callers; implementations must still validate them rather than
// treating them as filesystem paths or trusted object keys.
type Ref struct {
	Store      string
	Locator    string
	Digest     string
	ByteLength int64
	MediaType  string
	ExpiresAt  time.Time
}

func (ref Ref) Validate(now time.Time) error {
	if strings.TrimSpace(ref.Store) == "" || strings.TrimSpace(ref.Locator) == "" {
		return fmt.Errorf("blob store and locator are required")
	}
	if len(ref.Digest) != sha256.Size*2 {
		return fmt.Errorf("blob digest must be a 64-character SHA-256 hex value")
	}
	if _, err := hex.DecodeString(ref.Digest); err != nil {
		return fmt.Errorf("blob digest is not hexadecimal: %w", err)
	}
	if ref.ByteLength < 0 {
		return fmt.Errorf("blob byte length must not be negative")
	}
	if strings.TrimSpace(ref.MediaType) == "" {
		return fmt.Errorf("blob media type is required")
	}
	if !ref.ExpiresAt.IsZero() && !now.Before(ref.ExpiresAt) {
		return ErrExpired
	}
	return nil
}

type PutRequest struct {
	Tenant    string
	MediaType string
	Data      []byte
	ExpiresAt time.Time
}

// Store provides immutable, digest-verified blobs. Implementations must copy
// request and response bytes and must not log their content.
type Store interface {
	Put(context.Context, PutRequest) (Ref, error)
	Get(context.Context, string, Ref) ([]byte, error)
}

func Digest(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func TenantPrefix(tenant string) (string, error) {
	if strings.TrimSpace(tenant) == "" {
		return "", fmt.Errorf("blob tenant is required")
	}
	digest := sha256.Sum256([]byte(tenant))
	return hex.EncodeToString(digest[:]), nil
}

// Check validates the fields every Store requires before writing request,
// whose data may be at most maxBytes. clock is read once, only after the
// request itself is well formed, and now is that reading. It returns the
// tenant's key prefix and the content digest.
func (request PutRequest) Check(maxBytes int64, clock func() time.Time) (tenantPrefix, digest string, now time.Time, err error) {
	if request.Tenant == "" || request.MediaType == "" {
		return "", "", time.Time{}, fmt.Errorf("blob tenant and media type are required")
	}
	if int64(len(request.Data)) > maxBytes {
		return "", "", time.Time{}, fmt.Errorf("blob exceeds the configured size limit")
	}
	now = clock()
	if request.ExpiresAt.IsZero() || !now.Before(request.ExpiresAt) {
		return "", "", time.Time{}, ErrExpired
	}
	tenantPrefix, err = TenantPrefix(request.Tenant)
	if err != nil {
		return "", "", time.Time{}, err
	}
	return tenantPrefix, Digest(request.Data), now, nil
}

// Ref describes request's data stored by store at locator under digest.
func (request PutRequest) Ref(store, locator, digest string) Ref {
	return Ref{Store: store, Locator: locator, Digest: digest, ByteLength: int64(len(request.Data)), MediaType: request.MediaType, ExpiresAt: request.ExpiresAt}
}
