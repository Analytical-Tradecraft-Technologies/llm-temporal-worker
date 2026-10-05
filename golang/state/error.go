package state

import "errors"

// ErrInvalidHandle is intentionally deliberately non-specific. Callers must
// not be able to distinguish a missing handle from a handle owned by another
// tenant or one signed with a retired key.
var ErrInvalidHandle = errors.New("invalid continuation handle")

var (
	ErrNotFound       = errors.New("state record not found")
	ErrTenantMismatch = errors.New("state tenant mismatch")
	ErrExpired        = errors.New("state record expired")
	ErrConflict       = errors.New("state record conflict")
	// ErrLimitExceeded marks a deterministic lineage, depth, item or byte
	// limit violation. Retrying the same request cannot succeed.
	ErrLimitExceeded = errors.New("state limit exceeded")
)
