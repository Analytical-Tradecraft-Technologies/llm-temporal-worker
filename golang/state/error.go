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
	// ErrMaterializeLimit marks deterministic depth, row, item, or byte
	// rejection. Callers may terminalize it as a no-dispatch request failure;
	// storage availability and corruption errors must remain distinguishable.
	ErrMaterializeLimit = errors.New("checkpoint materialization exceeds configured limit")
	// ErrInvalidCheckpoint marks durable checkpoint metadata or content that
	// fails immutable graph, provenance, or transcript validation.
	ErrInvalidCheckpoint = errors.New("checkpoint is invalid")
)
