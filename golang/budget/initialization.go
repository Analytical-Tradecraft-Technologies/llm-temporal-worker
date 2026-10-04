package budget

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"time"

	"github.com/google/uuid"
)

var ErrInitializationInvalid = errors.New("invalid budget initialization")

var initializationNamespace = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}:\{[^{}[:space:]]{1,64}\}:$`)

// InitializationIdentity binds accounting to a Redis namespace and its key
// derivation secret. Limits and configuration versions deliberately are not
// part of this identity: a reload must not reset accumulated spending.
type InitializationIdentity struct {
	Namespace      string `json:"namespace"`
	KeyFingerprint string `json:"key_fingerprint"`
}

func (identity InitializationIdentity) Validate() error {
	key, err := hex.DecodeString(identity.KeyFingerprint)
	if !initializationNamespace.MatchString(identity.Namespace) || len(identity.Namespace) > 132 || err != nil || len(key) != sha256.Size || hex.EncodeToString(key) != identity.KeyFingerprint {
		return ErrInitializationInvalid
	}
	return nil
}

// Initialization is a permanent receipt, not a budget journal. It records no
// balances, limits, reservations or provider data. Once Ready, losing Redis
// authority requires a separate recovery procedure; initialization cannot be
// used to grant a fresh budget in its place.
type Initialization struct {
	Schema    string                 `json:"schema"`
	Identity  InitializationIdentity `json:"identity"`
	Epoch     string                 `json:"epoch"`
	CreatedAt time.Time              `json:"created_at"`
	Ready     bool                   `json:"ready"`
}

const InitializationSchema = "budget-initialization/v1"

func (value Initialization) Validate() error {
	id, err := uuid.Parse(value.Epoch)
	if value.Schema != InitializationSchema || value.Identity.Validate() != nil || err != nil || id == uuid.Nil || id.String() != value.Epoch || value.CreatedAt.IsZero() || value.CreatedAt.Year() < 1 || value.CreatedAt.Year() > 9999 {
		return ErrInitializationInvalid
	}
	return nil
}

// Marker is immutable across the preparing/ready transition and independent
// of JSON formatting. It contains only an epoch and one-way identity digests.
func (value Initialization) Marker() string {
	digest := sha256.Sum256([]byte(value.Identity.Namespace))
	return InitializationSchema + ":" + value.Epoch + ":" + hex.EncodeToString(digest[:]) + ":" + value.Identity.KeyFingerprint
}

// InitializationStore must use conditional creation/update and strongly
// consistent reads. Prepare may create a receipt only when none exists;
// Complete only advances that exact receipt to Ready. No reset/delete port is
// provided. A lost write acknowledgement is resolved by reading on retry.
type InitializationStore interface {
	ReadBudgetInitialization(context.Context, string) (Initialization, error)
	PrepareBudgetInitialization(context.Context, InitializationIdentity, time.Time) (InitializationPreparation, error)
	CompleteBudgetInitialization(context.Context, Initialization) error
}

// Created is an ephemeral, single-use permission to create the Redis marker.
// It is true only after an acknowledged conditional creation, never on replay.
// If that permission is lost before Redis is written, operators must investigate
// the interrupted installation; a retry cannot assume an empty budget is safe.
type InitializationPreparation struct {
	Receipt Initialization
	Created bool
}
