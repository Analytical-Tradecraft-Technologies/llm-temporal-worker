package state

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

const continuationSchemaVersion = "continuation/v1"

type BlobRef struct {
	Digest [32]byte
	Size   int64
	Media  string
}

func (ref BlobRef) Valid() bool { return ref.Size >= 0 && ref.Media != "" && ref.Digest != [32]byte{} }

func (ref BlobRef) DigestHex() string { return hex.EncodeToString(ref.Digest[:]) }

type OpaqueStateRef struct {
	Provider      string
	EndpointID    string
	AccountRegion string
	Family        string
	ModelLineage  string
	Media         string
	Data          []byte
	Required      bool
}

func (ref OpaqueStateRef) clone() OpaqueStateRef {
	ref.Data = append([]byte(nil), ref.Data...)
	return ref
}

type Pinning struct {
	Provider      string
	EndpointID    string
	AccountRegion string
	Family        string
	ModelLineage  string
}

func (pin Pinning) Empty() bool {
	return pin.Provider == "" && pin.EndpointID == "" && pin.AccountRegion == "" && pin.Family == "" && pin.ModelLineage == ""
}

type Continuation struct {
	ID                 string
	Tenant             string
	ParentID           string
	Transcript         []llm.Item
	TranscriptDigest   [32]byte
	TranscriptComplete bool
	ProviderState      []OpaqueStateRef
	// Affinities carries immutable provider prompt-cache observations from the
	// parent checkpoint. They are routing preferences only; route eligibility
	// is still evaluated against the current catalog and request.
	Affinities        ProviderCacheAffinitySet
	Pinning           Pinning
	LastOperationID   string
	CapabilityVersion string
	PriceVersion      string
	CreatedAt         time.Time
	ExpiresAt         time.Time
	Depth             int
}

func (continuation Continuation) Clone() Continuation {
	continuation.Transcript = cloneItems(continuation.Transcript)
	providerState := continuation.ProviderState
	continuation.ProviderState = make([]OpaqueStateRef, len(providerState))
	for index, state := range providerState {
		continuation.ProviderState[index] = state.clone()
	}
	continuation.Affinities = continuation.Affinities.Clone()
	return continuation
}

func CanonicalTranscript(items []llm.Item) ([]byte, [32]byte, error) {
	if items == nil {
		items = []llm.Item{}
	}
	encoded := make([]json.RawMessage, 0, len(items))
	for index, item := range items {
		if item == nil {
			return nil, [32]byte{}, fmt.Errorf("transcript item %d is nil", index)
		}
		data, err := json.Marshal(item)
		if err != nil {
			return nil, [32]byte{}, fmt.Errorf("transcript item %d: %w", index, err)
		}
		canonical, err := llm.CanonicalJSON(data)
		if err != nil {
			return nil, [32]byte{}, fmt.Errorf("transcript item %d canonicalization: %w", index, err)
		}
		encoded = append(encoded, canonical)
	}
	container, err := llm.CanonicalJSON(mustMarshal(map[string]any{
		"schema": continuationSchemaVersion,
		"items":  encoded,
	}))
	if err != nil {
		return nil, [32]byte{}, err
	}
	digest := sha256.Sum256(container)
	return container, digest, nil
}

func mustMarshal(value any) []byte {
	data, _ := json.Marshal(value)
	return data
}

func (continuation Continuation) Validate(now time.Time) error {
	if continuation.ID == "" || continuation.Tenant == "" {
		return fmt.Errorf("continuation ID and tenant are required")
	}
	if continuation.Depth < 0 {
		return fmt.Errorf("continuation depth must not be negative")
	}
	if continuation.ExpiresAt.IsZero() || !now.Before(continuation.ExpiresAt) {
		return ErrExpired
	}
	_, digest, err := CanonicalTranscript(continuation.Transcript)
	if err != nil {
		return err
	}
	if digest != continuation.TranscriptDigest {
		return fmt.Errorf("continuation transcript digest mismatch")
	}
	if len(continuation.ProviderState) > 0 {
		if err := ValidatePinning(continuation.Pinning); err != nil {
			return fmt.Errorf("provider state requires complete continuation pinning: %w", err)
		}
	}
	for index, state := range continuation.ProviderState {
		if state.Provider == "" || state.EndpointID == "" || state.Family == "" || state.Media == "" {
			return fmt.Errorf("provider state %d is incomplete", index)
		}
		if len(state.Data) == 0 {
			return fmt.Errorf("provider state %d is empty", index)
		}
		if !providerStateMatchesPinning(state, continuation.Pinning) {
			return fmt.Errorf("provider state %d does not match continuation pinning", index)
		}
		if state.Required {
			if err := ValidatePinning(continuation.Pinning); err != nil {
				return fmt.Errorf("required provider state %d has incomplete continuation pinning: %w", index, err)
			}
		}
	}
	if err := continuation.Affinities.Validate(now); err != nil {
		return fmt.Errorf("provider cache affinities: %w", err)
	}
	return nil
}

func providerStateMatchesPinning(state OpaqueStateRef, pin Pinning) bool {
	return state.Provider == pin.Provider && state.EndpointID == pin.EndpointID &&
		state.AccountRegion == pin.AccountRegion && state.Family == pin.Family &&
		state.ModelLineage == pin.ModelLineage
}

// Constraints are the subset of continuation lineage that route planning
// needs. It intentionally contains no prompt or provider secret.
type Constraints struct {
	Present             bool
	Tenant              string
	Provider            string
	EndpointID          string
	AccountRegion       string
	Family              string
	ModelLineage        string
	RequiresOpaqueState bool
	TranscriptComplete  bool
	Portability         llm.PortabilityMode
	// Affinities are copied from immutable continuation state and converted to
	// planner preferences by the engine. Keeping this state-level type avoids
	// making persistence depend on the routing package.
	Affinities ProviderCacheAffinitySet
}

func (continuation Continuation) Constraints(mode llm.PortabilityMode) Constraints {
	// Provider state is not automatically a hard pin. Adapters mark state
	// Required when replaying it is necessary to preserve semantics; optional
	// state may be dropped only by CheckPinning in best-effort mode when the
	// canonical transcript is complete. Keeping this distinction here prevents
	// a persisted optional observation from silently becoming an authorization
	// constraint.
	requires := false
	for _, state := range continuation.ProviderState {
		requires = requires || state.Required
	}
	return Constraints{
		Present:             continuation.ID != "",
		Tenant:              continuation.Tenant,
		Provider:            continuation.Pinning.Provider,
		EndpointID:          continuation.Pinning.EndpointID,
		AccountRegion:       continuation.Pinning.AccountRegion,
		Family:              continuation.Pinning.Family,
		ModelLineage:        continuation.Pinning.ModelLineage,
		RequiresOpaqueState: requires,
		TranscriptComplete:  continuation.TranscriptComplete,
		Portability:         mode,
		Affinities:          continuation.Affinities.Clone(),
	}
}

type Store interface {
	Get(context.Context, Handle) (Continuation, error)
	GetForTenant(context.Context, string, Handle) (Continuation, error)
	PutChild(context.Context, PutChildRequest) (Handle, error)
}

// ContinuationStore is the stable name used by the engine and Activity
// layers. Store remains as a short alias for small reusable callers.
type ContinuationStore = Store

type Handle string

func (handle Handle) String() string { return string(handle) }

type PutChildRequest struct {
	Parent       Handle
	Child        Continuation
	OperationKey string
}
