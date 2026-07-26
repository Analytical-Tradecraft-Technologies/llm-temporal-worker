package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type InventorySource string

const (
	InventoryProviderAPI    InventorySource = "provider_api"
	InventoryConfiguredOnly InventorySource = "configured_only"
	InventoryUnsupported    InventorySource = "unsupported"
)

type Lifecycle string

const (
	LifecycleAvailable   Lifecycle = "available"
	LifecycleDeprecated  Lifecycle = "deprecated"
	LifecycleUnavailable Lifecycle = "unavailable"
	LifecycleUnknown     Lifecycle = "unknown"
)

type Model struct {
	ProviderModelID  string
	DisplayName      string
	OwnedBy          string
	CreatedAt        time.Time
	Lifecycle        Lifecycle
	CapabilityDigest [32]byte
	SafeMetadata     map[string]string
}

type InventorySnapshot struct {
	ConfigDigest        [32]byte
	Provider            string
	EndpointID          string
	EndpointAccountHMAC [32]byte
	EndpointFamily      string
	Region              string
	Source              InventorySource
	ObservedAt          time.Time
	Complete            bool
	NextCursor          string
	InventoryDigest     [32]byte
	ExpiresAt           time.Time
	Models              []Model
}

func (snapshot InventorySnapshot) Validate() error {
	if snapshot.ConfigDigest == ([32]byte{}) || snapshot.EndpointAccountHMAC == ([32]byte{}) || snapshot.InventoryDigest == ([32]byte{}) {
		return errors.New("inventory snapshot requires config, account, and inventory digests")
	}
	for name, value := range map[string]string{"provider": snapshot.Provider, "endpoint_id": snapshot.EndpointID, "endpoint_family": snapshot.EndpointFamily, "region": snapshot.Region} {
		if err := validateIdentifier(name, value); err != nil {
			return err
		}
	}
	if snapshot.Source != InventoryProviderAPI && snapshot.Source != InventoryConfiguredOnly && snapshot.Source != InventoryUnsupported {
		return fmt.Errorf("inventory source %q is unsupported", snapshot.Source)
	}
	if snapshot.ObservedAt.IsZero() || snapshot.ExpiresAt.IsZero() || !snapshot.ExpiresAt.After(snapshot.ObservedAt) {
		return errors.New("inventory snapshot has an invalid observation interval")
	}
	if len(snapshot.NextCursor) > 2048 {
		return errors.New("inventory snapshot cursor exceeds limit")
	}
	if snapshot.Complete && snapshot.NextCursor != "" {
		return errors.New("complete inventory snapshot cannot have a continuation cursor")
	}
	if !snapshot.Complete && snapshot.Source == InventoryProviderAPI && snapshot.NextCursor == "" {
		return errors.New("incomplete provider inventory requires a continuation cursor")
	}
	if len(snapshot.Models) > 10000 {
		return errors.New("inventory snapshot exceeds model limit")
	}
	previous := ""
	for _, model := range snapshot.Models {
		if model.ProviderModelID == "" || len(model.ProviderModelID) > 256 || model.ProviderModelID <= previous {
			return errors.New("inventory models must be non-empty and sorted by provider model id")
		}
		previous = model.ProviderModelID
		if model.Lifecycle != LifecycleAvailable && model.Lifecycle != LifecycleDeprecated && model.Lifecycle != LifecycleUnavailable && model.Lifecycle != LifecycleUnknown {
			return fmt.Errorf("model %q has unsupported lifecycle", model.ProviderModelID)
		}
		if len(model.SafeMetadata) > 32 {
			return fmt.Errorf("model %q has too much metadata", model.ProviderModelID)
		}
		for key, value := range model.SafeMetadata {
			if err := validateSafeCode("metadata key", key); err != nil {
				return err
			}
			if err := validateSafeCode("metadata value", value); err != nil {
				return err
			}
		}
	}
	if snapshot.Source == InventoryUnsupported && len(snapshot.Models) != 0 {
		return errors.New("unsupported inventory cannot contain models")
	}
	return nil
}

// Provenance is intentionally explicit so callers cannot mistake a stale
// provider snapshot for a current, complete listing.
type Provenance string

const (
	ProvenanceCurrent     Provenance = "current"
	ProvenanceStale       Provenance = "stale"
	ProvenanceUnsupported Provenance = "unsupported"
)

func (snapshot InventorySnapshot) ProvenanceAt(now time.Time) Provenance {
	if snapshot.Source == InventoryUnsupported {
		return ProvenanceUnsupported
	}
	if now.IsZero() || !now.Before(snapshot.ExpiresAt) {
		return ProvenanceStale
	}
	return ProvenanceCurrent
}

// ConfiguredModel reports whether a discovered model is already configured.
// Discovery is informational: callers must use this predicate before adding
// any route, and normal routing never calls it to mutate its catalog.
func ConfiguredModel(configured []string, discovered string) bool {
	for _, model := range configured {
		if model == discovered {
			return true
		}
	}
	return false
}

// RefreshCoordinator collapses concurrent refreshes for one endpoint. The
// fetch function is called at most once while a refresh is in flight; a
// caller can still receive a stale snapshot if refresh fails.
type RefreshCoordinator struct {
	mu      sync.Mutex
	entries map[string]*refreshEntry
}

type refreshEntry struct {
	snapshot InventorySnapshot
	wait     chan struct{}
	err      error
}

func NewRefreshCoordinator() *RefreshCoordinator {
	return &RefreshCoordinator{entries: make(map[string]*refreshEntry)}
}

func (coordinator *RefreshCoordinator) Refresh(ctx context.Context, endpoint string, fetch func(context.Context) (InventorySnapshot, error)) (InventorySnapshot, error) {
	if coordinator == nil || endpoint == "" || fetch == nil {
		return InventorySnapshot{}, errors.New("endpoint and fetch are required")
	}
	coordinator.mu.Lock()
	if current, ok := coordinator.entries[endpoint]; ok && current.wait != nil {
		wait := current.wait
		coordinator.mu.Unlock()
		select {
		case <-ctx.Done():
			return InventorySnapshot{}, ctx.Err()
		case <-wait:
			// Keep the in-flight entry captured above. A later refresh can
			// replace the map entry immediately after this one completes; reading
			// the map again could therefore return that later attempt's
			// not-yet-completed result (and incorrectly report a nil error).
			return current.snapshot, current.err
		}
	}
	previous := coordinator.entries[endpoint]
	entry := &refreshEntry{wait: make(chan struct{})}
	if previous != nil {
		entry.snapshot = previous.snapshot
	}
	coordinator.entries[endpoint] = entry
	coordinator.mu.Unlock()

	snapshot, err := fetch(ctx)
	if err == nil {
		err = snapshot.Validate()
	}
	coordinator.mu.Lock()
	closeWait := entry.wait
	if err == nil {
		entry.snapshot = snapshot
	}
	entry.err = err
	result := entry.snapshot
	entry.wait = nil
	coordinator.entries[endpoint] = entry
	coordinator.mu.Unlock()
	if closeWait != nil {
		close(closeWait)
	}
	return result, err
}

// InventoryDigest is a deterministic digest over sorted model IDs and safe
// metadata. It is useful for persistence idempotency and query provenance.
func InventoryDigest(models []Model) [32]byte {
	copyModels := append([]Model(nil), models...)
	sort.Slice(copyModels, func(i, j int) bool { return copyModels[i].ProviderModelID < copyModels[j].ProviderModelID })
	var parts []string
	for _, model := range copyModels {
		parts = append(parts, model.ProviderModelID, model.DisplayName, model.OwnedBy, model.CreatedAt.UTC().Format(time.RFC3339Nano), string(model.Lifecycle), hex.EncodeToString(model.CapabilityDigest[:]))
		keys := make([]string, 0, len(model.SafeMetadata))
		for key := range model.SafeMetadata {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			parts = append(parts, key, model.SafeMetadata[key])
		}
	}
	return sha256.Sum256([]byte(strings.Join(parts, "\x00")))
}
