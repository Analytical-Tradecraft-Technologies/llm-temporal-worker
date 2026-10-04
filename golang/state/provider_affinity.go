package state

import (
	"encoding/hex"
	"fmt"
	"time"
)

// ProviderCacheAffinity is the non-secret checkpoint record used to prefer a
// provider's prompt/context cache on a subsequent turn. It deliberately
// stores only route identity and an HMAC digest; tenant IDs, prompt content,
// and provider credentials are never persisted as a cache key.
//
// The record is immutable once attached to a checkpoint. A new successful
// turn creates a new observation rather than mutating its parent record.
type ProviderCacheAffinity struct {
	// Rank is the stable order within a checkpoint. Lower ranks are preferred
	// after ordinary route eligibility has completed.
	Rank                     int
	Provider                 string
	RouteID                  string
	EndpointID               string
	EndpointAccountHMAC      [32]byte
	Region                   string
	EndpointFamily           string
	ModelLineage             string
	RouteModelRevision       string
	ProviderCacheKeyHMAC     [32]byte
	HasProviderCacheKey      bool
	CacheEpoch               string
	HardPinned               bool
	ObservedCacheReadTokens  int64
	ObservedCacheWriteTokens int64
	LastSuccessAt            time.Time
	ExpiresAt                *time.Time
}

// ProviderCacheAffinitySet is the ordered collection persisted with a
// checkpoint. The ordering is meaningful and is retained across forks.
type ProviderCacheAffinitySet []ProviderCacheAffinity

// Clone returns a defensive copy suitable for passing across a persistence
// boundary.
func (set ProviderCacheAffinitySet) Clone() ProviderCacheAffinitySet {
	if set == nil {
		return nil
	}
	result := make(ProviderCacheAffinitySet, len(set))
	copy(result, set)
	for index := range result {
		if set[index].ExpiresAt != nil {
			expires := *set[index].ExpiresAt
			result[index].ExpiresAt = &expires
		}
	}
	return result
}

// Validate checks the persistence invariants shared by durable stores and the
// in-memory route planner. now is used only for optional expiry validation.
func (affinity ProviderCacheAffinity) Validate(now time.Time) error {
	// Validation intentionally does not depend on wall-clock expiry: expired
	// observations remain valid immutable history and are filtered by Active.
	_ = now
	if affinity.Rank < 0 {
		return fmt.Errorf("provider cache affinity rank must not be negative")
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "provider", value: affinity.Provider},
		{name: "route_id", value: affinity.RouteID},
		{name: "endpoint_id", value: affinity.EndpointID},
		{name: "region", value: affinity.Region},
		{name: "endpoint_family", value: affinity.EndpointFamily},
		{name: "model_lineage", value: affinity.ModelLineage},
		{name: "route_model_revision", value: affinity.RouteModelRevision},
		{name: "cache_epoch", value: affinity.CacheEpoch},
	} {
		if field.value == "" {
			return fmt.Errorf("provider cache affinity %s is required", field.name)
		}
	}
	if affinity.EndpointAccountHMAC == [32]byte{} {
		return fmt.Errorf("provider cache affinity endpoint account HMAC is required")
	}
	if affinity.HasProviderCacheKey && affinity.ProviderCacheKeyHMAC == [32]byte{} {
		return fmt.Errorf("provider cache affinity provider cache key HMAC is empty")
	}
	if affinity.ObservedCacheReadTokens < 0 || affinity.ObservedCacheWriteTokens < 0 {
		return fmt.Errorf("provider cache affinity usage observations must not be negative")
	}
	if affinity.LastSuccessAt.IsZero() {
		return fmt.Errorf("provider cache affinity last success time is required")
	}
	if affinity.ExpiresAt != nil {
		if affinity.ExpiresAt.IsZero() || !affinity.ExpiresAt.After(affinity.LastSuccessAt) {
			return fmt.Errorf("provider cache affinity expiry must be after last success")
		}
		// An expired soft observation is valid persisted history, but callers
		// must not use it for preference. Keep validation independent of now so
		// immutable checkpoint reads can still be audited after expiry.
	}
	return nil
}

// Active reports whether the affinity may be used at now. Expired records are
// retained for audit but are never allowed to influence routing.
func (affinity ProviderCacheAffinity) Active(now time.Time) bool {
	if err := affinity.Validate(now); err != nil {
		return false
	}
	return affinity.ExpiresAt == nil || now.Before(*affinity.ExpiresAt)
}

// ProviderCacheKeyHex is a safe representation for diagnostics and provider
// metadata. It is empty when this record has no provider prompt-cache key.
func (affinity ProviderCacheAffinity) ProviderCacheKeyHex() string {
	if !affinity.HasProviderCacheKey {
		return ""
	}
	return hex.EncodeToString(affinity.ProviderCacheKeyHMAC[:])
}

// Validate checks rank uniqueness and each record's immutable fields.
func (set ProviderCacheAffinitySet) Validate(now time.Time) error {
	seen := make(map[int]struct{}, len(set))
	for _, affinity := range set {
		if err := affinity.Validate(now); err != nil {
			return err
		}
		if _, ok := seen[affinity.Rank]; ok {
			return fmt.Errorf("provider cache affinity rank %d is duplicated", affinity.Rank)
		}
		seen[affinity.Rank] = struct{}{}
	}
	return nil
}
