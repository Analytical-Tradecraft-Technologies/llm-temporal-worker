package engine

import (
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

func TestCarryProviderCacheAffinityRefreshesOnlyMatchingRoute(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	expires := now.Add(time.Hour)
	matching := state.ProviderCacheAffinity{Rank: 0, Provider: "openai", RouteID: "route-a", EndpointID: "endpoint-a", EndpointAccountHMAC: [32]byte{1}, Region: "us-east-1", EndpointFamily: "responses", ModelLineage: "lineage", RouteModelRevision: "revision", CacheEpoch: "epoch", ObservedCacheReadTokens: 1, ObservedCacheWriteTokens: 2, LastSuccessAt: now.Add(-time.Minute), ExpiresAt: &expires}
	nonMatching := matching
	nonMatching.Rank = 1
	nonMatching.RouteID = "route-b"
	nonMatching.EndpointID = "endpoint-b"
	parent := &state.Continuation{Affinities: state.ProviderCacheAffinitySet{matching, nonMatching}}
	candidate := routing.Candidate{Provider: "openai", RouteID: "route-a", EndpointID: "endpoint-a", EndpointAccountHMAC: [32]byte{1}, Region: "us-east-1", Family: "responses", ModelLineage: "lineage", ModelRevision: "revision"}
	result := carryProviderCacheAffinity(parent, candidate, llm.Usage{CacheReadTokens: 13, CacheWriteTokens: 8}, now)
	if len(result) != 2 || result[0].ObservedCacheReadTokens != 13 || result[0].ObservedCacheWriteTokens != 8 || !result[0].LastSuccessAt.Equal(now) {
		t.Fatalf("matching affinity = %#v, want refreshed usage", result)
	}
	if result[1].ObservedCacheReadTokens != nonMatching.ObservedCacheReadTokens || result[1].ObservedCacheWriteTokens != nonMatching.ObservedCacheWriteTokens || !result[1].LastSuccessAt.Equal(nonMatching.LastSuccessAt) {
		t.Fatalf("non-matching affinity changed: %#v", result[1])
	}
	result[0].RouteID = "mutated"
	if parent.Affinities[0].RouteID != "route-a" {
		t.Fatal("carry helper aliased parent affinity")
	}
}

func TestCarryProviderCacheAffinityDoesNotInventIdentity(t *testing.T) {
	candidate := routing.Candidate{Provider: "openai", RouteID: "route-a", EndpointID: "endpoint-a", EndpointAccountHMAC: [32]byte{1}, Region: "us-east-1", Family: "responses", ModelLineage: "lineage", ModelRevision: "revision"}
	if got := carryProviderCacheAffinity(nil, candidate, llm.Usage{CacheReadTokens: 1}, time.Unix(200, 0)); got != nil {
		t.Fatalf("root affinity = %#v, want nil without persisted key/epoch", got)
	}
	parent := &state.Continuation{}
	if got := carryProviderCacheAffinity(parent, candidate, llm.Usage{}, time.Unix(200, 0)); got != nil {
		t.Fatalf("empty parent affinity = %#v, want nil", got)
	}
}

func TestCarryProviderCacheAffinityKeepsAnExpiredObservationUnchanged(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	expired := now.Add(-time.Minute)
	affinity := state.ProviderCacheAffinity{Rank: 0, Provider: "openai", RouteID: "route-a", EndpointID: "endpoint-a", EndpointAccountHMAC: [32]byte{1}, Region: "us-east-1", EndpointFamily: "responses", ModelLineage: "lineage", RouteModelRevision: "revision", CacheEpoch: "epoch", ObservedCacheReadTokens: 1, ObservedCacheWriteTokens: 2, LastSuccessAt: now.Add(-time.Hour), ExpiresAt: &expired}
	if err := affinity.Validate(now); err != nil {
		t.Fatalf("expired parent affinity must be valid history: %v", err)
	}
	parent := &state.Continuation{Affinities: state.ProviderCacheAffinitySet{affinity}}
	candidate := routing.Candidate{Provider: "openai", RouteID: "route-a", EndpointID: "endpoint-a", EndpointAccountHMAC: [32]byte{1}, Region: "us-east-1", Family: "responses", ModelLineage: "lineage", ModelRevision: "revision"}
	result := carryProviderCacheAffinity(parent, candidate, llm.Usage{CacheReadTokens: 13}, now)
	if len(result) != 1 || !result[0].LastSuccessAt.Equal(affinity.LastSuccessAt) || result[0].ObservedCacheReadTokens != 1 {
		t.Fatalf("expired affinity = %#v, want it unchanged", result)
	}
	if err := result[0].Validate(now); err != nil {
		t.Fatalf("carried affinity is invalid: %v", err)
	}
}

func TestCarryProviderCacheAffinityDropsAnExpiredHardPin(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	expired := now.Add(-time.Second)
	active := now.Add(time.Hour)
	pin := state.ProviderCacheAffinity{Rank: 0, Provider: "openai", RouteID: "route-a", EndpointID: "endpoint-a", EndpointAccountHMAC: [32]byte{1}, Region: "us-east-1", EndpointFamily: "responses", ModelLineage: "lineage", RouteModelRevision: "revision", CacheEpoch: "epoch", HardPinned: true, LastSuccessAt: now.Add(-time.Hour), ExpiresAt: &expired}
	soft := pin
	soft.Rank, soft.HardPinned, soft.RouteID, soft.EndpointID, soft.ExpiresAt = 1, false, "route-b", "endpoint-b", &active
	parent := &state.Continuation{Affinities: state.ProviderCacheAffinitySet{pin, soft}}
	candidate := routing.Candidate{Provider: "openai", RouteID: "route-a", EndpointID: "endpoint-a", EndpointAccountHMAC: [32]byte{1}, Region: "us-east-1", Family: "responses", ModelLineage: "lineage", ModelRevision: "revision"}
	for _, usage := range []llm.Usage{{}, {CacheReadTokens: 5}} {
		result := carryProviderCacheAffinity(parent, candidate, usage, now)
		if len(result) != 1 || result[0].HardPinned || result[0].RouteID != "route-b" {
			t.Fatalf("carried affinities = %#v, want only the active soft observation", result)
		}
	}
}
