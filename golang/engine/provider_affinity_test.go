package engine

import (
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

func TestCarryProviderCacheAffinityRefreshesOnlyMatchingRoute(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	expires := now.Add(time.Hour)
	matching := affinityTestObservation(now.Add(-time.Minute), &expires)
	matching.ObservedCacheReadTokens, matching.ObservedCacheWriteTokens = 1, 2
	nonMatching := matching
	nonMatching.Rank = 1
	nonMatching.RouteID = "route-b"
	nonMatching.EndpointID = "endpoint-b"
	parent := &state.Continuation{Affinities: state.ProviderCacheAffinitySet{matching, nonMatching}}
	candidate := affinityTestCandidate()
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
	candidate := affinityTestCandidate()
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
	affinity := affinityTestObservation(now.Add(-time.Hour), &expired)
	affinity.ObservedCacheReadTokens, affinity.ObservedCacheWriteTokens = 1, 2
	testassert.NoError(t, affinity.Validate(now), "expired parent affinity must be valid history")
	parent := &state.Continuation{Affinities: state.ProviderCacheAffinitySet{affinity}}
	candidate := affinityTestCandidate()
	result := carryProviderCacheAffinity(parent, candidate, llm.Usage{CacheReadTokens: 13}, now)
	if len(result) != 1 || !result[0].LastSuccessAt.Equal(affinity.LastSuccessAt) || result[0].ObservedCacheReadTokens != 1 {
		t.Fatalf("expired affinity = %#v, want it unchanged", result)
	}
	testassert.NoError(t, result[0].Validate(now), "carried affinity is invalid")
}

func TestCarryProviderCacheAffinityDropsAnExpiredHardPin(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	expired := now.Add(-time.Second)
	active := now.Add(time.Hour)
	pin := affinityTestObservation(now.Add(-time.Hour), &expired)
	pin.HardPinned = true
	soft := pin
	soft.Rank, soft.HardPinned, soft.RouteID, soft.EndpointID, soft.ExpiresAt = 1, false, "route-b", "endpoint-b", &active
	parent := &state.Continuation{Affinities: state.ProviderCacheAffinitySet{pin, soft}}
	candidate := affinityTestCandidate()
	for _, usage := range []llm.Usage{{}, {CacheReadTokens: 5}} {
		result := carryProviderCacheAffinity(parent, candidate, usage, now)
		if len(result) != 1 || result[0].HardPinned || result[0].RouteID != "route-b" {
			t.Fatalf("carried affinities = %#v, want only the active soft observation", result)
		}
	}
}

// affinityTestCandidate is the route-a candidate every carry test resolves against.
func affinityTestCandidate() routing.Candidate {
	return routing.Candidate{Provider: "openai", RouteID: "route-a", EndpointID: "endpoint-a", EndpointAccountHMAC: [32]byte{1}, Region: "us-east-1", Family: "responses", ModelLineage: "lineage", ModelRevision: "revision"}
}

// affinityTestObservation is a rank-0 affinity bound to affinityTestCandidate.
func affinityTestObservation(lastSuccess time.Time, expires *time.Time) state.ProviderCacheAffinity {
	return state.ProviderCacheAffinity{Rank: 0, Provider: "openai", RouteID: "route-a", EndpointID: "endpoint-a", EndpointAccountHMAC: [32]byte{1}, Region: "us-east-1", EndpointFamily: "responses", ModelLineage: "lineage", RouteModelRevision: "revision", CacheEpoch: "epoch", LastSuccessAt: lastSuccess, ExpiresAt: expires}
}
