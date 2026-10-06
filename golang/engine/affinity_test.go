package engine

import (
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

func TestRoutingInputCarriesContinuationCacheAffinity(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	soft := state.ProviderCacheAffinity{Rank: 1, Provider: "openai", RouteID: "soft", EndpointID: "endpoint-soft", EndpointAccountHMAC: [32]byte{2}, Region: "us-east-1", EndpointFamily: "responses", ModelLineage: "lineage", RouteModelRevision: "revision", CacheEpoch: "epoch", LastSuccessAt: now}
	hard := soft
	hard.Rank = 0
	hard.RouteID = "hard"
	hard.EndpointID = "endpoint-hard"
	hard.HardPinned = true
	constraints := state.Constraints{Affinities: state.ProviderCacheAffinitySet{hard, soft}}
	input := routingInput(llm.Request{}, Snapshot{}, constraints, now)
	if input.Affinity == nil || input.Affinity.HardPin == nil {
		t.Fatalf("routing input affinity = %#v, want hard pin", input.Affinity)
	}
	if input.Affinity.HardPin.RouteID != "hard" {
		t.Fatalf("hard pin route = %q, want hard", input.Affinity.HardPin.RouteID)
	}
	if len(input.Affinity.Soft) != 1 || input.Affinity.Soft[0].RouteID != "soft" {
		t.Fatalf("soft affinity = %#v, want only soft record", input.Affinity.Soft)
	}
	input.Affinity.Soft[0].RouteID = "mutated"
	if constraints.Affinities[1].RouteID != "soft" {
		t.Fatal("routing input aliased continuation affinity")
	}
}
