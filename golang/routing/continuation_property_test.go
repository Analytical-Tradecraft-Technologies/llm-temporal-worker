package routing

import (
	"context"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

func timeForProperty() time.Time { return time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC) }

func TestPlannerNeverMovesAcrossRequiredContinuationPin(t *testing.T) {
	catalog := continuationPropertyCatalog(t)
	for _, test := range []struct {
		name        string
		constraints state.Constraints
		wantRoutes  map[string]bool
	}{
		{
			name:        "required opaque state",
			constraints: state.Constraints{Present: true, Provider: "anthropic", EndpointID: "endpoint-pinned", AccountRegion: "region", Family: "messages", ModelLineage: "lineage-pinned", RequiresOpaqueState: true, TranscriptComplete: true, Portability: llm.PortabilityBestEffort},
			wantRoutes:  map[string]bool{"pinned": true},
		},
		{
			name:        "optional state best effort",
			constraints: state.Constraints{Present: true, Provider: "anthropic", EndpointID: "endpoint-pinned", Family: "messages", ModelLineage: "lineage-pinned", TranscriptComplete: true, Portability: llm.PortabilityBestEffort},
			wantRoutes:  map[string]bool{"pinned": true, "portable": true},
		},
		{
			name:        "incomplete transcript",
			constraints: state.Constraints{Present: true, Provider: "anthropic", EndpointID: "endpoint-pinned", Family: "messages", ModelLineage: "lineage-pinned", TranscriptComplete: false, Portability: llm.PortabilityBestEffort},
			wantRoutes:  map[string]bool{"pinned": true},
		},
		{
			name:        "strict transcript",
			constraints: state.Constraints{Present: true, Provider: "anthropic", EndpointID: "endpoint-pinned", Family: "messages", ModelLineage: "lineage-pinned", TranscriptComplete: true, Portability: llm.PortabilityStrict},
			wantRoutes:  map[string]bool{"pinned": true},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan, err := (DeterministicPlanner{}).Plan(context.Background(), Input{
				Request: llm.Request{OperationKey: "continuation-property-" + test.name, Model: "logical", ServiceClass: llm.ServiceClassStandard, Portability: test.constraints.Portability, Context: llm.RequestContext{Tenant: "tenant"}},
				Catalog: catalog, Continuation: test.constraints,
			})
			if err != nil {
				t.Fatalf("Plan() = %v", err)
			}
			got := make(map[string]bool, len(plan.Candidates))
			for _, candidate := range plan.Candidates {
				got[candidate.RouteID] = true
			}
			if len(got) != len(test.wantRoutes) {
				t.Fatalf("routes = %#v, want %#v", got, test.wantRoutes)
			}
			for routeID := range got {
				if !test.wantRoutes[routeID] {
					t.Fatalf("route %q crossed continuation pin", routeID)
				}
			}
		})
	}
}

func TestProviderCacheAffinityCannotBypassHealthEligibility(t *testing.T) {
	catalog := continuationPropertyCatalog(t)
	request := llm.Request{OperationKey: "affinity-health-property", Model: "logical", ServiceClass: llm.ServiceClassStandard, Context: llm.RequestContext{Tenant: "tenant"}}
	health := HealthView{Routes: map[string]RouteHealth{
		"pinned":   {Enabled: true, Open: true, Reason: "operator-open"},
		"portable": {Enabled: true, Open: true, Reason: "operator-open"},
	}}
	preferred, err := (DeterministicPlanner{}).Plan(context.Background(), Input{
		Request: request, Catalog: catalog, Health: health,
		Now: timeForProperty(), Affinity: &AffinityPreferences{Soft: ProviderCacheAffinitySet{{
			Rank: 0, Provider: "openai", RouteID: "portable", EndpointID: "endpoint-portable", Region: "region", EndpointFamily: "responses", ModelLineage: "lineage-portable", RouteModelRevision: "revision-portable", CacheEpoch: "epoch", LastSuccessAt: timeForProperty(),
		}}},
	})
	if err == nil || len(preferred.Candidates) != 0 {
		t.Fatalf("affinity bypassed health rejection: plan=%#v err=%v", preferred, err)
	}
}

func continuationPropertyCatalog(t *testing.T) Catalog {
	t.Helper()
	catalog, err := CompileCatalog("continuation-property-v1", map[string]Model{"logical": {Routes: []Route{
		{ID: "pinned", EndpointID: "endpoint-pinned", Provider: "anthropic", Family: "messages", Region: "region", AccountRegion: "region", Model: "model-pinned", ModelLineage: "lineage-pinned", ModelRevision: "revision-pinned", Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard"}, Capabilities: testCapabilities(), PriceVersion: "price-v1", PriceAvailable: true},
		{ID: "portable", EndpointID: "endpoint-portable", Provider: "openai", Family: "responses", Region: "region", AccountRegion: "region", Model: "model-portable", ModelLineage: "lineage-portable", ModelRevision: "revision-portable", Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard"}, Capabilities: testCapabilities(), PriceVersion: "price-v1", PriceAvailable: true},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}
