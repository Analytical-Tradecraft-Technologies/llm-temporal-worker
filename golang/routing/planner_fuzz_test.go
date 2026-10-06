package routing

import (
	"context"
	"reflect"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// FuzzPlannerNeverSelectsUnauthorizedClasses exercises the class-major
// expansion with arbitrary route ordering and fallback lists. Every generated
// byte is reduced to one of the three public classes before normalization, so
// the property tests planner behavior rather than the request validator.
func FuzzPlannerNeverSelectsUnauthorizedClasses(f *testing.F) {
	f.Add([]byte{0, 1, 2}, byte(1), []byte{2, 0})
	f.Add([]byte{2, 2, 0, 1}, byte(2), []byte{1, 1, 0})
	f.Fuzz(func(t *testing.T, routeBytes []byte, requestedByte byte, fallbackBytes []byte) {
		if len(routeBytes) == 0 || len(routeBytes) > 32 || len(fallbackBytes) > 32 {
			t.Skip()
		}
		requested := fuzzClass(requestedByte)
		fallbacks := make([]llm.ServiceClass, 0, 2)
		seen := map[llm.ServiceClass]struct{}{requested: {}}
		for _, value := range fallbackBytes {
			class := fuzzClass(value)
			if _, exists := seen[class]; exists {
				continue
			}
			seen[class] = struct{}{}
			fallbacks = append(fallbacks, class)
		}

		routes := make([]Route, len(routeBytes))
		configured := make(map[string]Route, len(routeBytes))
		for index, value := range routeBytes {
			class := fuzzClass(value)
			route := Route{
				ID: "route-" + string(rune('a'+index)), EndpointID: "endpoint-" + string(rune('a'+index)),
				Provider: "provider", Family: "family", Region: "region", AccountRegion: "region",
				Model: "model-" + string(rune('a'+index)), ModelLineage: "lineage-" + string(rune('a'+index)),
				Classes: []llm.ServiceClass{class}, ProviderTiers: map[llm.ServiceClass]string{class: string(class) + "-tier"},
				Capabilities: testCapabilities(), PriceVersion: "price-v1", PriceAvailable: true,
			}
			routes[index] = route
			configured[route.ID] = route
		}
		catalog, err := CompileCatalog("catalog-v1", map[string]Model{"logical": {Routes: routes}})
		if err != nil {
			t.Fatalf("CompileCatalog() = %v", err)
		}
		request := llm.Request{
			OperationKey: "fuzz-operation", Model: "logical", ServiceClass: requested,
			ServiceClassFallbacks: fallbacks,
			Context:               llm.RequestContext{Tenant: "tenant"},
			Input:                 []llm.Item{llm.Message{Actor: llm.ActorHuman}},
		}
		planner := DeterministicPlanner{}
		plan, err := planner.Plan(context.Background(), Input{Request: request, Catalog: catalog})
		if err != nil && len(plan.Candidates) != 0 {
			t.Fatalf("failed plan returned candidates: %#v (%v)", plan.Candidates, err)
		}
		allowedClasses := map[llm.ServiceClass]struct{}{requested: {}}
		for _, class := range fallbacks {
			allowedClasses[class] = struct{}{}
		}
		for index, candidate := range plan.Candidates {
			route, exists := configured[candidate.RouteID]
			if !exists {
				t.Fatalf("candidate %d selected route %q absent from catalog", index, candidate.RouteID)
			}
			if _, allowed := allowedClasses[candidate.AttemptedClass]; !allowed {
				t.Fatalf("candidate %d selected unauthorized class %q", index, candidate.AttemptedClass)
			}
			if candidate.RouteIndex < 0 || candidate.RouteIndex >= len(routes) || routes[candidate.RouteIndex].ID != route.ID {
				t.Fatalf("candidate %d route index = %d does not identify %q", index, candidate.RouteIndex, route.ID)
			}
			if candidate.FallbackIndex < 0 || candidate.FallbackIndex >= len(fallbacks)+1 {
				t.Fatalf("candidate %d fallback index = %d out of range", index, candidate.FallbackIndex)
			}
			if candidate.ID == "" {
				t.Fatalf("candidate %d has an empty identity", index)
			}
		}
		if len(plan.Candidates) > 0 {
			repeated, repeatErr := planner.Plan(context.Background(), Input{Request: request, Catalog: catalog})
			if repeatErr != nil || !reflect.DeepEqual(plan, repeated) {
				t.Fatalf("planner was not deterministic: first=%#v/%v second=%#v/%v", plan, err, repeated, repeatErr)
			}
		}
	})
}

func fuzzClass(value byte) llm.ServiceClass {
	switch value % 3 {
	case 0:
		return llm.ServiceClassEconomy
	case 1:
		return llm.ServiceClassStandard
	default:
		return llm.ServiceClassPriority
	}
}
