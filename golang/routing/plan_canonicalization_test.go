package routing

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestCompileCatalogKeepsPublicServiceClassVocabularyClosed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Route)
	}{
		{
			name: "route class",
			mutate: func(route *Route) {
				route.Classes = []llm.ServiceClass{"provider_default"}
				route.ProviderTiers = map[llm.ServiceClass]string{"provider_default": "provider-default"}
			},
		},
		{
			name: "provider tier key",
			mutate: func(route *Route) {
				route.ProviderTiers["provider_default"] = "provider-default"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			route := canonicalizationRoute("route", []llm.ServiceClass{llm.ServiceClassStandard})
			test.mutate(&route)
			if _, err := CompileCatalog("route-v1", map[string]Model{"logical": {Routes: []Route{route}}}); err == nil {
				t.Fatal("CompileCatalog accepted a non-public service class")
			}
		})
	}
}

func TestPlannerCanonicalizesEquivalentRequestsIntoByteStablePlans(t *testing.T) {
	catalog := canonicalizationCatalog(t)
	firstRequest := canonicalizationRequest([]string{"alpha", "beta"})
	secondRequest := canonicalizationRequest([]string{"beta", "alpha"})
	secondRequest.Extensions["alpha"] = json.RawMessage(`{"nested":{"a":1,"b":2}}`)
	secondRequest.Extensions["beta"] = json.RawMessage(`{"a":1,"b":2}`)

	planner := DeterministicPlanner{}
	first, err := planner.Plan(context.Background(), Input{Request: firstRequest, Catalog: catalog})
	if err != nil {
		t.Fatal(err)
	}
	second, err := planner.Plan(context.Background(), Input{Request: secondRequest, Catalog: catalog})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("equivalent requests produced different plans:\nfirst:  %#v\nsecond: %#v", first, second)
	}
	if first.Digest != second.Digest || first.DigestHex != second.DigestHex {
		t.Fatalf("equivalent requests changed plan digest: %x/%s != %x/%s", first.Digest, first.DigestHex, second.Digest, second.DigestHex)
	}
	if firstBytes, secondBytes := canonicalPlanJSON(t, first), canonicalPlanJSON(t, second); !bytes.Equal(firstBytes, secondBytes) {
		t.Fatalf("equivalent requests changed canonical plan bytes:\nfirst:  %s\nsecond: %s", firstBytes, secondBytes)
	}

	want := []struct {
		route    string
		class    llm.ServiceClass
		fallback int
	}{
		{route: "priority-standard", class: llm.ServiceClassPriority, fallback: 0},
		{route: "all", class: llm.ServiceClassPriority, fallback: 0},
		{route: "economy", class: llm.ServiceClassEconomy, fallback: 1},
		{route: "all", class: llm.ServiceClassEconomy, fallback: 1},
		{route: "priority-standard", class: llm.ServiceClassStandard, fallback: 2},
		{route: "all", class: llm.ServiceClassStandard, fallback: 2},
	}
	if len(first.Candidates) != len(want) {
		t.Fatalf("candidate count = %d, want %d: %#v", len(first.Candidates), len(want), first.Candidates)
	}
	for index, candidate := range first.Candidates {
		if candidate.RouteID != want[index].route || candidate.AttemptedClass != want[index].class || candidate.FallbackIndex != want[index].fallback {
			t.Fatalf("candidate[%d] = %#v, want route=%q class=%q fallback=%d", index, candidate, want[index].route, want[index].class, want[index].fallback)
		}
		if candidate.RequestedClass != llm.ServiceClassPriority {
			t.Fatalf("candidate[%d] changed requested class to %q", index, candidate.RequestedClass)
		}
	}
}

func TestPlannerDoesNotMoveToAnUnrequestedClass(t *testing.T) {
	request := canonicalizationRequest([]string{"alpha", "beta"})
	request.ServiceClass = llm.ServiceClassStandard
	request.ServiceClassFallbacks = []llm.ServiceClass{llm.ServiceClassPriority}

	plan, err := (DeterministicPlanner{}).Plan(context.Background(), Input{Request: request, Catalog: canonicalizationCatalog(t)})
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range plan.Candidates {
		if candidate.AttemptedClass == llm.ServiceClassEconomy {
			t.Fatalf("planner moved to unrequested economy class: %#v", candidate)
		}
	}
}

func canonicalizationCatalog(tb testing.TB) Catalog {
	tb.Helper()
	catalog, err := CompileCatalog("route-v1", map[string]Model{
		"logical": {Routes: []Route{
			canonicalizationRoute("economy", []llm.ServiceClass{llm.ServiceClassEconomy}),
			canonicalizationRoute("priority-standard", []llm.ServiceClass{llm.ServiceClassPriority, llm.ServiceClassStandard}),
			canonicalizationRoute("all", []llm.ServiceClass{llm.ServiceClassEconomy, llm.ServiceClassStandard, llm.ServiceClassPriority}),
		}},
	})
	if err != nil {
		tb.Fatalf("compile canonicalization catalog: %v", err)
	}
	return catalog
}

func canonicalizationRoute(id string, classes []llm.ServiceClass) Route {
	tiers := make(map[llm.ServiceClass]string, len(classes))
	for _, class := range classes {
		tiers[class] = string(class) + "-tier"
	}
	return Route{
		ID:             id,
		EndpointID:     "endpoint-" + id,
		Provider:       "provider-" + id,
		Family:         "openai_responses",
		Region:         "us-east-1",
		AccountRegion:  "us-east-1",
		Model:          "provider-model-" + id,
		ModelLineage:   "provider-model-" + id,
		Classes:        append([]llm.ServiceClass(nil), classes...),
		ProviderTiers:  tiers,
		AllowedRegions: []string{"us-east-1"},
		Capabilities:   testCapabilities(),
		PriceVersion:   "price-v1",
		PriceAvailable: true,
		ExtensionNames: []string{"alpha", "beta"},
	}
}

func canonicalizationRequest(extensionOrder []string) llm.Request {
	extensions := make(map[string]json.RawMessage, len(extensionOrder))
	for _, name := range extensionOrder {
		switch name {
		case "alpha":
			extensions[name] = json.RawMessage(`{"nested":{"b":2,"a":1}}`)
		case "beta":
			extensions[name] = json.RawMessage(`{"b":2,"a":1}`)
		}
	}
	return llm.Request{
		OperationKey:          "canonicalization-operation",
		Model:                 "logical",
		ServiceClass:          llm.ServiceClassPriority,
		ServiceClassFallbacks: []llm.ServiceClass{llm.ServiceClassEconomy, llm.ServiceClassStandard},
		Context:               llm.RequestContext{Tags: map[string]string{"region": "us-east-1", "request_kind": "canonicalization"}},
		Extensions:            extensions,
	}
}

func canonicalPlanJSON(tb testing.TB, plan Plan) []byte {
	tb.Helper()
	encoded, err := json.Marshal(plan)
	if err != nil {
		tb.Fatalf("marshal plan: %v", err)
	}
	canonical, err := llm.CanonicalJSON(encoded)
	if err != nil {
		tb.Fatalf("canonicalize plan: %v", err)
	}
	return canonical
}
