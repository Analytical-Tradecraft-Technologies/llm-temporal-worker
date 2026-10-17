package routing

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func classPolicyCatalog(t *testing.T, policies ...ClassPolicy) Catalog {
	t.Helper()
	all := []llm.ServiceClass{llm.ServiceClassEconomy, llm.ServiceClassStandard, llm.ServiceClassPriority}
	catalog, err := CompileCatalog("route-v1", map[string]Model{
		"tiered": {Routes: []Route{{ID: "openai", EndpointID: "openai", Provider: "openai", Family: "openai_responses", Model: "gpt", Classes: all,
			ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassEconomy: "flex", llm.ServiceClassStandard: "default", llm.ServiceClassPriority: "priority"}, Capabilities: testCapabilities(), PriceVersion: "p"}}},
		"standard-only": {Routes: []Route{{ID: "anthropic", EndpointID: "anthropic", Provider: "anthropic", Family: "anthropic_messages", Model: "claude", Classes: []llm.ServiceClass{llm.ServiceClassStandard},
			ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard_only"}, Capabilities: testCapabilities(), PriceVersion: "p"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if catalog, err = catalog.WithClassPolicies(policies); err != nil {
		t.Fatal(err)
	}
	return catalog
}

// A class the project disables, or the model cannot serve, is served at
// standard instead of failing the request.
func TestPlannerAdjustsDisabledAndUnsupportedClasses(t *testing.T) {
	noFast := ClassPolicy{ID: "no-fast", Tenant: "acme", Classes: []llm.ServiceClass{llm.ServiceClassEconomy, llm.ServiceClassStandard}}
	noEconomy := ClassPolicy{ID: "no-economy", Project: "billing", Classes: []llm.ServiceClass{llm.ServiceClassStandard, llm.ServiceClassPriority}}
	catalog := classPolicyCatalog(t, noFast, noEconomy)
	for _, tc := range []struct {
		name      string
		model     string
		context   llm.RequestContext
		class     llm.ServiceClass
		fallbacks []llm.ServiceClass
		want      []llm.ServiceClass
	}{
		{name: "omitted class is standard", model: "tiered", want: []llm.ServiceClass{llm.ServiceClassStandard}},
		{name: "priority served", model: "tiered", class: llm.ServiceClassPriority, want: []llm.ServiceClass{llm.ServiceClassPriority}},
		{name: "economy served", model: "tiered", class: llm.ServiceClassEconomy, want: []llm.ServiceClass{llm.ServiceClassEconomy}},
		{name: "disabled priority downgrades", model: "tiered", context: llm.RequestContext{Tenant: "acme"}, class: llm.ServiceClassPriority, want: []llm.ServiceClass{llm.ServiceClassStandard}},
		{name: "disabled economy upgrades", model: "tiered", context: llm.RequestContext{Tenant: "other", Project: "billing"}, class: llm.ServiceClassEconomy, want: []llm.ServiceClass{llm.ServiceClassStandard}},
		{name: "every matching policy applies", model: "tiered", context: llm.RequestContext{Tenant: "acme", Project: "billing"}, class: llm.ServiceClassEconomy, want: []llm.ServiceClass{llm.ServiceClassStandard}},
		{name: "other tenant keeps priority", model: "tiered", context: llm.RequestContext{Tenant: "other"}, class: llm.ServiceClassPriority, want: []llm.ServiceClass{llm.ServiceClassPriority}},
		{name: "unsupported priority ignored", model: "standard-only", class: llm.ServiceClassPriority, want: []llm.ServiceClass{llm.ServiceClassStandard}},
		{name: "unsupported economy ignored", model: "standard-only", class: llm.ServiceClassEconomy, want: []llm.ServiceClass{llm.ServiceClassStandard}},
		{name: "adjusted fallbacks deduplicate", model: "tiered", context: llm.RequestContext{Tenant: "acme"}, class: llm.ServiceClassPriority, fallbacks: []llm.ServiceClass{llm.ServiceClassStandard, llm.ServiceClassEconomy}, want: []llm.ServiceClass{llm.ServiceClassStandard, llm.ServiceClassEconomy}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := llm.Request{OperationKey: "op", Model: tc.model, ServiceClass: tc.class, ServiceClassFallbacks: tc.fallbacks, Context: tc.context, Input: []llm.Item{llm.Message{Actor: llm.ActorHuman}}}
			plan, err := (DeterministicPlanner{}).Plan(context.Background(), Input{Request: request, Catalog: catalog})
			if err != nil {
				t.Fatal(err)
			}
			got := make([]llm.ServiceClass, 0, len(plan.Candidates))
			requested, _ := llm.NormalizeServiceClass(tc.class)
			for index, candidate := range plan.Candidates {
				got = append(got, candidate.AttemptedClass)
				if candidate.RequestedClass != requested || candidate.FallbackIndex != index {
					t.Fatalf("candidate %d = %#v, want requested %s at fallback %d", index, candidate, requested, index)
				}
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("attempted classes = %v, want %v", got, tc.want)
			}
			classes, err := catalog.ServiceClasses(request)
			if err != nil || !slices.Equal(classes, tc.want) {
				t.Fatalf("ServiceClasses = %v, %v; want %v", classes, err, tc.want)
			}
		})
	}
}

func TestClassPoliciesMustKeepStandard(t *testing.T) {
	for _, tc := range []struct {
		policies []ClassPolicy
		want     string
	}{
		{policies: []ClassPolicy{{ID: "fast-and-economy", Classes: []llm.ServiceClass{llm.ServiceClassEconomy, llm.ServiceClassPriority}}}, want: "must enable standard"},
		{policies: []ClassPolicy{{ID: "empty"}}, want: "must enable standard"},
		{policies: []ClassPolicy{{ID: "bad", Classes: []llm.ServiceClass{llm.ServiceClassStandard, "turbo"}}}, want: "unknown class"},
		{policies: []ClassPolicy{{ID: "twice", Classes: []llm.ServiceClass{llm.ServiceClassStandard, llm.ServiceClassStandard}}}, want: "repeats"},
		{policies: []ClassPolicy{{Classes: []llm.ServiceClass{llm.ServiceClassStandard}}}, want: "ID is required"},
		{policies: []ClassPolicy{{ID: "a", Classes: []llm.ServiceClass{llm.ServiceClassStandard}}, {ID: "a", Classes: []llm.ServiceClass{llm.ServiceClassStandard}}}, want: "duplicate"},
	} {
		if _, err := (Catalog{Version: "v"}).WithClassPolicies(tc.policies); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("WithClassPolicies(%+v) = %v, want %q", tc.policies, err, tc.want)
		}
	}
}
