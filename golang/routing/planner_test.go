package routing

import (
	"context"
	"fmt"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func testCapabilities() CapabilitySet {
	features := map[Feature]Capability{}
	for _, feature := range []Feature{FeatureText, FeatureToolCall, FeatureStructuredOutput, FeatureReasoning, FeatureContinuation} {
		features[feature] = Capability{State: CapabilityNative}
	}
	return CapabilitySet{Version: "cap-v1", Features: features}
}

func TestPlannerClassMajorAndExplicitFallbackOnly(t *testing.T) {
	catalog, err := CompileCatalog("route-v1", map[string]Model{"logical": {Routes: []Route{
		{ID: "r1", EndpointID: "ep1", Provider: "openai", Family: "openai_responses", Model: "model-1", Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "default"}, Capabilities: testCapabilities(), PriceVersion: "price-v1", PriceAvailable: true},
		{ID: "r2", EndpointID: "ep2", Provider: "anthropic", Family: "anthropic_messages", Model: "model-2", Classes: []llm.ServiceClass{llm.ServiceClassPriority, llm.ServiceClassEconomy}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassPriority: "high", llm.ServiceClassEconomy: "low"}, Capabilities: testCapabilities(), PriceVersion: "price-v1", PriceAvailable: true},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	request := llm.Request{OperationKey: "op-1", Model: "logical", ServiceClass: llm.ServiceClassPriority, ServiceClassFallbacks: []llm.ServiceClass{llm.ServiceClassStandard}, Context: llm.RequestContext{Tenant: "tenant"}, Input: []llm.Item{llm.Message{Actor: llm.ActorHuman}}}
	plan, err := (DeterministicPlanner{}).Plan(context.Background(), Input{Request: request, Catalog: catalog})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Candidates) != 2 || plan.Candidates[0].RouteID != "r2" || plan.Candidates[0].AttemptedClass != llm.ServiceClassPriority || plan.Candidates[1].RouteID != "r1" || plan.Candidates[1].AttemptedClass != llm.ServiceClassStandard {
		t.Fatalf("unexpected candidates: %#v", plan.Candidates)
	}
	for _, candidate := range plan.Candidates {
		if candidate.AttemptedClass != llm.ServiceClassPriority && candidate.AttemptedClass != llm.ServiceClassStandard {
			t.Fatalf("planner invented class %q", candidate.AttemptedClass)
		}
	}
}

func TestPlannerCollectsSafeRejections(t *testing.T) {
	catalog, err := CompileCatalog("route-v1", map[string]Model{"logical": {Routes: []Route{
		{ID: "tenant-blocked", EndpointID: "ep", Provider: "openai", Family: "openai_responses", Model: "model", Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "default"}, AllowedTenants: []string{"other"}, Capabilities: testCapabilities(), PriceVersion: "price-v1"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	request := llm.Request{OperationKey: "op-2", Model: "logical", Context: llm.RequestContext{Tenant: "tenant"}}
	_, err = (DeterministicPlanner{}).Plan(context.Background(), Input{Request: request, Catalog: catalog})
	if err == nil {
		t.Fatal("expected no eligible route")
	}
	plan, _ := (DeterministicPlanner{}).Plan(context.Background(), Input{Request: request, Catalog: catalog})
	if len(plan.Rejections) != 1 || plan.Rejections[0].Code != RejectTenant {
		t.Fatalf("unexpected rejection: %#v", plan.Rejections)
	}
}

func TestPlannerRejectsMissingTenantForTenantScopedRoute(t *testing.T) {
	catalog, err := CompileCatalog("route-v1", map[string]Model{"logical": {Routes: []Route{
		{ID: "tenant-scoped", EndpointID: "ep", Provider: "openai", Family: "openai_responses", Model: "model", Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "default"}, AllowedTenants: []string{"tenant-a"}, Capabilities: testCapabilities(), PriceVersion: "price-v1"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	request := llm.Request{OperationKey: "op-tenantless", Model: "logical"}
	plan, err := (DeterministicPlanner{}).Plan(context.Background(), Input{Request: request, Catalog: catalog})
	if err == nil {
		t.Fatal("tenantless request unexpectedly matched tenant-scoped route")
	}
	if len(plan.Rejections) != 1 || plan.Rejections[0].Code != RejectTenant || plan.Rejections[0].Path != "context.tenant" {
		t.Fatalf("unexpected rejection: %#v", plan.Rejections)
	}
}

func TestPlannerDoesNotTrustCallerBudgetedTagForPricePolicy(t *testing.T) {
	catalog, err := CompileCatalog("route-v1", map[string]Model{"logical": {Routes: []Route{{
		ID: "unpriced", EndpointID: "ep", Provider: "openai", Family: "openai_responses", Model: "model",
		Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard"},
		Capabilities: testCapabilities(), PriceAvailable: false,
	}}}})
	if err != nil {
		t.Fatal(err)
	}
	request := llm.Request{OperationKey: "op-unpriced", Model: "logical", Context: llm.RequestContext{Tags: map[string]string{"budgeted": "true"}}}
	plan, err := (DeterministicPlanner{}).Plan(context.Background(), Input{Request: request, Catalog: catalog})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if len(plan.Candidates) != 1 || plan.Candidates[0].RouteID != "unpriced" {
		t.Fatalf("plan candidates = %#v, want unpriced route", plan.Candidates)
	}
}

// A diagnostic cap must never change which routes are eligible.
func TestPlannerValidatesRoutesAfterRejectionLimit(t *testing.T) {
	for _, limit := range []int{1, 2, 256} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			valid := Route{ID: "valid", EndpointID: "ep", Provider: "openai", Family: "responses", Model: "model", Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "default"}, Capabilities: testCapabilities()}
			routes := make([]Route, limit)
			for i := range routes {
				routes[i] = valid
				routes[i].ID = fmt.Sprintf("blocked-%d", i)
				routes[i].AllowedTenants = []string{"another-tenant"}
			}
			invalid := valid
			invalid.ID = "missing-endpoint"
			invalid.EndpointID = ""
			routes = append(routes, invalid, valid)
			input := Input{Request: llm.Request{OperationKey: "op", Model: "logical"}, Catalog: Catalog{Version: "v1", Models: map[string]Model{"logical": {Routes: routes}}}}
			plan, err := (DeterministicPlanner{MaxRejections: limit}).Plan(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Rejections) != limit {
				t.Fatalf("got %d rejections, want %d", len(plan.Rejections), limit)
			}
			if len(plan.Candidates) != 1 || plan.Candidates[0].RouteID != "valid" {
				t.Fatalf("invalid route admitted after diagnostic cap: %#v", plan.Candidates)
			}
			input.Catalog.Models["logical"] = Model{Routes: routes[:len(routes)-1]}
			plan, err = (DeterministicPlanner{MaxRejections: limit}).Plan(context.Background(), input)
			if err == nil || len(plan.Candidates) != 0 {
				t.Fatalf("expected no eligible routes, got %#v, %v", plan.Candidates, err)
			}
		})
	}
}
