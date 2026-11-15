package routing

import (
	"context"
	"fmt"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func testCapabilities() CapabilitySet {
	features := map[Feature]Capability{}
	for _, feature := range []Feature{FeatureText, FeatureToolCall, FeatureStructuredOutput, FeatureReasoning, FeatureContinuation} {
		features[feature] = Capability{State: CapabilityNative}
	}
	return CapabilitySet{Version: "cap-v1", Features: features}
}

// mustCompileLogicalCatalog compiles routes as the "logical" model of a
// route-v1 catalog, failing the test on an error.
func mustCompileLogicalCatalog(t *testing.T, routes []Route) Catalog {
	t.Helper()
	catalog, err := CompileCatalog("route-v1", map[string]Model{"logical": {Routes: routes}})
	testassert.NoError(t, err)
	return catalog
}

// classRoute completes route so it offers only class, at the given provider
// tier, with the shared test capabilities.
func classRoute(class llm.ServiceClass, tier string, route Route) Route {
	route.Classes = []llm.ServiceClass{class}
	route.ProviderTiers = map[llm.ServiceClass]string{class: tier}
	route.Capabilities = testCapabilities()
	return route
}

// standardRoute is classRoute for the standard class.
func standardRoute(tier string, route Route) Route {
	return classRoute(llm.ServiceClassStandard, tier, route)
}

// pricedRoute is classRoute at the provider tier named after class, with the
// price-v1 price available.
func pricedRoute(class llm.ServiceClass, route Route) Route {
	route.PriceVersion, route.PriceAvailable = "price-v1", true
	return classRoute(class, string(class), route)
}

// mustPlan plans input with the default deterministic planner, failing the
// test on an error.
func mustPlan(tb testing.TB, input Input) Plan {
	tb.Helper()
	plan, err := (DeterministicPlanner{}).Plan(context.Background(), input)
	testassert.NoError(tb, err, "Plan()")
	return plan
}

func TestPlannerClassMajorAndExplicitFallbackOnly(t *testing.T) {
	catalog := mustCompileLogicalCatalog(t, []Route{
		standardRoute("default", Route{ID: "r1", EndpointID: "ep1", Provider: "openai", Family: "openai_responses", Model: "model-1", PriceVersion: "price-v1", PriceAvailable: true}),
		{ID: "r2", EndpointID: "ep2", Provider: "anthropic", Family: "anthropic_messages", Model: "model-2", Classes: []llm.ServiceClass{llm.ServiceClassPriority, llm.ServiceClassEconomy}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassPriority: "high", llm.ServiceClassEconomy: "low"}, Capabilities: testCapabilities(), PriceVersion: "price-v1", PriceAvailable: true},
	})
	request := llm.Request{OperationKey: "op-1", Model: "logical", ServiceClass: llm.ServiceClassPriority, ServiceClassFallbacks: []llm.ServiceClass{llm.ServiceClassStandard}, Context: llm.RequestContext{Tenant: "tenant"}, Input: []llm.Item{llm.Message{Actor: llm.ActorHuman}}}
	plan := mustPlan(t, Input{Request: request, Catalog: catalog})
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
	catalog := mustCompileLogicalCatalog(t, []Route{
		standardRoute("default", Route{ID: "tenant-blocked", EndpointID: "ep", Provider: "openai", Family: "openai_responses", Model: "model", AllowedTenants: []string{"other"}, PriceVersion: "price-v1"}),
	})
	request := llm.Request{OperationKey: "op-2", Model: "logical", Context: llm.RequestContext{Tenant: "tenant"}}
	plan, err := (DeterministicPlanner{}).Plan(context.Background(), Input{Request: request, Catalog: catalog})
	if err == nil {
		t.Fatal("expected no eligible route")
	}
	if len(plan.Rejections) != 1 || plan.Rejections[0].Code != RejectTenant {
		t.Fatalf("unexpected rejection: %#v", plan.Rejections)
	}
}

func TestPlannerRejectsMissingTenantForTenantScopedRoute(t *testing.T) {
	catalog := mustCompileLogicalCatalog(t, []Route{
		standardRoute("default", Route{ID: "tenant-scoped", EndpointID: "ep", Provider: "openai", Family: "openai_responses", Model: "model", AllowedTenants: []string{"tenant-a"}, PriceVersion: "price-v1"}),
	})
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
	catalog := mustCompileLogicalCatalog(t, []Route{standardRoute("standard", Route{ID: "unpriced", EndpointID: "ep", Provider: "openai", Family: "openai_responses", Model: "model", PriceAvailable: false})})
	request := llm.Request{OperationKey: "op-unpriced", Model: "logical", Context: llm.RequestContext{Tags: map[string]string{"budgeted": "true"}}}
	plan := mustPlan(t, Input{Request: request, Catalog: catalog})
	if len(plan.Candidates) != 1 || plan.Candidates[0].RouteID != "unpriced" {
		t.Fatalf("plan candidates = %#v, want unpriced route", plan.Candidates)
	}
}

// A diagnostic cap must never change which routes are eligible.
func TestPlannerValidatesRoutesAfterRejectionLimit(t *testing.T) {
	for _, limit := range []int{1, 2, 256} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			valid := standardRoute("default", Route{ID: "valid", EndpointID: "ep", Provider: "openai", Family: "responses", Model: "model"})
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
			testassert.NoError(t, err)
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

// The OpenAI families add a text prefix to failed tool results at lowering, so
// the context check must count it for those routes only.
func TestPlannerContextLimitCountsToolResultErrorPrefix(t *testing.T) {
	request := llm.Request{OperationKey: "op-tool-error", Model: "logical", Context: llm.RequestContext{Tenant: "tenant"}, Input: []llm.Item{
		llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{}`)},
		llm.ToolResult{CallID: "call-1", Content: []llm.Part{llm.TextPart{Text: "timed out"}}, IsError: true},
	}}
	if AdapterTextOverheadBytes(request, "openai_chat") != len(llm.ToolResultErrorTextPrefix)+1 || AdapterTextOverheadBytes(request, "bedrock_converse") != 0 {
		t.Fatal("unexpected tool-result error overhead")
	}
	encoded, err := llm.CanonicalJSON(mustRequestJSON(request))
	testassert.NoError(t, err)
	route := func(id, family string) Route {
		return standardRoute("default", Route{ID: id, EndpointID: id, Provider: id, Family: family, Model: "model", PriceVersion: "price-v1", PriceAvailable: true, ContextBytes: len(encoded) + 30})
	}
	catalog := mustCompileLogicalCatalog(t, []Route{route("openai", "openai_responses"), route("anthropic", "anthropic_messages")})
	plan := mustPlan(t, Input{Request: request, Catalog: catalog})
	if len(plan.Candidates) != 1 || plan.Candidates[0].RouteID != "anthropic" || len(plan.Rejections) != 1 || plan.Rejections[0].Code != RejectContext {
		t.Fatalf("candidates=%#v rejections=%#v", plan.Candidates, plan.Rejections)
	}
}
