package routing

import (
	"context"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"testing"
)

func TestPlannerEnforcesPerRouteOutputLimits(t *testing.T) {
	for _, test := range []struct {
		name  string
		limit *int
		want  int
	}{
		{"missing", nil, 0}, {"zero", outputLimitPointer(0), 0},
		{"below", outputLimitPointer(99), 2}, {"boundary", outputLimitPointer(100), 2},
		{"fallback", outputLimitPointer(101), 1}, {"large boundary", outputLimitPointer(200), 1},
		{"too large", outputLimitPointer(201), 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			route := Route{ID: "small", EndpointID: "ep", Provider: "openai", Family: "responses", Model: "model", Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "default"}, Capabilities: testCapabilities(), OutputTokens: 100}
			larger := route
			larger.ID = "large"
			larger.OutputTokens = 200
			catalog, err := CompileCatalog("v1", map[string]Model{"logical": {Routes: []Route{route, larger}}})
			if err != nil {
				t.Fatal(err)
			}
			request := llm.Request{OperationKey: "op", Model: "logical", Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindText}, MaxTokens: test.limit}}
			originalLimit := 0
			if test.limit != nil {
				originalLimit = *test.limit
			}
			plan, err := (DeterministicPlanner{}).Plan(context.Background(), Input{Request: request, Catalog: catalog})
			if (err != nil) != (test.want == 0) || len(plan.Candidates) != test.want {
				t.Fatalf("got %d candidates, %v; want %d", len(plan.Candidates), err, test.want)
			}
			if test.want == 1 && plan.Candidates[0].RouteID != "large" {
				t.Fatalf("wrong route: %#v", plan.Candidates)
			}
			if test.limit != nil && *request.Output.MaxTokens != originalLimit {
				t.Fatal("output cap was silently changed")
			}
		})
	}
}

func TestUnspecifiedRouteOutputLimitPreservesEligibility(t *testing.T) {
	if !(Route{}).SupportsOutputLimit(llm.Request{}) {
		t.Fatal("unspecified limit rejected request")
	}
	if err := validateRouteShape(Route{OutputTokens: -1}); err == nil {
		t.Fatal("negative limit accepted")
	}
}
func outputLimitPointer(value int) *int { return &value }
