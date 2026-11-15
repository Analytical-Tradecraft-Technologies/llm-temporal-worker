package routing

import "testing"

func TestRoutePlanDeterminismInvariants(t *testing.T) {
	catalog := canonicalizationCatalog(t)
	first := mustPlan(t, Input{Request: canonicalizationRequest([]string{"alpha", "beta"}), Catalog: catalog})
	second := mustPlan(t, Input{Request: canonicalizationRequest([]string{"beta", "alpha"}), Catalog: catalog})
	assertEquivalentPlans(t, "equivalent route plans", first, second)
}
