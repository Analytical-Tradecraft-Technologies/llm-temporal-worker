package routing

import (
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestPlannerEvaluatesPriceAvailabilityAtPlanTime(t *testing.T) {
	start := time.Date(2026, time.July, 14, 1, 0, 0, 0, time.UTC)
	catalog, err := CompileCatalog("routes-1", map[string]Model{"model": {Routes: []Route{standardRoute("default", Route{
		ID: "route", EndpointID: "endpoint", Provider: "openai", Family: "openai_responses", Region: "us-east-1", AccountRegion: "us-east-1",
		Model: "provider-model", ModelLineage: "lineage", PriceAvailable: false, PricedWindows: []PriceWindow{{From: start}},
	})}}})
	testassert.NoError(t, err)
	request := llm.Request{OperationKey: "op", Model: "model", ServiceClass: llm.ServiceClassStandard, Input: []llm.Item{llm.HumanText("hi")}}
	for now, want := range map[time.Time]bool{start.Add(-time.Minute): false, start.Add(time.Minute): true} {
		if got := mustPlan(t, Input{Request: request, Catalog: catalog, Now: now}).Candidates[0].PriceAvailable; got != want {
			t.Fatalf("PriceAvailable at %s = %t, want %t", now, got, want)
		}
	}
}
