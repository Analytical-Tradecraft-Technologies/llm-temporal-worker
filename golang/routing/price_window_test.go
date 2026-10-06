package routing

import (
	"context"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func TestPlannerEvaluatesPriceAvailabilityAtPlanTime(t *testing.T) {
	start := time.Date(2026, time.July, 14, 1, 0, 0, 0, time.UTC)
	catalog, err := CompileCatalog("routes-1", map[string]Model{"model": {Routes: []Route{{
		ID: "route", EndpointID: "endpoint", Provider: "openai", Family: "openai_responses", Region: "us-east-1", AccountRegion: "us-east-1",
		Model: "provider-model", ModelLineage: "lineage", Classes: []llm.ServiceClass{llm.ServiceClassStandard},
		ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "default"}, Capabilities: testCapabilities(),
		PriceAvailable: false, PricedWindows: []PriceWindow{{From: start}},
	}}}})
	if err != nil {
		t.Fatal(err)
	}
	request := llm.Request{OperationKey: "op", Model: "model", ServiceClass: llm.ServiceClassStandard, Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hi"}}}}}
	for now, want := range map[time.Time]bool{start.Add(-time.Minute): false, start.Add(time.Minute): true} {
		plan, err := DeterministicPlanner{}.Plan(context.Background(), Input{Request: request, Catalog: catalog, Now: now})
		if err != nil {
			t.Fatal(err)
		}
		if got := plan.Candidates[0].PriceAvailable; got != want {
			t.Fatalf("PriceAvailable at %s = %t, want %t", now, got, want)
		}
	}
}
