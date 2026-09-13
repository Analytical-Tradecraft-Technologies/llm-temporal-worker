package durable

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func TestGenerateCostAdmissionRejectsOverCeilingBeforeReserveOrProvider(t *testing.T) {
	request := testGenerateRequest()
	request.Context.Tags = map[string]string{llm.CostAdmissionContextTag: llm.CostAdmissionForecastV1}
	request.CostAdmission = &llm.CostAdmissionV1{
		APIVersion: llm.CostAdmissionAPIVersion, BudgetID: "budget-1", CustomerID: "customer-1", RunID: "run-1",
		OperationKey: request.OperationKey, GatewayAttemptOrdinal: 7, PricingGenerationID: "prices-v1", PricingManifestSHA256: strings.Repeat("a", 64),
		RemainingMaxCostMicrounits: 10000,
	}
	events := []string{}
	ports := testGeneratePorts(&events, "")
	ports.Route = func(context.Context, llm.GenerateRequestV1, GenerateReplay, CompactionDecision) (RoutePlan, error) {
		events = append(events, "route")
		route := testRoutePlan()
		route.PricingGenerationID = request.CostAdmission.PricingGenerationID
		route.PricingManifestSHA256 = request.CostAdmission.PricingManifestSHA256
		route.Execution = &RouteExecution{EstimatedUSD: pricing.MustUSD("0.010001")}
		return route, nil
	}
	if _, err := GenerateV1(context.Background(), request, ports); err == nil {
		t.Fatal("over-ceiling forecast reached admission")
	}
	if want := []string{"replay", "cache", "compaction", "route"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("over-ceiling phases = %v, want %v", events, want)
	}
}

func TestGenerateCostAdmissionCompletedReplayDoesNotReserveOrChargeAgain(t *testing.T) {
	request := testGenerateRequest()
	request.Context.Tags = map[string]string{llm.CostAdmissionContextTag: llm.CostAdmissionForecastV1}
	admission := llm.CostAdmissionV1{
		APIVersion: llm.CostAdmissionAPIVersion, BudgetID: "budget-1", CustomerID: "customer-1", RunID: "run-1",
		OperationKey: request.OperationKey, GatewayAttemptOrdinal: 7, PricingGenerationID: "prices-v1", PricingManifestSHA256: strings.Repeat("a", 64),
		RemainingMaxCostMicrounits: 10000,
		BatchID:                    "escrow-batch", BatchSHA256: strings.Repeat("b", 64),
		GrantMaterializationRequestSHA256: strings.Repeat("e", 64),
		GrantID:                           "grant-1", GrantSHA256: strings.Repeat("c", 64), GrantKeyID: "grant-key", GrantHMACSHA256: strings.Repeat("d", 64),
	}
	request.CostAdmission = &admission
	events := []string{}
	ports := testGeneratePorts(&events, "")
	completed := testFinalization(request).Response
	completed.Cost.CatalogVersion = admission.PricingGenerationID
	completed.CostAdmission = &admission
	encoded, err := json.Marshal(completed)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &completed); err != nil {
		t.Fatal(err)
	}
	ports.Replay = func(context.Context, llm.GenerateRequestV1) (GenerateReplay, error) {
		events = append(events, "replay")
		return GenerateReplay{Completed: &completed}, nil
	}
	response, err := GenerateV1(context.Background(), request, ports)
	if err != nil {
		t.Fatal(err)
	}
	if response.CostAdmission == nil || *response.CostAdmission != admission {
		t.Fatalf("completed receipt changed signed materialization identity: %#v", response.CostAdmission)
	}
	if want := []string{"replay"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("completed replay phases = %v, want %v", events, want)
	}
}

func TestGenerateCostAdmissionCacheHitReturnsBoundExactZero(t *testing.T) {
	request := testGenerateRequest()
	request.Context.Tags = map[string]string{llm.CostAdmissionContextTag: llm.CostAdmissionForecastV1}
	admission := llm.CostAdmissionV1{
		APIVersion: llm.CostAdmissionAPIVersion, BudgetID: "budget-1", CustomerID: "customer-1", RunID: "run-1",
		OperationKey: request.OperationKey, GatewayAttemptOrdinal: 7, PricingGenerationID: "prices-v1", PricingManifestSHA256: strings.Repeat("a", 64),
		RemainingMaxCostMicrounits: 1,
	}
	request.CostAdmission = &admission
	events := []string{}
	ports := testGeneratePorts(&events, "")
	origin := testFinalization(request).Response
	origin.OperationKey, origin.OperationID = "origin-operation", "origin-id"
	ports.CacheLookup = func(context.Context, llm.GenerateRequestV1, GenerateReplay) (CacheDecision, error) {
		events = append(events, "cache")
		return CacheDecision{Disposition: CacheHit, Response: &origin}, nil
	}
	ports.FinalizeCache = func(context.Context, llm.GenerateRequestV1, GenerateReplay, CacheDecision) (GenerateFinalization, error) {
		events = append(events, "cache-finalize")
		result := testFinalization(request)
		result.Response.OperationID = "cache-child-id"
		result.Response.Checkpoint.Handle = "cache-child-checkpoint"
		result.Response.Checkpoint.Kind = "cache_replay"
		result.Response.Cache.Disposition = "hit"
		result.Response.Cost.CatalogVersion = admission.PricingGenerationID
		result.Response.CostAdmission = &admission
		return result, nil
	}
	if _, err := GenerateV1(context.Background(), request, ports); err != nil {
		t.Fatal(err)
	}
	if want := []string{"replay", "cache", "cache-finalize"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("cache hit phases = %v, want %v", events, want)
	}
}

func TestGenerateCostAdmissionRejectsUnknownPricingSnapshotBeforeReserve(t *testing.T) {
	request := testGenerateRequest()
	request.Context.Tags = map[string]string{llm.CostAdmissionContextTag: llm.CostAdmissionForecastV1}
	admission := llm.CostAdmissionV1{
		APIVersion: llm.CostAdmissionAPIVersion, BudgetID: "budget-1", CustomerID: "customer-1", RunID: "run-1",
		OperationKey: request.OperationKey, GatewayAttemptOrdinal: 7, PricingGenerationID: "prices-v1", PricingManifestSHA256: strings.Repeat("a", 64),
		RemainingMaxCostMicrounits: 1000000,
	}
	request.CostAdmission = &admission
	events := []string{}
	ports := testGeneratePorts(&events, "")
	ports.Route = func(context.Context, llm.GenerateRequestV1, GenerateReplay, CompactionDecision) (RoutePlan, error) {
		events = append(events, "route")
		route := testRoutePlan()
		route.PricingGenerationID = "unknown-prices"
		route.PricingManifestSHA256 = strings.Repeat("b", 64)
		route.Execution = &RouteExecution{EstimatedUSD: pricing.MustUSD("0")}
		return route, nil
	}
	if _, err := GenerateV1(context.Background(), request, ports); err == nil {
		t.Fatal("unknown pricing snapshot reached reservation")
	}
	if want := []string{"replay", "cache", "compaction", "route"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("unknown-price phases = %v, want %v", events, want)
	}
}
