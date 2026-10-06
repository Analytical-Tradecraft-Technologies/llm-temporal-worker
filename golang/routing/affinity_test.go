package routing

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestDeriveProviderCacheKeyIsStableAndDomainSeparated(t *testing.T) {
	var parent [32]byte
	for index := range parent {
		parent[index] = byte(index + 1)
	}
	secret := []byte("provider-cache-test-secret")
	first, err := DeriveProviderCacheKey(secret, "tenant-a/project-a", parent, "epoch-1", "lineage-a")
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := DeriveProviderCacheKey(secret, "tenant-a/project-a", parent, "epoch-1", "lineage-a")
	if err != nil {
		t.Fatal(err)
	}
	if first != repeated {
		t.Fatalf("same parent produced different cache keys: %x != %x", first, repeated)
	}
	for name, mutate := range map[string]func(string, [32]byte, string, string) ([32]byte, error){
		"tenant": func(_ string, digest [32]byte, epoch, lineage string) ([32]byte, error) {
			return DeriveProviderCacheKey(secret, "tenant-b/project-a", digest, epoch, lineage)
		},
		"parent": func(scope string, digest [32]byte, epoch, lineage string) ([32]byte, error) {
			digest[0]++
			return DeriveProviderCacheKey(secret, scope, digest, epoch, lineage)
		},
		"epoch": func(scope string, digest [32]byte, _, lineage string) ([32]byte, error) {
			return DeriveProviderCacheKey(secret, scope, digest, "epoch-2", lineage)
		},
		"lineage": func(scope string, digest [32]byte, epoch, _ string) ([32]byte, error) {
			return DeriveProviderCacheKey(secret, scope, digest, epoch, "lineage-b")
		},
	} {
		mutated, err := mutate("tenant-a/project-a", parent, "epoch-1", "lineage-a")
		if err != nil {
			t.Fatal(err)
		}
		if mutated == first {
			t.Fatalf("%s did not domain-separate cache key", name)
		}
	}
	hexKey := hex.EncodeToString(first[:])
	for _, raw := range []string{"tenant-a", "project-a", "lineage-a", "epoch-1"} {
		if bytes.Contains([]byte(hexKey), []byte(raw)) {
			t.Fatalf("cache key hex contains raw identity %q: %s", raw, hexKey)
		}
	}
}

func TestDeriveProviderCacheKeyRejectsWhitespaceTenantScope(t *testing.T) {
	var parent [32]byte
	parent[0] = 1
	for _, scope := range []string{"", " ", "\t\n"} {
		if _, err := DeriveProviderCacheKey([]byte("secret"), scope, parent, "epoch", "lineage"); err == nil {
			t.Fatalf("tenant scope %q was accepted", scope)
		}
	}
}

func TestPreferProviderCacheAffinityStaysWithinEligibleClass(t *testing.T) {
	accountA := [32]byte{1}
	accountB := [32]byte{2}
	modelRevision := "revision-1"
	catalog, err := CompileCatalog("route-v1", map[string]Model{"logical": {Routes: []Route{
		{
			ID: "priority-first", EndpointID: "ep-priority", Provider: "openai", Family: "openai_responses", Region: "us-east-1", AccountRegion: "us-east-1", EndpointAccountHMAC: accountA,
			Model: "model-priority", ModelLineage: "lineage-priority", ModelRevision: modelRevision, Classes: []llm.ServiceClass{llm.ServiceClassPriority}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassPriority: "priority"}, Capabilities: testCapabilities(), PriceVersion: "price-v1", PriceAvailable: true,
		},
		{
			ID: "standard-first", EndpointID: "ep-standard", Provider: "openai", Family: "openai_responses", Region: "us-east-1", AccountRegion: "us-east-1", EndpointAccountHMAC: accountB,
			Model: "model-standard", ModelLineage: "lineage-standard", ModelRevision: modelRevision, Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard"}, Capabilities: testCapabilities(), PriceVersion: "price-v1", PriceAvailable: true,
		},
		{
			ID: "economy-first", EndpointID: "ep-economy", Provider: "openai", Family: "openai_responses", Region: "us-east-1", AccountRegion: "us-east-1", EndpointAccountHMAC: accountA,
			Model: "model-economy", ModelLineage: "lineage-economy", ModelRevision: modelRevision, Classes: []llm.ServiceClass{llm.ServiceClassEconomy}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassEconomy: "economy"}, Capabilities: testCapabilities(), PriceVersion: "price-v1", PriceAvailable: true,
		},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	request := llm.Request{OperationKey: "affinity", Model: "logical", ServiceClass: llm.ServiceClassPriority, ServiceClassFallbacks: []llm.ServiceClass{llm.ServiceClassStandard, llm.ServiceClassEconomy}, Context: llm.RequestContext{Tenant: "tenant"}}
	plan, err := (DeterministicPlanner{}).Plan(context.Background(), Input{Request: request, Catalog: catalog, Now: now, Affinity: &AffinityPreferences{Soft: ProviderCacheAffinitySet{affinityForRoute("economy-first", "ep-economy", "openai", "openai_responses", "us-east-1", accountA, "lineage-economy", modelRevision, now)}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Candidates) != 3 {
		t.Fatalf("candidate count = %d, want 3: %#v", len(plan.Candidates), plan.Candidates)
	}
	if plan.Candidates[0].RouteID != "priority-first" || plan.Candidates[1].RouteID != "standard-first" || plan.Candidates[2].RouteID != "economy-first" {
		t.Fatalf("affinity crossed class boundary: %#v", plan.Candidates)
	}
}

func TestPreferProviderCacheAffinityMovesExactRouteWithoutAuthorizingIt(t *testing.T) {
	accountA := [32]byte{3}
	accountB := [32]byte{4}
	now := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	catalog, err := CompileCatalog("route-v1", map[string]Model{"logical": {Routes: []Route{
		{ID: "first", EndpointID: "ep-first", Provider: "openai", Family: "openai_responses", Region: "us-east-1", AccountRegion: "us-east-1", EndpointAccountHMAC: accountA, Model: "model", ModelLineage: "lineage", ModelRevision: "revision", Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard"}, Capabilities: testCapabilities(), PriceVersion: "price-v1", PriceAvailable: true},
		{ID: "second", EndpointID: "ep-second", Provider: "openai", Family: "openai_responses", Region: "us-east-1", AccountRegion: "us-east-1", EndpointAccountHMAC: accountB, Model: "model", ModelLineage: "lineage", ModelRevision: "revision", Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard"}, Capabilities: testCapabilities(), PriceVersion: "price-v1", PriceAvailable: true},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	request := llm.Request{OperationKey: "affinity", Model: "logical", ServiceClass: llm.ServiceClassStandard, Context: llm.RequestContext{Tenant: "tenant"}}
	preferred, err := (DeterministicPlanner{}).Plan(context.Background(), Input{Request: request, Catalog: catalog, Now: now, Affinity: &AffinityPreferences{Soft: ProviderCacheAffinitySet{affinityForRoute("second", "ep-second", "openai", "openai_responses", "us-east-1", accountB, "lineage", "revision", now)}}})
	if err != nil {
		t.Fatal(err)
	}
	if preferred.Candidates[0].RouteID != "second" {
		t.Fatalf("eligible affinity route was not preferred: %#v", preferred.Candidates)
	}
	// An affinity record for a route the catalog does not authorize cannot
	// create a candidate or replace the configured route list.
	unauthorized := ProviderCacheAffinitySet{affinityForRoute("not-configured", "ep-nope", "openai", "openai_responses", "us-east-1", accountB, "lineage", "revision", now)}
	unchanged, err := (DeterministicPlanner{}).Plan(context.Background(), Input{Request: request, Catalog: catalog, Now: now, Affinity: &AffinityPreferences{Soft: unauthorized}})
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Candidates[0].RouteID != "first" || unchanged.Candidates[1].RouteID != "second" {
		t.Fatalf("unauthorized affinity changed route list: %#v", unchanged.Candidates)
	}
}

func TestPreferProviderCacheAffinityHardPinRejectsFallback(t *testing.T) {
	accountA := [32]byte{6}
	accountB := [32]byte{7}
	now := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	catalog, err := CompileCatalog("route-v1", map[string]Model{"logical": {Routes: []Route{
		{ID: "first", EndpointID: "ep-first", Provider: "openai", Family: "openai_responses", Region: "us-east-1", AccountRegion: "us-east-1", EndpointAccountHMAC: accountA, Model: "model", ModelLineage: "lineage", ModelRevision: "revision", Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard"}, Capabilities: testCapabilities(), PriceVersion: "price-v1", PriceAvailable: true},
		{ID: "second", EndpointID: "ep-second", Provider: "openai", Family: "openai_responses", Region: "us-east-1", AccountRegion: "us-east-1", EndpointAccountHMAC: accountB, Model: "model", ModelLineage: "lineage", ModelRevision: "revision", Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard"}, Capabilities: testCapabilities(), PriceVersion: "price-v1", PriceAvailable: true},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	request := llm.Request{OperationKey: "hard-affinity", Model: "logical", ServiceClass: llm.ServiceClassStandard, Context: llm.RequestContext{Tenant: "tenant"}}
	hard := affinityForRoute("second", "ep-second", "openai", "openai_responses", "us-east-1", accountB, "lineage", "revision", now)
	hard.HardPinned = true
	plan, err := (DeterministicPlanner{}).Plan(context.Background(), Input{Request: request, Catalog: catalog, Now: now, Affinity: &AffinityPreferences{HardPin: &hard}})
	if err != nil || len(plan.Candidates) != 1 || plan.Candidates[0].RouteID != "second" {
		t.Fatalf("hard pin plan = %#v, err=%v", plan, err)
	}
	missing := hard
	missing.RouteID = "not-configured"
	missing.EndpointID = "ep-nope"
	plan, err = (DeterministicPlanner{}).Plan(context.Background(), Input{Request: request, Catalog: catalog, Now: now, Affinity: &AffinityPreferences{HardPin: &missing}})
	if err == nil || len(plan.Candidates) != 0 {
		t.Fatalf("missing hard pin plan = %#v, err=%v; want pin error", plan, err)
	}
}

func TestExpiredProviderCacheAffinityIsIgnored(t *testing.T) {
	account := [32]byte{5}
	now := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	expired := now.Add(-time.Minute)
	lastSuccess := now.Add(-2 * time.Minute)
	catalog, err := CompileCatalog("route-v1", map[string]Model{"logical": {Routes: []Route{
		{ID: "first", EndpointID: "ep-first", Provider: "openai", Family: "openai_responses", Region: "us-east-1", AccountRegion: "us-east-1", EndpointAccountHMAC: account, Model: "model", ModelLineage: "lineage", ModelRevision: "revision", Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard"}, Capabilities: testCapabilities(), PriceVersion: "price-v1", PriceAvailable: true},
		{ID: "second", EndpointID: "ep-second", Provider: "openai", Family: "openai_responses", Region: "us-east-1", AccountRegion: "us-east-1", EndpointAccountHMAC: account, Model: "model", ModelLineage: "lineage", ModelRevision: "revision", Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard"}, Capabilities: testCapabilities(), PriceVersion: "price-v1", PriceAvailable: true},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	affinity := affinityForRoute("second", "ep-second", "openai", "openai_responses", "us-east-1", account, "lineage", "revision", lastSuccess)
	affinity.ExpiresAt = &expired
	plan, err := (DeterministicPlanner{}).Plan(context.Background(), Input{Request: llm.Request{OperationKey: "expired", Model: "logical", ServiceClass: llm.ServiceClassStandard, Context: llm.RequestContext{Tenant: "tenant"}}, Catalog: catalog, Now: now, Affinity: &AffinityPreferences{Soft: ProviderCacheAffinitySet{affinity}}})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Candidates[0].RouteID != "first" {
		t.Fatalf("expired affinity was used: %#v", plan.Candidates)
	}
}

func TestUnpricedProviderCacheAffinityDoesNotMoveRoute(t *testing.T) {
	account := [32]byte{6}
	now := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	catalog, err := CompileCatalog("route-v1", map[string]Model{"logical": {Routes: []Route{
		{ID: "first", EndpointID: "ep-first", Provider: "openai", Family: "openai_responses", Region: "us-east-1", AccountRegion: "us-east-1", EndpointAccountHMAC: account, Model: "model", ModelLineage: "lineage", ModelRevision: "revision", Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard"}, Capabilities: testCapabilities(), PriceVersion: "price-v1", PriceAvailable: true},
		{ID: "second", EndpointID: "ep-second", Provider: "openai", Family: "openai_responses", Region: "us-east-1", AccountRegion: "us-east-1", EndpointAccountHMAC: account, Model: "model", ModelLineage: "lineage", ModelRevision: "revision", Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard"}, Capabilities: testCapabilities(), PriceVersion: "price-v1", PriceAvailable: false},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	request := llm.Request{OperationKey: "unpriced-affinity", Model: "logical", ServiceClass: llm.ServiceClassStandard, Context: llm.RequestContext{Tenant: "tenant"}}
	plan, err := (DeterministicPlanner{}).Plan(context.Background(), Input{Request: request, Catalog: catalog, Now: now, Affinity: &AffinityPreferences{Soft: ProviderCacheAffinitySet{affinityForRoute("second", "ep-second", "openai", "openai_responses", "us-east-1", account, "lineage", "revision", now)}}})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Candidates[0].RouteID != "first" {
		t.Fatalf("unpriced affinity moved ahead of priced route: %#v", plan.Candidates)
	}
}

func affinityForRoute(routeID, endpoint, providerName, family, region string, account [32]byte, lineage, revision string, now time.Time) ProviderCacheAffinity {
	return ProviderCacheAffinity{
		Rank: 0, Provider: providerName, RouteID: routeID, EndpointID: endpoint, EndpointAccountHMAC: account,
		Region: region, EndpointFamily: family, ModelLineage: lineage, RouteModelRevision: revision, CacheEpoch: "epoch-1",
		LastSuccessAt: now.Add(-time.Minute),
	}
}
