package routing

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestDeriveProviderCacheKeyIsStableAndDomainSeparated(t *testing.T) {
	var parent [32]byte
	for index := range parent {
		parent[index] = byte(index + 1)
	}
	secret := []byte("provider-cache-test-secret")
	first := mustDeriveProviderCacheKey(t, secret, "tenant-a/project-a", parent, "epoch-1", "lineage-a")
	repeated := mustDeriveProviderCacheKey(t, secret, "tenant-a/project-a", parent, "epoch-1", "lineage-a")
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
		testassert.NoError(t, err)
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
	catalog := mustCompileLogicalCatalog(t, []Route{
		openAIAccountRoute(llm.ServiceClassPriority, "priority-first", "ep-priority", accountA, "model-priority", "lineage-priority", modelRevision),
		openAIAccountRoute(llm.ServiceClassStandard, "standard-first", "ep-standard", accountB, "model-standard", "lineage-standard", modelRevision),
		openAIAccountRoute(llm.ServiceClassEconomy, "economy-first", "ep-economy", accountA, "model-economy", "lineage-economy", modelRevision),
	})
	now := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	request := llm.Request{OperationKey: "affinity", Model: "logical", ServiceClass: llm.ServiceClassPriority, ServiceClassFallbacks: []llm.ServiceClass{llm.ServiceClassStandard, llm.ServiceClassEconomy}, Context: llm.RequestContext{Tenant: "tenant"}}
	plan := mustPlan(t, Input{Request: request, Catalog: catalog, Now: now, Affinity: softAffinity(affinityForRoute("economy-first", "ep-economy", "openai", "openai_responses", "us-east-1", accountA, "lineage-economy", modelRevision, now))})
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
	catalog := twoAccountCatalog(t, accountA, accountB)
	request := standardTenantRequest("affinity")
	preferred := mustPlan(t, Input{Request: request, Catalog: catalog, Now: now, Affinity: softAffinity(secondRouteAffinity(accountB, now))})
	if preferred.Candidates[0].RouteID != "second" {
		t.Fatalf("eligible affinity route was not preferred: %#v", preferred.Candidates)
	}
	// An affinity record for a route the catalog does not authorize cannot
	// create a candidate or replace the configured route list.
	unauthorized := affinityForRoute("not-configured", "ep-nope", "openai", "openai_responses", "us-east-1", accountB, "lineage", "revision", now)
	unchanged := mustPlan(t, Input{Request: request, Catalog: catalog, Now: now, Affinity: softAffinity(unauthorized)})
	if unchanged.Candidates[0].RouteID != "first" || unchanged.Candidates[1].RouteID != "second" {
		t.Fatalf("unauthorized affinity changed route list: %#v", unchanged.Candidates)
	}
}

func TestPreferProviderCacheAffinityHardPinRejectsFallback(t *testing.T) {
	accountA := [32]byte{6}
	accountB := [32]byte{7}
	now := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	catalog := twoAccountCatalog(t, accountA, accountB)
	request := standardTenantRequest("hard-affinity")
	hard := secondRouteAffinity(accountB, now)
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
	catalog := twoAccountCatalog(t, account, account)
	affinity := secondRouteAffinity(account, lastSuccess)
	affinity.ExpiresAt = &expired
	plan := mustPlan(t, Input{Request: standardTenantRequest("expired"), Catalog: catalog, Now: now, Affinity: softAffinity(affinity)})
	if plan.Candidates[0].RouteID != "first" {
		t.Fatalf("expired affinity was used: %#v", plan.Candidates)
	}
}

func TestUnpricedProviderCacheAffinityDoesNotMoveRoute(t *testing.T) {
	account := [32]byte{6}
	now := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	routes := twoAccountRoutes(account, account)
	routes[1].PriceAvailable = false
	catalog := mustCompileLogicalCatalog(t, routes)
	plan := mustPlan(t, Input{Request: standardTenantRequest("unpriced-affinity"), Catalog: catalog, Now: now, Affinity: softAffinity(secondRouteAffinity(account, now))})
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

// secondRouteAffinity is the affinity record for twoAccountRoutes' "second"
// route under account.
func secondRouteAffinity(account [32]byte, now time.Time) ProviderCacheAffinity {
	return affinityForRoute("second", "ep-second", "openai", "openai_responses", "us-east-1", account, "lineage", "revision", now)
}

// softAffinity wraps records as soft affinity preferences.
func softAffinity(records ...ProviderCacheAffinity) *AffinityPreferences {
	return &AffinityPreferences{Soft: ProviderCacheAffinitySet(records)}
}

// standardTenantRequest is a standard-class request for the logical model
// from tenant "tenant".
func standardTenantRequest(operation string) llm.Request {
	return llm.Request{OperationKey: operation, Model: "logical", ServiceClass: llm.ServiceClassStandard, Context: llm.RequestContext{Tenant: "tenant"}}
}

// mustDeriveProviderCacheKey derives a provider cache key, failing the test on
// an error.
func mustDeriveProviderCacheKey(tb testing.TB, secret []byte, scope string, parent [32]byte, epoch, lineage string) [32]byte {
	tb.Helper()
	key, err := DeriveProviderCacheKey(secret, scope, parent, epoch, lineage)
	testassert.NoError(tb, err, "DeriveProviderCacheKey")
	return key
}

// openAIAccountRoute returns a priced us-east-1 OpenAI Responses route bound
// to account that offers only class.
func openAIAccountRoute(class llm.ServiceClass, id, endpoint string, account [32]byte, model, lineage, revision string) Route {
	return pricedRoute(class, Route{ID: id, EndpointID: endpoint, Provider: "openai", Family: "openai_responses", Region: "us-east-1", AccountRegion: "us-east-1", EndpointAccountHMAC: account, Model: model, ModelLineage: lineage, ModelRevision: revision})
}

// twoAccountCatalog compiles one logical model from twoAccountRoutes.
func twoAccountCatalog(t *testing.T, accountA, accountB [32]byte) Catalog {
	t.Helper()
	return mustCompileLogicalCatalog(t, twoAccountRoutes(accountA, accountB))
}

// twoAccountRoutes returns two priced standard-class OpenAI Responses routes,
// "first" and "second", that differ only in their endpoint and account.
func twoAccountRoutes(accountA, accountB [32]byte) []Route {
	return []Route{
		openAIAccountRoute(llm.ServiceClassStandard, "first", "ep-first", accountA, "model", "lineage", "revision"),
		openAIAccountRoute(llm.ServiceClassStandard, "second", "ep-second", accountB, "model", "lineage", "revision"),
	}
}
