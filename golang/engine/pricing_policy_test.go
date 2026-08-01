package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

func TestGenerateSkipsBudgetedUnpricedCandidateAndUsesPricedFallback(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
	harness := newHarness(t, adapter)
	snapshot := harness.engine.dependencies.Snapshots.(StaticSnapshot).Value
	snapshot.Prices = pricing.NewResolver(testPriceCatalog(t, priceEntryForTier("standard-tier")))
	snapshot.BudgetPolicies = []budget.Policy{{
		ID:      "priority-only",
		Match:   budget.Matcher{Tenant: "tenant-1", ServiceClass: llm.ServiceClassPriority},
		Windows: []budget.Window{{ID: "priority-only/hour", Duration: time.Hour, Bucket: time.Minute, Limit: 1_000}},
	}}
	harness.engine.dependencies.Snapshots = StaticSnapshot{Value: snapshot}

	request := baseRequest("skip-budgeted-unpriced")
	request.ServiceClass = llm.ServiceClassPriority
	request.ServiceClassFallbacks = []llm.ServiceClass{llm.ServiceClassStandard}
	response, err := harness.engine.Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Service.Attempted != llm.ServiceClassStandard || response.Cost.Status != llm.CostStatusKnown {
		t.Fatalf("response = %#v, want priced standard fallback", response)
	}
	adapter.mu.Lock()
	calls := append([]provider.Call(nil), adapter.calls...)
	adapter.mu.Unlock()
	if len(calls) != 1 || calls[0].ServiceClass != llm.ServiceClassStandard {
		t.Fatalf("provider calls = %#v, want only priced standard fallback", calls)
	}
}

func TestGenerateAllowsUnbudgetedUnpricedCandidateWithUnknownCost(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
	harness := newHarness(t, adapter)
	snapshot := harness.engine.dependencies.Snapshots.(StaticSnapshot).Value
	snapshot.Prices = pricing.NewResolver(testPriceCatalog(t, priceEntryForTier("standard-tier")))
	snapshot.RequirePriceWhenBudgeted = true
	harness.engine.dependencies.Snapshots = StaticSnapshot{Value: snapshot}

	request := baseRequest("allow-unbudgeted-unpriced")
	request.ServiceClass = llm.ServiceClassPriority
	response, err := harness.engine.Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Cost.Status != llm.CostStatusUnknown || response.Cost.ReservedCostUSD != nil || response.Cost.ActualCostUSD != nil || response.Cost.Method != "" || response.Cost.CatalogVersion != "" {
		t.Fatalf("unpriced response cost = %#v, want unknown zero-value monetary facts", response.Cost)
	}
	operation, err := harness.admission.Get(context.Background(), response.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if operation.ReservedMicroUSD != 0 || operation.IncurredMicroUSD != 0 || operation.PriceVersion != "" {
		t.Fatalf("unpriced operation = %#v, want zero unknown-price accounting", operation)
	}
}

func TestGenerateSkipsActivePartialPriceAndUsesCompleteFallback(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
	harness := newHarness(t, adapter)
	snapshot := harness.engine.dependencies.Snapshots.(StaticSnapshot).Value
	partial := priceEntryForTier("priority-tier")
	partial.UnknownComponents = []pricing.PriceComponent{pricing.PriceComponentInput}
	snapshot.Prices = pricing.NewResolver(testPriceCatalog(t, partial, priceEntryForTier("standard-tier")))
	harness.engine.dependencies.Snapshots = StaticSnapshot{Value: snapshot}

	request := baseRequest("skip-active-partial-price")
	request.ServiceClass = llm.ServiceClassPriority
	request.ServiceClassFallbacks = []llm.ServiceClass{llm.ServiceClassStandard}
	response, err := harness.engine.Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Service.Attempted != llm.ServiceClassStandard || response.Cost.Status != llm.CostStatusKnown {
		t.Fatalf("response = %#v, want complete standard fallback", response)
	}
	adapter.mu.Lock()
	calls := append([]provider.Call(nil), adapter.calls...)
	adapter.mu.Unlock()
	if len(calls) != 1 || calls[0].ServiceClass != llm.ServiceClassStandard {
		t.Fatalf("provider calls = %#v, want only standard fallback", calls)
	}
}

func TestGenerateReturnsNoRouteWhenAllCandidatePricesAreUnusable(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
	harness := newHarness(t, adapter)
	snapshot := harness.engine.dependencies.Snapshots.(StaticSnapshot).Value
	partial := priceEntryForTier("priority-tier")
	partial.UnknownComponents = []pricing.PriceComponent{pricing.PriceComponentInput}
	overflow := priceEntryForTier("standard-tier")
	// The exact USD value is valid, but the legacy Redis microUSD projection
	// cannot represent it. The candidate must not abort planning for the route
	// that follows it, nor can it be admitted itself.
	overflow.Prices.PerRequest = pricing.MustDecimalUSD("99999999999999999999")
	snapshot.Prices = pricing.NewResolver(testPriceCatalog(t, partial, overflow))
	harness.engine.dependencies.Snapshots = StaticSnapshot{Value: snapshot}

	request := baseRequest("all-unusable-prices")
	request.ServiceClass = llm.ServiceClassPriority
	request.ServiceClassFallbacks = []llm.ServiceClass{llm.ServiceClassStandard}
	_, err := harness.engine.Generate(context.Background(), request)
	if err == nil {
		t.Fatal("all unusable candidates unexpectedly dispatched")
	}
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeNoRoute || mapped.Phase != provider.PhasePrice || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever {
		t.Fatalf("error = %#v, want no-route price failure", err)
	}
	if mapped.SafeDetails["reason"] != "no_eligible_price" {
		t.Fatalf("safe details = %#v, want no_eligible_price", mapped.SafeDetails)
	}
	requireNoOperation(t, harness, request)
	adapter.mu.Lock()
	invokes := adapter.invokes
	adapter.mu.Unlock()
	if invokes != 0 {
		t.Fatalf("provider invoke count = %d, want zero", invokes)
	}
}

func TestGenerateKeepsTokenizerErrorsFatal(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
	harness := newHarness(t, adapter)
	harness.engine.dependencies.Estimator = budget.Estimator{Tokenizer: func(llm.Request, routing.Candidate) (int64, error) {
		return 0, errors.New("tokenizer unavailable")
	}}

	request := baseRequest("tokenizer-error-is-fatal")
	request.ServiceClass = llm.ServiceClassPriority
	request.ServiceClassFallbacks = []llm.ServiceClass{llm.ServiceClassStandard}
	_, err := harness.engine.Generate(context.Background(), request)
	if err == nil {
		t.Fatal("tokenizer error unexpectedly fell back")
	}
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeInvalidArgument || mapped.Phase != provider.PhasePrice || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever {
		t.Fatalf("error = %#v, want fatal price-estimate failure", err)
	}
	requireNoOperation(t, harness, request)
}

func TestGenerateRejectsUnavailablePriceWhenPolicyRequiresIt(t *testing.T) {
	for _, test := range []struct {
		name    string
		entries []pricing.Entry
	}{
		{name: "missing", entries: []pricing.Entry{priceEntryForTier("standard-tier")}},
		{name: "stale", entries: []pricing.Entry{priceEntryForTier("standard-tier"), func() pricing.Entry {
			entry := priceEntryForTier("priority-tier")
			entry.EffectiveUntil = time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
			return entry
		}()}},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
			harness := newHarness(t, adapter)
			snapshot := harness.engine.dependencies.Snapshots.(StaticSnapshot).Value
			snapshot.Prices = pricing.NewResolver(testPriceCatalog(t, test.entries...))
			snapshot.RequirePriceWhenBudgeted = true
			snapshot.BudgetPolicies = []budget.Policy{{
				ID:      "priority-only",
				Match:   budget.Matcher{Tenant: "tenant-1", ServiceClass: llm.ServiceClassPriority},
				Windows: []budget.Window{{ID: "priority-only/hour", Duration: time.Hour, Bucket: time.Minute, Limit: 1_000}},
			}}
			harness.engine.dependencies.Snapshots = StaticSnapshot{Value: snapshot}

			request := baseRequest("reject-budgeted-unpriced-" + test.name)
			request.ServiceClass = llm.ServiceClassPriority
			_, err := harness.engine.Generate(context.Background(), request)
			if err == nil {
				t.Fatal("budgeted unpriced request unexpectedly dispatched")
			}
			var mapped *provider.Error
			if !errors.As(err, &mapped) || mapped.Code != provider.CodeNoRoute || mapped.Phase != provider.PhasePrice || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever {
				t.Fatalf("error = %#v, want no-route price failure", err)
			}
			if mapped.SafeDetails["reason"] != "no_eligible_price" {
				t.Fatalf("safe details = %#v, want no_eligible_price", mapped.SafeDetails)
			}
			requireNoOperation(t, harness, request)
			adapter.mu.Lock()
			invokes := adapter.invokes
			adapter.mu.Unlock()
			if invokes != 0 {
				t.Fatalf("provider invoke count = %d, want zero", invokes)
			}
		})
	}
}

func TestGenerateRejectsUnbudgetedUnpricedCandidateWhenPolicyIsStrict(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
	harness := newHarness(t, adapter)
	snapshot := harness.engine.dependencies.Snapshots.(StaticSnapshot).Value
	snapshot.Prices = pricing.NewResolver(testPriceCatalog(t, priceEntryForTier("standard-tier")))
	snapshot.RequirePriceWhenBudgeted = false
	harness.engine.dependencies.Snapshots = StaticSnapshot{Value: snapshot}

	request := baseRequest("reject-strict-unpriced")
	request.ServiceClass = llm.ServiceClassPriority
	_, err := harness.engine.Generate(context.Background(), request)
	if err == nil {
		t.Fatal("strict unpriced request unexpectedly dispatched")
	}
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeNoRoute || mapped.Phase != provider.PhasePrice {
		t.Fatalf("error = %#v, want no-route price failure", err)
	}
	if mapped.SafeDetails["reason"] != "no_eligible_price" {
		t.Fatalf("safe details = %#v, want no_eligible_price", mapped.SafeDetails)
	}
	requireNoOperation(t, harness, request)
}

func testPriceCatalog(t *testing.T, entries ...pricing.Entry) pricing.Catalog {
	t.Helper()
	catalog, err := pricing.CompileUSD("prices-1", entries)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func priceEntryForTier(tier string) pricing.Entry {
	return pricing.Entry{
		Provider: "provider-1", Family: string(provider.FamilyOpenAIResponses), EndpointID: "endpoint-1", Region: "us-east-1", Model: "provider-model", ProviderTier: tier,
		Version: "prices-1", Prices: pricing.UnitPrices{PerRequest: pricing.MustDecimalUSD("0.000001"), OutputPerMillion: pricing.MustDecimalUSD("1")},
	}
}

func requireNoOperation(t *testing.T, harness testHarness, request llm.Request) {
	t.Helper()
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := llm.RequestDigest(normalized)
	if err != nil {
		t.Fatal(err)
	}
	operationID, _ := operationIdentity(normalized, digest)
	if _, err := harness.admission.Get(context.Background(), operationID); !errors.Is(err, admission.ErrOperationNotFound) {
		t.Fatalf("admission Get(%q) error = %v, want ErrOperationNotFound", operationID, err)
	}
}
