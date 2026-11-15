package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
)

func TestGenerateSkipsBudgetedUnpricedCandidateAndUsesPricedFallback(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
	harness := newHarness(t, adapter)
	harness.updateSnapshot(func(snapshot *Snapshot) {
		snapshot.Prices = pricing.NewResolver(testPriceCatalog(t, priceEntryForTier("standard-tier")))
		snapshot.BudgetPolicies = []budget.Policy{tenantClassBudgetPolicy("priority-only", llm.ServiceClassPriority)}
	})

	request := priorityWithStandardFallback("skip-budgeted-unpriced")
	response, err := harness.engine.Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Service.Attempted != llm.ServiceClassStandard || response.Cost.Status != llm.CostStatusKnown {
		t.Fatalf("response = %#v, want priced standard fallback", response)
	}
	adapter.requireOnlyStandardCall(t, "only priced standard fallback")
}

func TestGenerateAllowsUnbudgetedUnpricedCandidateWithUnknownCost(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
	harness := newHarness(t, adapter)
	harness.updateSnapshot(func(snapshot *Snapshot) {
		snapshot.Prices = pricing.NewResolver(testPriceCatalog(t, priceEntryForTier("standard-tier")))
		snapshot.RequirePriceWhenBudgeted = true
	})

	request := baseRequest("allow-unbudgeted-unpriced")
	request.ServiceClass = llm.ServiceClassPriority
	response, err := harness.engine.Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Cost.Status != llm.CostStatusUnknown || response.Cost.ReservedCostUSD != nil || response.Cost.ActualCostUSD != nil || response.Cost.Method != "" || response.Cost.CatalogVersion != "" {
		t.Fatalf("unpriced response cost = %#v, want unknown zero-value monetary facts", response.Cost)
	}
	operation := harness.operation(t, response.OperationID)
	if operation.ReservedMicroUSD != 0 || operation.IncurredMicroUSD != 0 || operation.PriceVersion != "" {
		t.Fatalf("unpriced operation = %#v, want zero unknown-price accounting", operation)
	}
}

func TestGenerateSkipsActivePartialPriceAndUsesCompleteFallback(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
	harness := newHarness(t, adapter)
	partial := priceEntryForTier("priority-tier")
	partial.UnknownComponents = []pricing.PriceComponent{pricing.PriceComponentInput}
	harness.updateSnapshot(func(snapshot *Snapshot) {
		snapshot.Prices = pricing.NewResolver(testPriceCatalog(t, partial, priceEntryForTier("standard-tier")))
	})

	request := priorityWithStandardFallback("skip-active-partial-price")
	response, err := harness.engine.Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Service.Attempted != llm.ServiceClassStandard || response.Cost.Status != llm.CostStatusKnown {
		t.Fatalf("response = %#v, want complete standard fallback", response)
	}
	adapter.requireOnlyStandardCall(t, "only standard fallback")
}

func TestGenerateReturnsNoRouteWhenAllCandidatePricesAreUnusable(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
	harness := newHarness(t, adapter)
	partial := priceEntryForTier("priority-tier")
	partial.UnknownComponents = []pricing.PriceComponent{pricing.PriceComponentInput}
	overflow := priceEntryForTier("standard-tier")
	// The exact USD value is valid, but the legacy Redis microUSD projection
	// cannot represent it. The candidate must not abort planning for the route
	// that follows it, nor can it be admitted itself.
	overflow.Prices.PerRequest = pricing.MustDecimalUSD("99999999999999999999")
	harness.updateSnapshot(func(snapshot *Snapshot) {
		snapshot.Prices = pricing.NewResolver(testPriceCatalog(t, partial, overflow))
	})

	request := priorityWithStandardFallback("all-unusable-prices")
	_, err := harness.engine.Generate(context.Background(), request)
	requireNoEligiblePrice(t, err, "all unusable candidates unexpectedly dispatched")
	requireNoOperation(t, harness, request)
	adapter.requireInvokes(t, 0, "provider invoke count = %d, want zero")
}

func TestGenerateKeepsTokenizerErrorsFatal(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
	harness := newHarness(t, adapter)
	harness.engine.dependencies.Estimator = budget.Estimator{Tokenizer: func(llm.Request, routing.Candidate) (int64, error) {
		return 0, errors.New("tokenizer unavailable")
	}}

	request := priorityWithStandardFallback("tokenizer-error-is-fatal")
	_, err := harness.engine.Generate(context.Background(), request)
	if err == nil {
		t.Fatal("tokenizer error unexpectedly fell back")
	}
	if _, ok := isNotDispatchedFailure(err, provider.CodeInvalidArgument, provider.PhasePrice); !ok {
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
			harness.updateSnapshot(func(snapshot *Snapshot) {
				snapshot.Prices = pricing.NewResolver(testPriceCatalog(t, test.entries...))
				snapshot.RequirePriceWhenBudgeted = true
				snapshot.BudgetPolicies = []budget.Policy{tenantClassBudgetPolicy("priority-only", llm.ServiceClassPriority)}
			})

			request := baseRequest("reject-budgeted-unpriced-" + test.name)
			request.ServiceClass = llm.ServiceClassPriority
			_, err := harness.engine.Generate(context.Background(), request)
			requireNoEligiblePrice(t, err, "budgeted unpriced request unexpectedly dispatched")
			requireNoOperation(t, harness, request)
			adapter.requireInvokes(t, 0, "provider invoke count = %d, want zero")
		})
	}
}

func TestGenerateRejectsUnbudgetedUnpricedCandidateWhenPolicyIsStrict(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
	harness := newHarness(t, adapter)
	harness.updateSnapshot(func(snapshot *Snapshot) {
		snapshot.Prices = pricing.NewResolver(testPriceCatalog(t, priceEntryForTier("standard-tier")))
		snapshot.RequirePriceWhenBudgeted = false
	})

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
	operationID := normalizedIdentityForTest(t, request).id
	if _, err := harness.admission.Get(context.Background(), operationID); !errors.Is(err, admission.ErrOperationNotFound) {
		t.Fatalf("admission Get(%q) error = %v, want ErrOperationNotFound", operationID, err)
	}
}

func TestGenerateRejectsContextOverflowBeforePricedOrUnpricedDispatch(t *testing.T) {
	for _, unpriced := range []bool{false, true} {
		adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
		harness := newHarness(t, adapter)
		snapshot := harness.engine.dependencies.Snapshots.(StaticSnapshot).Value
		for name, model := range snapshot.Routes.Models {
			for i := range model.Routes {
				model.Routes[i].ContextTokens = 1
			}
			snapshot.Routes.Models[name] = model
		}
		request := baseRequest("context-overflow")
		if unpriced {
			snapshot.Prices = pricing.NewResolver(testPriceCatalog(t, priceEntryForTier("standard-tier")))
			snapshot.RequirePriceWhenBudgeted = true
			request.ServiceClass = llm.ServiceClassPriority
		}
		harness.engine.dependencies.Snapshots = StaticSnapshot{Value: snapshot}
		_, err := harness.engine.Generate(context.Background(), request)
		var mapped *provider.Error
		if !errors.As(err, &mapped) || mapped.Code != provider.CodeNoRoute || mapped.SafeDetails["reason"] != "context_limit" {
			t.Fatalf("unpriced=%t: %v", unpriced, err)
		}
		if calls := len(adapter.recordedCalls()); calls != 0 {
			t.Fatalf("unpriced=%t: dispatched %d calls", unpriced, calls)
		}
	}
}

func TestGenerateKeepsUnknownProviderChargeReserved(t *testing.T) {
	paid := successfulResponse()
	paid.Cost.Status = llm.CostStatusUnknown
	adapter := &fakeAdapter{name: "fake", response: paid}
	harness := newHarness(t, adapter)
	response, err := harness.engine.Generate(context.Background(), baseRequest("unknown-hosted-charge"))
	if err != nil {
		t.Fatal(err)
	}
	if response.Cost.Status != llm.CostStatusUnknown || response.Cost.ActualCostUSD != nil || response.Cost.ReservedCostUSD == nil {
		t.Fatalf("unknown charge lost its reservation: %#v", response.Cost)
	}
	operation := harness.operation(t, response.OperationID)
	if operation.IncurredCostUSD == nil || operation.IncurredCostUSD.Cmp(*response.Cost.ReservedCostUSD) != 0 {
		t.Fatalf("unknown paid work released its reserve: %#v", operation)
	}
	if len(response.Output) == 0 {
		t.Fatal("unknown charge discarded the paid answer")
	}
}
