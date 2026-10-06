package runtime

import (
	"context"
	"errors"
	"math/big"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

type budgetPlanningFixture struct {
	cap       V1RuntimeCapabilities
	source    *planningSource
	adapter   *planningAdapter
	gen       llm.GenerateRequestV1
	compact   llm.CompactRequestV1
	generate  PreparedGenerateInput
	summary   PreparedCompactInput
	estimator budget.Estimator
	entry     pricing.Entry
	attempt   BudgetAttempt
	// parentSnapshotBlob makes boundedCloud's repository store parent
	// snapshots as referenced blobs (#1112).
	parentSnapshotBlob bool
}

// withParentSnapshotBlob is a boundedCloud option for the referenced form.
func withParentSnapshotBlob(b *budgetPlanningFixture) { b.parentSnapshotBlob = true }

func newBudgetPlanningFixture(t testing.TB) *budgetPlanningFixture {
	t.Helper()
	f := &budgetPlanningFixture{}
	var replay durable.CompactReplay
	f.cap, f.source, f.adapter, f.gen, replay, f.compact = planningFixture()
	f.attempt = BudgetAttempt{OperationID: "paid-attempt-1", GenerationID: "generation-1",
		QuotedAt: time.Date(2026, 10, 2, 0, 4, 0, 0, time.UTC)}
	f.attempt.ExpiresAt = f.attempt.QuotedAt.Add(24 * time.Hour)
	f.source.value.Environment, f.source.value.RequireBudgetMatch = "production", true
	f.source.value.BudgetPolicies = []budget.Policy{{ID: "policy", Match: budget.Matcher{
		Tenant: "tenant", Project: "project", ActorPrefix: "act", Environment: "production", LogicalModel: "alias", EndpointID: "endpoint", ServiceClass: llm.ServiceClassStandard},
		Windows: []budget.Window{{ID: "policy/hour", Duration: time.Hour, Bucket: 5 * time.Minute, LimitUSD: pricing.MustUSD("0.01")},
			{ID: "policy/day", Duration: 24 * time.Hour, Bucket: time.Hour, LimitUSD: pricing.MustUSD("0.1")}}}}
	f.entry = pricing.Entry{Provider: "openai", Family: string(provider.FamilyOpenAIResponses), EndpointID: "endpoint", Region: "region",
		Model: "provider-model", ProviderTier: "default", Version: "price/v1", Prices: pricing.UnitPrices{
			InputPerMillion: pricing.MustDecimalUSD("1"), OutputPerMillion: pricing.MustDecimalUSD("2"), PerRequest: pricing.MustDecimalUSD("0.0000000001")}}
	f.prices(t, []pricing.Entry{f.entry})
	f.estimator = budget.Estimator{MaxOutput: 16, SafetyRatio: big.NewRat(3, 2), Tokenizer: func(request llm.Request, candidate routing.Candidate) (int64, error) {
		if request.Model != "provider-model" || request.ServiceClass != llm.ServiceClassStandard || len(request.ServiceClassFallbacks) != 0 || candidate.ProviderTier != "default" {
			return 0, errors.New("sensitive wrong provider estimate")
		}
		return 10, nil
	}}
	var err error
	f.generate, err = PrepareGenerateInput(context.Background(), f.gen, durable.GenerateReplay{})
	if err != nil {
		t.Fatal(err)
	}
	f.summary, err = PrepareCompactInput(context.Background(), f.compact, replay)
	if err != nil {
		t.Fatal(err)
	}
	// Give both semantic requests the same small bound to assert the quote by
	// hand. Preparation and the normal summarizer bounds are covered separately.
	for _, request := range []*llm.Request{&f.generate.Request, f.summary.Request} {
		if request.Output == nil {
			request.Output = &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindText}}
		}
		request.Output.MaxTokens = preparationPointer(16)
	}
	return f
}

func (f *budgetPlanningFixture) prices(t testing.TB, entries []pricing.Entry) {
	t.Helper()
	catalog, err := pricing.CompileUSD("prices/v1", entries)
	if err != nil {
		t.Fatal(err)
	}
	f.source.value.Prices = pricing.NewResolver(catalog)
}

func (f *budgetPlanningFixture) planning(t *testing.T) *BudgetPlanning {
	t.Helper()
	f.cap.BudgetEstimator, f.cap.MaxBudgetBucketsPerWindow = f.estimator, 100
	planning, err := f.cap.NewBudgetPlanning(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return planning
}

func assertBudgetPlanningError(t *testing.T, err error, code provider.Code) {
	t.Helper()
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != code || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever ||
		mapped.Cause != nil || strings.Contains(err.Error(), "sensitive") || unsafePlanningDetails(mapped) {
		t.Fatalf("unsafe or unexpected budget planning error: %#v", err)
	}
}

func TestBudgetPlanningQuotesBothPhasesAndEveryMatchedWindow(t *testing.T) {
	f := newBudgetPlanningFixture(t)
	p := f.planning(t)
	for _, compact := range []bool{false, true} {
		t.Run(map[bool]string{false: "generate", true: "compact"}[compact], func(t *testing.T) {
			var planned PlannedBudgetCall
			var err error
			if compact {
				planned, err = p.Compact(context.Background(), f.summary, f.attempt)
			} else {
				planned, err = p.Generate(context.Background(), f.generate, f.attempt)
			}
			if err != nil {
				t.Fatal(err)
			}
			if !planned.RequiresReservation() || planned.Estimate.CostUSD.Cmp(pricing.MustUSD("0.00006300015")) != 0 || planned.Estimate.MicroUSD != 65 ||
				planned.Estimate.InputTokens != 10 || planned.Estimate.OutputTokens != 16 || planned.Quote.Entry.Version != "price/v1" ||
				planned.Route.PriceVersion != "price/v1" || planned.Route.CacheIdentity != planned.Provider.CacheIdentity {
				t.Fatalf("quote lost exact cost or selected identity: %#v", planned)
			}
			if planned.Reservation.OperationID != f.attempt.OperationID || planned.Reservation.GenerationID != f.attempt.GenerationID ||
				!planned.Reservation.ExpiresAt.Equal(f.attempt.ExpiresAt) || !planned.QuotedAt.Equal(f.attempt.QuotedAt) || len(planned.Reservation.Reservations) != 2 {
				t.Fatal("quote lost stable attempt or windows")
			}
			for index, value := range planned.Reservation.Reservations {
				window := f.source.value.BudgetPolicies[0].Windows[index]
				if value.PolicyID != "policy" || value.WindowID != window.ID || value.Bucket != f.attempt.QuotedAt.UnixNano()/int64(window.Bucket) ||
					value.BucketNanos != int64(window.Bucket) || value.DurationNanos != int64(window.Duration) || value.LimitUSD.Cmp(window.LimitUSD) != 0 ||
					value.AmountUSD.Cmp(planned.Estimate.CostUSD) != 0 || value.Amount != 65 {
					t.Fatalf("incorrect window quote: %#v", value)
				}
			}
		})
	}
	if f.source.reads != 1 || f.generate.Settings.Model != "alias" || f.generate.Settings.ServiceClass != llm.ServiceClassPriority || len(f.adapter.inputs) != 2 {
		t.Fatal("planning recaptured configuration or changed application settings")
	}
}

func TestBudgetPlanningCapturedPricesPoliciesAndEstimatorSurviveMutation(t *testing.T) {
	f := newBudgetPlanningFixture(t)
	providers, err := f.cap.NewProviderPlanning(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Mutate the source between provider and budget planner construction.
	resolver := f.source.value.Prices.(*pricing.PriceResolver)
	changed := f.entry
	changed.Prices.InputPerMillion = pricing.MustDecimalUSD("9")
	catalog, _ := pricing.CompileUSD("prices/v2", []pricing.Entry{changed})
	if err := resolver.ReloadValidated(catalog); err != nil {
		t.Fatal(err)
	}
	f.source.value.BudgetPolicies[0].Windows[0].LimitUSD = pricing.MustUSD("8")
	f.source.value.BudgetPolicies[0].Match.LogicalModel = "changed"
	f.source.value.Environment = "changed"
	p, err := providers.NewBudgetPlanning(f.estimator, 100)
	if err != nil {
		t.Fatal(err)
	}
	f.estimator.SafetyRatio.SetInt64(9)
	first, err := p.Generate(context.Background(), f.generate, f.attempt)
	if err != nil || first.Estimate.CostUSD.Cmp(pricing.MustUSD("0.00006300015")) != 0 || first.Reservation.Reservations[0].LimitUSD.Cmp(pricing.MustUSD("0.01")) != 0 {
		t.Fatalf("source mutation changed captured quote: %#v, %v", first, err)
	}
	first.Reservation.Reservations[0].WindowID = "changed"
	first.Quote.Entry.UnknownComponents = append(first.Quote.Entry.UnknownComponents, pricing.PriceComponentInput)
	second, err := p.Generate(context.Background(), f.generate, f.attempt)
	if err != nil || second.Reservation.Reservations[0].WindowID != "policy/hour" || second.Quote.Entry.ComponentUnknown(pricing.PriceComponentInput) {
		t.Fatal("result mutation changed subsequent planning")
	}
}

func TestBudgetPlanningFallsBackBeforeAnyBudgetAdmission(t *testing.T) {
	for _, reason := range []string{"missing price", "unknown input", "nano overflow", "no matching policy"} {
		t.Run(reason, func(t *testing.T) {
			f := newBudgetPlanningFixture(t)
			model := f.source.value.Routes.Models["alias"]
			first := model.Routes[0]
			first.ID, first.EndpointID = "first", "first-endpoint"
			model.Routes = append([]routing.Route{first}, model.Routes...)
			f.source.value.Routes.Models["alias"] = model
			f.cap.Adapters.(engine.AdapterMap)["first-endpoint"] = f.adapter
			if reason != "no matching policy" {
				f.source.value.BudgetPolicies[0].Match.EndpointID = ""
			}
			bad := f.entry
			bad.EndpointID = "first-endpoint"
			entries := []pricing.Entry{f.entry}
			switch reason {
			case "unknown input":
				bad.Prices.InputPerMillion = pricing.MustDecimalUSD("0")
				bad.UnknownComponents = []pricing.PriceComponent{pricing.PriceComponentInput}
				entries = append(entries, bad)
			case "nano overflow":
				bad.Prices.PerRequest = pricing.MustDecimalUSD("10000000")
				entries = append(entries, bad)
			case "no matching policy":
				entries = append(entries, bad)
			}
			f.prices(t, entries)
			result, err := f.planning(t).Generate(context.Background(), f.generate, f.attempt)
			if err != nil || result.Route.EndpointID != "endpoint" || !result.RequiresReservation() || len(f.adapter.inputs) != 2 {
				t.Fatalf("unusable first route did not yield to authorized fallback: %#v, %v", result, err)
			}
		})
	}
}

func TestBudgetPlanningDoesNotTreatMissingOrPartialPricesAsFree(t *testing.T) {
	for _, reason := range []string{"missing", "expired", "future", "unknown", "no policy"} {
		t.Run(reason, func(t *testing.T) {
			f := newBudgetPlanningFixture(t)
			switch reason {
			case "missing":
				f.prices(t, nil)
			case "expired":
				f.entry.EffectiveUntil = f.attempt.QuotedAt
				f.prices(t, []pricing.Entry{f.entry})
			case "future":
				f.entry.EffectiveFrom = f.attempt.QuotedAt.Add(time.Second)
				f.prices(t, []pricing.Entry{f.entry})
			case "unknown":
				f.entry.Prices.InputPerMillion = pricing.MustDecimalUSD("0")
				f.entry.UnknownComponents = []pricing.PriceComponent{pricing.PriceComponentInput}
				f.prices(t, []pricing.Entry{f.entry})
			case "no policy":
				f.source.value.BudgetPolicies = nil
			}
			_, err := f.planning(t).Generate(context.Background(), f.generate, f.attempt)
			assertBudgetPlanningError(t, err, provider.CodeNoRoute)
		})
	}
}

func TestBudgetPlanningFreeAndUnbudgetedPathsAreExplicit(t *testing.T) {
	for _, reason := range []string{"free", "priced unmatched", "permitted unpriced", "unpriced forbidden", "unpriced budgeted"} {
		t.Run(reason, func(t *testing.T) {
			f := newBudgetPlanningFixture(t)
			f.source.value.RequireBudgetMatch = false
			if reason == "free" {
				f.entry.Prices = pricing.UnitPrices{}
				f.prices(t, []pricing.Entry{f.entry})
			} else if reason != "unpriced budgeted" {
				f.source.value.BudgetPolicies = nil
			}
			if strings.Contains(reason, "unpriced") {
				f.prices(t, nil)
				f.source.value.RequirePriceWhenBudgeted = reason != "unpriced forbidden"
			}
			result, err := f.planning(t).Generate(context.Background(), f.generate, f.attempt)
			if reason == "unpriced forbidden" || reason == "unpriced budgeted" {
				assertBudgetPlanningError(t, err, provider.CodeNoRoute)
				return
			}
			if err != nil || result.RequiresReservation() || result.Reservation.OperationID != f.attempt.OperationID {
				t.Fatalf("empty reservation was hidden or became an error: %#v, %v", result, err)
			}
			if (result.Quote == nil) != (reason == "permitted unpriced") || (result.Estimate.CostUSD.IsZero()) != (reason != "priced unmatched") {
				t.Fatal("unpriced work was confused with known-free or priced work")
			}
		})
	}
}

type budgetResolverFunc func(pricing.Query) (pricing.Quote, error)

func (function budgetResolverFunc) Resolve(query pricing.Query) (pricing.Quote, error) {
	return function(query)
}

func TestBudgetPlanningRejectsMalformedOrMismatchedQuotes(t *testing.T) {
	for _, field := range []string{"provider", "family", "endpoint", "region", "model", "tier", "version", "digest", "zero digest", "catalog", "active", "resolver error", "tokenizer error"} {
		t.Run(field, func(t *testing.T) {
			f := newBudgetPlanningFixture(t)
			original := f.source.value.Prices
			f.source.value.Prices = budgetResolverFunc(func(query pricing.Query) (pricing.Quote, error) {
				if field == "resolver error" {
					return pricing.Quote{}, errors.New("sensitive pricing error")
				}
				quote, err := original.Resolve(query)
				switch field {
				case "provider":
					quote.Entry.Provider = "changed"
				case "family":
					quote.Entry.Family = "changed"
				case "endpoint":
					quote.Entry.EndpointID = "changed"
				case "region":
					quote.Entry.Region = "changed"
				case "model":
					quote.Entry.Model = "changed"
				case "tier":
					quote.Entry.ProviderTier = "changed"
				case "version":
					quote.Entry.Version = "changed"
				case "digest":
					quote.CatalogDigest = "sensitive"
				case "zero digest":
					quote.CatalogDigest = strings.Repeat("0", 64)
				case "catalog":
					quote.CatalogVersion = ""
				case "active":
					quote.Entry.EffectiveUntil = query.At
				}
				return quote, err
			})
			code := provider.CodeConfiguration
			if field == "tokenizer error" {
				code = provider.CodeInvalidArgument
				f.estimator.Tokenizer = func(llm.Request, routing.Candidate) (int64, error) { return 0, errors.New("sensitive tokenizer error") }
			}
			_, err := f.planning(t).Generate(context.Background(), f.generate, f.attempt)
			assertBudgetPlanningError(t, err, code)
		})
	}
}

func TestBudgetPlanningRejectsUnsafeConfiguration(t *testing.T) {
	for _, reason := range []string{"nil prices", "typed nil prices", "bad catalog", "empty window", "duplicate window", "duplicate policy", "unrestricted policy", "tiny limit", "huge limit", "max buckets", "output", "reasoning", "ratio"} {
		t.Run(reason, func(t *testing.T) {
			f := newBudgetPlanningFixture(t)
			maxBuckets := 100
			switch reason {
			case "nil prices":
				f.source.value.Prices = nil
			case "typed nil prices":
				var resolver *pricing.PriceResolver
				f.source.value.Prices = resolver
			case "bad catalog":
				f.source.value.Prices = pricing.Catalog{}
			case "empty window":
				f.source.value.BudgetPolicies[0].Windows[0].ID = ""
			case "duplicate window":
				f.source.value.BudgetPolicies[0].Windows[1].ID = "policy/hour"
			case "duplicate policy":
				f.source.value.BudgetPolicies = append(f.source.value.BudgetPolicies, f.source.value.BudgetPolicies[0])
			case "unrestricted policy":
				f.source.value.BudgetPolicies[0].Match = budget.Matcher{}
			case "tiny limit":
				f.source.value.BudgetPolicies[0].Windows[0].LimitUSD = pricing.MustUSD("0.0000000001")
			case "huge limit":
				f.source.value.BudgetPolicies[0].Windows[0].LimitUSD = pricing.MustUSD("10000000")
			case "max buckets":
				maxBuckets = 10
			case "output":
				f.estimator.MaxOutput = 0
			case "reasoning":
				f.estimator.MaxReasoning = -1
			case "ratio":
				f.estimator.SafetyRatio = big.NewRat(-1, 1)
			}
			providers, err := f.cap.NewProviderPlanning(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			_, err = providers.NewBudgetPlanning(f.estimator, maxBuckets)
			assertBudgetPlanningError(t, err, provider.CodeConfiguration)
		})
	}
}

func TestBudgetPlanningValidatesStableAttemptAndCancellation(t *testing.T) {
	f := newBudgetPlanningFixture(t)
	p := f.planning(t)
	for _, field := range []string{"operation", "generation", "quote time", "expiry", "earlier expiry", "overflow", "pre epoch"} {
		t.Run(field, func(t *testing.T) {
			attempt := f.attempt
			switch field {
			case "operation":
				attempt.OperationID = ""
			case "generation":
				attempt.GenerationID = ""
			case "quote time":
				attempt.QuotedAt = time.Time{}
			case "expiry":
				attempt.ExpiresAt = time.Time{}
			case "earlier expiry":
				attempt.ExpiresAt = attempt.QuotedAt
			case "overflow":
				attempt.QuotedAt = time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)
			case "pre epoch":
				attempt.QuotedAt = time.Unix(-1, 0)
			}
			_, err := p.Generate(context.Background(), f.generate, attempt)
			assertBudgetPlanningError(t, err, provider.CodeInvalidArgument)
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Generate(ctx, f.generate, f.attempt); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := p.Compact(context.Background(), PreparedCompactInput{}, f.attempt); err == nil {
		t.Fatal("empty compaction was quoted")
	}
	if len(f.adapter.inputs) != 0 {
		t.Fatal("invalid attempt or cancellation reached compilation")
	}
	_, err := p.Generate(nil, f.generate, f.attempt)
	assertBudgetPlanningError(t, err, provider.CodeConfiguration)
}

func TestBudgetPlanningUsesCapturedTimeAndLegacyWindowLimit(t *testing.T) {
	f := newBudgetPlanningFixture(t)
	f.source.value.BudgetPolicies[0].Windows[0].LimitUSD = pricing.USD{}
	f.source.value.BudgetPolicies[0].Windows[0].Limit = 10000
	f.entry.EffectiveUntil = f.attempt.QuotedAt.Add(time.Second)
	f.prices(t, []pricing.Entry{f.entry})
	result, err := f.planning(t).Generate(context.Background(), f.generate, f.attempt)
	if err != nil || result.Reservation.Reservations[0].LimitUSD.Cmp(pricing.MustUSD("0.01")) != 0 || result.Reservation.Reservations[0].Limit != 10000 {
		t.Fatalf("historical quote or legacy exact conversion failed: %#v %v", result, err)
	}
}

func TestBudgetPlanningConcurrentResultsAreIndependent(t *testing.T) {
	f := newBudgetPlanningFixture(t)
	p := f.planning(t)
	before, _ := f.generate.Request.MarshalJSON()
	var group sync.WaitGroup
	for i := 0; i < 20; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := p.Generate(context.Background(), f.generate, f.attempt)
			if err != nil || result.Reservation.Reservations[0].LimitUSD.Cmp(pricing.MustUSD("0.01")) != 0 {
				t.Error(err)
				return
			}
			result.Reservation.Reservations[0].WindowID = "changed"
		}()
	}
	group.Wait()
	after, _ := f.generate.Request.MarshalJSON()
	if string(before) != string(after) || f.source.reads != 1 || len(f.adapter.inputs) != 20 {
		t.Fatal("shared input or captured configuration changed")
	}
}

func TestBudgetPlanningCancellationDuringQuoteStopsSelection(t *testing.T) {
	f := newBudgetPlanningFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	original := f.source.value.Prices
	f.source.value.Prices = budgetResolverFunc(func(query pricing.Query) (pricing.Quote, error) {
		quote, err := original.Resolve(query)
		cancel()
		return quote, err
	})
	result, err := f.planning(t).Generate(ctx, f.generate, f.attempt)
	if !errors.Is(err, context.Canceled) || result.RequiresReservation() || result.Quote != nil || len(f.adapter.inputs) != 1 {
		t.Fatalf("canceled quote became a usable selection: %v", err)
	}
}

func TestBudgetPlanningResolvesOmittedEntryAndRouteVersions(t *testing.T) {
	f := newBudgetPlanningFixture(t)
	model := f.source.value.Routes.Models["alias"]
	model.Routes[0].PriceVersion = ""
	f.source.value.Routes.Models["alias"] = model
	f.entry.Version = ""
	f.prices(t, []pricing.Entry{f.entry})
	result, err := f.planning(t).Generate(context.Background(), f.generate, f.attempt)
	if err != nil || result.Quote.Entry.Version != "prices/v1" || result.Estimate.CatalogVersion != "prices/v1" || result.Route.PriceVersion != "prices/v1" {
		t.Fatalf("persisted quote lost the actual price version: %v", err)
	}
}

func TestBudgetPlanningEstimatesUnmodifiedPreparedContentWithoutTokenizer(t *testing.T) {
	f := newBudgetPlanningFixture(t)
	f.estimator.Tokenizer = nil
	p := f.planning(t)
	first, err := p.Generate(context.Background(), f.generate, f.attempt)
	if err != nil {
		t.Fatal(err)
	}
	f.generate.Request.Input = append(f.generate.Request.Input, preparationMessage(strings.Repeat("more content ", 1000)))
	second, err := p.Generate(context.Background(), f.generate, f.attempt)
	if err != nil || second.Estimate.InputTokens <= first.Estimate.InputTokens || second.Estimate.CostUSD.Cmp(first.Estimate.CostUSD) <= 0 ||
		second.Provider.Call.Metadata.SchemaDigest == first.Provider.Call.Metadata.SchemaDigest {
		t.Fatal("additional effective content did not change input estimation and compilation")
	}
}

func TestBudgetPlanningQuotesReachAdmissionAndUncertainAcceptanceReplaysIdentically(t *testing.T) {
	for _, compact := range []bool{false, true} {
		t.Run(map[bool]string{false: "generate", true: "compact"}[compact], func(t *testing.T) {
			f := newBudgetPlanningFixture(t)
			p := f.planning(t)
			var planned PlannedBudgetCall
			var err error
			if compact {
				planned, err = p.Compact(context.Background(), f.summary, f.attempt)
			} else {
				planned, err = p.Generate(context.Background(), f.generate, f.attempt)
			}
			if err != nil {
				t.Fatal(err)
			}
			admissionFixture := newBudgetAdmissionFixture(t)
			admissionFixture.gen, admissionFixture.compact = f.gen, f.compact
			admissionFixture.route, admissionFixture.plan, admissionFixture.now = planned.Route, planned.Reservation, f.attempt.QuotedAt
			first := true
			admissionFixture.leaser.accept = func(ctx context.Context, request durable.ReserveRequest) (durable.ReserveResult, error) {
				if !reflect.DeepEqual(request, planned.Reservation) {
					t.Fatal("changed quote during uncertain retry")
				}
				accepted, err := admissionFixture.leaser.BudgetLeaser.Accept(ctx, request)
				if err == nil && first {
					first = false
					return durable.ReserveResult{}, errors.New("sensitive lost acceptance reply")
				}
				return accepted, err
			}
			reserve := func() (durable.ReserveResult, error) {
				if compact {
					return admissionFixture.helper.ReserveCompact(context.Background(), f.compact, planned.Route)
				}
				return admissionFixture.helper.ReserveGenerate(context.Background(), f.gen, planned.Route)
			}
			_, err = reserve()
			assertBudgetAdmissionError(t, err, provider.CodeStateUnavailable, provider.DispatchNotDispatched, provider.RetrySameOperation)
			// Crossing a bucket boundary cannot refresh the quote or lease.
			admissionFixture.now = admissionFixture.now.Add(2 * time.Minute)
			accepted, err := reserve()
			if err != nil || !accepted.Accepted || len(accepted.Events) != 2 {
				t.Fatalf("identical acceptance retry failed: %#v %v", accepted, err)
			}
			var receipt durable.ClaimReceipt
			if compact {
				receipt, err = admissionFixture.helper.ClaimCompact(context.Background(), f.compact, planned.Route, accepted)
			} else {
				receipt, err = admissionFixture.helper.ClaimGenerate(context.Background(), f.gen, planned.Route, accepted)
			}
			if err != nil || receipt.OperationID != f.attempt.OperationID {
				t.Fatalf("quote did not authorize a single claim: %#v %v", receipt, err)
			}
			_, err = admissionFixture.helper.ClaimGenerate(context.Background(), f.gen, planned.Route, accepted)
			assertBudgetAdmissionError(t, err, provider.CodeAmbiguousDispatch, provider.DispatchAmbiguous, provider.RetryNever)
		})
	}
}

func TestBudgetPlanningPrefersLessTriedCandidates(t *testing.T) {
	f := newBudgetPlanningFixture(t)
	model := f.source.value.Routes.Models["alias"]
	alternate := model.Routes[0]
	alternate.ID, alternate.EndpointID = "alternate", "alternate-endpoint"
	model.Routes = append(model.Routes, alternate)
	f.source.value.Routes.Models["alias"] = model
	f.cap.Adapters.(engine.AdapterMap)["alternate-endpoint"] = f.adapter
	f.source.value.BudgetPolicies[0].Match.EndpointID = ""
	entry := f.entry
	entry.EndpointID = "alternate-endpoint"
	f.prices(t, []pricing.Entry{f.entry, entry})
	planning := f.planning(t)
	first, err := planning.Generate(context.Background(), f.generate, f.attempt)
	if err != nil {
		t.Fatal(err)
	}
	if first.Route.EndpointID != "endpoint" {
		t.Fatal("initial priority changed")
	}
	f.attempt.PriorCandidates = []string{first.Provider.Candidate.ID}
	second, err := planning.Generate(context.Background(), f.generate, f.attempt)
	if err != nil {
		t.Fatal(err)
	}
	if second.Route.EndpointID != "alternate-endpoint" {
		t.Fatal("failed candidate starved alternative")
	}
	f.attempt.PriorCandidates = append(f.attempt.PriorCandidates, second.Provider.Candidate.ID)
	third, err := planning.Generate(context.Background(), f.generate, f.attempt)
	if err != nil {
		t.Fatal(err)
	}
	if third.Route.EndpointID != "endpoint" {
		t.Fatal("equal attempt counts lost configured priority")
	}
}

// A reservation larger than a matched window's limit can never be admitted, so
// planning fails fast with budget_denied instead of waiting for capacity until
// the workflow deadline (#1168).
func TestBudgetPlanningRejectsReservationLargerThanTheWindowLimit(t *testing.T) {
	f := newBudgetPlanningFixture(t)
	planned, err := f.planning(t).Generate(context.Background(), f.generate, f.attempt)
	if err != nil {
		t.Fatal(err)
	}
	cost := planned.Reservation.Reservations[0].AmountUSD
	for i := range f.source.value.BudgetPolicies[0].Windows {
		f.source.value.BudgetPolicies[0].Windows[i].LimitUSD = pricing.MustUSD("0.000000001")
	}
	if cost.Cmp(pricing.MustUSD("0.000000001")) <= 0 {
		t.Skip("fixture quote is below the smallest limit")
	}
	_, err = f.planning(t).Generate(context.Background(), f.generate, f.attempt)
	assertBudgetPlanningError(t, err, provider.CodeBudgetDenied)
}
