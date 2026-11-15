package engine

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	memory "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/memory"
)

type fakeAdapter struct {
	mu                  sync.Mutex
	name                string
	rejectFirst         bool
	redirectFirst       bool
	egressFirst         bool
	preDispatchFirst    bool
	preDispatchDeadline bool
	rawDeadlineFirst    bool
	progressPhase       provider.Phase
	ambiguous           bool
	capabilities        int
	compiles            int
	invokes             int
	calls               []provider.Call
	compiledClasses     []llm.ServiceClass
	response            llm.Response
}

func (adapter *fakeAdapter) Name() string { return adapter.name }

func (adapter *fakeAdapter) Capabilities(context.Context, provider.CapabilityQuery) (provider.CapabilitySet, error) {
	adapter.mu.Lock()
	adapter.capabilities++
	adapter.mu.Unlock()
	return provider.CapabilitySet{Version: "provider-cap-1", Features: map[provider.Feature]provider.Capability{
		provider.FeatureText:      {State: provider.CapabilityNative},
		provider.FeatureStreaming: {State: provider.CapabilityNative},
		provider.FeatureUsage:     {State: provider.CapabilityNative},
	}}, nil
}

func (adapter *fakeAdapter) Compile(_ context.Context, input provider.CompileInput) (provider.Call, error) {
	adapter.mu.Lock()
	adapter.compiles++
	adapter.compiledClasses = append(adapter.compiledClasses, input.Request.ServiceClass)
	adapter.mu.Unlock()
	return provider.Call{EndpointID: input.Query.EndpointID, Family: input.Query.Family, Model: input.Query.Model, OperationKey: input.Request.OperationKey, ServiceClass: input.Query.ServiceClass, Metadata: input.Metadata}, nil
}

func (adapter *fakeAdapter) Invoke(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
	adapter.mu.Lock()
	adapter.invokes++
	index := adapter.invokes
	adapter.calls = append(adapter.calls, call)
	response := adapter.response
	phase := adapter.progressPhase
	adapter.mu.Unlock()
	if err := observer.BeforePossibleWrite(ctx); err != nil {
		return provider.Result{}, err
	}
	if adapter.ambiguous {
		return provider.Result{}, provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "provider outcome is ambiguous")
	}
	if adapter.rejectFirst && index == 1 {
		return provider.Result{}, provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNextRoute, "provider rejected dispatch")
	}
	if adapter.redirectFirst && index == 1 {
		return provider.Result{}, provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "provider redirect response is ambiguous")
	}
	if adapter.egressFirst && index == 1 {
		return provider.Result{}, provider.NewEgressDeniedError(provider.ErrProviderEgressDenied)
	}
	if adapter.preDispatchFirst && index == 1 {
		return provider.Result{}, provider.NewPreDispatchUnavailableError(provider.ErrProviderPreDispatch)
	}
	if adapter.preDispatchDeadline && index == 1 {
		return provider.Result{}, provider.NewPreDispatchContextError(context.DeadlineExceeded)
	}
	if adapter.rawDeadlineFirst && index == 1 {
		return provider.Result{}, context.DeadlineExceeded
	}
	if err := observer.AfterResponseHeaders(ctx, provider.ResponseMetadata{}); err != nil {
		return provider.Result{}, err
	}
	if phase == "" {
		phase = provider.PhaseLift
	}
	observer.OnProgress(ctx, provider.Progress{Phase: string(phase), OutputItems: len(response.Output)})
	return provider.Result{Response: response}, nil
}

type fakeResultStore struct {
	mu     sync.Mutex
	values map[string]llm.Response
	gets   int
	puts   int
}

func (store *fakeResultStore) Get(_ context.Context, operationID string) (llm.Response, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.gets++
	value, ok := store.values[operationID]
	if !ok {
		return llm.Response{}, ErrResultNotFound
	}
	return value, nil
}

func (store *fakeResultStore) Put(_ context.Context, operationID string, response llm.Response) (state.BlobRef, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.puts++
	if _, ok := store.values[operationID]; ok {
		return resultRef(operationID), nil
	}
	if store.values == nil {
		store.values = make(map[string]llm.Response)
	}
	store.values[operationID] = response
	return resultRef(operationID), nil
}

func resultRef(operationID string) state.BlobRef {
	return state.BlobRef{Digest: sha256.Sum256([]byte(operationID)), Size: int64(len(operationID)), Media: "application/json"}
}

type testHarness struct {
	engine    *Engine
	admission admission.AdmissionStore
	results   *fakeResultStore
	clock     time.Time
}

// recordingAdmission observes the exact reservation envelope without changing
// the behavior of the in-memory compatibility store.
type recordingAdmission struct {
	admission.AdmissionStore
	begin admission.BeginRequest
}

func (store *recordingAdmission) Begin(ctx context.Context, request admission.BeginRequest) (admission.BeginResult, error) {
	store.begin = request
	return store.AdmissionStore.Begin(ctx, request)
}

func newHarness(t testing.TB, adapter provider.Adapter) testHarness {
	t.Helper()
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	return newHarnessWithAdmission(t, adapter, memory.NewAdmissionStore(memory.AdmissionOptions{Clock: func() time.Time { return now }}), func() time.Time { return now })
}

func newHarnessWithAdmission(t testing.TB, adapter provider.Adapter, admissionStore admission.AdmissionStore, clock func() time.Time) testHarness {
	t.Helper()
	if admissionStore == nil {
		t.Fatal("admission store is required")
	}
	if clock == nil {
		t.Fatal("clock is required")
	}
	classes := []llm.ServiceClass{llm.ServiceClassEconomy, llm.ServiceClassStandard, llm.ServiceClassPriority}
	tiers := map[llm.ServiceClass]string{
		llm.ServiceClassEconomy:  "economy-tier",
		llm.ServiceClassStandard: "standard-tier",
		llm.ServiceClassPriority: "priority-tier",
	}
	routes, err := routing.CompileCatalog("routes-1", map[string]routing.Model{
		"logical-model": {Routes: []routing.Route{{
			ID: "route-1", EndpointID: "endpoint-1", Provider: "provider-1", Family: string(provider.FamilyOpenAIResponses), Region: "us-east-1", AccountRegion: "us-east-1", Model: "provider-model", ModelLineage: "provider-lineage", Classes: classes, ProviderTiers: tiers, PriceVersion: "prices-1", PriceAvailable: true,
			Capabilities: routing.CapabilitySet{Version: "route-cap-1", Features: map[routing.Feature]routing.Capability{routing.FeatureText: {State: routing.CapabilityNative}}},
		}}},
	})
	testassert.NoError(t, err)
	entries := make([]pricing.Entry, 0, len(classes))
	for _, class := range classes {
		entries = append(entries, pricing.Entry{Provider: "provider-1", Family: string(provider.FamilyOpenAIResponses), EndpointID: "endpoint-1", Region: "us-east-1", Model: "provider-model", ProviderTier: tiers[class], Version: "prices-1", Prices: pricing.UnitPrices{PerRequest: pricing.MustDecimalUSD("0.000001"), OutputPerMillion: pricing.MustDecimalUSD("1")}})
	}
	priceCatalog, err := pricing.CompileUSD("prices-1", entries)
	testassert.NoError(t, err)
	results := &fakeResultStore{values: make(map[string]llm.Response)}
	engineValue, err := New(Dependencies{
		Snapshots:   StaticSnapshot{Value: Snapshot{Version: "snapshot-1", Routes: routes, Prices: pricing.NewResolver(priceCatalog), ReservationLease: time.Minute, OperationRetention: time.Hour}},
		Planner:     routing.DeterministicPlanner{},
		Adapters:    AdapterMap{"endpoint-1": adapter},
		Admission:   admissionStore,
		Results:     results,
		Clock:       clock,
		Estimator:   budget.Estimator{MaxOutput: 1},
		MaxAttempts: 8,
	})
	testassert.NoError(t, err)
	return testHarness{engine: engineValue, admission: admissionStore, results: results, clock: clock()}
}

// priorityWithStandardFallback is baseRequest asking for the priority class
// with standard as its only fallback.
func priorityWithStandardFallback(operationKey string) llm.Request {
	request := baseRequest(operationKey)
	request.ServiceClass = llm.ServiceClassPriority
	request.ServiceClassFallbacks = []llm.ServiceClass{llm.ServiceClassStandard}
	return request
}

// recordedCalls copies the provider calls the adapter has received.
func (adapter *fakeAdapter) recordedCalls() []provider.Call {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	return append([]provider.Call(nil), adapter.calls...)
}

func baseRequest(operationKey string) llm.Request {
	return llm.Request{OperationKey: operationKey, Context: llm.RequestContext{Tenant: "tenant-1"}, Model: "logical-model", Input: []llm.Item{llm.HumanText("hello")}}
}

func successfulResponse() llm.Response {
	return llm.Response{Status: llm.ResponseStatusCompleted, Output: []llm.Item{llm.ModelText("world")}, Usage: llm.Usage{OutputTokens: 1}, Provider: llm.ProviderFacts{RequestID: "provider-request-1"}}
}

func TestLoadContinuationRequiresRequestTenant(t *testing.T) {
	harness := newHarness(t, &fakeAdapter{name: "continuation-tenant", response: successfulResponse()})
	store := harness.useMemoryContinuations(t, 7)
	items := []llm.Item{llm.HumanText("hello")}
	_, digest, err := state.CanonicalTranscript(items)
	testassert.NoError(t, err)
	handle, err := store.CreateRoot(context.Background(), state.Continuation{Tenant: "tenant-1", Transcript: items, TranscriptDigest: digest, TranscriptComplete: true, ExpiresAt: harness.clock.Add(time.Hour)})
	testassert.NoError(t, err)

	request := baseRequest("tenantless-continuation")
	request.Context.Tenant = ""
	request.Continuation = &llm.Continuation{Handle: handle.String()}
	if _, _, _, err := harness.engine.loadContinuation(context.Background(), request, harness.clock); err == nil {
		t.Fatal("tenantless continuation request succeeded, want error")
	}

	request.Context.Tenant = "tenant-1"
	if _, _, _, err := harness.engine.loadContinuation(context.Background(), request, harness.clock); err != nil {
		t.Fatalf("tenant-bound continuation request failed: %v", err)
	}
}

func TestGenerateDefaultsOmittedServiceClassToStandard(t *testing.T) {
	adapter, harness := newFakeHarness(t)
	response := harness.mustGenerate(t, baseRequest("default-class"))
	if response.Service.Requested != llm.ServiceClassStandard || response.Service.Attempted != llm.ServiceClassStandard {
		t.Fatalf("service facts = %#v, want standard/standard", response.Service)
	}
	adapter.requireOnlyStandardCall(t, "one standard call")
}

func TestGenerateRejectsUnmatchedRequiredBudgetPolicyBeforeAdmission(t *testing.T) {
	adapter, harness := newFakeHarness(t)
	harness.updateSnapshot(func(snapshot *Snapshot) {
		snapshot.RequireBudgetMatch = true
		snapshot.Environment = "production"
		snapshot.BudgetPolicies = []budget.Policy{hourlyBudgetPolicy("other-tenant", budget.Matcher{Tenant: "other", Environment: "production"})}
	})

	request := baseRequest("unmatched-required-budget")
	mapped := harness.generateFailure(t, request, "unmatched required budget unexpectedly dispatched")
	if _, ok := isNotDispatchedFailure(mapped, provider.CodeNoRoute, provider.PhasePrice); !ok {
		t.Fatalf("budget-policy error = %#v", mapped)
	}
	if mapped.SafeDetails["reason"] != "no_matching_budget_policy" {
		t.Fatalf("budget-policy safe details = %#v, want no_matching_budget_policy", mapped.SafeDetails)
	}
	requireNothingDispatched(t, harness, adapter, request)
}

func TestGenerateUsesOnlyExplicitFallbackWithMatchingRequiredBudgetPolicy(t *testing.T) {
	adapter, harness := newFakeHarness(t)
	harness.updateSnapshot(func(snapshot *Snapshot) {
		snapshot.RequireBudgetMatch = true
		snapshot.BudgetPolicies = []budget.Policy{tenantClassBudgetPolicy("standard-only", llm.ServiceClassStandard)}
	})

	request := priorityWithStandardFallback("budgeted-explicit-fallback")
	response := harness.mustGenerate(t, request)
	if response.Service.Requested != llm.ServiceClassPriority || response.Service.Attempted != llm.ServiceClassStandard || response.Service.FallbackIndex != 1 {
		t.Fatalf("service facts = %#v, want explicit standard fallback", response.Service)
	}
	adapter.requireOnlyStandardCall(t, "only standard fallback")
	operation := harness.operation(t, response.OperationID)
	if len(operation.Reservations) != 1 || operation.Reservations[0].PolicyID != "standard-only" {
		t.Fatalf("operation reservations = %#v, want standard-only policy", operation.Reservations)
	}
}

func TestGenerateCarriesExactReservationAlongsideRedisCompatibilityAmount(t *testing.T) {
	adapter := &fakeAdapter{name: "exact-reservation", response: successfulResponse()}
	base := memory.NewAdmissionStore(memory.AdmissionOptions{Clock: func() time.Time {
		return time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	}})
	recording := &recordingAdmission{AdmissionStore: base}
	harness := newHarnessWithAdmission(t, adapter, recording, func() time.Time {
		return time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	})
	entries := []pricing.Entry{{
		Provider: "provider-1", Family: string(provider.FamilyOpenAIResponses), EndpointID: "endpoint-1",
		Region: "us-east-1", Model: "provider-model", ProviderTier: "standard-tier", Version: "submicro",
		Prices: pricing.UnitPrices{InputPerMillion: pricing.MustDecimalUSD("0"), OutputPerMillion: pricing.MustDecimalUSD("0"), PerRequest: pricing.MustDecimalUSD("0.000000000000000001")},
	}}
	catalog, err := pricing.CompileUSD("submicro", entries)
	testassert.NoError(t, err)
	harness.updateSnapshot(func(snapshot *Snapshot) { snapshot.Prices = pricing.NewResolver(catalog) })
	response := harness.mustGenerate(t, baseRequest("exact-reservation"))
	if got, want := recording.begin.ReservationUSD.String(), "0.000000000000000001"; got != want {
		t.Fatalf("exact reservation = %s, want %s", got, want)
	}
	if recording.begin.Reservation != 1 {
		t.Fatalf("microUSD compatibility reservation = %d, want conservative ceiling 1", recording.begin.Reservation)
	}
	operation := harness.operation(t, response.OperationID)
	if operation.ReservedCostUSD == nil || operation.ReservedCostUSD.String() != "0.000000000000000001" {
		t.Fatalf("durable exact reservation = %v, want one atto-dollar", operation.ReservedCostUSD)
	}
	if response.Cost.ReservedCostUSD == nil || response.Cost.ReservedCostUSD.String() != "0.000000000000000001" {
		t.Fatalf("response exact reservation = %v, want one atto-dollar", response.Cost.ReservedCostUSD)
	}
}

func TestGenerateFallbackDoesNotReplayRejectedDispatch(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", rejectFirst: true, response: successfulResponse()}
	harness := newHarness(t, adapter)
	request := priorityWithStandardFallback("fallback")
	response := harness.mustGenerate(t, request)
	if response.Service.Requested != llm.ServiceClassPriority || response.Service.Attempted != llm.ServiceClassStandard || response.Service.FallbackIndex != 1 {
		t.Fatalf("service facts = %#v, want priority requested and standard fallback", response.Service)
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if len(adapter.calls) != 2 || adapter.calls[0].ServiceClass != llm.ServiceClassPriority || adapter.calls[1].ServiceClass != llm.ServiceClassStandard {
		t.Fatalf("provider calls = %#v, want priority then standard", adapter.calls)
	}
	// Adapters lower the provider tier from the compiled request, so the
	// fallback candidate must be compiled with its attempted class.
	if len(adapter.compiledClasses) != 2 || adapter.compiledClasses[0] != llm.ServiceClassPriority || adapter.compiledClasses[1] != llm.ServiceClassStandard {
		t.Fatalf("compiled request classes = %v, want priority then standard", adapter.compiledClasses)
	}
	operation := harness.operation(t, response.OperationID)
	if operation.State != admissionStateCompleted {
		t.Fatalf("operation state = %s, want completed", operation.State)
	}
}

// certifiedPreDispatchFailures are the first-attempt failures an adapter
// certifies as never having reached the provider.
var certifiedPreDispatchFailures = []struct {
	name        string
	egress      bool
	preDispatch bool
}{
	{name: "policy denial", egress: true},
	{name: "availability", preDispatch: true},
}

func TestGenerateCertifiedPreDispatchFailureAfterMarkFallsBackWithoutAmbiguity(t *testing.T) {
	for _, test := range certifiedPreDispatchFailures {
		t.Run(test.name, func(t *testing.T) {
			adapter := &fakeAdapter{name: "fake", egressFirst: test.egress, preDispatchFirst: test.preDispatch, response: successfulResponse()}
			harness := newHarness(t, adapter)
			request := priorityWithStandardFallback("egress-fallback-" + test.name)
			response := harness.mustGenerate(t, request)
			if response.Service.Attempted != llm.ServiceClassStandard || response.Service.FallbackIndex != 1 {
				t.Fatalf("service facts = %#v, want standard fallback", response.Service)
			}
			adapter.requireInvokes(t, 2, "provider invoke count = %d, want two attempts")
			operation := harness.operation(t, response.OperationID)
			if operation.State != admission.StateCompleted || operation.Attempt.Dispatch != admission.Accepted {
				t.Fatalf("operation after fallback = %#v, want completed accepted operation", operation)
			}
			if response.Cost.ActualCostUSD == nil {
				t.Fatal("fallback response omitted exact actual cost")
			}
			actual, err := compatibilityActualMicroUSD(*response.Cost.ActualCostUSD)
			testassert.NoError(t, err)
			if got, want := int64(operation.IncurredMicroUSD), int64(actual); got != want {
				t.Fatalf("incurred cost = %d, want fallback actual cost %d", got, want)
			}
		})
	}
}

func TestGenerateCertifiedPreDispatchFailureAfterMarkRecordsNoCost(t *testing.T) {
	for _, test := range certifiedPreDispatchFailures {
		t.Run(test.name, func(t *testing.T) {
			adapter := &fakeAdapter{name: "fake", egressFirst: test.egress, preDispatchFirst: test.preDispatch, response: successfulResponse()}
			harness := newHarness(t, adapter)
			mapped := harness.generateFailure(t, baseRequest("egress-no-cost-"+test.name), "egress-denied request unexpectedly succeeded")
			if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute {
				t.Fatalf("mapped egress error = %#v", mapped)
			}
			if test.egress && !errors.Is(mapped, provider.ErrProviderEgressDenied) {
				t.Fatal("mapped policy error did not retain its marker")
			}
			if test.preDispatch && !errors.Is(mapped, provider.ErrProviderPreDispatch) {
				t.Fatal("mapped pre-dispatch error did not retain its marker")
			}
			operation := harness.operation(t, mapped.OperationID)
			if !isZeroCostNoDispatchFailure(operation) {
				t.Fatalf("operation after egress denial = %#v, want definite zero-cost no-dispatch failure", operation)
			}
		})
	}
}

func TestGenerateCertifiedPreDispatchCallerDeadlineAfterMarkDoesNotFallback(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", preDispatchDeadline: true, response: successfulResponse()}
	harness := newHarness(t, adapter)
	request := priorityWithStandardFallback("pre-dispatch-caller-deadline")

	mapped := harness.generateFailure(t, request, "certified caller deadline unexpectedly succeeded")
	if mapped.Code != provider.CodeDeadlineExceeded || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever {
		t.Fatalf("mapped caller deadline = %#v, want non-retryable not-dispatched deadline", mapped)
	}
	if !errors.Is(mapped, provider.ErrProviderPreDispatch) || !errors.Is(mapped, context.DeadlineExceeded) {
		t.Fatalf("mapped caller deadline = %v, want certified pre-dispatch deadline cause", mapped)
	}
	adapter.requireInvokes(t, 1, "provider invoke count = %d, want one without fallback")
	operation := harness.operation(t, mapped.OperationID)
	if !isZeroCostNoDispatchFailure(operation) {
		t.Fatalf("operation after certified caller deadline = %#v, want definite zero-cost no-dispatch failure", operation)
	}
}

func TestGenerateRawDeadlineAfterMarkRemainsAmbiguous(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", rawDeadlineFirst: true, response: successfulResponse()}
	harness := newHarness(t, adapter)
	request := priorityWithStandardFallback("raw-post-mark-deadline")

	mapped := harness.generateFailure(t, request, "raw post-mark deadline unexpectedly succeeded")
	if mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever {
		t.Fatalf("mapped raw deadline = %#v, want ambiguous non-retryable result", mapped)
	}
	adapter.requireInvokes(t, 1, "provider invoke count = %d, want one without fallback")
}

func TestGenerateRedirectResponseAfterMarkDoesNotFallback(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", redirectFirst: true, response: successfulResponse()}
	harness := newHarness(t, adapter)
	request := priorityWithStandardFallback("redirect-no-fallback")
	mapped := harness.generateFailure(t, request, "redirect response unexpectedly succeeded")
	if mapped.Code != provider.CodeAmbiguousDispatch || mapped.Phase != provider.PhaseDispatch || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever {
		t.Fatalf("mapped redirect error = %#v, want ambiguous non-retriable dispatch error", mapped)
	}
	adapter.requireInvokes(t, 1, "provider invoke count = %d, want one without fallback resend")
	operation := harness.operation(t, mapped.OperationID)
	if operation.State != admission.StateAmbiguous || operation.Attempt.Dispatch != admission.Ambiguous {
		t.Fatalf("operation after redirect = %#v, want ambiguous operation", operation)
	}
}

func TestGenerateReplaysCompletedResultWithoutProviderCall(t *testing.T) {
	adapter, harness := newFakeHarness(t)
	request := baseRequest("replay")
	first := harness.mustGenerate(t, request)
	second := harness.mustGenerate(t, request)
	if first.OperationID == "" || first.OperationID != second.OperationID || first.Output[0].(llm.Message).Content[0].(llm.TextPart).Text != second.Output[0].(llm.Message).Content[0].(llm.TextPart).Text {
		t.Fatalf("replayed response differs: first=%#v second=%#v", first, second)
	}
	adapter.requireInvokes(t, 1, "provider invoke count = %d, want one")
	if harness.results.puts != 1 {
		t.Fatalf("result writes = %d, want one", harness.results.puts)
	}
}

func TestGenerateDoesNotRetryAmbiguousOperation(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", ambiguous: true, response: successfulResponse()}
	harness := newHarness(t, adapter)
	request := baseRequest("ambiguous")
	for _, attempt := range []struct{ request, err string }{{"first", "first"}, {"replayed", "second"}} {
		_, err := harness.engine.Generate(context.Background(), request)
		if err == nil {
			t.Fatalf("%s ambiguous request unexpectedly succeeded", attempt.request)
		}
		var mapped *provider.Error
		if !errors.As(err, &mapped) || mapped.Code != provider.CodeAmbiguousDispatch {
			t.Fatalf("%s error = %#v, want ambiguous dispatch", attempt.err, err)
		}
	}
	adapter.requireInvokes(t, 1, "provider invoke count = %d, want one")
}

// Keep the test independent of an exported alias solely to make the expected
// admission state obvious at the assertion site.
const admissionStateCompleted = "completed"
