package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	memory "github.com/mfow/llm-temporal-worker/golang/storage/memory"
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
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]pricing.Entry, 0, len(classes))
	for _, class := range classes {
		entries = append(entries, pricing.Entry{Provider: "provider-1", Family: string(provider.FamilyOpenAIResponses), EndpointID: "endpoint-1", Region: "us-east-1", Model: "provider-model", ProviderTier: tiers[class], Version: "prices-1", Prices: pricing.UnitPrices{PerRequest: pricing.MustDecimalUSD("0.000001"), OutputPerMillion: pricing.MustDecimalUSD("1")}})
	}
	priceCatalog, err := pricing.CompileUSD("prices-1", entries)
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	return testHarness{engine: engineValue, admission: admissionStore, results: results, clock: clock()}
}

func baseRequest(operationKey string) llm.Request {
	return llm.Request{OperationKey: operationKey, Context: llm.RequestContext{Tenant: "tenant-1"}, Model: "logical-model", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}}
}

func successfulResponse() llm.Response {
	return llm.Response{Status: llm.ResponseStatusCompleted, Output: []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "world"}}}}, Usage: llm.Usage{OutputTokens: 1}, Provider: llm.ProviderFacts{RequestID: "provider-request-1"}}
}

func TestLoadContinuationRequiresRequestTenant(t *testing.T) {
	harness := newHarness(t, &fakeAdapter{name: "continuation-tenant", response: successfulResponse()})
	keyring, err := state.NewKeyring([]state.Key{{ID: "k1", Secret: bytes.Repeat([]byte{7}, 32), Primary: true}}, bytes.NewReader(bytes.Repeat([]byte{8}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	store, err := memory.NewContinuationStore(memory.ContinuationOptions{Keyring: keyring, Clock: func() time.Time { return harness.clock }})
	if err != nil {
		t.Fatal(err)
	}
	harness.engine.dependencies.Continuations = store
	items := []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}
	_, digest, err := state.CanonicalTranscript(items)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := store.CreateRoot(context.Background(), state.Continuation{Tenant: "tenant-1", Transcript: items, TranscriptDigest: digest, TranscriptComplete: true, ExpiresAt: harness.clock.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}

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
	adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
	harness := newHarness(t, adapter)
	response, err := harness.engine.Generate(context.Background(), baseRequest("default-class"))
	if err != nil {
		t.Fatal(err)
	}
	if response.Service.Requested != llm.ServiceClassStandard || response.Service.Attempted != llm.ServiceClassStandard {
		t.Fatalf("service facts = %#v, want standard/standard", response.Service)
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if len(adapter.calls) != 1 || adapter.calls[0].ServiceClass != llm.ServiceClassStandard {
		t.Fatalf("provider calls = %#v, want one standard call", adapter.calls)
	}
}

func TestGenerateRejectsUnmatchedRequiredBudgetPolicyBeforeAdmission(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
	harness := newHarness(t, adapter)
	snapshot := harness.engine.dependencies.Snapshots.(StaticSnapshot).Value
	snapshot.RequireBudgetMatch = true
	snapshot.Environment = "production"
	snapshot.BudgetPolicies = []budget.Policy{{
		ID:      "other-tenant",
		Match:   budget.Matcher{Tenant: "other", Environment: "production"},
		Windows: []budget.Window{{ID: "other-tenant/hour", Duration: time.Hour, Bucket: time.Minute, Limit: 1_000}},
	}}
	harness.engine.dependencies.Snapshots = StaticSnapshot{Value: snapshot}

	request := baseRequest("unmatched-required-budget")
	_, err := harness.engine.Generate(context.Background(), request)
	if err == nil {
		t.Fatal("unmatched required budget unexpectedly dispatched")
	}
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("error = %T, want *provider.Error", err)
	}
	if mapped.Code != provider.CodeNoRoute || mapped.Phase != provider.PhasePrice || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever {
		t.Fatalf("budget-policy error = %#v", mapped)
	}
	if mapped.SafeDetails["reason"] != "no_matching_budget_policy" {
		t.Fatalf("budget-policy safe details = %#v, want no_matching_budget_policy", mapped.SafeDetails)
	}
	adapter.mu.Lock()
	invokes := adapter.invokes
	adapter.mu.Unlock()
	if invokes != 0 {
		t.Fatalf("provider invoke count = %d, want zero", invokes)
	}
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	operationID, _ := operationIdentity(normalized)
	if _, getErr := harness.admission.Get(context.Background(), operationID); !errors.Is(getErr, admission.ErrOperationNotFound) {
		t.Fatalf("admission Get(%q) error = %v, want ErrOperationNotFound", operationID, getErr)
	}
}

func TestGenerateUsesOnlyExplicitFallbackWithMatchingRequiredBudgetPolicy(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
	harness := newHarness(t, adapter)
	snapshot := harness.engine.dependencies.Snapshots.(StaticSnapshot).Value
	snapshot.RequireBudgetMatch = true
	snapshot.BudgetPolicies = []budget.Policy{{
		ID:      "standard-only",
		Match:   budget.Matcher{Tenant: "tenant-1", ServiceClass: llm.ServiceClassStandard},
		Windows: []budget.Window{{ID: "standard-only/hour", Duration: time.Hour, Bucket: time.Minute, Limit: 1_000}},
	}}
	harness.engine.dependencies.Snapshots = StaticSnapshot{Value: snapshot}

	request := baseRequest("budgeted-explicit-fallback")
	request.ServiceClass = llm.ServiceClassPriority
	request.ServiceClassFallbacks = []llm.ServiceClass{llm.ServiceClassStandard}
	response, err := harness.engine.Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Service.Requested != llm.ServiceClassPriority || response.Service.Attempted != llm.ServiceClassStandard || response.Service.FallbackIndex != 1 {
		t.Fatalf("service facts = %#v, want explicit standard fallback", response.Service)
	}
	adapter.mu.Lock()
	calls := append([]provider.Call(nil), adapter.calls...)
	adapter.mu.Unlock()
	if len(calls) != 1 || calls[0].ServiceClass != llm.ServiceClassStandard {
		t.Fatalf("provider calls = %#v, want only standard fallback", calls)
	}
	operation, err := harness.admission.Get(context.Background(), response.OperationID)
	if err != nil {
		t.Fatal(err)
	}
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
	snapshot := harness.engine.dependencies.Snapshots.(StaticSnapshot).Value
	entries := []pricing.Entry{{
		Provider: "provider-1", Family: string(provider.FamilyOpenAIResponses), EndpointID: "endpoint-1",
		Region: "us-east-1", Model: "provider-model", ProviderTier: "standard-tier", Version: "submicro",
		Prices: pricing.UnitPrices{InputPerMillion: pricing.MustDecimalUSD("0"), OutputPerMillion: pricing.MustDecimalUSD("0"), PerRequest: pricing.MustDecimalUSD("0.000000000000000001")},
	}}
	catalog, err := pricing.CompileUSD("submicro", entries)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Prices = pricing.NewResolver(catalog)
	harness.engine.dependencies.Snapshots = StaticSnapshot{Value: snapshot}
	response, err := harness.engine.Generate(context.Background(), baseRequest("exact-reservation"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := recording.begin.ReservationUSD.String(), "0.000000000000000001"; got != want {
		t.Fatalf("exact reservation = %s, want %s", got, want)
	}
	if recording.begin.Reservation != 1 {
		t.Fatalf("microUSD compatibility reservation = %d, want conservative ceiling 1", recording.begin.Reservation)
	}
	operation, err := harness.admission.Get(context.Background(), response.OperationID)
	if err != nil {
		t.Fatal(err)
	}
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
	request := baseRequest("fallback")
	request.ServiceClass = llm.ServiceClassPriority
	request.ServiceClassFallbacks = []llm.ServiceClass{llm.ServiceClassStandard}
	response, err := harness.engine.Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Service.Requested != llm.ServiceClassPriority || response.Service.Attempted != llm.ServiceClassStandard || response.Service.FallbackIndex != 1 {
		t.Fatalf("service facts = %#v, want priority requested and standard fallback", response.Service)
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if len(adapter.calls) != 2 || adapter.calls[0].ServiceClass != llm.ServiceClassPriority || adapter.calls[1].ServiceClass != llm.ServiceClassStandard {
		t.Fatalf("provider calls = %#v, want priority then standard", adapter.calls)
	}
	operation, err := harness.admission.Get(context.Background(), response.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if operation.State != admissionStateCompleted {
		t.Fatalf("operation state = %s, want completed", operation.State)
	}
}

func TestGenerateCertifiedPreDispatchFailureAfterMarkFallsBackWithoutAmbiguity(t *testing.T) {
	for _, test := range []struct {
		name        string
		egress      bool
		preDispatch bool
	}{
		{name: "policy denial", egress: true},
		{name: "availability", preDispatch: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter := &fakeAdapter{name: "fake", egressFirst: test.egress, preDispatchFirst: test.preDispatch, response: successfulResponse()}
			harness := newHarness(t, adapter)
			request := baseRequest("egress-fallback-" + test.name)
			request.ServiceClass = llm.ServiceClassPriority
			request.ServiceClassFallbacks = []llm.ServiceClass{llm.ServiceClassStandard}
			response, err := harness.engine.Generate(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if response.Service.Attempted != llm.ServiceClassStandard || response.Service.FallbackIndex != 1 {
				t.Fatalf("service facts = %#v, want standard fallback", response.Service)
			}
			adapter.mu.Lock()
			invokes := adapter.invokes
			adapter.mu.Unlock()
			if invokes != 2 {
				t.Fatalf("provider invoke count = %d, want two attempts", invokes)
			}
			operation, err := harness.admission.Get(context.Background(), response.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			if operation.State != admission.StateCompleted || operation.Attempt.Dispatch != admission.Accepted {
				t.Fatalf("operation after fallback = %#v, want completed accepted operation", operation)
			}
			if response.Cost.ActualCostUSD == nil {
				t.Fatal("fallback response omitted exact actual cost")
			}
			actual, err := compatibilityActualMicroUSD(*response.Cost.ActualCostUSD)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := int64(operation.IncurredMicroUSD), int64(actual); got != want {
				t.Fatalf("incurred cost = %d, want fallback actual cost %d", got, want)
			}
		})
	}
}

func TestGenerateCertifiedPreDispatchFailureAfterMarkRecordsNoCost(t *testing.T) {
	for _, test := range []struct {
		name        string
		egress      bool
		preDispatch bool
	}{
		{name: "policy denial", egress: true},
		{name: "availability", preDispatch: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter := &fakeAdapter{name: "fake", egressFirst: test.egress, preDispatchFirst: test.preDispatch, response: successfulResponse()}
			harness := newHarness(t, adapter)
			_, err := harness.engine.Generate(context.Background(), baseRequest("egress-no-cost-"+test.name))
			if err == nil {
				t.Fatal("egress-denied request unexpectedly succeeded")
			}
			var mapped *provider.Error
			if !errors.As(err, &mapped) {
				t.Fatalf("error = %T, want *provider.Error", err)
			}
			if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute {
				t.Fatalf("mapped egress error = %#v", mapped)
			}
			if test.egress && !errors.Is(mapped, provider.ErrProviderEgressDenied) {
				t.Fatal("mapped policy error did not retain its marker")
			}
			if test.preDispatch && !errors.Is(mapped, provider.ErrProviderPreDispatch) {
				t.Fatal("mapped pre-dispatch error did not retain its marker")
			}
			operation, err := harness.admission.Get(context.Background(), mapped.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			if operation.State != admission.StateDefiniteFailed || operation.Attempt.Dispatch != admission.NotDispatched || operation.IncurredMicroUSD != 0 || operation.FinalMicroUSD != 0 {
				t.Fatalf("operation after egress denial = %#v, want definite zero-cost no-dispatch failure", operation)
			}
		})
	}
}

func TestGenerateCertifiedPreDispatchCallerDeadlineAfterMarkDoesNotFallback(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", preDispatchDeadline: true, response: successfulResponse()}
	harness := newHarness(t, adapter)
	request := baseRequest("pre-dispatch-caller-deadline")
	request.ServiceClass = llm.ServiceClassPriority
	request.ServiceClassFallbacks = []llm.ServiceClass{llm.ServiceClassStandard}

	_, err := harness.engine.Generate(context.Background(), request)
	if err == nil {
		t.Fatal("certified caller deadline unexpectedly succeeded")
	}
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("error = %T, want *provider.Error", err)
	}
	if mapped.Code != provider.CodeDeadlineExceeded || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever {
		t.Fatalf("mapped caller deadline = %#v, want non-retryable not-dispatched deadline", mapped)
	}
	if !errors.Is(mapped, provider.ErrProviderPreDispatch) || !errors.Is(mapped, context.DeadlineExceeded) {
		t.Fatalf("mapped caller deadline = %v, want certified pre-dispatch deadline cause", mapped)
	}
	adapter.mu.Lock()
	invokes := adapter.invokes
	adapter.mu.Unlock()
	if invokes != 1 {
		t.Fatalf("provider invoke count = %d, want one without fallback", invokes)
	}
	operation, getErr := harness.admission.Get(context.Background(), mapped.OperationID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if operation.State != admission.StateDefiniteFailed || operation.Attempt.Dispatch != admission.NotDispatched || operation.IncurredMicroUSD != 0 || operation.FinalMicroUSD != 0 {
		t.Fatalf("operation after certified caller deadline = %#v, want definite zero-cost no-dispatch failure", operation)
	}
}

func TestGenerateRawDeadlineAfterMarkRemainsAmbiguous(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", rawDeadlineFirst: true, response: successfulResponse()}
	harness := newHarness(t, adapter)
	request := baseRequest("raw-post-mark-deadline")
	request.ServiceClass = llm.ServiceClassPriority
	request.ServiceClassFallbacks = []llm.ServiceClass{llm.ServiceClassStandard}

	_, err := harness.engine.Generate(context.Background(), request)
	if err == nil {
		t.Fatal("raw post-mark deadline unexpectedly succeeded")
	}
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("error = %T, want *provider.Error", err)
	}
	if mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever {
		t.Fatalf("mapped raw deadline = %#v, want ambiguous non-retryable result", mapped)
	}
	adapter.mu.Lock()
	invokes := adapter.invokes
	adapter.mu.Unlock()
	if invokes != 1 {
		t.Fatalf("provider invoke count = %d, want one without fallback", invokes)
	}
}

func TestGenerateRedirectResponseAfterMarkDoesNotFallback(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", redirectFirst: true, response: successfulResponse()}
	harness := newHarness(t, adapter)
	request := baseRequest("redirect-no-fallback")
	request.ServiceClass = llm.ServiceClassPriority
	request.ServiceClassFallbacks = []llm.ServiceClass{llm.ServiceClassStandard}
	_, err := harness.engine.Generate(context.Background(), request)
	if err == nil {
		t.Fatal("redirect response unexpectedly succeeded")
	}
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("error = %T, want *provider.Error", err)
	}
	if mapped.Code != provider.CodeAmbiguousDispatch || mapped.Phase != provider.PhaseDispatch || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever {
		t.Fatalf("mapped redirect error = %#v, want ambiguous non-retriable dispatch error", mapped)
	}
	adapter.mu.Lock()
	invokes := adapter.invokes
	adapter.mu.Unlock()
	if invokes != 1 {
		t.Fatalf("provider invoke count = %d, want one without fallback resend", invokes)
	}
	operation, getErr := harness.admission.Get(context.Background(), mapped.OperationID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if operation.State != admission.StateAmbiguous || operation.Attempt.Dispatch != admission.Ambiguous {
		t.Fatalf("operation after redirect = %#v, want ambiguous operation", operation)
	}
}

func TestGenerateReplaysCompletedResultWithoutProviderCall(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
	harness := newHarness(t, adapter)
	request := baseRequest("replay")
	first, err := harness.engine.Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := harness.engine.Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.OperationID == "" || first.OperationID != second.OperationID || first.Output[0].(llm.Message).Content[0].(llm.TextPart).Text != second.Output[0].(llm.Message).Content[0].(llm.TextPart).Text {
		t.Fatalf("replayed response differs: first=%#v second=%#v", first, second)
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if adapter.invokes != 1 {
		t.Fatalf("provider invoke count = %d, want one", adapter.invokes)
	}
	if harness.results.puts != 1 {
		t.Fatalf("result writes = %d, want one", harness.results.puts)
	}
}

func TestGenerateDoesNotRetryAmbiguousOperation(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", ambiguous: true, response: successfulResponse()}
	harness := newHarness(t, adapter)
	request := baseRequest("ambiguous")
	_, firstErr := harness.engine.Generate(context.Background(), request)
	if firstErr == nil {
		t.Fatal("first ambiguous request unexpectedly succeeded")
	}
	var firstProviderErr *provider.Error
	if !errors.As(firstErr, &firstProviderErr) || firstProviderErr.Code != provider.CodeAmbiguousDispatch {
		t.Fatalf("first error = %#v, want ambiguous dispatch", firstErr)
	}
	_, secondErr := harness.engine.Generate(context.Background(), request)
	if secondErr == nil {
		t.Fatal("replayed ambiguous request unexpectedly succeeded")
	}
	var secondProviderErr *provider.Error
	if !errors.As(secondErr, &secondProviderErr) || secondProviderErr.Code != provider.CodeAmbiguousDispatch {
		t.Fatalf("second error = %#v, want ambiguous dispatch", secondErr)
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if adapter.invokes != 1 {
		t.Fatalf("provider invoke count = %d, want one", adapter.invokes)
	}
}

// Keep the test independent of an exported alias solely to make the expected
// admission state obvious at the assertion site.
const admissionStateCompleted = "completed"
