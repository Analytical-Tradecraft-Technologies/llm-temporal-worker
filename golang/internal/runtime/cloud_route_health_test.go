package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/control"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type sharedHealthFixture struct {
	mu           sync.Mutex
	status       control.RouteStatus
	observations int
}

func (h *sharedHealthFixture) RecordProviderStatus(_ context.Context, o control.StatusObservation) error {
	event, err := control.NewStatusEvent(o)
	if err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.status.Apply(event) {
		h.observations++
	}
	return nil
}
func (h *sharedHealthFixture) GetRouteStatus(_ context.Context, digest [32]byte, route, endpoint, model, tier string) (control.RouteStatus, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.status.RouteID != route || h.status.EndpointID != endpoint || h.status.Model != model || h.status.ProviderTier != tier || h.status.ConfigDigest != digest {
		return control.RouteStatus{}, control.ErrProviderStatusNotFound
	}
	return h.status, nil
}

func TestCloudRouteHealthAcrossWorkersAndCooldown(t *testing.T) {
	f := boundedCloud(t, false)
	shared := &sharedHealthFixture{}
	f.cap.ProviderRouteStatus = shared
	f.cap.ProviderStatusRecorder = shared
	f.restart(t)
	success := f.adapter.invoke
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		return provider.Result{}, provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNextRoute, "unavailable")
	}
	for i := 0; i < 3; i++ {
		f.request.OperationKey = fmt.Sprintf("failure-%d", i)
		result, err := f.runtime.GenerateStepV1(context.Background(), f.request)
		boundedState(t, result, err, llm.ExecutionFailed)
		f.now = f.now.Add(time.Second)
		f.restart(t)
	}
	if shared.status.Circuit != control.CircuitOpen || shared.observations != 3 {
		t.Fatalf("shared failures missing: %+v events=%d", shared.status, shared.observations)
	}
	f.request.OperationKey = "blocked"
	_, err := f.runtime.GenerateStepV1(context.Background(), f.request)
	var classified *provider.Error
	if !errors.As(err, &classified) || classified.Code != provider.CodeProviderUnavailable || classified.Retry == provider.RetryNever {
		t.Fatal("open circuit not retryable", err)
	}
	if f.submits.Load() != 3 {
		t.Fatal("open circuit dispatched")
	}
	f.now = f.now.Add(time.Minute)
	f.adapter.invoke = success
	f.restart(t)
	result, err := f.runtime.GenerateStepV1(context.Background(), f.request)
	boundedState(t, result, err, llm.ExecutionProviderCompleted)
	if shared.status.Circuit != control.CircuitClosed || shared.status.ConsecutiveDefiniteFailures != 0 {
		t.Fatal("successful probe did not close circuit")
	}
	before := shared.observations
	ref := llm.ExecutionReferenceV1{RequestID: result.RequestID, Context: f.request.Context}
	_, err = f.runtime.PollExecutionV1(context.Background(), ref)
	if err != nil || shared.observations != before {
		t.Fatal("replay duplicated health observation", err)
	}
}

func TestCloudUnknownOutcomeDoesNotOpenCircuit(t *testing.T) {
	f := boundedCloud(t, false)
	shared := &sharedHealthFixture{}
	f.cap.ProviderStatusRecorder = shared
	f.restart(t)
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		return provider.Result{}, errors.New("response lost")
	}
	result, err := f.runtime.GenerateStepV1(context.Background(), f.request)
	boundedState(t, result, err, llm.ExecutionPending)
	if shared.observations != 0 {
		t.Fatal("unknown outcome counted as endpoint failure")
	}
}

func TestCloudOpenCircuitDoesNotAbandonPendingProviderWork(t *testing.T) {
	f := boundedCloud(t, true)
	shared := &sharedHealthFixture{}
	f.cap.ProviderRouteStatus = shared
	f.cap.ProviderStatusRecorder = shared
	f.restart(t)
	result, err := f.runtime.GenerateStepV1(context.Background(), f.request)
	boundedState(t, result, err, llm.ExecutionPending)
	snapshot, err := f.cap.Snapshot.Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	route := snapshot.Routes.Models["alias"].Routes[0]
	shared.status = control.RouteStatus{ConfigDigest: snapshot.ConfigDigest, ConfigEpoch: snapshot.ConfigEpoch, RouteID: route.ID, EndpointID: route.EndpointID, EndpointAccountHMAC: route.EndpointAccountHMAC, Provider: route.Provider, EndpointFamily: route.Family, Model: route.Model, ProviderTier: route.ProviderTiers[llm.ServiceClassStandard], Circuit: control.CircuitOpen, ObservedAt: f.now, StaleAfter: f.now.Add(time.Minute), Credit: control.CreditOK, Billing: control.BillingOK}
	f.now = f.now.Add(2 * time.Second)
	f.restart(t)
	ref := llm.ExecutionReferenceV1{RequestID: result.RequestID, Context: f.request.Context}
	result, err = f.runtime.PollExecutionV1(context.Background(), ref)
	boundedState(t, result, err, llm.ExecutionProviderCompleted)
	if f.submits.Load() != 1 || f.polls.Load() != 1 {
		t.Fatal("paid recovery abandoned or resubmitted")
	}
}

// keyedHealthFixture holds one projection per route, endpoint, provider model
// and provider tier under a configuration digest, exactly like
// storage/redis.ProviderStateStore.
type keyedHealthFixture struct {
	mu      sync.Mutex
	records map[[4]string]control.RouteStatus
}

func (h *keyedHealthFixture) RecordProviderStatus(_ context.Context, o control.StatusObservation) error {
	event, err := control.NewStatusEvent(o)
	if err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.records == nil {
		h.records = map[[4]string]control.RouteStatus{}
	}
	key := [4]string{o.RouteID, o.EndpointID, o.Model, o.ProviderTier}
	status := h.records[key]
	if status.Apply(event) {
		h.records[key] = status
	}
	return nil
}

func (h *keyedHealthFixture) GetRouteStatus(_ context.Context, digest [32]byte, route, endpoint, model, tier string) (control.RouteStatus, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	status, ok := h.records[[4]string{route, endpoint, model, tier}]
	if !ok || status.ConfigDigest != digest {
		return control.RouteStatus{}, control.ErrProviderStatusNotFound
	}
	return status, nil
}

// Route IDs are unique per model only. Two models that both name a route
// "route" on different endpoints must keep separate health records, so traffic
// on one model cannot break the other.
func TestCloudRouteHealthSameRouteIDAcrossModels(t *testing.T) {
	f := boundedCloud(t, false, func(b *budgetPlanningFixture) {
		first := b.source.value.Routes.Models["alias"].Routes[0]
		second := first
		second.EndpointID, second.EndpointAccountHMAC = "endpoint-two", [32]byte{9}
		b.source.value.Routes.Models["alias-two"] = routing.Model{Name: "alias-two", Routes: []routing.Route{second}}
		policy := b.source.value.BudgetPolicies[0]
		policy.ID, policy.Match.LogicalModel, policy.Match.EndpointID = "policy-two", "alias-two", "endpoint-two"
		policy.Windows = []budget.Window{{ID: "policy-two/hour", Duration: time.Hour, Bucket: 5 * time.Minute, LimitUSD: pricing.MustUSD("100")}}
		b.source.value.BudgetPolicies = append(b.source.value.BudgetPolicies, policy)
		entry := b.entry
		entry.EndpointID = "endpoint-two"
		b.prices(t, []pricing.Entry{b.entry, entry})
	})
	shared := &keyedHealthFixture{}
	f.cap.ProviderRouteStatus, f.cap.ProviderStatusRecorder = shared, shared
	f.cap.Adapters = engine.AdapterMap{"endpoint": f.adapter.executionSyncAdapter, "endpoint-two": f.adapter.executionSyncAdapter}
	f.restart(t)
	result, err := f.runtime.GenerateStepV1(context.Background(), f.request)
	boundedState(t, result, err, llm.ExecutionProviderCompleted)
	if len(shared.records) != 1 {
		t.Fatalf("first model did not record one route status: %+v", shared.records)
	}
	f.now = f.now.Add(time.Second)
	f.restart(t)
	f.request.OperationKey = "second-model"
	f.request.SettingsPatch.Model.Set = preparationPointer("alias-two")
	result, err = f.runtime.GenerateStepV1(context.Background(), f.request)
	boundedState(t, result, err, llm.ExecutionProviderCompleted)
	if len(shared.records) != 2 || f.submits.Load() != 2 {
		t.Fatalf("second model shared the first model's route status: %+v submits=%d", shared.records, f.submits.Load())
	}
}

// A stored record whose identity differs from the candidate (for example a
// route repointed to another account) says nothing about that candidate. It is
// ignored, even when it reports an open circuit, instead of failing planning.
func TestCloudRouteHealthIgnoresMismatchedIdentity(t *testing.T) {
	f := boundedCloud(t, false)
	shared := &sharedHealthFixture{}
	f.cap.ProviderRouteStatus = shared
	snapshot, err := f.cap.Snapshot.Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	route := snapshot.Routes.Models["alias"].Routes[0]
	shared.status = control.RouteStatus{ConfigDigest: snapshot.ConfigDigest, ConfigEpoch: snapshot.ConfigEpoch, RouteID: route.ID, EndpointID: route.EndpointID, EndpointAccountHMAC: [32]byte{9}, Provider: "other-provider", EndpointFamily: route.Family, Model: route.Model, ProviderTier: route.ProviderTiers[llm.ServiceClassStandard], Circuit: control.CircuitOpen, ObservedAt: f.now, StaleAfter: f.now.Add(time.Minute), Credit: control.CreditExhausted, Billing: control.BillingOK}
	f.restart(t)
	result, err := f.runtime.GenerateStepV1(context.Background(), f.request)
	boundedState(t, result, err, llm.ExecutionProviderCompleted)
	if f.submits.Load() != 1 {
		t.Fatal("mismatched route status blocked the candidate")
	}
}

// The reported configuration: two models, each with a route "primary" on a
// different endpoint. It runs the real configuration compiler, snapshot
// loader and planner, records a completed request for the first model through
// the cloud recorder, and then asks route health about the second model.
func TestCloudRouteHealthConfiguredDuplicateRouteIDs(t *testing.T) {
	read := func(name string) string {
		data, err := os.ReadFile(filepath.Join("../../deploy/local", name))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	data, prices := read("config.yaml"), read("prices.yaml")
	_, endpoint, ok := strings.Cut(data, "\n  provider-mock:\n")
	endpoint, _, found := strings.Cut(endpoint, "\nmodels:\n")
	_, models, hasModels := strings.Cut(data, "\nmodels:\n")
	models, _, hasCatalogs := strings.Cut(models, "\ncapabilities:\n")
	_, entries, hasEntries := strings.Cut(prices, "entries:\n")
	if !ok || !found || !hasModels || !hasCatalogs || !hasEntries {
		t.Fatal("local fixture changed shape")
	}
	second := "\n  provider-mock-two:\n" + strings.ReplaceAll(endpoint, "provider-mock", "provider-mock-two")
	route := "\n    allowed_tenants: [local]\n    data_regions: [local]\n    routes:\n      - id: primary\n        model: demo-model\n        classes: [economy, standard, priority]\n        endpoint: "
	data = strings.Replace(data, endpoint, endpoint+second, 1)
	data = strings.Replace(data, models, "  chat:"+route+"provider-mock\n  summarize:"+route+"provider-mock-two\n", 1)
	oldPrices := sha256.Sum256([]byte(prices))
	prices += strings.ReplaceAll(entries, "endpoint: provider-mock", "endpoint: provider-mock-two")
	newPrices := sha256.Sum256([]byte(prices))
	data = strings.Replace(data, hex.EncodeToString(oldPrices[:]), hex.EncodeToString(newPrices[:]), 1)
	directory := t.TempDir()
	for name, content := range map[string]string{"capabilities.yaml": read("capabilities.yaml"), "prices.yaml": prices} {
		path := filepath.Join(directory, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		data = strings.ReplaceAll(data, "/etc/llmtw/"+name, path)
	}
	compiled, err := config.Compile(context.Background(), []byte(data), nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 4, 1, 0, 0, 0, time.UTC)
	loaded, err := (CatalogSnapshotLoader{Clock: func() time.Time { return now }}).Load(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	candidate := func(model string) routing.Candidate {
		t.Helper()
		request := llm.Request{OperationKey: "operation", Model: model, ServiceClass: llm.ServiceClassStandard, Context: llm.RequestContext{Tenant: "local", Project: "project", Actor: "actor"}, Input: []llm.Item{preparationMessage("hello")},
			Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindText}, MaxTokens: preparationPointer(16)}}
		plan, err := routing.DeterministicPlanner{}.Plan(context.Background(), routing.Input{Request: request, Catalog: loaded.Routes, Health: loaded.Health, Now: now})
		if err != nil || len(plan.Candidates) == 0 {
			t.Fatalf("model %s has no candidate: %+v %v", model, plan.Rejections, err)
		}
		return plan.Candidates[0]
	}
	chat, summarize := candidate("chat"), candidate("summarize")
	if chat.RouteID != "primary" || summarize.RouteID != "primary" || chat.EndpointID == summarize.EndpointID {
		t.Fatalf("fixture does not reuse one route ID across endpoints: %+v %+v", chat, summarize)
	}
	shared := &keyedHealthFixture{}
	planning := &ProviderPlanning{routeStatus: shared, clock: func() time.Time { return now }, configDigest: loaded.ConfigDigest, configEpoch: loaded.ConfigEpoch}
	(&CloudProviderExecution{statusRecorder: shared}).recordRouteStatus(context.Background(), cloudstate.SavedProviderExecution{
		Plan: cloudstate.BudgetPlan{ConfigDigest: loaded.ConfigDigest, ConfigEpoch: loaded.ConfigEpoch, Family: chat.Family, ProviderTier: chat.ProviderTier, Route: durable.RoutePlan{OperationID: "operation", RouteID: chat.RouteID, EndpointID: chat.EndpointID, Provider: chat.Provider, Model: chat.Model,
			CacheIdentity: cache.RouteIdentity{Account: cache.Account(hex.EncodeToString(chat.EndpointAccountHMAC[:]))}}},
		Execution: cloudstate.ProviderExecution{Stage: cloudstate.ExecutionSucceeded, CompletedAt: now},
	})
	if len(shared.records) != 1 {
		t.Fatalf("chat success was not recorded: %+v", shared.records)
	}
	for _, check := range []routing.Candidate{chat, summarize} {
		if blocked, err := planning.routeBlocked(context.Background(), check); err != nil || blocked {
			t.Fatalf("endpoint %s after chat traffic: blocked=%v err=%v", check.EndpointID, blocked, err)
		}
	}
}

// Model sync gives every synced model on an endpoint one route ID, and
// provider rate limits are per model and tier. Failures of one model or tier
// must not open the circuit of another, and a sibling's success must not reset
// the failing model's count.
func TestCloudRouteHealthScopedToModelAndTier(t *testing.T) {
	now := time.Date(2026, 10, 4, 1, 0, 0, 0, time.UTC)
	digest := sha256.Sum256([]byte("config"))
	account := [32]byte{7}
	base := routing.Candidate{RouteID: "sync-openai-direct", EndpointID: "openai-direct", Provider: "openai", Family: "responses", EndpointAccountHMAC: account}
	scoped := func(model, tier string) routing.Candidate {
		candidate := base
		candidate.Model, candidate.ProviderTier = model, tier
		return candidate
	}
	x, y, xStandard := scoped("gpt-5-mini", "flex"), scoped("gpt-6-astra", "flex"), scoped("gpt-5-mini", "default")
	shared := &keyedHealthFixture{}
	planning := &ProviderPlanning{routeStatus: shared, clock: func() time.Time { return now }, configDigest: digest, configEpoch: "epoch"}
	execution := &CloudProviderExecution{statusRecorder: shared}
	operation := 0
	record := func(candidate routing.Candidate, failure *cloudstate.ExecutionFailure) {
		t.Helper()
		operation++
		now = now.Add(time.Second)
		stage := cloudstate.ExecutionSucceeded
		if failure != nil {
			stage = cloudstate.ExecutionFailed
		}
		execution.recordRouteStatus(context.Background(), cloudstate.SavedProviderExecution{
			Plan: cloudstate.BudgetPlan{ConfigDigest: digest, ConfigEpoch: "epoch", Family: candidate.Family, ProviderTier: candidate.ProviderTier, Route: durable.RoutePlan{OperationID: durable.OperationID(fmt.Sprintf("operation-%d", operation)), RouteID: candidate.RouteID, EndpointID: candidate.EndpointID, Provider: candidate.Provider, Model: candidate.Model,
				CacheIdentity: cache.RouteIdentity{Account: cache.Account(hex.EncodeToString(account[:]))}}},
			Execution: cloudstate.ProviderExecution{Stage: stage, CompletedAt: now, Failure: failure},
		})
	}
	blocked := func(candidate routing.Candidate) bool {
		t.Helper()
		result, err := planning.routeBlocked(context.Background(), candidate)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	rateLimited := &cloudstate.ExecutionFailure{Code: provider.CodeProviderRateLimited, Dispatch: provider.DispatchRejected}
	for i := 0; i < 2; i++ {
		record(x, rateLimited)
		// A sibling model's success must not reset x's failure count.
		record(y, nil)
	}
	record(x, rateLimited)
	if !blocked(x) {
		t.Fatalf("three rate limits did not open x's circuit: %+v", shared.records)
	}
	if blocked(y) || blocked(xStandard) {
		t.Fatalf("x's failures blocked another model or tier: %+v", shared.records)
	}
	denied := &cloudstate.ExecutionFailure{Code: provider.CodePermissionDenied, Dispatch: provider.DispatchRejected}
	record(y, denied)
	if blocked(y) {
		t.Fatalf("a single permission denial opened y for every caller: %+v", shared.records)
	}
	record(y, denied)
	record(y, denied)
	if !blocked(y) || blocked(xStandard) {
		t.Fatalf("repeated model denials on y were not confined to y: %+v", shared.records)
	}
}

func TestCloudRouteHealthHoldsAuthenticationFailureForSnapshot(t *testing.T) {
	now := time.Date(2026, 10, 4, 1, 0, 0, 0, time.UTC)
	digest := sha256.Sum256([]byte("config"))
	account := [32]byte{7}
	candidate := routing.Candidate{RouteID: "primary", EndpointID: "openai-direct", Provider: "openai", Family: "responses", Model: "gpt-5-mini", ProviderTier: "default", EndpointAccountHMAC: account}
	shared := &keyedHealthFixture{}
	planning := &ProviderPlanning{routeStatus: shared, clock: func() time.Time { return now }, configDigest: digest, configEpoch: "epoch"}
	execution := &CloudProviderExecution{statusRecorder: shared}
	operation := 0
	record := func(code provider.Code, dispatch provider.DispatchCertainty) {
		t.Helper()
		operation++
		now = now.Add(time.Second)
		execution.recordRouteStatus(context.Background(), cloudstate.SavedProviderExecution{
			Plan: cloudstate.BudgetPlan{ConfigDigest: digest, ConfigEpoch: "epoch", Family: candidate.Family, ProviderTier: candidate.ProviderTier, Route: durable.RoutePlan{OperationID: durable.OperationID(fmt.Sprintf("operation-%d", operation)), RouteID: candidate.RouteID, EndpointID: candidate.EndpointID, Provider: candidate.Provider, Model: candidate.Model,
				CacheIdentity: cache.RouteIdentity{Account: cache.Account(hex.EncodeToString(account[:]))}}},
			Execution: cloudstate.ProviderExecution{Stage: cloudstate.ExecutionFailed, CompletedAt: now, Failure: &cloudstate.ExecutionFailure{Code: code, Dispatch: dispatch}},
		})
	}
	blocked := func(planning *ProviderPlanning) bool {
		t.Helper()
		result, err := planning.routeBlocked(context.Background(), candidate)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	// A credential lookup that failed before dispatch can recover without a
	// snapshot change, so it keeps the short cooldown.
	record(provider.CodeAuthentication, provider.DispatchNotDispatched)
	if !blocked(planning) {
		t.Fatalf("a credential lookup failure did not open the route: %+v", shared.records)
	}
	now = now.Add(time.Minute)
	if blocked(planning) {
		t.Fatalf("a credential lookup failure was held past the cooldown: %+v", shared.records)
	}
	record(provider.CodeAuthentication, provider.DispatchRejected)
	if !blocked(planning) {
		t.Fatalf("an authentication failure did not open the route: %+v", shared.records)
	}
	// An in-flight transient failure must not shorten the credential hold.
	record(provider.CodeProviderUnavailable, provider.DispatchRejected)
	now = now.Add(time.Hour)
	if !blocked(planning) {
		t.Fatalf("an authentication failure stopped blocking the route within its snapshot: %+v", shared.records)
	}
	replacement := *planning
	replacement.configDigest = sha256.Sum256([]byte("replacement"))
	if blocked(&replacement) {
		t.Fatal("an authentication failure blocked the route in a replacement snapshot")
	}
}
