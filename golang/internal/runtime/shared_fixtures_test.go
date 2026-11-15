package runtime

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/openairesponses"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

// mustProviderPlanning builds the provider planning for capabilities or fails
// the test.
func mustProviderPlanning(t testing.TB, capabilities V1RuntimeCapabilities) *ProviderPlanning {
	t.Helper()
	planning, err := capabilities.NewProviderPlanning(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return planning
}

// mustProviderRecovery builds the provider recovery for capabilities or fails
// the test.
func mustProviderRecovery(t testing.TB, capabilities V1RuntimeCapabilities) *ProviderRecovery {
	t.Helper()
	recovery, err := capabilities.NewProviderRecovery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return recovery
}

// mustPrepareGenerate prepares a root Generate request (no replay) or fails
// the test.
func mustPrepareGenerate(t testing.TB, request llm.GenerateRequestV1) PreparedGenerateInput {
	t.Helper()
	prepared, err := PrepareGenerateInput(context.Background(), request, durable.GenerateReplay{})
	if err != nil {
		t.Fatal(err)
	}
	return prepared
}

// mustPrepareCompact prepares a Compact request against replay or fails the
// test.
func mustPrepareCompact(t testing.TB, request llm.CompactRequestV1, replay durable.CompactReplay) PreparedCompactInput {
	t.Helper()
	prepared, err := PrepareCompactInput(context.Background(), request, replay)
	if err != nil {
		t.Fatal(err)
	}
	return prepared
}

// invokeFailingAfterWrite returns an adapter Invoke that counts the
// submission in submits (when non-nil), crosses the possible-write boundary
// and then fails with failure.
func invokeFailingAfterWrite(submits *atomic.Int32, failure error) func(context.Context, provider.Call, provider.Observer) (provider.Result, error) {
	return func(ctx context.Context, _ provider.Call, observer provider.Observer) (provider.Result, error) {
		if err := countedPossibleWrite(ctx, submits, observer); err != nil {
			return provider.Result{}, err
		}
		return provider.Result{}, failure
	}
}

// countedPossibleWrite counts a provider submission in submits (when
// non-nil) and crosses observer's possible-write boundary.
func countedPossibleWrite(ctx context.Context, submits *atomic.Int32, observer provider.Observer) error {
	if submits != nil {
		submits.Add(1)
	}
	return observer.BeforePossibleWrite(ctx)
}

// submitFailingAfterWrite is invokeFailingAfterWrite for an async Submit.
func submitFailingAfterWrite(submits *atomic.Int32, failure error) func(context.Context, provider.Call, provider.Observer) (provider.ResumableResult, error) {
	invoke := invokeFailingAfterWrite(submits, failure)
	return func(ctx context.Context, call provider.Call, observer provider.Observer) (provider.ResumableResult, error) {
		_, err := invoke(ctx, call, observer)
		return provider.ResumableResult{}, err
	}
}

// unavailableRejection is the retryable, definitely-rejected provider outage
// the cloud tests use to move to the next route.
func unavailableRejection() error {
	return provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNextRoute, "unavailable")
}

// testRequestScope is the caller scope every bounded cloud fixture request
// runs under.
var testRequestScope = cloudstate.Scope{Tenant: "tenant", Project: "project"}

// ref returns the execution reference of v for the fixture's Generate request.
func (f *boundedCloudFixture) ref(v llm.ExecutionResultV1) llm.ExecutionReferenceV1 {
	return llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context, Capability: v.Capability}
}

// firstAttempt returns the execution reference of v, the request scope and
// the request's current attempt.
func (f *boundedCloudFixture) firstAttempt(t testing.TB, ctx context.Context, v llm.ExecutionResultV1) (llm.ExecutionReferenceV1, cloudstate.Scope, cloudstate.RequestAttempt) {
	t.Helper()
	first, err := f.repository.LoadRequestAttempt(ctx, testRequestScope, cloudstate.RequestID(v.RequestID))
	if err != nil {
		t.Fatal(err)
	}
	return f.ref(v), testRequestScope, first
}

// requireRenewedAttemptCompletes acquires ref's budget again, checks the
// request moved from first to a fresh attempt with no prior candidates, and
// runs that attempt to completion with exactly one submission in all.
func (f *boundedCloudFixture) requireRenewedAttemptCompletes(t testing.TB, ctx context.Context, ref llm.ExecutionReferenceV1, scope cloudstate.Scope, first cloudstate.RequestAttempt) {
	t.Helper()
	v, err := f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionAcquired)
	next, err := f.repository.LoadRequestAttempt(ctx, scope, cloudstate.RequestID(v.RequestID))
	if err != nil || next.ID == first.ID || len(next.PriorCandidates) != 0 {
		t.Fatalf("expired attempt not renewed: %+v, %v", next, err)
	}
	v, err = f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionCompleted)
	if f.submits.Load() != 1 {
		t.Fatal("renewed attempt not submitted once", f.submits.Load())
	}
}

// appendAliasRoute adds a copy of the alias model's first route as routeID on
// endpointID, after the existing routes.
func (s *planningSource) appendAliasRoute(routeID, endpointID string) {
	model := s.value.Routes.Models["alias"]
	route := model.Routes[0]
	route.ID, route.EndpointID = routeID, endpointID
	model.Routes = append(model.Routes, route)
	s.value.Routes.Models["alias"] = model
}

// addFallbackRoute appends an alias route on endpointID priced like the
// first one, under a budget policy that matches every endpoint.
func (f *budgetPlanningFixture) addFallbackRoute(t testing.TB, routeID, endpointID string) {
	t.Helper()
	f.source.appendAliasRoute(routeID, endpointID)
	f.source.value.BudgetPolicies[0].Match.EndpointID = ""
	entry := f.entry
	entry.EndpointID = endpointID
	f.prices(t, []pricing.Entry{f.entry, entry})
}

// acquireGenerateComplete acquires ref's budget, runs the fixture's Generate
// step to a provider result and completes the execution, without restarts.
func (f *boundedCloudFixture) acquireGenerateComplete(t testing.TB, ctx context.Context, ref llm.ExecutionReferenceV1) llm.ExecutionResultV1 {
	t.Helper()
	v, err := f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionAcquired)
	v, err = f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	return boundedState(t, v, err, llm.ExecutionCompleted)
}

// runCompact drives request through Prepare, Acquire, CompactStep and
// Complete and returns the completed result. With restart set the runtime is
// rebuilt before the step and before completion, as a new worker would be.
func (f *boundedCloudFixture) runCompact(t testing.TB, ctx context.Context, request llm.CompactRequestV1, restart bool) llm.ExecutionResultV1 {
	t.Helper()
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Compact: &request})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: request.Context, Capability: v.Capability}
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionAcquired)
	if restart {
		f.restart(t)
	}
	v, err = f.runtime.CompactStepV1(ctx, request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	if restart {
		f.restart(t)
	}
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	return boundedState(t, v, err, llm.ExecutionCompleted)
}

// startTestRuntime starts runtime, skipping the test where the sandbox does
// not permit loopback listeners.
func startTestRuntime(t testing.TB, runtime *Runtime) {
	t.Helper()
	if err := runtime.Start(); err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("sandbox does not permit loopback listeners: %v", err)
		}
		t.Fatal(err)
	}
}

// newStartedRuntime builds a runtime from data and options, starts it with
// startTestRuntime and shuts it down when the test ends.
func newStartedRuntime(t testing.TB, ctx context.Context, data []byte, options Options) *Runtime {
	t.Helper()
	runtime, err := New(ctx, data, options)
	if err != nil {
		t.Fatal(err)
	}
	startTestRuntime(t, runtime)
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	return runtime
}

// plan quotes the fixture's prepared Compact (compact set) or Generate
// request through planning or fails the test.
func (f *budgetPlanningFixture) plan(t testing.TB, planning *BudgetPlanning, compact bool) PlannedBudgetCall {
	t.Helper()
	var planned PlannedBudgetCall
	var err error
	if compact {
		planned, err = planning.Compact(context.Background(), f.summary, f.attempt)
	} else {
		planned, err = planning.Generate(context.Background(), f.generate, f.attempt)
	}
	if err != nil {
		t.Fatal(err)
	}
	return planned
}

// responsesAdapter is a real OpenAI Responses adapter for endpoint
// "endpoint" at capability profile/v1 that sends through httpClient.
func responsesAdapter(t testing.TB, httpClient *http.Client) *openairesponses.Adapter {
	t.Helper()
	client, err := openairesponses.NewClient(openairesponses.ClientConfig{BaseURL: "https://api.openai.com/v1/", APIKey: "test-key", HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := openairesponses.New(client, "endpoint", "profile/v1")
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

// exampleConfig reads the repository example configuration.
func exampleConfig(t testing.TB) []byte {
	t.Helper()
	data, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// generateStep runs the fixture's Generate step under a background context
// and requires state want, returning the context and the result.
func (f *boundedCloudFixture) generateStep(t testing.TB, want llm.ExecutionStateV1) (context.Context, llm.ExecutionResultV1) {
	t.Helper()
	ctx := context.Background()
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	return ctx, boundedState(t, v, err, want)
}

// prepareGenerate prepares the fixture's Generate request, requires it to
// need a budget and returns its result.
func (f *boundedCloudFixture) prepareGenerate(t testing.TB, ctx context.Context) llm.ExecutionResultV1 {
	t.Helper()
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	return boundedState(t, v, err, llm.ExecutionBudgetRequired)
}

// isProviderError reports whether err is a provider error with code and
// retry disposition retry.
func isProviderError(err error, code provider.Code, retry provider.RetryDisposition) bool {
	var mapped *provider.Error
	return errors.As(err, &mapped) && mapped.Code == code && mapped.Retry == retry
}

// modelMessage is a model message holding one text part.
func modelMessage(text string) llm.Message {
	return llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: text}}}
}

// requireCacheOutcome requires the Generate result v to report cache
// disposition after exactly submits provider submissions; what names the
// step in the failure.
func (f *boundedCloudFixture) requireCacheOutcome(t testing.TB, what string, v llm.ExecutionResultV1, disposition string, submits int32) {
	t.Helper()
	if v.Generate.Cache.Disposition != disposition || f.submits.Load() != submits {
		t.Fatalf("%s: disposition=%s submits=%d", what, v.Generate.Cache.Disposition, f.submits.Load())
	}
}

// newTestRuntime builds a runtime from data and options and shuts it down
// when the test ends.
func newTestRuntime(t testing.TB, ctx context.Context, data []byte, options Options) *Runtime {
	t.Helper()
	runtime, err := New(ctx, data, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	return runtime
}

// singleRouteSnapshot is an engine snapshot whose only model routes to
// endpointID at capability version.
func singleRouteSnapshot(model, endpointID, version string) engine.Snapshot {
	return engine.Snapshot{Routes: routing.Catalog{Models: map[string]routing.Model{
		model: {Routes: []routing.Route{{EndpointID: endpointID, Capabilities: routing.CapabilitySet{Version: version}}}},
	}}}
}

// requireCleanReturn waits up to two seconds for the run named name to send
// its result on done, failing the test unless it returns nil in time.
func requireCleanReturn(t testing.TB, done <-chan error, name string) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s returned %v", name, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not shut down", name)
	}
}
