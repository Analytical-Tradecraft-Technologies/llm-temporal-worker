package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/openairesponses"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
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

// invokeFailing returns an adapter Invoke that counts the submission in
// submits and fails with failure before any possible write.
func invokeFailing(submits *atomic.Int32, failure error) func(context.Context, provider.Call, provider.Observer) (provider.Result, error) {
	return func(context.Context, provider.Call, provider.Observer) (provider.Result, error) {
		submits.Add(1)
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

// deniedRejection is the permanent, definitely-rejected permission failure
// the cloud tests use to fail a request outright.
func deniedRejection() error {
	return provider.NewError(provider.CodePermissionDenied, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNever, "rejected")
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
	return executionRef(v, f.request.Context)
}

// executionRef returns the execution reference of v for a request made under
// caller.
func executionRef(v llm.ExecutionResultV1, caller llm.RequestContext) llm.ExecutionReferenceV1 {
	return llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: caller, Capability: v.Capability}
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
	v := f.acquire(t, ctx, ref, llm.ExecutionAcquired)
	next, err := f.repository.LoadRequestAttempt(ctx, scope, cloudstate.RequestID(v.RequestID))
	if err != nil || next.ID == first.ID || len(next.PriorCandidates) != 0 {
		t.Fatalf("expired attempt not renewed: %+v, %v", next, err)
	}
	f.step(t, ctx, f.request, llm.ExecutionProviderCompleted)
	f.complete(t, ctx, ref, llm.ExecutionCompleted)
	requireCount(t, &f.submits, 1, "renewed attempt not submitted once")
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
	f.acquire(t, ctx, ref, llm.ExecutionAcquired)
	f.step(t, ctx, f.request, llm.ExecutionProviderCompleted)
	return f.complete(t, ctx, ref, llm.ExecutionCompleted)
}

// runCompact drives request through Prepare, Acquire, CompactStep and
// Complete and returns the completed result. With restart set the runtime is
// rebuilt before the step and before completion, as a new worker would be.
func (f *boundedCloudFixture) runCompact(t testing.TB, ctx context.Context, request llm.CompactRequestV1, restart bool) llm.ExecutionResultV1 {
	t.Helper()
	v := f.prepare(t, ctx, llm.PrepareExecutionV1{Compact: &request}, llm.ExecutionBudgetRequired)
	ref := executionRef(v, request.Context)
	f.acquire(t, ctx, ref, llm.ExecutionAcquired)
	if restart {
		f.restart(t)
	}
	f.compactStep(t, ctx, request, llm.ExecutionProviderCompleted)
	if restart {
		f.restart(t)
	}
	return f.complete(t, ctx, ref, llm.ExecutionCompleted)
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

// planGenerate plans request through a fresh provider planning for
// capabilities and returns its result.
func planGenerate(t testing.TB, capabilities V1RuntimeCapabilities, request llm.GenerateRequestV1) (PlannedProviderCall, error) {
	t.Helper()
	return mustProviderPlanning(t, capabilities).Generate(context.Background(), mustPrepareGenerate(t, request))
}

// recoveryBinding returns the recovery binding of planned's provider call on
// its route or fails the test.
func recoveryBinding(t testing.TB, planned PlannedBudgetCall) ProviderRecoveryBinding {
	t.Helper()
	binding, err := planned.Provider.RecoveryBinding(planned.Route)
	if err != nil {
		t.Fatal(err)
	}
	return binding
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
	return mustReadFile(t, "../../config.example.yaml")
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
	return f.prepare(t, ctx, llm.PrepareExecutionV1{Generate: &f.request}, llm.ExecutionBudgetRequired)
}

// isProviderError reports whether err is a provider error with code and
// retry disposition retry.
func isProviderError(err error, code provider.Code, retry provider.RetryDisposition) bool {
	var mapped *provider.Error
	return errors.As(err, &mapped) && mapped.Code == code && mapped.Retry == retry
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

// keepNoRecentTurns sets request's compaction policy to keep no recent turns,
// so every earlier turn is eligible for compaction.
func keepNoRecentTurns(request *llm.GenerateRequestV1) {
	policy := json.RawMessage(`{"recent_turns":0}`)
	request.SettingsPatch.CompactionPolicy.Set = &policy
}

// prepare prepares input and requires state want.
func (f *boundedCloudFixture) prepare(t testing.TB, ctx context.Context, input llm.PrepareExecutionV1, want llm.ExecutionStateV1) llm.ExecutionResultV1 {
	t.Helper()
	v, err := f.runtime.PrepareExecutionV1(ctx, input)
	return boundedState(t, v, err, want)
}

// acquire acquires ref's budget and requires state want.
func (f *boundedCloudFixture) acquire(t testing.TB, ctx context.Context, ref llm.ExecutionReferenceV1, want llm.ExecutionStateV1) llm.ExecutionResultV1 {
	t.Helper()
	v, err := f.runtime.AcquireBudgetV1(ctx, ref)
	return boundedState(t, v, err, want)
}

// step runs request's Generate step and requires state want.
func (f *boundedCloudFixture) step(t testing.TB, ctx context.Context, request llm.GenerateRequestV1, want llm.ExecutionStateV1) llm.ExecutionResultV1 {
	t.Helper()
	v, err := f.runtime.GenerateStepV1(ctx, request)
	return boundedState(t, v, err, want)
}

// compactStep runs request's Compact step and requires state want.
func (f *boundedCloudFixture) compactStep(t testing.TB, ctx context.Context, request llm.CompactRequestV1, want llm.ExecutionStateV1) llm.ExecutionResultV1 {
	t.Helper()
	v, err := f.runtime.CompactStepV1(ctx, request)
	return boundedState(t, v, err, want)
}

// poll polls ref's execution and requires state want.
func (f *boundedCloudFixture) poll(t testing.TB, ctx context.Context, ref llm.ExecutionReferenceV1, want llm.ExecutionStateV1) llm.ExecutionResultV1 {
	t.Helper()
	v, err := f.runtime.PollExecutionV1(ctx, ref)
	return boundedState(t, v, err, want)
}

// complete completes ref's execution and requires state want.
func (f *boundedCloudFixture) complete(t testing.TB, ctx context.Context, ref llm.ExecutionReferenceV1, want llm.ExecutionStateV1) llm.ExecutionResultV1 {
	t.Helper()
	v, err := f.runtime.CompleteExecutionV1(ctx, ref)
	return boundedState(t, v, err, want)
}

// compactRequest is a Compact request keyed "compact" for parent's
// checkpoint under the fixture's caller, with cache policy cache.
func (f *boundedCloudFixture) compactRequest(parent llm.ExecutionResultV1, cache *llm.CachePolicyV1) llm.CompactRequestV1 {
	return llm.CompactRequestV1{OperationKey: "compact", Context: f.request.Context, Parent: parent.Generate.Checkpoint.Handle, Cache: cache}
}

// followUp is a Generate request keyed key that continues parent under the
// fixture's caller with one human message holding text.
func (f *boundedCloudFixture) followUp(parent llm.CheckpointHandle, key, text string) llm.GenerateRequestV1 {
	return llm.GenerateRequestV1{OperationKey: key, Context: f.request.Context, Parent: &parent, Append: []llm.Item{llm.HumanText(text)}}
}

// failCompile is an adapter compile hook that fails every call with err.
func failCompile(err error) func(provider.CompileInput) (provider.Call, error) {
	return func(provider.CompileInput) (provider.Call, error) { return provider.Call{}, err }
}

// newTestApp builds an application from options, failing the test on error,
// and closes it when the test ends.
func newTestApp(t testing.TB, options app.Options) *app.App {
	t.Helper()
	application, err := app.New(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = application.Close(context.Background()) })
	return application
}

// testKeyring builds a handle keyring holding key or fails the test.
func testKeyring(t testing.TB, key state.Key) *state.Keyring {
	t.Helper()
	keyring, err := state.NewKeyring([]state.Key{key}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return keyring
}

// testLogger builds a logger with options or fails the test.
func testLogger(t testing.TB, options observability.LogOptions) *observability.Logger {
	t.Helper()
	logger, err := observability.NewLogger(options)
	if err != nil {
		t.Fatal(err)
	}
	return logger
}

// mustNewRuntime builds a runtime from data and options or fails the test;
// the caller owns its shutdown.
func mustNewRuntime(t testing.TB, data []byte, options Options) *Runtime {
	t.Helper()
	runtime, err := New(context.Background(), data, options)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

// mustCompileConfig compiles data without a reference resolver or fails the
// test.
func mustCompileConfig(t testing.TB, data []byte) *config.Snapshot {
	t.Helper()
	snapshot, err := config.Compile(context.Background(), data, nil)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

// mustReadFile reads path or fails the test.
func mustReadFile(t testing.TB, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// encodeJSON encodes value or fails the test.
func encodeJSON(t testing.TB, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// decodeJSON decodes data into a new T or fails the test.
func decodeJSON[T any](t testing.TB, data []byte) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

// requireCount fails the test with message and both counts unless counter
// holds want.
func requireCount(t testing.TB, counter *atomic.Int32, want int32, message string) {
	t.Helper()
	if got := counter.Load(); got != want {
		t.Fatalf("%s: got %d, want %d", message, got, want)
	}
}

// continueTurn points the fixture's Generate request at parent under
// operation key, without the root turn's settings patch.
func (f *boundedCloudFixture) continueTurn(parent llm.CheckpointHandle, key string) {
	f.request.Parent, f.request.OperationKey, f.request.SettingsPatch = &parent, key, llm.SettingsPatchV1{}
}
