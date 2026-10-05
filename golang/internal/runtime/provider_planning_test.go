package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/compaction"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/openairesponses"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

type planningAdapter struct {
	mu            sync.Mutex
	inputs        []provider.CompileInput
	compile       func(provider.CompileInput) (provider.Call, error)
	capabilityErr error
	version       string
}

func (*planningAdapter) Name() string { return "test" }
func (adapter *planningAdapter) Capabilities(context.Context, provider.CapabilityQuery) (provider.CapabilitySet, error) {
	return provider.CapabilitySet{Version: adapter.version}, adapter.capabilityErr
}
func (adapter *planningAdapter) Compile(_ context.Context, input provider.CompileInput) (provider.Call, error) {
	adapter.mu.Lock()
	adapter.inputs = append(adapter.inputs, input)
	adapter.mu.Unlock()
	if adapter.compile != nil {
		return adapter.compile(input)
	}
	return planningCall(input), nil
}
func (*planningAdapter) Invoke(context.Context, provider.Call, provider.Observer) (provider.Result, error) {
	panic("planning must never invoke providers")
}
func planningCall(input provider.CompileInput) provider.Call {
	return provider.Call{EndpointID: input.Query.EndpointID, Family: input.Query.Family,
		Model: input.Request.Model, OperationKey: input.Request.OperationKey,
		ServiceClass: input.Request.ServiceClass, Metadata: input.Metadata, SDKParams: struct{}{}}
}

type planningSource struct {
	value engine.Snapshot
	reads int
	err   error
}

func (source *planningSource) Current(context.Context) (engine.Snapshot, error) {
	source.reads++
	return source.value, source.err
}

type planningPlannerFunc func(context.Context, routing.Input) (routing.Plan, error)

func (function planningPlannerFunc) Plan(ctx context.Context, input routing.Input) (routing.Plan, error) {
	return function(ctx, input)
}

type planningRegistryFunc func(context.Context, routing.Candidate) (provider.Adapter, error)

func (function planningRegistryFunc) Adapter(ctx context.Context, candidate routing.Candidate) (provider.Adapter, error) {
	return function(ctx, candidate)
}

type planningTransportFunc func(*http.Request) (*http.Response, error)

func (function planningTransportFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func planningFixture() (V1RuntimeCapabilities, *planningSource, *planningAdapter, llm.GenerateRequestV1, durable.CompactReplay, llm.CompactRequestV1) {
	adapter := &planningAdapter{version: "profile/v1"}
	route := routing.Route{ID: "route", EndpointID: "endpoint", Provider: "openai", Family: string(provider.FamilyOpenAIResponses),
		Model: "provider-model", ModelRevision: "provider-revision", Region: "region", EndpointAccountHMAC: [32]byte{8},
		Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "default"},
		Capabilities:   routing.CapabilitySet{Version: adapter.version, Features: map[routing.Feature]routing.Capability{routing.FeatureText: {State: routing.CapabilityNative}}},
		AllowedTenants: []string{"tenant"}, PriceVersion: "price/v1", PriceAvailable: true}
	source := &planningSource{value: engine.Snapshot{ConfigDigest: [32]byte{1}, ConfigEpoch: "epoch/v1",
		Routes: routing.Catalog{Version: "catalog/v1", Models: map[string]routing.Model{"alias": {Name: "alias", Routes: []routing.Route{route}}}}}}
	capabilities := V1RuntimeCapabilities{ConfigDigest: source.value.ConfigDigest, Snapshot: source,
		Planner: routing.DeterministicPlanner{}, Adapters: engine.AdapterMap{"endpoint": adapter}}
	request := llm.GenerateRequestV1{OperationKey: "operation", Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
		Append: []llm.Item{preparationMessage("sensitive prompt")}, SettingsPatch: llm.SettingsPatchV1{}}
	request.SettingsPatch.Model.Set = preparationPointer("alias")
	request.SettingsPatch.ServiceClass.Set = preparationPointer(llm.ServiceClassPriority)
	request.SettingsPatch.ServiceClassFallbacks.Set = preparationPointer([]llm.ServiceClass{llm.ServiceClassStandard})
	settings := state.RootModelState("alias")
	settings.ServiceClass = llm.ServiceClassPriority
	settings.ServiceClassFallbacks = []llm.ServiceClass{llm.ServiceClassStandard}
	policy := compaction.DefaultPolicy()
	policy.RecentTurns = 0
	settings.CompactionPolicy, _ = json.Marshal(policy)
	replay := durable.CompactReplay{State: state.MaterializedState{Handle: "cp1.parent", Tenant: "tenant", Project: "project",
		Settings: settings, Items: []llm.Item{preparationMessage("sensitive history")}}}
	compact := llm.CompactRequestV1{OperationKey: "compact-operation", Context: request.Context, Parent: "cp1.parent"}
	return capabilities, source, adapter, request, replay, compact
}

func assertPlanningError(t *testing.T, err error, code provider.Code) {
	t.Helper()
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != code || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Cause != nil || len(mapped.SafeDetails) != 0 || strings.Contains(err.Error(), "sensitive") {
		t.Fatalf("unsafe or unexpected planning error: %#v", err)
	}
}

func TestProviderPlanningRealCompilerResolvesAliasesAndFallbackForBothPhases(t *testing.T) {
	capabilities, _, _, request, replay, compact := planningFixture()
	var httpCalls atomic.Int32
	client, err := openairesponses.NewClient(openairesponses.ClientConfig{BaseURL: "https://api.openai.com/v1/", APIKey: "test-key", HTTPClient: &http.Client{Transport: planningTransportFunc(func(*http.Request) (*http.Response, error) {
		httpCalls.Add(1)
		return nil, errors.New("unexpected HTTP request")
	})}})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := openairesponses.New(client, "endpoint", "profile/v1")
	if err != nil {
		t.Fatal(err)
	}
	capabilities.Adapters = engine.AdapterMap{"endpoint": adapter}
	planning, err := capabilities.NewProviderPlanning(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	generate, err := PrepareGenerateInput(context.Background(), request, durable.GenerateReplay{})
	if err != nil {
		t.Fatal(err)
	}
	summary, err := PrepareCompactInput(context.Background(), compact, replay)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"generate", "compact"} {
		t.Run(kind, func(t *testing.T) {
			var planned PlannedProviderCall
			if kind == "generate" {
				planned, err = planning.Generate(context.Background(), generate)
			} else {
				planned, err = planning.Compact(context.Background(), summary)
			}
			if err != nil {
				t.Fatal(err)
			}
			if planned.Candidate.RequestedClass != llm.ServiceClassPriority || planned.Candidate.AttemptedClass != llm.ServiceClassStandard || planned.Call.Model != "provider-model" || planned.Call.ServiceClass != llm.ServiceClassStandard {
				t.Fatalf("unresolved provider selection: %+v", planned)
			}
			data, err := json.Marshal(planned.Call.SDKParams)
			if err != nil {
				t.Fatal(err)
			}
			var wire map[string]any
			if err := json.Unmarshal(data, &wire); err != nil {
				t.Fatal(err)
			}
			if wire["model"] != "provider-model" || wire["service_tier"] != "default" {
				t.Fatalf("wrong SDK projection: %s", data)
			}
			if kind == "compact" && (wire["tools"] != nil || wire["max_output_tokens"] == nil) {
				t.Fatalf("unbounded or tool-enabled compaction: %s", data)
			}
			route, err := planned.Route("operation-id", "generation-id")
			if err != nil || route.CacheIdentity != planned.CacheIdentity || route.Model != planned.Call.Model || route.PriceVersion != "price/v1" {
				t.Fatalf("route = %+v, %v", route, err)
			}
			if planned.CacheIdentity.Account != "0800000000000000000000000000000000000000000000000000000000000000" || planned.CacheIdentity.Region != "region" || planned.CacheIdentity.Revision != "provider-revision" || planned.CacheIdentity.Compiler != "openai_responses/cloud-v1" || planned.ConfigDigest != capabilities.ConfigDigest || planned.ConfigEpoch != "epoch/v1" {
				t.Fatalf("incomplete cache binding: %+v", planned)
			}
		})
	}
	if httpCalls.Load() != 0 || generate.Request.Model != "alias" || generate.Request.ServiceClass != llm.ServiceClassPriority || summary.Request.Model != "alias" {
		t.Fatal("planning dispatched or changed the semantic request")
	}
}

func TestProviderPlanningRejectsIncompleteSnapshot(t *testing.T) {
	for _, name := range []string{"nil context", "canceled", "source", "planner", "registry", "source error", "digest", "catalog"} {
		t.Run(name, func(t *testing.T) {
			capabilities, source, _, _, _, _ := planningFixture()
			ctx := context.Background()
			code := provider.CodeConfiguration
			switch name {
			case "nil context":
				ctx = nil
			case "canceled":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			case "source":
				capabilities.Snapshot = (*planningSource)(nil)
			case "planner":
				capabilities.Planner = planningPlannerFunc(nil)
			case "registry":
				capabilities.Adapters = planningRegistryFunc(nil)
			case "source error":
				source.err = errors.New("sensitive configuration")
				code = provider.CodeStateUnavailable
			case "digest":
				source.value.ConfigDigest = [32]byte{2}
			case "catalog":
				source.value.Routes.Version = ""
			}
			_, err := capabilities.NewProviderPlanning(ctx)
			if name == "canceled" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				return
			}
			assertPlanningError(t, err, code)
		})
	}
}

func TestProviderPlanningCapturesSnapshotOnceAndIsolatesPlannerMutations(t *testing.T) {
	capabilities, source, _, request, _, _ := planningFixture()
	capabilities.Planner = planningPlannerFunc(func(ctx context.Context, input routing.Input) (routing.Plan, error) {
		plan, err := (routing.DeterministicPlanner{}).Plan(ctx, input)
		input.Catalog.Models["alias"].Routes[0].Model = "sensitive replacement"
		input.Catalog.Models["alias"].Routes[0].Capabilities.Features[routing.FeatureText] = routing.Capability{State: routing.CapabilityUnsupported}
		input.Health.Routes["route"] = routing.RouteHealth{Open: true}
		input.Request.Input[0].(llm.Message).Content[0] = llm.TextPart{Text: "sensitive replacement"}
		return plan, err
	})
	planning, err := capabilities.NewProviderPlanning(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	source.value.Routes.Models["alias"].Routes[0].Model = "new-model"
	source.value.Routes.Models["alias"].Routes[0].Capabilities.Features[routing.FeatureText] = routing.Capability{State: routing.CapabilityUnsupported}
	source.value.ConfigDigest = [32]byte{2}
	prepared, err := PrepareGenerateInput(context.Background(), request, durable.GenerateReplay{})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		planned, err := planning.Generate(context.Background(), prepared)
		if err != nil || planned.Call.Model != "provider-model" || planned.ConfigDigest != [32]byte{1} {
			t.Fatalf("snapshot changed: %+v, %v", planned, err)
		}
	}
	if source.reads != 1 || prepared.Request.Input[0].(llm.Message).Content[0].(llm.TextPart).Text != "sensitive prompt" {
		t.Fatal("snapshot reread or semantic input mutated")
	}
}

func TestProviderPlanningFallbackUsesIndependentCompilerInputs(t *testing.T) {
	capabilities, source, first, request, _, _ := planningFixture()
	second := &planningAdapter{version: "profile/v1"}
	model := source.value.Routes.Models["alias"]
	route := model.Routes[0]
	route.ID, route.EndpointID = "second-route", "second-endpoint"
	model.Routes = append(model.Routes, route)
	source.value.Routes.Models["alias"] = model
	capabilities.Adapters = engine.AdapterMap{"endpoint": first, "second-endpoint": second}
	first.compile = func(input provider.CompileInput) (provider.Call, error) {
		input.Request.Input[0].(llm.Message).Content[0] = llm.TextPart{Text: "sensitive mutated input"}
		return provider.Call{}, provider.NewError(provider.CodeUnsupportedCapability, provider.PhaseCompile, provider.DispatchNotDispatched, provider.RetryNextRoute, "sensitive compiler error")
	}
	planning, err := capabilities.NewProviderPlanning(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareGenerateInput(context.Background(), request, durable.GenerateReplay{})
	if err != nil {
		t.Fatal(err)
	}
	planned, err := planning.Generate(context.Background(), prepared)
	if err != nil || planned.Call.EndpointID != "second-endpoint" || len(first.inputs) != 1 || len(second.inputs) != 1 {
		t.Fatalf("fallback failed: %+v, %v", planned, err)
	}
	if second.inputs[0].Request.Input[0].(llm.Message).Content[0].(llm.TextPart).Text != "sensitive prompt" {
		t.Fatal("failed compiler contaminated next route")
	}
	if !second.inputs[0].Strict || len(second.inputs[0].Request.ServiceClassFallbacks) != 0 {
		t.Fatal("lost portability or passed route fallbacks to compiler")
	}
}

func TestProviderPlanningRejectsWrongCompilerAndCandidateBindings(t *testing.T) {
	for _, name := range []string{"endpoint", "family", "model", "operation", "class", "capability", "tier", "digest", "opaque state", "SDK params", "adapter capability", "nil adapter", "candidate endpoint", "candidate account", "candidate revision"} {
		t.Run(name, func(t *testing.T) {
			capabilities, _, adapter, request, _, _ := planningFixture()
			adapter.compile = func(input provider.CompileInput) (provider.Call, error) {
				call := planningCall(input)
				switch name {
				case "endpoint":
					call.EndpointID = "other"
				case "family":
					call.Family = provider.FamilyOpenAIChat
				case "model":
					call.Model = "alias"
				case "operation":
					call.OperationKey = "other"
				case "class":
					call.ServiceClass = llm.ServiceClassPriority
				case "capability":
					call.Metadata.CapabilityVersion = "other"
				case "tier":
					call.Metadata.ProviderTier = "other"
				case "digest":
					call.Metadata.SchemaDigest = [32]byte{9}
				case "opaque state":
					call.Metadata.OpaqueStateRequired = true
				case "SDK params":
					call.SDKParams = (*string)(nil)
				}
				return call, nil
			}
			if name == "adapter capability" {
				adapter.version = "other"
			}
			if name == "nil adapter" {
				capabilities.Adapters = planningRegistryFunc(func(context.Context, routing.Candidate) (provider.Adapter, error) {
					return (*planningAdapter)(nil), nil
				})
			}
			if strings.HasPrefix(name, "candidate") {
				capabilities.Planner = planningPlannerFunc(func(ctx context.Context, input routing.Input) (routing.Plan, error) {
					plan, err := (routing.DeterministicPlanner{}).Plan(ctx, input)
					switch name {
					case "candidate endpoint":
						plan.Candidates[0].EndpointID = "other"
					case "candidate account":
						plan.Candidates[0].EndpointAccountHMAC = [32]byte{9}
					case "candidate revision":
						plan.Candidates[0].ModelRevision = "other"
					}
					return plan, err
				})
			}
			planning, err := capabilities.NewProviderPlanning(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := PrepareGenerateInput(context.Background(), request, durable.GenerateReplay{})
			if err != nil {
				t.Fatal(err)
			}
			_, err = planning.Generate(context.Background(), prepared)
			assertPlanningError(t, err, provider.CodeConfiguration)
		})
	}
}

func TestProviderPlanningDoesNotFallbackAfterPossibleDispatch(t *testing.T) {
	for _, dispatch := range []provider.DispatchCertainty{provider.DispatchAccepted, provider.DispatchAmbiguous, provider.DispatchRejected} {
		t.Run(string(dispatch), func(t *testing.T) {
			capabilities, _, adapter, request, _, _ := planningFixture()
			adapter.compile = func(provider.CompileInput) (provider.Call, error) {
				return provider.Call{}, provider.NewError(provider.CodeInternal, provider.PhaseCompile, dispatch, provider.RetryNextRoute, "sensitive paid outcome")
			}
			planning, err := capabilities.NewProviderPlanning(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := PrepareGenerateInput(context.Background(), request, durable.GenerateReplay{})
			if err != nil {
				t.Fatal(err)
			}
			_, err = planning.Generate(context.Background(), prepared)
			var mapped *provider.Error
			if !errors.As(err, &mapped) || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever || mapped.Cause != nil {
				t.Fatalf("possible dispatch became fallback: %#v", err)
			}
		})
	}
}

func TestProviderPlanningRejectsNoWorkNoRouteAndCanceledPreparation(t *testing.T) {
	capabilities, source, adapter, request, _, _ := planningFixture()
	source.value.Health.Routes = map[string]routing.RouteHealth{"route": {Open: true}}
	planning, err := capabilities.NewProviderPlanning(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareGenerateInput(context.Background(), request, durable.GenerateReplay{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = planning.Generate(context.Background(), prepared)
	assertPlanningError(t, err, provider.CodeNoRoute)
	_, err = planning.Compact(context.Background(), PreparedCompactInput{})
	assertPlanningError(t, err, provider.CodeInvalidArgument)
	_, err = (&ProviderPlanning{}).Generate(context.Background(), prepared)
	assertPlanningError(t, err, provider.CodeConfiguration)
	if len(adapter.inputs) != 0 {
		t.Fatal("no-work or no-route request reached compiler")
	}
	source.value.Health.Routes = nil
	planning, err = capabilities.NewProviderPlanning(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	adapter.compile = func(input provider.CompileInput) (provider.Call, error) { cancel(); return planningCall(input), nil }
	_, err = planning.Generate(ctx, prepared)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled compile returned a usable plan: %v", err)
	}
}

func TestProviderPlanningConcurrentCallsKeepInputAndPlanIndependent(t *testing.T) {
	capabilities, _, adapter, request, _, _ := planningFixture()
	request.SettingsPatch.Portability.Set = preparationPointer(llm.PortabilityBestEffort)
	planning, err := capabilities.NewProviderPlanning(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareGenerateInput(context.Background(), request, durable.GenerateReplay{})
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for range 20 {
		group.Add(1)
		go func() {
			defer group.Done()
			planned, err := planning.Generate(context.Background(), prepared)
			if err != nil {
				t.Error(err)
				return
			}
			if planned.Call.OperationKey != request.OperationKey {
				t.Error("lost operation binding")
			}
			planned.Candidate.EndpointID = "changed"
			if _, err := planned.Route("operation", "generation"); err == nil {
				t.Error("changed route binding accepted")
			}
		}()
	}
	group.Wait()
	if len(adapter.inputs) != 20 {
		t.Fatal("lost concurrent compilation")
	}
	for _, input := range adapter.inputs {
		if input.Strict || !reflect.DeepEqual(input.Request.Input, prepared.Request.Input) {
			t.Fatal("compile mutated independent input or portability")
		}
	}
}

func TestProviderPlanningSharesCacheRouteWithBothRunners(t *testing.T) {
	for _, compactPhase := range []bool{false, true} {
		t.Run(map[bool]string{false: "generate", true: "compact"}[compactPhase], func(t *testing.T) {
			f := newCacheLookupFixture(t)
			capabilities, source, adapter, request, replay, compact := planningFixture()
			capabilities.ConfigDigest, source.value.ConfigDigest = f.cap.ConfigDigest, f.cap.ConfigDigest
			planning, err := capabilities.NewProviderPlanning(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			f.gen, f.compact, f.compactReplay = request, compact, replay
			f.gen.Cache, f.compact.Cache = &llm.CachePolicyV1{MaxAgeSeconds: 60}, &llm.CachePolicyV1{MaxAgeSeconds: 60}
			var selected PlannedProviderCall
			lookup, err := f.cap.NewResponseCacheLookup(
				func(ctx context.Context, _ llm.GenerateRequestV1, prepared PreparedGenerateInput) (cache.FillLease, error) {
					var err error
					selected, err = planning.Generate(ctx, prepared)
					lease := f.genLease
					lease.Key.Route = selected.CacheIdentity
					return lease, err
				},
				func(ctx context.Context, _ llm.CompactRequestV1, prepared PreparedCompactInput) (cache.FillLease, error) {
					var err error
					selected, err = planning.Compact(ctx, prepared)
					lease := f.compLease
					lease.Key.Route = selected.CacheIdentity
					return lease, err
				})
			if err != nil {
				t.Fatal(err)
			}
			ports := validBuilderGeneratePorts(&f.events)
			ports.Replay = func(context.Context, llm.GenerateRequestV1) (durable.GenerateReplay, error) {
				return durable.GenerateReplay{}, nil
			}
			ports.CacheLookup = lookup.Generate
			ports.Route = func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.CompactionDecision) (durable.RoutePlan, error) {
				return selected.Route(durable.OperationID(f.genLease.OperationID), durable.GenerationID(f.genLease.Attempt))
			}
			stop := errors.New("stop before actual provider submission")
			ports.Dispatch = func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.RoutePlan, durable.ClaimReceipt) (durable.DispatchResult, error) {
				f.events = append(f.events, "dispatch")
				return durable.DispatchResult{}, stop
			}
			if compactPhase {
				p := validCompactPorts()
				p.Replay = func(context.Context, llm.CompactRequestV1) (durable.CompactReplay, error) { return replay, nil }
				p.CacheLookup = lookup.Compact
				p.Route = func(context.Context, llm.CompactRequestV1, durable.CompactReplay) (durable.RoutePlan, error) {
					return selected.Route(durable.OperationID(f.compLease.OperationID), durable.GenerationID(f.compLease.Attempt))
				}
				p.Reserve = func(ctx context.Context, _ llm.CompactRequestV1, route durable.RoutePlan) (durable.ReserveResult, error) {
					return ports.Reserve(ctx, f.gen, route)
				}
				p.Claim = func(ctx context.Context, _ llm.CompactRequestV1, route durable.RoutePlan, reservation durable.ReserveResult) (durable.ClaimReceipt, error) {
					return ports.Claim(ctx, f.gen, route, reservation)
				}
				p.Dispatch = func(context.Context, llm.CompactRequestV1, durable.CompactReplay, durable.RoutePlan, durable.ClaimReceipt) (durable.CompactDispatchResult, error) {
					f.events = append(f.events, "dispatch")
					return durable.CompactDispatchResult{}, stop
				}
				_, err = durable.CompactV1(context.Background(), f.compact, p)
			} else {
				_, err = durable.GenerateV1(context.Background(), f.gen, ports)
			}
			if !errors.Is(err, stop) || len(adapter.inputs) != 1 {
				t.Fatalf("compiled route did not reach dispatch gate once: %v", err)
			}
			if len(f.lookups) != 2 || f.lookups[0].Key.Route != selected.CacheIdentity {
				t.Fatal("cache and dispatch used different provider selections")
			}
			last := f.events[len(f.events)-4:]
			if !reflect.DeepEqual(last, []string{"reserve", "start", "claim", "dispatch"}) {
				t.Fatalf("admission order = %v", f.events)
			}
		})
	}
}

func TestProviderPlanningEnforcesOutputCeilingWithCustomPlanner(t *testing.T) {
	for _, limit := range []int64{999, 1000} {
		capabilities, source, adapter, request, _, _ := planningFixture()
		source.value.Routes.Models["alias"].Routes[0].OutputTokens = limit
		capabilities.BudgetEstimator.MaxOutput = 1000
		capabilities.Planner = planningPlannerFunc(func(ctx context.Context, input routing.Input) (routing.Plan, error) {
			// An injected planner cannot remove the snapshot's admission ceiling.
			input.Catalog.Models["alias"].Routes[0].OutputTokens = 0
			return (routing.DeterministicPlanner{}).Plan(ctx, input)
		})
		planning, err := capabilities.NewProviderPlanning(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := PrepareGenerateInput(context.Background(), request, durable.GenerateReplay{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = planning.Generate(context.Background(), prepared)
		if limit < 1000 {
			if err == nil || len(adapter.inputs) != 0 {
				t.Fatal("oversize output reached compiler")
			}
		} else if err != nil || len(adapter.inputs) != 1 || *adapter.inputs[0].Request.Output.MaxTokens != 1000 {
			t.Fatalf("boundary request failed: %v", err)
		}
	}
}

func TestProviderPlanningContextLimitFallbackBeforeCompile(t *testing.T) {
	capabilities, source, adapter, request, _, _ := planningFixture()
	model := source.value.Routes.Models["alias"]
	model.Routes[0].ContextTokens = 29
	larger := model.Routes[0]
	larger.ID = "larger-context"
	larger.ContextTokens = 30
	larger.OutputTokens = 20
	outputTooSmall := larger
	outputTooSmall.ID = "small-output"
	outputTooSmall.OutputTokens = 19
	model.Routes = append(model.Routes, outputTooSmall, larger)
	source.value.Routes.Models["alias"] = model
	capabilities.BudgetEstimator.MaxOutput = 20
	capabilities.BudgetEstimator.Tokenizer = func(llm.Request, routing.Candidate) (int64, error) { return 10, nil }
	planning, err := capabilities.NewProviderPlanning(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareGenerateInput(context.Background(), request, durable.GenerateReplay{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := planning.Generate(context.Background(), prepared)
	if err != nil || result.Candidate.RouteID != larger.ID || len(adapter.inputs) != 1 {
		t.Fatalf("fallback=%+v, compiles=%d, err=%v", result.Candidate, len(adapter.inputs), err)
	}
	capabilities.Planner = planningPlannerFunc(func(ctx context.Context, input routing.Input) (routing.Plan, error) {
		plan, err := (routing.DeterministicPlanner{}).Plan(ctx, input)
		for i := range plan.Candidates {
			plan.Candidates[i].ContextTokens = 0
		}
		return plan, err
	})
	planning, err = capabilities.NewProviderPlanning(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := planning.Generate(context.Background(), prepared); err == nil || len(adapter.inputs) != 1 {
		t.Fatal("custom planner bypassed context limit")
	}
}

func TestProviderPlanningFlattensSummarizerInstructionsOnlyForSingleSystemFamilies(t *testing.T) {
	for _, test := range []struct {
		family provider.Family
		want   llm.InstructionLevel
	}{
		{family: provider.FamilyOpenAIResponses, want: llm.InstructionLevelPolicy},
		{family: provider.FamilyOpenAIChat, want: llm.InstructionLevelPolicy},
		{family: provider.FamilyAnthropicMessages, want: llm.InstructionLevelApplication},
		{family: provider.FamilyBedrockMessages, want: llm.InstructionLevelApplication},
		{family: provider.FamilyBedrockConverse, want: llm.InstructionLevelApplication},
	} {
		t.Run(string(test.family), func(t *testing.T) {
			capabilities, source, adapter, _, replay, compact := planningFixture()
			model := source.value.Routes.Models["alias"]
			model.Routes[0].Family = string(test.family)
			source.value.Routes.Models["alias"] = model
			replay.State.Settings.Instructions = []llm.Instruction{{Kind: llm.InstructionKindText, Level: llm.InstructionLevelApplication, Text: "You are a helpful assistant"}}
			planning, err := capabilities.NewProviderPlanning(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			summary, err := PrepareCompactInput(context.Background(), compact, replay)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := planning.Compact(context.Background(), summary); err != nil {
				t.Fatal(err)
			}
			if len(adapter.inputs) != 1 {
				t.Fatalf("compile inputs = %d", len(adapter.inputs))
			}
			instructions := adapter.inputs[0].Request.Instructions
			if len(instructions) != 3 || instructions[0].Level != test.want || instructions[1].Level != test.want || instructions[2].Level != llm.InstructionLevelApplication {
				t.Fatalf("compiled instructions = %#v, want summarizer level %q", instructions, test.want)
			}
			if summary.Request.Instructions[0].Level != llm.InstructionLevelPolicy {
				t.Fatal("planning changed the semantic summarizer request")
			}
		})
	}
}
