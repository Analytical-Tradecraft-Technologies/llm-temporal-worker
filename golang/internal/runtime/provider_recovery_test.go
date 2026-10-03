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

	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/openairesponses"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

func recoveryFixture(t *testing.T) (V1RuntimeCapabilities, *planningSource, *planningAdapter, PreparedGenerateInput, PlannedProviderCall, ProviderRecoveryBinding) {
	t.Helper()
	capabilities, source, adapter, request, _, _ := planningFixture()
	planning, err := capabilities.NewProviderPlanning(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareGenerateInput(context.Background(), request, durable.GenerateReplay{})
	if err != nil {
		t.Fatal(err)
	}
	planned, err := planning.Generate(context.Background(), prepared)
	if err != nil {
		t.Fatal(err)
	}
	route, err := planned.Route("paid-operation", "generation")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := planned.RecoveryBinding(route)
	if err != nil {
		t.Fatal(err)
	}
	adapter.inputs = nil
	return capabilities, source, adapter, prepared, planned, binding
}

func assertRecoveryError(t *testing.T, err error, code provider.Code, retry provider.RetryDisposition) {
	t.Helper()
	assertPlanningError(t, err, code)
	var mapped *provider.Error
	errors.As(err, &mapped)
	if mapped.Retry != retry {
		t.Fatalf("recovery retry = %s, want %s", mapped.Retry, retry)
	}
}

func TestProviderRecoveryRealCompilerRestoresBothActivitiesWithoutHTTP(t *testing.T) {
	capabilities, _, _, request, replay, compact := planningFixture()
	var httpCalls atomic.Int32
	client, err := openairesponses.NewClient(openairesponses.ClientConfig{BaseURL: "https://api.openai.com/v1/", APIKey: "test-key",
		HTTPClient: &http.Client{Transport: planningTransportFunc(func(*http.Request) (*http.Response, error) {
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
	capabilities.Planner = nil // Recovery requires no selection planner or pricer.
	recovery, err := capabilities.NewProviderRecovery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"generate", "compact"} {
		t.Run(kind, func(t *testing.T) {
			var original PlannedProviderCall
			var err error
			if kind == "generate" {
				original, err = planning.Generate(context.Background(), generate)
			} else {
				original, err = planning.Compact(context.Background(), summary)
			}
			if err != nil {
				t.Fatal(err)
			}
			route, _ := original.Route("paid-operation", "generation")
			binding, err := original.RecoveryBinding(route)
			if err != nil {
				t.Fatal(err)
			}
			// The projection round-trips without SDK objects or the raw key.
			data, err := json.Marshal(binding)
			if err != nil {
				t.Fatal(err)
			}
			for _, value := range []string{"SDKParams", "Adapter", "sensitive prompt", "sensitive history", `"OperationKey"`} {
				if strings.Contains(string(data), value) {
					t.Fatal("recovery binding contains invocation-local data")
				}
			}
			var stored ProviderRecoveryBinding
			if err := json.Unmarshal(data, &stored); err != nil {
				t.Fatal(err)
			}
			var restored PlannedProviderCall
			if kind == "generate" {
				restored, err = recovery.Generate(context.Background(), generate, stored)
			} else {
				restored, err = recovery.Compact(context.Background(), summary, stored)
			}
			if err != nil || restored.Candidate != original.Candidate || restored.CacheIdentity != original.CacheIdentity ||
				restored.ConfigDigest != original.ConfigDigest || restored.ConfigEpoch != original.ConfigEpoch || restored.Call.Metadata != original.Call.Metadata {
				t.Fatalf("reconstruction changed bindings: %v", err)
			}
			before, _ := json.Marshal(original.Call.SDKParams)
			after, _ := json.Marshal(restored.Call.SDKParams)
			if string(before) != string(after) {
				t.Fatal("reconstructed SDK request differs from the original")
			}
			if got, err := restored.RecoveryBinding(route); err != nil || got != binding {
				t.Fatalf("reconstructed identity differs: %v", err)
			}
		})
	}
	if httpCalls.Load() != 0 {
		t.Fatal("recovery performed HTTP requests")
	}
}

func TestProviderRecoveryDoesNotSelectNewRouteOrFallBack(t *testing.T) {
	capabilities, source, first, request, _, _ := planningFixture()
	second := &planningAdapter{version: first.version}
	model := source.value.Routes.Models["alias"]
	route := model.Routes[0]
	route.ID, route.EndpointID = "second-route", "second-endpoint"
	model.Routes = append(model.Routes, route)
	source.value.Routes.Models["alias"] = model
	capabilities.Adapters = engine.AdapterMap{"endpoint": first, "second-endpoint": second}
	first.compile = func(provider.CompileInput) (provider.Call, error) {
		return provider.Call{}, errors.New("sensitive unavailable compiler")
	}
	planning, err := capabilities.NewProviderPlanning(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareGenerateInput(context.Background(), request, durable.GenerateReplay{})
	if err != nil {
		t.Fatal(err)
	}
	original, err := planning.Generate(context.Background(), prepared)
	if err != nil || original.Candidate.RouteID != "second-route" {
		t.Fatalf("failed to select the original second route: %v", err)
	}
	savedRoute, _ := original.Route("paid-operation", "generation")
	binding, _ := original.RecoveryBinding(savedRoute)
	first.inputs, second.inputs = nil, nil
	first.compile = nil // The earlier route can now compile, but cannot win replay.
	capabilities.Planner = planningPlannerFunc(func(context.Context, routing.Input) (routing.Plan, error) {
		panic("recovery must not call selection planner")
	})
	var lookups []string
	capabilities.Adapters = planningRegistryFunc(func(_ context.Context, candidate routing.Candidate) (provider.Adapter, error) {
		lookups = append(lookups, candidate.EndpointID)
		if candidate.EndpointID != "second-endpoint" {
			t.Fatal("recovery looked up a different route")
		}
		return second, nil
	})
	recovery, err := capabilities.NewProviderRecovery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	restored, err := recovery.Generate(context.Background(), prepared, binding)
	if err != nil || restored.Candidate != original.Candidate || len(first.inputs) != 0 || len(second.inputs) != 1 {
		t.Fatalf("recovery changed the selected route: %v", err)
	}
	second.compile = func(provider.CompileInput) (provider.Call, error) {
		return provider.Call{}, errors.New("sensitive local failure")
	}
	_, err = recovery.Generate(context.Background(), prepared, binding)
	assertRecoveryError(t, err, provider.CodeStateUnavailable, provider.RetrySameOperation)
	if !reflect.DeepEqual(lookups, []string{"second-endpoint", "second-endpoint"}) || len(first.inputs) != 0 {
		t.Fatal("failed reconstruction fell back")
	}
}

func TestProviderRecoveryRejectsChangedBindingsBeforeAdapterLookup(t *testing.T) {
	mutations := map[string]func(*ProviderRecoveryBinding){
		"zero config": func(b *ProviderRecoveryBinding) { b.ConfigDigest = [32]byte{} },
		"config":      func(b *ProviderRecoveryBinding) { b.ConfigDigest[0]++ }, "epoch": func(b *ProviderRecoveryBinding) { b.ConfigEpoch = "other" },
		"request": func(b *ProviderRecoveryBinding) { b.RequestDigest[0]++ }, "operation key": func(b *ProviderRecoveryBinding) { b.OperationKeyDigest[0]++ },
		"candidate": func(b *ProviderRecoveryBinding) { b.CandidateID = "other" }, "family": func(b *ProviderRecoveryBinding) { b.Family = string(provider.FamilyOpenAIChat) },
		"capability": func(b *ProviderRecoveryBinding) { b.CapabilityVersion = "other" }, "tier": func(b *ProviderRecoveryBinding) { b.ProviderTier = "other" },
		"requested class": func(b *ProviderRecoveryBinding) { b.RequestedClass = llm.ServiceClassStandard },
		"attempted class": func(b *ProviderRecoveryBinding) { b.AttemptedClass = llm.ServiceClassPriority },
		"route":           func(b *ProviderRecoveryBinding) { b.Route.RouteID = "other" }, "endpoint": func(b *ProviderRecoveryBinding) { b.Route.EndpointID = "other" },
		"provider": func(b *ProviderRecoveryBinding) { b.Route.Provider = "other" }, "model": func(b *ProviderRecoveryBinding) { b.Route.Model = "other" },
		"revision": func(b *ProviderRecoveryBinding) { b.Route.CacheIdentity.Revision = "other" }, "account": func(b *ProviderRecoveryBinding) { b.Route.CacheIdentity.Account = "other" },
		"region": func(b *ProviderRecoveryBinding) { b.Route.CacheIdentity.Region = "other" }, "compiler": func(b *ProviderRecoveryBinding) { b.Route.CacheIdentity.Compiler = "other" },
		"price": func(b *ProviderRecoveryBinding) { b.Route.PriceVersion = "other" }, "operation ID": func(b *ProviderRecoveryBinding) { b.Route.OperationID = "" },
		"generation ID": func(b *ProviderRecoveryBinding) { b.Route.GenerationID = "" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			capabilities, _, _, prepared, _, binding := recoveryFixture(t)
			capabilities.Adapters = planningRegistryFunc(func(context.Context, routing.Candidate) (provider.Adapter, error) {
				t.Fatal("invalid recovery reached adapter lookup")
				return nil, nil
			})
			recovery, err := capabilities.NewProviderRecovery(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			mutate(&binding)
			_, err = recovery.Generate(context.Background(), prepared, binding)
			assertRecoveryError(t, err, provider.CodeConfiguration, provider.RetryNever)
		})
	}
}

func TestProviderRecoveryRejectsChangedSemanticInputAndEligibility(t *testing.T) {
	for _, name := range []string{"input", "operation key", "tenant", "project", "actor", "fallbacks", "portability", "removed route", "unauthorized tenant", "region", "context limit"} {
		t.Run(name, func(t *testing.T) {
			capabilities, source, _, prepared, _, binding := recoveryFixture(t)
			input, _ := llm.NormalizeRequest(prepared.Request)
			prepared.Request = input
			model := source.value.Routes.Models["alias"]
			switch name {
			case "input":
				prepared.Request.Input[0].(llm.Message).Content[0] = llm.TextPart{Text: "other content"}
			case "operation key":
				// Semantic/cache identity excludes this key; recovery must bind it separately.
				prepared.Request.OperationKey = "other-operation"
			case "tenant":
				prepared.Request.Context.Tenant = "other"
			case "project":
				prepared.Request.Context.Project = "other"
			case "actor":
				prepared.Request.Context.Actor = "other"
			case "fallbacks":
				prepared.Request.ServiceClassFallbacks = nil
			case "portability":
				prepared.Request.Portability = llm.PortabilityBestEffort
			case "removed route":
				model.Routes[0].ID = "replacement"
			case "unauthorized tenant":
				model.Routes[0].AllowedTenants = []string{"other"}
			case "region":
				model.Routes[0].AllowedRegions = []string{"allowed"}
				prepared.Request.Context.Tags = map[string]string{"region": "denied"}
			case "context limit":
				model.Routes[0].ContextBytes = 1
			}
			source.value.Routes.Models["alias"] = model
			capabilities.Adapters = planningRegistryFunc(func(context.Context, routing.Candidate) (provider.Adapter, error) {
				t.Fatal("changed input/eligibility reached adapter lookup")
				return nil, nil
			})
			recovery, err := capabilities.NewProviderRecovery(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			_, err = recovery.Generate(context.Background(), prepared, binding)
			assertRecoveryError(t, err, provider.CodeConfiguration, provider.RetryNever)
		})
	}
}

func TestProviderRecoveryCapturesSnapshotAndHonorsHealthBlock(t *testing.T) {
	for _, name := range []string{"disabled", "open", "auth open"} {
		t.Run(name, func(t *testing.T) {
			capabilities, source, adapter, prepared, _, binding := recoveryFixture(t)
			health := routing.RouteHealth{Enabled: name != "disabled", Open: name == "open", AuthOpen: name == "auth open"}
			source.value.Health.Routes = map[string]routing.RouteHealth{"route": health}
			recovery, err := capabilities.NewProviderRecovery(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			_, err = recovery.Generate(context.Background(), prepared, binding)
			assertRecoveryError(t, err, provider.CodeNoRoute, provider.RetrySameOperation)
			if len(adapter.inputs) != 0 {
				t.Fatal("blocked route reached compilation")
			}
		})
	}
	capabilities, source, _, prepared, original, binding := recoveryFixture(t)
	recovery, err := capabilities.NewProviderRecovery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	source.value.ConfigDigest[0]++
	source.value.ConfigEpoch = "new epoch"
	source.value.Routes.Models["alias"].Routes[0].Model = "new-model"
	source.value.Routes.Models["alias"].Routes[0].Capabilities.Features[routing.FeatureText] = routing.Capability{State: routing.CapabilityUnsupported}
	restored, err := recovery.Generate(context.Background(), prepared, binding)
	if err != nil || restored.Candidate != original.Candidate || source.reads != 2 {
		t.Fatalf("captured recovery snapshot changed or was reread: %v", err)
	}
	capabilities.ConfigDigest = source.value.ConfigDigest
	reloaded, err := capabilities.NewProviderRecovery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, err = reloaded.Generate(context.Background(), prepared, binding)
	assertRecoveryError(t, err, provider.CodeConfiguration, provider.RetryNever)
}

func TestProviderRecoveryPreservesResolvedQuoteVersionWithoutPricing(t *testing.T) {
	capabilities, source, _, request, _, _ := planningFixture()
	source.value.Routes.Models["alias"].Routes[0].PriceVersion = ""
	planning, err := capabilities.NewProviderPlanning(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	prepared, _ := PrepareGenerateInput(context.Background(), request, durable.GenerateReplay{})
	original, err := planning.Generate(context.Background(), prepared)
	if err != nil {
		t.Fatal(err)
	}
	route, _ := original.Route("paid-operation", "generation")
	route.PriceVersion = "original-quoted-price"
	binding, err := original.RecoveryBinding(route)
	if err != nil {
		t.Fatal(err)
	}
	recovery, err := capabilities.NewProviderRecovery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	restored, err := recovery.Generate(context.Background(), prepared, binding)
	if err != nil {
		t.Fatal(err)
	}
	if rebound, err := restored.RecoveryBinding(route); err != nil || rebound.Route.PriceVersion != "original-quoted-price" || restored.Candidate.PriceVersion != "" {
		t.Fatalf("lost original quote version or changed configured candidate: %v", err)
	}
}

func TestProviderRecoveryCompilerFailuresNeverBecomeFallbackOrPermission(t *testing.T) {
	for _, name := range []string{"registry", "capability", "compile", "nil adapter", "capability version", "wrong call", "possible dispatch", "canceled compile"} {
		t.Run(name, func(t *testing.T) {
			capabilities, _, adapter, prepared, _, binding := recoveryFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			code, retry := provider.CodeStateUnavailable, provider.RetrySameOperation
			switch name {
			case "registry":
				capabilities.Adapters = planningRegistryFunc(func(context.Context, routing.Candidate) (provider.Adapter, error) {
					return nil, errors.New("sensitive registry error")
				})
			case "capability":
				adapter.capabilityErr = errors.New("sensitive capability error")
			case "compile":
				adapter.compile = func(provider.CompileInput) (provider.Call, error) {
					return provider.Call{}, errors.New("sensitive compile error")
				}
			case "nil adapter":
				code, retry = provider.CodeConfiguration, provider.RetryNever
				capabilities.Adapters = engine.AdapterMap{"endpoint": (*planningAdapter)(nil)}
			case "capability version":
				code, retry = provider.CodeConfiguration, provider.RetryNever
				adapter.version = "other"
			case "wrong call":
				code, retry = provider.CodeConfiguration, provider.RetryNever
				adapter.compile = func(input provider.CompileInput) (provider.Call, error) {
					call := planningCall(input)
					call.Metadata.SchemaDigest[0]++
					return call, nil
				}
			case "possible dispatch":
				adapter.compile = func(provider.CompileInput) (provider.Call, error) {
					return provider.Call{}, provider.NewError(provider.CodeInternal, provider.PhaseCompile, provider.DispatchAccepted, provider.RetryNextRoute, "sensitive paid result")
				}
			case "canceled compile":
				adapter.compile = func(input provider.CompileInput) (provider.Call, error) { cancel(); return planningCall(input), nil }
			}
			recovery, err := capabilities.NewProviderRecovery(ctx)
			if err != nil {
				t.Fatal(err)
			}
			result, err := recovery.Generate(ctx, prepared, binding)
			if result.Adapter != nil || result.Call.SDKParams != nil {
				t.Fatal("failed recovery returned a usable call")
			}
			if name == "possible dispatch" {
				var mapped *provider.Error
				if !errors.As(err, &mapped) || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever || mapped.Cause != nil || strings.Contains(err.Error(), "sensitive") {
					t.Fatalf("possible dispatch was lost: %#v", err)
				}
			} else if name == "canceled compile" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal("canceled compiler returned a usable call", err)
				}
			} else {
				assertRecoveryError(t, err, code, retry)
			}
		})
	}
}

func TestProviderRecoveryRejectsMissingCapabilitiesNoWorkAndInvalidProjection(t *testing.T) {
	for _, name := range []string{"nil context", "canceled", "source", "registry", "source error", "digest mismatch", "zero digest", "empty epoch", "catalog"} {
		t.Run(name, func(t *testing.T) {
			capabilities, source, _, _, _, _ := planningFixture()
			ctx := context.Background()
			code := provider.CodeConfiguration
			retry := provider.RetryNever
			switch name {
			case "nil context":
				ctx = nil
			case "canceled":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			case "source":
				capabilities.Snapshot = (*planningSource)(nil)
			case "registry":
				capabilities.Adapters = planningRegistryFunc(nil)
			case "source error":
				source.err = errors.New("sensitive snapshot error")
				code = provider.CodeStateUnavailable
				retry = provider.RetrySameOperation
			case "digest mismatch":
				source.value.ConfigDigest[0]++
			case "zero digest":
				capabilities.ConfigDigest, source.value.ConfigDigest = [32]byte{}, [32]byte{}
			case "empty epoch":
				source.value.ConfigEpoch = ""
			case "catalog":
				source.value.Routes.Version = ""
			}
			_, err := capabilities.NewProviderRecovery(ctx)
			if name == "canceled" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else {
				assertRecoveryError(t, err, code, retry)
			}
		})
	}
	capabilities, _, adapter, prepared, planned, binding := recoveryFixture(t)
	recovery, _ := capabilities.NewProviderRecovery(context.Background())
	_, err := recovery.Compact(context.Background(), PreparedCompactInput{}, binding)
	assertRecoveryError(t, err, provider.CodeInvalidArgument, provider.RetryNever)
	for _, value := range []*ProviderRecovery{nil, {}} {
		_, err := value.Generate(context.Background(), prepared, binding)
		assertRecoveryError(t, err, provider.CodeConfiguration, provider.RetryNever)
	}
	if _, err := recovery.Generate(nil, prepared, binding); err == nil {
		t.Fatal("nil context accepted")
	}
	if len(adapter.inputs) != 0 {
		t.Fatal("invalid recovery compiled")
	}
	for _, mutate := range []func(*PlannedProviderCall, *durable.RoutePlan){
		func(p *PlannedProviderCall, _ *durable.RoutePlan) { p.ConfigDigest = [32]byte{} },
		func(p *PlannedProviderCall, _ *durable.RoutePlan) { p.Adapter = nil },
		func(_ *PlannedProviderCall, r *durable.RoutePlan) { r.EndpointID = "other" },
		func(_ *PlannedProviderCall, r *durable.RoutePlan) { r.PriceVersion = "other" },
	} {
		invalid, route := planned, binding.Route
		mutate(&invalid, &route)
		if _, err := invalid.RecoveryBinding(route); err == nil {
			t.Fatal("invalid invocation projected a recovery binding")
		}
	}
}

func TestProviderRecoveryConcurrentCallsUseIndependentCompilerInput(t *testing.T) {
	capabilities, _, adapter, prepared, _, binding := recoveryFixture(t)
	recovery, err := capabilities.NewProviderRecovery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for range 20 {
		group.Add(1)
		go func() {
			defer group.Done()
			restored, err := recovery.Generate(context.Background(), prepared, binding)
			if err != nil {
				t.Error(err)
				return
			}
			if restored.Call.OperationKey != prepared.Request.OperationKey {
				t.Error("operation binding changed")
			}
		}()
	}
	group.Wait()
	if len(adapter.inputs) != 20 {
		t.Fatal("lost concurrent reconstruction")
	}
	for _, input := range adapter.inputs {
		if !input.Strict || !reflect.DeepEqual(input.Request.Input, prepared.Request.Input) {
			t.Fatal("recovery changed content or portability")
		}
	}
}
