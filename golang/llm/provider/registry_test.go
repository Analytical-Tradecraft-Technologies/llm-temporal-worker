package provider_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

type registryFixtureParams struct{}

type registryFixtureAdapter struct {
	family          provider.Family
	capabilityCalls *int
}

type registryFixtureModelLister struct{ registryFixtureAdapter }

func (*registryFixtureModelLister) ListModels(_ context.Context, query provider.ModelListQuery) (provider.ModelListPage, error) {
	if err := query.Validate(); err != nil {
		return provider.ModelListPage{}, err
	}
	return provider.ModelListPage{Complete: true, Models: []provider.Model{{ProviderModelID: "fixture-model", Lifecycle: provider.ModelAvailable}}}, nil
}

func (adapter *registryFixtureAdapter) Name() string { return "fixture/adapter" }

func (adapter *registryFixtureAdapter) Capabilities(context.Context, provider.CapabilityQuery) (provider.CapabilitySet, error) {
	if adapter.capabilityCalls != nil {
		*adapter.capabilityCalls++
	}
	return registryFixtureCapabilities(), nil
}

func (adapter *registryFixtureAdapter) Compile(context.Context, provider.CompileInput) (provider.Call, error) {
	return provider.Call{Family: adapter.family, SDKParams: registryFixtureParams{}}, nil
}

func (*registryFixtureAdapter) Invoke(context.Context, provider.Call, provider.Observer) (provider.Result, error) {
	return provider.Result{}, errors.New("fixture adapter must not be invoked by registry")
}

func registryFixtureCapabilities() provider.CapabilitySet {
	features := map[provider.Feature]provider.Capability{}
	for _, feature := range []provider.Feature{
		provider.FeatureText,
		provider.FeatureImage,
		provider.FeatureDocument,
		provider.FeatureToolCall,
		provider.FeatureStructuredOutput,
		provider.FeatureReasoning,
		provider.FeatureContinuation,
		provider.FeatureUsage,
	} {
		features[feature] = provider.Capability{State: provider.CapabilityNative}
	}
	features[provider.FeatureStreaming] = provider.Capability{State: provider.CapabilityUnsupported, Reason: "one-shot fixture"}
	return provider.CapabilitySet{Version: "fixture/v1", Features: features}
}

func registryFixtureRegistration() provider.Registration {
	adapter := &registryFixtureAdapter{family: provider.FamilyOpenAIChat}
	return provider.Registration{
		Family:               provider.FamilyOpenAIChat,
		ProfileID:            "fixture-chat",
		Adapter:              adapter,
		Capabilities:         registryFixtureCapabilities(),
		ServiceTiers:         map[llm.ServiceClass]string{llm.ServiceClassEconomy: "flex", llm.ServiceClassStandard: "default", llm.ServiceClassPriority: "priority"},
		SDKParameterTypeName: "registryFixtureParams",
		SDKParameterCheck: func(value any) bool {
			_, ok := value.(registryFixtureParams)
			return ok
		},
		CompileProbe: func(ctx context.Context, adapter provider.Adapter) (provider.Call, error) {
			return adapter.Compile(ctx, provider.CompileInput{})
		},
		ClientFactory: func() (any, error) { return struct{}{}, nil },
	}
}

func TestRegistryValidatesAndResolvesExplicitProfile(t *testing.T) {
	registration := registryFixtureRegistration()
	constructed := 0
	registration.ClientFactory = func() (any, error) {
		constructed++
		return struct{}{}, nil
	}
	registry, err := provider.NewRegistry(registration)
	if err != nil {
		t.Fatal(err)
	}
	if constructed != 1 {
		t.Fatalf("client factory calls = %d, want exactly one", constructed)
	}
	adapter, err := registry.Adapter(provider.FamilyOpenAIChat, "fixture-chat")
	if err != nil {
		t.Fatal(err)
	}
	if adapter.Name() != "fixture/adapter" {
		t.Fatalf("resolved adapter name = %q", adapter.Name())
	}
	if _, err := registry.Adapter(provider.FamilyOpenAIChat, "missing"); err == nil {
		t.Fatal("missing profile resolved successfully")
	}
	entries := registry.Registrations()
	if len(entries) != 1 || entries[0].ProfileID != "fixture-chat" {
		t.Fatalf("registrations = %#v", entries)
	}

	entry, ok := registry.Lookup(provider.FamilyOpenAIChat, "fixture-chat")
	if !ok {
		t.Fatal("Lookup did not find registered profile")
	}
	entry.ServiceTiers[llm.ServiceClassStandard] = "mutated"
	entry.Capabilities.Features[provider.FeatureText] = provider.Capability{State: provider.CapabilityUnsupported}
	again, ok := registry.Lookup(provider.FamilyOpenAIChat, "fixture-chat")
	if !ok || again.ServiceTiers[llm.ServiceClassStandard] != "default" || again.Capabilities.Features[provider.FeatureText].State != provider.CapabilityNative {
		t.Fatal("registry lookup exposed mutable contract maps")
	}
}

func TestRegistryResolvesOptionalModelListerWithoutMakingItMandatory(t *testing.T) {
	registration := registryFixtureRegistration()
	registration.Adapter = &registryFixtureModelLister{registryFixtureAdapter{family: provider.FamilyOpenAIChat}}
	registry, err := provider.NewRegistry(registration)
	if err != nil {
		t.Fatal(err)
	}
	lister, err := registry.ModelLister(provider.FamilyOpenAIChat, "fixture-chat")
	if err != nil {
		t.Fatal(err)
	}
	page, err := lister.ListModels(context.Background(), provider.ModelListQuery{EndpointID: "fixture-endpoint", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := page.Validate(); err != nil {
		t.Fatal(err)
	}
	oneShotRegistry, err := provider.NewRegistry(registryFixtureRegistration())
	if err != nil {
		// A one-shot adapter remains a valid registration even without the
		// optional model-management capability.
		t.Fatal(err)
	}
	if _, err := oneShotRegistry.ModelLister(provider.FamilyOpenAIChat, "fixture-chat"); !errors.Is(err, provider.ErrModelInventoryUnsupported) {
		t.Fatalf("one-shot model-lister error = %v, want ErrModelInventoryUnsupported", err)
	}
	if _, err := registry.ModelLister(provider.FamilyOpenAIChat, "missing"); err == nil {
		t.Fatal("missing model-lister profile resolved successfully")
	}
}

func TestRegistryOrdersProfilesByFamilyAndID(t *testing.T) {
	first := registryFixtureRegistration()
	second := registryFixtureRegistration()
	second.ProfileID = "fixture-responses"
	second.Family = provider.FamilyOpenAIResponses
	second.Adapter = &registryFixtureAdapter{family: provider.FamilyOpenAIResponses}
	registry, err := provider.NewRegistry(first, second)
	if err != nil {
		t.Fatal(err)
	}
	entries := registry.Registrations()
	if got := []string{string(entries[0].Family) + "/" + entries[0].ProfileID, string(entries[1].Family) + "/" + entries[1].ProfileID}; !reflect.DeepEqual(got, []string{"openai_chat/fixture-chat", "openai_responses/fixture-responses"}) {
		t.Fatalf("registration order = %#v", got)
	}
}

func TestRegistryRejectsInvalidContractsWithoutFactorySideEffects(t *testing.T) {
	tests := []struct {
		name string
		edit func(*provider.Registration)
		want string
	}{
		{name: "invalid family", edit: func(value *provider.Registration) { value.Family = provider.Family("other") }, want: "family"},
		{name: "invalid profile ID", edit: func(value *provider.Registration) { value.ProfileID = "Fixture Chat" }, want: "profile ID"},
		{name: "typed nil adapter", edit: func(value *provider.Registration) { var adapter *registryFixtureAdapter; value.Adapter = adapter }, want: "concrete adapter"},
		{name: "missing feature", edit: func(value *provider.Registration) { delete(value.Capabilities.Features, provider.FeatureUsage) }, want: "usage"},
		{name: "capability drift", edit: func(value *provider.Registration) {
			value.Capabilities.Features[provider.FeatureText] = provider.Capability{State: provider.CapabilityUnsupported}
		}, want: "do not match"},
		{name: "unknown feature", edit: func(value *provider.Registration) {
			value.Capabilities.Features[provider.Feature("future_feature")] = provider.Capability{State: provider.CapabilityUnsupported}
		}, want: "not governed"},
		{name: "streaming advertised", edit: func(value *provider.Registration) {
			value.Capabilities.Features[provider.FeatureStreaming] = provider.Capability{State: provider.CapabilityNative}
		}, want: "streaming"},
		{name: "missing tier", edit: func(value *provider.Registration) { delete(value.ServiceTiers, llm.ServiceClassPriority) }, want: "priority"},
		{name: "non-canonical tier", edit: func(value *provider.Registration) { value.ServiceTiers[llm.ServiceClassStandard] = " default " }, want: "canonical"},
		{name: "probe mismatch", edit: func(value *provider.Registration) { value.SDKParameterCheck = func(any) bool { return false } }, want: "SDK parameter"},
		{name: "missing probe", edit: func(value *provider.Registration) { value.CompileProbe = nil }, want: "compile probe"},
		{name: "missing factory", edit: func(value *provider.Registration) { value.ClientFactory = nil }, want: "client factory"},
		{name: "nil factory result", edit: func(value *provider.Registration) { value.ClientFactory = func() (any, error) { return nil, nil } }, want: "client construction"},
		{name: "factory error", edit: func(value *provider.Registration) {
			value.ClientFactory = func() (any, error) { return nil, errors.New("secret api key") }
		}, want: "client construction"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registration := registryFixtureRegistration()
			factoryCalls := 0
			registration.ClientFactory = func() (any, error) {
				factoryCalls++
				return struct{}{}, nil
			}
			test.edit(&registration)
			_, err := provider.NewRegistry(registration)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(test.want)) {
				t.Fatalf("NewRegistry() error = %v, want substring %q", err, test.want)
			}
			if strings.Contains(err.Error(), "secret api key") {
				t.Fatal("factory error leaked through registry diagnostics")
			}
			if test.name == "invalid family" || test.name == "invalid profile ID" || test.name == "typed nil adapter" || test.name == "missing feature" || test.name == "capability drift" || test.name == "unknown feature" || test.name == "streaming advertised" || test.name == "missing tier" || test.name == "non-canonical tier" || test.name == "probe mismatch" || test.name == "missing probe" || test.name == "missing factory" {
				if factoryCalls != 0 {
					t.Fatalf("factory calls = %d for pre-construction validation", factoryCalls)
				}
			}
		})
	}
}

func TestRegistryRejectsDuplicateFamilyAndProfile(t *testing.T) {
	registration := registryFixtureRegistration()
	capabilityCalls := 0
	probeCalls := 0
	factoryCalls := 0
	registration.Adapter = &registryFixtureAdapter{family: provider.FamilyOpenAIChat, capabilityCalls: &capabilityCalls}
	registration.CompileProbe = func(ctx context.Context, adapter provider.Adapter) (provider.Call, error) {
		probeCalls++
		return adapter.Compile(ctx, provider.CompileInput{})
	}
	registration.ClientFactory = func() (any, error) {
		factoryCalls++
		return struct{}{}, nil
	}
	registry, err := provider.NewRegistry(registration)
	if err != nil {
		t.Fatal(err)
	}
	if capabilityCalls != 1 || probeCalls != 1 || factoryCalls != 1 {
		t.Fatalf("initial registration side effects = capabilities %d, probe %d, factory %d; want one each", capabilityCalls, probeCalls, factoryCalls)
	}
	if err := registry.Register(registration); err == nil {
		t.Fatal("duplicate registration succeeded")
	}
	if capabilityCalls != 1 || probeCalls != 1 || factoryCalls != 1 {
		t.Fatalf("duplicate registration triggered side effects = capabilities %d, probe %d, factory %d; want unchanged at one each", capabilityCalls, probeCalls, factoryCalls)
	}
}
