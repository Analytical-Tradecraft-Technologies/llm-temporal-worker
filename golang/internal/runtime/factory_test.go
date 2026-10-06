package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/anthropicmessages"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/blob"
	redisstore "github.com/mfow/llm-temporal-worker/golang/storage/redis"
	redisclient "github.com/redis/go-redis/v9"
)

func TestRedisKeyOptionsUseConfiguredPrefix(t *testing.T) {
	value := config.Config{}
	value.State.Redis.KeyPrefix = "worker-a.v1"
	value.State.Redis.AdmissionHashTag = "admission"
	options, err := redisKeyOptions(value, []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatal(err)
	}
	if options.Prefix != "worker-a.v1" {
		t.Fatalf("Redis key prefix = %q, want worker-a.v1", options.Prefix)
	}
	if _, err := redisstore.NewKeyOptions("bad prefix", "admission", options.KeySecret); err == nil {
		t.Fatal("invalid Redis key prefix accepted")
	}
}

func TestComposeBudgetStatusReaderBindsSnapshotOwnedRedisCapabilities(t *testing.T) {
	client := redisclient.NewClient(&redisclient.Options{Addr: "127.0.0.1:0"})
	defer client.Close()
	keys, err := redisstore.NewBudgetKeySpace(redisstore.KeyOptions{
		Prefix:    "worker",
		HashTag:   "budget",
		KeySecret: []byte("01234567890123456789012345678901"),
	})
	if err != nil {
		t.Fatal(err)
	}
	generation := &fakeBudgetGenerationProbe{}
	snapshot := &config.Snapshot{}
	var observed redisstore.BudgetStatusReaderOptions
	calls := 0
	factory := func(_ context.Context, gotSnapshot *config.Snapshot, options redisstore.BudgetStatusReaderOptions) (BudgetStatusReader, error) {
		calls++
		if gotSnapshot != snapshot {
			t.Fatalf("snapshot = %p, want %p", gotSnapshot, snapshot)
		}
		observed = options
		return &fakeBudgetStatus{}, nil
	}
	reader, err := composeBudgetStatusReader(context.Background(), snapshot, time.Now, string(redisstore.AdmissionModeFunction), client, generation, keys, factory)
	if err != nil {
		t.Fatalf("composeBudgetStatusReader() error = %v", err)
	}
	if reader == nil || calls != 1 {
		t.Fatalf("reader=%T calls=%d, want one snapshot reader", reader, calls)
	}
	if observed.Client != client || observed.Generation != generation || observed.Keys.ActiveGenerationKey() != keys.ActiveGenerationKey() || observed.Mode != redisstore.AdmissionModeFunction || observed.FunctionVersion != redisstore.BudgetStatusFunctionVersion {
		t.Fatalf("reader options were not snapshot-owned: client=%T generation=%T mode=%q version=%q", observed.Client, observed.Generation, observed.Mode, observed.FunctionVersion)
	}

	calls = 0
	if reader, err := composeBudgetStatusReader(context.Background(), snapshot, time.Now, "function", nil, generation, keys, factory); err != nil || reader != nil || calls != 0 {
		t.Fatalf("missing Redis client should remain unavailable: reader=%T err=%v calls=%d", reader, err, calls)
	}
	if reader, err := composeBudgetStatusReader(context.Background(), snapshot, time.Now, "function", client, nil, keys, factory); err != nil || reader != nil || calls != 0 {
		t.Fatalf("missing generation should remain unavailable: reader=%T err=%v calls=%d", reader, err, calls)
	}
}

func TestProductionFactoryAttachesSnapshotV1RuntimeAfterClientConstruction(t *testing.T) {
	snapshot := &config.Snapshot{}
	expected := testV1Runtime{}
	var gotSnapshot *config.Snapshot
	var gotEngine llm.Engine
	var gotClients app.ClientSet
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
		V1RuntimeBuilder: func(_ context.Context, got *config.Snapshot, engineValue llm.Engine, clients app.ClientSet) (activity.V1Runtime, error) {
			gotSnapshot, gotEngine, gotClients = got, engineValue, clients
			return expected, nil
		},
	}}
	clients := &productionClientSet{}
	engineValue := testEngine{}
	gotEngineValue, gotClientSet, err := factory.attachV1Runtime(context.Background(), snapshot, engineValue, clients)
	if err != nil {
		t.Fatalf("attachV1Runtime() error = %v", err)
	}
	if gotSnapshot != snapshot || gotEngine != engineValue || gotClients != clients {
		t.Fatalf("builder inputs snapshot=%p engine=%T clients=%T", gotSnapshot, gotEngine, gotClients)
	}
	if gotEngineValue != engineValue || gotClientSet != clients {
		t.Fatalf("attached values engine=%T clients=%T", gotEngineValue, gotClientSet)
	}
	if got := clients.V1Runtime(); got != expected {
		t.Fatalf("attached v1 runtime = %T, want %T", got, expected)
	}
}

func TestProductionFactoryV1RuntimeBuilderFailureClosesSnapshotClients(t *testing.T) {
	closed := false
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
		V1RuntimeBuilder: func(context.Context, *config.Snapshot, llm.Engine, app.ClientSet) (activity.V1Runtime, error) {
			return nil, errors.New("durable ports are incomplete")
		},
	}}
	clients := &productionClientSet{close: func(context.Context) error { closed = true; return nil }}
	_, _, err := factory.attachV1Runtime(context.Background(), &config.Snapshot{}, testEngine{}, clients)
	if err == nil || !strings.Contains(err.Error(), "construct durable v1 runtime") {
		t.Fatalf("attachV1Runtime() error = %v, want wrapped builder failure", err)
	}
	if !closed {
		t.Fatal("builder failure did not close snapshot clients")
	}
}

func TestProductionFactoryRejectsTypedNilV1RuntimeAndClosesSnapshotClients(t *testing.T) {
	closed := false
	var runtime *testV1Runtime
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
		V1RuntimeBuilder: func(context.Context, *config.Snapshot, llm.Engine, app.ClientSet) (activity.V1Runtime, error) {
			return runtime, nil
		},
	}}
	clients := &productionClientSet{close: func(context.Context) error { closed = true; return nil }}
	_, _, err := factory.attachV1Runtime(context.Background(), &config.Snapshot{}, testEngine{}, clients)
	if err == nil || !strings.Contains(err.Error(), "nil runtime") {
		t.Fatalf("attachV1Runtime() error = %v, want typed-nil runtime rejection", err)
	}
	if !closed {
		t.Fatal("typed-nil runtime rejection did not close snapshot clients")
	}
	if clients.v1Runtime != nil {
		t.Fatalf("typed-nil runtime was attached: %T", clients.v1Runtime)
	}
}

type checkpointBlobReaderStub struct{}

func (checkpointBlobReaderStub) Read(context.Context, string, state.CheckpointBlobReference) ([]byte, error) {
	return nil, nil
}

type checkpointMaterializerStub struct{}

func (*checkpointMaterializerStub) Materialize(context.Context, string, state.CheckpointID, state.MaterializeLimits) (state.MaterializedState, error) {
	return state.MaterializedState{}, nil
}

func (*checkpointMaterializerStub) MaterializeHandle(context.Context, string, string, state.MaterializeLimits) (state.MaterializedState, error) {
	return state.MaterializedState{}, nil
}

func TestCheckpointCapabilitiesRejectsIncompleteMaterializerBundles(t *testing.T) {
	materializer := &checkpointMaterializerStub{}
	reader := checkpointBlobReaderStub{}
	repository := builderCheckpointRepository{}
	tests := []struct {
		name         string
		capabilities CheckpointCapabilities
		wantValidate bool
	}{
		{name: "optional empty", capabilities: CheckpointCapabilities{}, wantValidate: true},
		{name: "optional repository", capabilities: CheckpointCapabilities{Repository: repository}, wantValidate: true},
		{name: "materializer without repository", capabilities: CheckpointCapabilities{Blobs: reader, Materializer: materializer}, wantValidate: false},
		{name: "materializer without blobs", capabilities: CheckpointCapabilities{Repository: repository, Materializer: materializer}, wantValidate: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.capabilities.Validate(); (err == nil) != test.wantValidate {
				t.Fatalf("Validate() error = %v, wantValid=%v", err, test.wantValidate)
			}
			if test.name == "optional empty" {
				if err := test.capabilities.RequireMaterializer(); err == nil {
					t.Fatal("RequireMaterializer accepted empty capability")
				}
			}
		})
	}
}

func TestBudgetCapabilityBelongsToSnapshot(t *testing.T) {
	budgets := builderBudgets{}
	set := &productionClientSet{budgets: budgets, v1Capabilities: V1RuntimeCapabilities{Budgets: budgets}}
	if set.Budgets() != budgets || set.V1RuntimeCapabilities().Budgets != budgets {
		t.Fatal("budget capability changed")
	}
	if (&productionClientSet{}).Budgets() != nil || (*productionClientSet)(nil).Budgets() != nil {
		t.Fatal("empty set exposed budget capability")
	}
}

func TestProductionClientSetReturnsSnapshotOwnedV1CapabilitiesWithoutFallback(t *testing.T) {
	firstClock := nowFunc(time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC))
	secondClock := nowFunc(time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC))
	first := V1RuntimeCapabilities{
		Snapshot: engine.StaticSnapshot{Value: engine.Snapshot{Version: "first"}},
		Planner:  routing.DeterministicPlanner{MaxRejections: 1},
		Adapters: engine.AdapterMap{},
		Clock:    firstClock,
	}
	second := V1RuntimeCapabilities{
		Snapshot: engine.StaticSnapshot{Value: engine.Snapshot{Version: "second"}},
		Planner:  routing.DeterministicPlanner{MaxRejections: 2},
		Adapters: engine.AdapterMap{},
		Clock:    secondClock,
	}
	firstSet := &productionClientSet{v1Capabilities: first}
	secondSet := &productionClientSet{v1Capabilities: second, v1Runtime: testV1Runtime{}}

	gotFirst := firstSet.V1RuntimeCapabilities()
	if gotFirst.Snapshot == nil || gotFirst.Planner == nil || gotFirst.Adapters == nil || gotFirst.Clock == nil {
		t.Fatalf("first snapshot capability bundle is incomplete: %#v", gotFirst)
	}
	firstSnapshot, err := gotFirst.Snapshot.Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if firstSnapshot.Version != "first" || gotFirst.Clock() != firstClock().UTC() {
		t.Fatalf("first snapshot capabilities = %#v, snapshot=%#v", gotFirst, firstSnapshot)
	}
	gotSecond := secondSet.V1RuntimeCapabilities()
	if gotSecond.Snapshot == nil || gotSecond.Planner == nil || gotSecond.Adapters == nil || gotSecond.Clock == nil {
		t.Fatalf("second snapshot capability bundle is incomplete: %#v", gotSecond)
	}
	secondSnapshot, err := gotSecond.Snapshot.Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if secondSnapshot.Version != "second" || gotSecond.Clock() != secondClock().UTC() {
		t.Fatalf("second snapshot capabilities = %#v, snapshot=%#v", gotSecond, secondSnapshot)
	}

	// A client set with only the legacy v1 runtime must not synthesize a
	// capability bundle from that process-level value.
	empty := (&productionClientSet{v1Runtime: testV1Runtime{}}).V1RuntimeCapabilities()
	if empty.Snapshot != nil || empty.Planner != nil || empty.Adapters != nil || empty.Budgets != nil || empty.ProviderStatusRecorder != nil || empty.Clock != nil {
		t.Fatalf("legacy runtime leaked into empty capability bundle: %#v", empty)
	}
	if empty.Checkpoints.Repository != nil || empty.Checkpoints.Blobs != nil || empty.Checkpoints.Materializer != nil {
		t.Fatalf("legacy runtime leaked checkpoint capabilities: %#v", empty.Checkpoints)
	}
}

type capabilityAdapterStub struct{}

func (*capabilityAdapterStub) Name() string { return "capability-test" }
func (*capabilityAdapterStub) Capabilities(context.Context, provider.CapabilityQuery) (provider.CapabilitySet, error) {
	return provider.CapabilitySet{}, nil
}
func (*capabilityAdapterStub) Compile(context.Context, provider.CompileInput) (provider.Call, error) {
	return provider.Call{}, nil
}
func (*capabilityAdapterStub) Invoke(context.Context, provider.Call, provider.Observer) (provider.Result, error) {
	return provider.Result{}, nil
}

func TestSnapshotAdapterRegistryOwnsPrivateMap(t *testing.T) {
	adapter := &capabilityAdapterStub{}
	input := engine.AdapterMap{"endpoint": adapter}
	registry := newSnapshotAdapterRegistry(input)
	if _, aliasesLegacyMap := registry.(engine.AdapterMap); aliasesLegacyMap {
		t.Fatal("snapshot adapter registry exposed mutable engine.AdapterMap type")
	}

	input["endpoint"] = nil
	got, err := registry.Adapter(context.Background(), routing.Candidate{EndpointID: "endpoint"})
	if err != nil {
		t.Fatalf("snapshot adapter registry lost copied adapter after input mutation: %v", err)
	}
	if got != adapter {
		t.Fatalf("snapshot adapter registry adapter = %p, want %p", got, adapter)
	}
	input["other"] = adapter
	if _, err := registry.Adapter(context.Background(), routing.Candidate{EndpointID: "other"}); err == nil {
		t.Fatal("snapshot adapter registry observed endpoint added to original map")
	}
}

type queryServiceStub struct{}

func (queryServiceStub) Execute(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	return llm.QueryResponseV1{}, nil
}

func TestBuildMemoryUsesOnlyProcessLocalState(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	called := false
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
		Clock: nowFunc(now),
		Resolver: secrets.ResolverFunc(func(_ context.Context, ref config.SecretRef) ([]byte, error) {
			if ref.Name != "CONTINUATION_KEY" {
				t.Fatalf("resolved unexpected secret %q", ref.Name)
			}
			return []byte("01234567890123456789012345678901"), nil
		}),
		RedisFactory: func(context.Context, config.RedisConfig, string, string) (redisclient.UniversalClient, error) {
			called = true
			return nil, errors.New("Redis must not be constructed for memory state")
		},
		BlobFactory: func(context.Context, config.Config) (blob.Store, io.Closer, error) {
			called = true
			return nil, nil, errors.New("external blob store must not be constructed for memory state")
		},
	}}
	value := config.Config{
		State:        config.StateConfig{Kind: config.StateKindMemory, ContinuationRetention: config.Duration(time.Hour), ReservationLease: config.Duration(time.Minute)},
		BlobStore:    config.BlobStoreConfig{Kind: "memory", InlineBytes: 256},
		Limits:       config.LimitsConfig{RequestBytes: 1024, ContinuationDepth: 4, RouteAttempts: 1, TokenEstimateSafetyRatio: "1", MaxOutputTokens: 16, MaxBudgetBucketsPerWindow: 100},
		Continuation: config.ContinuationConfig{HandleKeys: []config.HandleKey{{ID: "key-2026-07", Primary: true, Secret: config.SecretRef{Kind: config.SecretEnv, Name: "CONTINUATION_KEY"}}}},
	}
	engineValue, clients, err := factory.buildMemory(context.Background(), value, engine.StaticSnapshot{}, nil, nil, [32]byte{})
	if err != nil {
		t.Fatalf("buildMemory() error = %v", err)
	}
	if engineValue == nil || clients == nil {
		t.Fatalf("buildMemory() returned engine=%#v clients=%#v", engineValue, clients)
	}
	if called {
		t.Fatal("memory composition constructed an external state or blob dependency")
	}
	if probes := clients.(*productionClientSet).DependencyProbes(); len(probes) != 0 {
		t.Fatalf("memory composition exposed external dependency probes: %d", len(probes))
	}
	if recorder := clients.(*productionClientSet).ProviderStatusRecorder(); recorder != nil {
		t.Fatal("memory composition exposed a durable provider status recorder")
	}
	capabilities := clients.(*productionClientSet).V1RuntimeCapabilities()
	if capabilities.Snapshot == nil || capabilities.Planner == nil || capabilities.Adapters == nil || capabilities.Clock == nil {
		t.Fatalf("memory composition omitted v1 capability: %#v", capabilities)
	}
	if capabilities.Budgets != nil {
		t.Fatal("memory composition exposed a durable budget capability")
	}
	if capabilities.BudgetEstimator.MaxOutput != 16 || capabilities.BudgetEstimator.SafetyRatio.RatString() != "1" || capabilities.MaxBudgetBucketsPerWindow != 100 {
		t.Fatal("memory composition omitted configured budget estimation settings")
	}
	capabilities.BudgetEstimator.SafetyRatio.SetInt64(9)
	if clients.(*productionClientSet).V1RuntimeCapabilities().BudgetEstimator.SafetyRatio.RatString() != "1" {
		t.Fatal("returned capabilities mutated snapshot-owned budget estimation")
	}
	if err := clients.Close(context.Background()); err != nil {
		t.Fatalf("memory client close = %v", err)
	}
}

func nowFunc(now time.Time) func() time.Time { return func() time.Time { return now } }

func TestDefaultRedisFactoryDisablesClientRetries(t *testing.T) {
	client, err := defaultRedisFactory(context.Background(), config.RedisConfig{Addresses: []string{"127.0.0.1:6379"}}, "", "")
	if err != nil {
		t.Fatalf("defaultRedisFactory() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	standalone, ok := client.(*redisclient.Client)
	if !ok {
		t.Fatalf("defaultRedisFactory() client = %T, want *redis.Client", client)
	}
	if got := standalone.Options().MaxRetries; got != 0 {
		t.Fatalf("MaxRetries = %d, want effective zero retries", got)
	}
	if !standalone.Options().ContextTimeoutEnabled {
		t.Fatal("ContextTimeoutEnabled = false, want context deadlines honoured")
	}
}

func TestProductionFactoryProviderSecretFailsClosed(t *testing.T) {
	called := false
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
		Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
			called = true
			return []byte("should-not-be-called"), nil
		}),
	}}
	_, err := factory.providerSecret(context.Background(), config.AuthConfig{Kind: "workload_identity", Audience: "provider"}, "endpoint")
	if !errors.Is(err, ErrUnsupportedProviderAuth) {
		t.Fatalf("error = %v, want ErrUnsupportedProviderAuth", err)
	}
	if called {
		t.Fatal("unsupported auth attempted secret resolution")
	}
}

func TestProductionFactoryBuildsOpenAIResponsesAdapter(t *testing.T) {
	factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
		Resolver: secrets.ResolverFunc(func(_ context.Context, ref config.SecretRef) ([]byte, error) {
			if ref.Kind != config.SecretEnv || ref.Name != "OPENAI_KEY" {
				t.Fatalf("resolved unexpected secret reference: %#v", ref)
			}
			return []byte("test-key"), nil
		}),
		SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) {
			return engine.Snapshot{}, nil
		}),
		HTTPClient: &http.Client{},
	})
	if err != nil {
		t.Fatalf("NewProductionEngineFactory() error = %v", err)
	}
	value := config.Config{Endpoints: map[string]config.EndpointConfig{
		"openai": {Family: "openai_responses", BaseURL: "https://api.openai.com/v1", OutboundHosts: []string{"api.openai.com"}, Auth: config.AuthConfig{Kind: "bearer_env", Name: "OPENAI_KEY"}},
	}}
	snapshot := engine.Snapshot{Routes: routing.Catalog{Models: map[string]routing.Model{
		"model": {Routes: []routing.Route{{EndpointID: "openai", Capabilities: routing.CapabilitySet{Version: "cap-v1"}}}},
	}}}
	adapter, err := factory.buildAdapter(context.Background(), value, snapshot, "openai", nil)
	if err != nil {
		t.Fatalf("buildAdapter() error = %v", err)
	}
	if adapter == nil || adapter.Name() != "openai.responses" {
		t.Fatalf("adapter = %#v, want openai.responses adapter", adapter)
	}

	for _, permitted := range []bool{false, true} {
		endpoint := value.Endpoints["openai"]
		endpoint.ProviderStorage.Permitted = permitted
		value.Endpoints["openai"] = endpoint
		adapter, err := factory.buildAdapter(context.Background(), value, snapshot, "openai", nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = adapter.Compile(context.Background(), provider.CompileInput{
			Request: llm.Request{OperationKey: "policy", Model: "model", Extensions: map[string]json.RawMessage{"openai.responses": json.RawMessage(`{"store":true}`)}},
			Query:   provider.CapabilityQuery{EndpointID: "openai", Family: provider.FamilyOpenAIResponses, Model: "model"},
		})
		if permitted && err != nil {
			t.Fatalf("permitted storage rejected: %v", err)
		}
		if !permitted && err == nil {
			t.Fatal("factory ignored endpoint storage policy")
		}
	}
}

func TestProductionFactoryBuildsAnthropicAWSGatewayAdapterWithoutSecretResolution(t *testing.T) {
	resolvedSecret := false
	var constructed anthropicmessages.AWSClientConfig
	factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
		Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
			resolvedSecret = true
			return nil, errors.New("AWS gateway must not resolve a provider secret")
		}),
		SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) {
			return engine.Snapshot{}, nil
		}),
		HTTPClient: &http.Client{},
		AnthropicAWSClientFactory: func(ctx context.Context, value anthropicmessages.AWSClientConfig) (*anthropicmessages.Client, error) {
			constructed = value
			value.AWSConfig.SkipAuth = true
			return anthropicmessages.NewAWSClient(ctx, value)
		},
	})
	if err != nil {
		t.Fatalf("NewProductionEngineFactory() error = %v", err)
	}
	value := config.Config{Endpoints: map[string]config.EndpointConfig{
		"anthropic-aws": {
			Family: "anthropic_aws_messages", BaseURL: "https://aws-external-anthropic.us-east-1.api.aws", OutboundHosts: []string{"aws-external-anthropic.us-east-1.api.aws"},
			Region: "us-east-1", AWSWorkspaceID: "ws-example-123", Auth: config.AuthConfig{Kind: "aws_default_chain"},
			ServiceClasses: map[llm.ServiceClass]config.TierConfig{llm.ServiceClassStandard: {ProviderValue: "standard_only"}},
		},
	}}
	snapshot := engine.Snapshot{Routes: routing.Catalog{Models: map[string]routing.Model{
		"model": {Routes: []routing.Route{{EndpointID: "anthropic-aws", Capabilities: routing.CapabilitySet{Version: "cap-v1"}}}},
	}}}
	adapter, err := factory.buildAdapter(context.Background(), value, snapshot, "anthropic-aws", nil)
	if err != nil {
		t.Fatalf("buildAdapter() error = %v", err)
	}
	if adapter == nil || adapter.Name() != "anthropic.messages/anthropic-aws" {
		t.Fatalf("adapter = %#v, want Anthropic AWS messages adapter", adapter)
	}
	if resolvedSecret {
		t.Fatal("AWS gateway resolved a provider secret")
	}
	if constructed.BaseURL != value.Endpoints["anthropic-aws"].BaseURL || constructed.AWSConfig.AWSRegion != "us-east-1" || constructed.AWSConfig.WorkspaceID != "ws-example-123" {
		t.Fatalf("AWS gateway client configuration = %#v", constructed)
	}
	if constructed.AWSConfig.APIKey != "" || constructed.AWSConfig.AWSAccessKey != "" || constructed.AWSConfig.AWSSecretAccessKey != "" || constructed.AWSConfig.AWSSessionToken != "" || constructed.AWSConfig.AWSProfile != "" {
		t.Fatalf("AWS gateway client accepted static credentials: %#v", constructed)
	}
}

func TestProductionFactoryRejectsSecretAuthForAnthropicAWSGateway(t *testing.T) {
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{HTTPClient: &http.Client{}}}
	value := config.Config{Endpoints: map[string]config.EndpointConfig{
		"anthropic-aws": {Family: "anthropic_aws_messages", BaseURL: "https://aws-external-anthropic.us-east-1.api.aws", OutboundHosts: []string{"aws-external-anthropic.us-east-1.api.aws"}, Region: "us-east-1", AWSWorkspaceID: "ws-example-123", Auth: config.AuthConfig{Kind: "bearer_env", Name: "ANTHROPIC_AWS_API_KEY"}},
	}}
	snapshot := engine.Snapshot{Routes: routing.Catalog{Models: map[string]routing.Model{
		"model": {Routes: []routing.Route{{EndpointID: "anthropic-aws", Capabilities: routing.CapabilitySet{Version: "cap-v1"}}}},
	}}}
	_, err := factory.buildAdapter(context.Background(), value, snapshot, "anthropic-aws", nil)
	if !errors.Is(err, ErrUnsupportedProviderAuth) {
		t.Fatalf("buildAdapter() error = %v, want ErrUnsupportedProviderAuth", err)
	}
}

func TestProductionFactoryRejectsUnknownFamily(t *testing.T) {
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{HTTPClient: &http.Client{}}}
	value := config.Config{Endpoints: map[string]config.EndpointConfig{
		"unknown": {Family: "provider_future", BaseURL: "https://example.test", OutboundHosts: []string{"example.test"}, Auth: config.AuthConfig{Kind: "bearer_env", Name: "KEY"}},
	}}
	snapshot := engine.Snapshot{Routes: routing.Catalog{Models: map[string]routing.Model{
		"model": {Routes: []routing.Route{{EndpointID: "unknown", Capabilities: routing.CapabilitySet{Version: "cap-v1"}}}},
	}}}
	_, err := factory.buildAdapter(context.Background(), value, snapshot, "unknown", nil)
	if err == nil || !strings.Contains(err.Error(), "unsupported provider family") {
		t.Fatalf("error = %v, want unsupported provider family", err)
	}
}

func TestChatProfileRequiresSpecializedDialect(t *testing.T) {
	factory := &ProductionEngineFactory{}
	endpoint := config.EndpointConfig{Family: "openai_chat", BaseURL: "https://openrouter.ai/api/v1"}
	_, err := factory.chatProfile("openrouter", endpoint, provider.CapabilitySet{Version: "cap-v1"}, EndpointProfile{}, false)
	if err == nil || !strings.Contains(err.Error(), "specialized chat dialect must be explicit") {
		t.Fatalf("error = %v, want explicit dialect failure", err)
	}
}

func TestEndpointFamilyMapsAzureAndBedrock(t *testing.T) {
	if got := endpointFamily("azure_openai_responses"); got != provider.FamilyOpenAIResponses {
		t.Fatalf("Azure family = %q, want %q", got, provider.FamilyOpenAIResponses)
	}
	if got := endpointFamily("azure_openai_chat"); got != provider.FamilyOpenAIChat {
		t.Fatalf("Azure Chat family = %q, want %q", got, provider.FamilyOpenAIChat)
	}
	if got := endpointFamily("bedrock_anthropic_messages"); got != provider.FamilyBedrockMessages {
		t.Fatalf("Bedrock family = %q, want %q", got, provider.FamilyBedrockMessages)
	}
	if got := endpointFamily("bedrock_converse"); got != provider.FamilyBedrockConverse {
		t.Fatalf("Bedrock Converse family = %q, want %q", got, provider.FamilyBedrockConverse)
	}
	if got := endpointFamily("anthropic_aws_messages"); got != provider.FamilyAnthropicMessages {
		t.Fatalf("Anthropic AWS family = %q, want %q", got, provider.FamilyAnthropicMessages)
	}
	if !llm.ServiceClassPriority.Valid() {
		t.Fatal("priority service class is unexpectedly invalid")
	}
}

func TestProductionFactoryBuildsAzureOpenAIChatAdapter(t *testing.T) {
	factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
		Resolver: secrets.ResolverFunc(func(_ context.Context, ref config.SecretRef) ([]byte, error) {
			if ref.Kind != config.SecretEnv || ref.Name != "AZURE_OPENAI_API_KEY" {
				t.Fatalf("resolved unexpected secret reference: %#v", ref)
			}
			return []byte("test-key"), nil
		}),
		SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) { return engine.Snapshot{}, nil }),
		HTTPClient:     &http.Client{},
	})
	if err != nil {
		t.Fatal(err)
	}
	value := azureOpenAIChatConfig(config.AuthConfig{Kind: "header_env", Name: "AZURE_OPENAI_API_KEY"})
	adapter, err := factory.buildAdapter(context.Background(), value, azureOpenAIChatSnapshot(), "azure-chat", nil)
	if err != nil {
		t.Fatal(err)
	}
	if adapter == nil || adapter.Name() != "openai.chat/azure-chat" {
		t.Fatalf("adapter = %#v, want Azure OpenAI Chat adapter", adapter)
	}
	_, err = adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "azure-model-pin", Model: "other-deployment", ServiceClass: llm.ServiceClassStandard},
		Query:   provider.CapabilityQuery{EndpointID: "azure-chat", Family: provider.FamilyOpenAIChat, Model: "other-deployment"},
		Strict:  true,
	})
	if err == nil || !strings.Contains(err.Error(), "pinned profile model") {
		t.Fatalf("model pin error = %v", err)
	}
}

func TestProductionFactoryAzureOpenAIChatFailsClosedBeforeSecretResolution(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*config.EndpointConfig)
		want   string
	}{
		{name: "missing API version", mutate: func(endpoint *config.EndpointConfig) { endpoint.Extensions["azure"]["api_version"] = "" }, want: "Azure API version is required"},
		{name: "whitespace API version", mutate: func(endpoint *config.EndpointConfig) { endpoint.Extensions["azure"]["api_version"] = " \t " }, want: "Azure API version is required"},
		{name: "missing deployment", mutate: func(endpoint *config.EndpointConfig) { delete(endpoint.Extensions["azure"], "deployment") }, want: "Azure deployment is required"},
		{name: "non-string deployment", mutate: func(endpoint *config.EndpointConfig) { endpoint.Extensions["azure"]["deployment"] = 7 }, want: "Azure deployment is required"},
		{name: "bearer auth", mutate: func(endpoint *config.EndpointConfig) {
			endpoint.Auth = config.AuthConfig{Kind: "bearer_env", Name: "AZURE_OPENAI_API_KEY"}
		}, want: "provider authentication mode is unsupported"},
		{name: "Azure default credential", mutate: func(endpoint *config.EndpointConfig) {
			endpoint.Auth = config.AuthConfig{Kind: "azure_default_credential"}
		}, want: "provider authentication mode is unsupported"},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolved := false
			factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
				Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
					resolved = true
					return []byte("must-not-resolve"), nil
				}),
				SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) { return engine.Snapshot{}, nil }),
				HTTPClient:     &http.Client{},
			})
			if err != nil {
				t.Fatal(err)
			}
			value := azureOpenAIChatConfig(config.AuthConfig{Kind: "header_env", Name: "AZURE_OPENAI_API_KEY"})
			endpoint := value.Endpoints["azure-chat"]
			test.mutate(&endpoint)
			value.Endpoints["azure-chat"] = endpoint
			_, err = factory.buildAdapter(context.Background(), value, azureOpenAIChatSnapshot(), "azure-chat", nil)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("buildAdapter() error = %v, want %q", err, test.want)
			}
			if resolved {
				t.Fatal("invalid Azure Chat configuration resolved a secret")
			}
		})
	}
}

func azureOpenAIChatConfig(auth config.AuthConfig) config.Config {
	return config.Config{Endpoints: map[string]config.EndpointConfig{
		"azure-chat": {
			Family: "azure_openai_chat", BaseURL: "https://example.openai.azure.com", OutboundHosts: []string{"example.openai.azure.com"}, Auth: auth,
			ServiceClasses: map[llm.ServiceClass]config.TierConfig{llm.ServiceClassStandard: {ProviderValue: "default"}},
			Extensions:     map[string]map[string]any{"azure": {"api_version": "2025-01-01", "deployment": "chat-deployment"}},
		},
	}}
}

func azureOpenAIChatSnapshot() engine.Snapshot {
	return engine.Snapshot{Routes: routing.Catalog{Models: map[string]routing.Model{
		"model": {Routes: []routing.Route{{EndpointID: "azure-chat", Capabilities: routing.CapabilitySet{Version: "azure-chat/v1"}}}},
	}}}
}
