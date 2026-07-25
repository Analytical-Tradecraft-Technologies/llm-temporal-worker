package runtime

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/mfow/llm-temporal-worker/golang/storage/blob"
	postgresstore "github.com/mfow/llm-temporal-worker/golang/storage/postgres"
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

func TestBuildPostgresResolvesDurableNamespaceAndKeepsSecretsOutOfProbe(t *testing.T) {
	var got config.PostgresConfig
	var gotNamespace postgresstore.Namespace
	var gotUsername, gotPassword string
	probe := DependencyProbeFunc(func(context.Context) ProbeResult {
		return ProbeResult{Dependency: DependencyPostgres, Status: ProbeStatusReady, Reason: ProbeReasonReady}
	})
	factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
		Resolver: secrets.ResolverFunc(func(_ context.Context, ref config.SecretRef) ([]byte, error) {
			switch ref.Name {
			case "POSTGRES_USER":
				return []byte("worker-user"), nil
			case "POSTGRES_PASSWORD":
				return []byte("worker-password"), nil
			default:
				return nil, fmt.Errorf("unexpected secret %q", ref.Name)
			}
		}),
		SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) { return engine.Snapshot{}, nil }),
		PostgresFactory: func(_ context.Context, value config.PostgresConfig, namespace postgresstore.Namespace, username, password string) (DependencyProbe, io.Closer, error) {
			got, gotNamespace, gotUsername, gotPassword = value, namespace, username, password
			return probe, nil, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	value := config.Config{State: config.StateConfig{Kind: config.StateKindDurable, Postgres: config.PostgresConfig{
		Addresses: []string{"postgres:5432"}, Database: "worker_db", Schema: "worker_state", TablePrefix: "tenant_",
		Username: config.SecretRef{Kind: config.SecretEnv, Name: "POSTGRES_USER"}, Password: config.SecretRef{Kind: config.SecretEnv, Name: "POSTGRES_PASSWORD"},
	}}}
	gotProbe, closer, err := factory.buildPostgres(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	if closer != nil {
		t.Fatal("test Postgres factory unexpectedly returned a closer")
	}
	if gotProbe == nil || gotUsername != "worker-user" || gotPassword != "worker-password" {
		t.Fatalf("Postgres factory inputs probe=%#v username=%q password=%q", gotProbe, gotUsername, gotPassword)
	}
	if gotNamespace.String() != "worker_db/worker_state/tenant_" || got.Database != "worker_db" {
		t.Fatalf("Postgres namespace/config = %s/%#v", gotNamespace, got)
	}
}

func TestBuildPostgresSkipsRedisOnlyComposition(t *testing.T) {
	called := false
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
		Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) { called = true; return nil, nil }),
		PostgresFactory: func(context.Context, config.PostgresConfig, postgresstore.Namespace, string, string) (DependencyProbe, io.Closer, error) {
			called = true
			return nil, nil, nil
		},
	}}
	probe, closer, err := factory.buildPostgres(context.Background(), config.Config{State: config.StateConfig{Kind: config.StateKindRedis}})
	if err != nil || probe != nil || closer != nil || called {
		t.Fatalf("Redis-only composition built PostgreSQL: probe=%#v closer=%#v err=%v called=%v", probe, closer, err, called)
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

func TestPostgresCloserExposesStatusRepositoryFromSamePool(t *testing.T) {
	namespace, err := postgresstore.NewNamespace("worker", "state", "tenant_")
	if err != nil {
		t.Fatal(err)
	}
	closer := postgresPoolCloser{namespace: namespace}
	repository := closer.ProviderStatusRepository()
	if repository.Pool != closer.pool {
		t.Fatalf("status repository pool = %p, want %p", repository.Pool, closer.pool)
	}
	if repository.Namespace != namespace {
		t.Fatalf("status repository namespace = %v, want %v", repository.Namespace, namespace)
	}
	if recorder := newPostgresProviderStatusRecorder(closer); recorder == nil {
		t.Fatal("same-pool status recorder was not composed")
	}
	repositories := queryRepositoriesFromCloser(closer)
	if repositories.ProviderStatus == nil || repositories.ProviderStatus.Pool != closer.pool {
		t.Fatalf("query repository bundle = %#v, want same-pool provider status repository", repositories)
	}
	if repositories.Inventory != nil || repositories.QueryAudit != nil {
		t.Fatal("default closer exposed unconfigured inventory or query-audit repositories")
	}
}

type queryCompositionCloser struct {
	postgresPoolCloser
	repositories PostgresQueryRepositories
	service      activity.QueryService
}

type queryServiceStub struct{}

func (queryServiceStub) Execute(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	return llm.QueryResponseV1{}, nil
}

func (closer queryCompositionCloser) QueryRepositories() PostgresQueryRepositories {
	return closer.repositories
}

func (closer queryCompositionCloser) QueryService() activity.QueryService { return closer.service }

func TestProductionClientSetRetainsSnapshotQueryBundleAndService(t *testing.T) {
	var service activity.QueryService = queryServiceStub{}
	queryCloser := queryCompositionCloser{
		repositories: PostgresQueryRepositories{Inventory: &postgresstore.InventoryRepository{}},
		service:      service,
	}
	set := &productionClientSet{queryRepos: queryRepositoriesFromCloser(queryCloser), queryService: queryCloser.QueryService()}
	if set.QueryRepositories().Inventory == nil {
		t.Fatal("inventory repository was not retained in snapshot client set")
	}
	if set.QueryService() != service {
		t.Fatal("query service was not retained in snapshot client set")
	}
	if got := (&productionClientSet{}).QueryRepositories(); got.ProviderStatus != nil || got.Inventory != nil || got.QueryAudit != nil {
		t.Fatalf("nil capability set = %#v", got)
	}
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
		PostgresFactory: func(context.Context, config.PostgresConfig, postgresstore.Namespace, string, string) (DependencyProbe, io.Closer, error) {
			called = true
			return nil, nil, errors.New("PostgreSQL must not be constructed for memory state")
		},
		BlobFactory: func(context.Context, config.Config) (blob.Store, io.Closer, error) {
			called = true
			return nil, nil, errors.New("external blob store must not be constructed for memory state")
		},
	}}
	value := config.Config{
		State:        config.StateConfig{Kind: config.StateKindMemory, ContinuationRetention: config.Duration(time.Hour), ReservationLease: config.Duration(time.Minute)},
		BlobStore:    config.BlobStoreConfig{Kind: "memory", InlineBytes: 256},
		Limits:       config.LimitsConfig{RequestBytes: 1024, ContinuationDepth: 4, RouteAttempts: 1, TokenEstimateSafetyRatio: "1", MaxOutputTokens: 16},
		Continuation: config.ContinuationConfig{HandleKeys: []config.HandleKey{{ID: "key-2026-07", Primary: true, Secret: config.SecretRef{Kind: config.SecretEnv, Name: "CONTINUATION_KEY"}}}},
	}
	engineValue, clients, err := factory.buildMemory(context.Background(), value, engine.Snapshot{}, nil)
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
	adapter, err := factory.buildAdapter(context.Background(), value, snapshot, "openai")
	if err != nil {
		t.Fatalf("buildAdapter() error = %v", err)
	}
	if adapter == nil || adapter.Name() != "openai.responses" {
		t.Fatalf("adapter = %#v, want openai.responses adapter", adapter)
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
	adapter, err := factory.buildAdapter(context.Background(), value, snapshot, "anthropic-aws")
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
	_, err := factory.buildAdapter(context.Background(), value, snapshot, "anthropic-aws")
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
	_, err := factory.buildAdapter(context.Background(), value, snapshot, "unknown")
	if err == nil || !strings.Contains(err.Error(), "unsupported provider family") {
		t.Fatalf("error = %v, want unsupported provider family", err)
	}
}

func TestChatProfileRequiresSpecializedDialect(t *testing.T) {
	factory := &ProductionEngineFactory{}
	endpoint := config.EndpointConfig{Family: "openai_chat", BaseURL: "https://openrouter.ai/api/v1"}
	_, err := factory.chatProfile("openrouter", endpoint, provider.CapabilitySet{Version: "cap-v1"}, EndpointProfile{})
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
	adapter, err := factory.buildAdapter(context.Background(), value, azureOpenAIChatSnapshot(), "azure-chat")
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
			_, err = factory.buildAdapter(context.Background(), value, azureOpenAIChatSnapshot(), "azure-chat")
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
