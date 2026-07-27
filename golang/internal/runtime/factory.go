package runtime

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	anthropicaws "github.com/anthropics/anthropic-sdk-go/aws"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/anthropicmessages"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/bedrockmessages"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/openaichat"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/openairesponses"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/blob"
	durablestore "github.com/mfow/llm-temporal-worker/golang/storage/durable"
	"github.com/mfow/llm-temporal-worker/golang/storage/fileblob"
	memorystore "github.com/mfow/llm-temporal-worker/golang/storage/memory"
	postgresstore "github.com/mfow/llm-temporal-worker/golang/storage/postgres"
	redisstore "github.com/mfow/llm-temporal-worker/golang/storage/redis"
	"github.com/mfow/llm-temporal-worker/golang/storage/s3blob"
)

var (
	ErrProductionFactoryInvalid = errors.New("production engine factory is invalid")
	ErrUnsupportedProviderAuth  = errors.New("provider authentication mode is unsupported")
	ErrProviderProfileMissing   = errors.New("provider profile is missing")
	ErrDependencyUnavailable    = errors.New("runtime dependency is unavailable")
)

// ChatDialect identifies a protocol-compatible OpenAI Chat endpoint. The
// factory never infers provider-specific wire defaults from a hostname; a
// specialized dialect must be selected explicitly by the profile builder.
type ChatDialect string

const (
	ChatDialectGeneric    ChatDialect = "generic"
	ChatDialectOpenRouter ChatDialect = "openrouter"
	ChatDialectExa        ChatDialect = "exa"
)

// EndpointProfile is the verified capability/service-tier contract attached
// to one endpoint adapter. The pointers are optional because the factory can
// derive safe generic profiles from the compiled route snapshot. Specialized
// OpenRouter/Exa profiles should be supplied explicitly.
type EndpointProfile struct {
	ChatDialect ChatDialect
	Chat        *openaichat.Profile
	Anthropic   *anthropicmessages.Profile
	Bedrock     *bedrockmessages.Profile
}

type RedisFactory func(context.Context, config.RedisConfig, string, string) (redis.UniversalClient, error)

// PostgresFactory builds the read-only durable-state dependency. Schema
// installation is intentionally not part of this hook: deployment owns
// provisioning, while workers only verify the immutable contract.
type PostgresFactory func(context.Context, config.PostgresConfig, postgresstore.Namespace, string, string) (DependencyProbe, io.Closer, error)
type BlobFactory func(context.Context, config.Config) (blob.Store, io.Closer, error)
type AWSConfigFactory func(context.Context, string) (aws.Config, error)
type AzureCredentialFactory func(context.Context, config.EndpointConfig) (azcore.TokenCredential, error)
type AnthropicAWSClientFactory func(context.Context, anthropicmessages.AWSClientConfig) (*anthropicmessages.Client, error)

// V1RuntimeBuilder is the snapshot-scoped composition seam for the durable
// one-shot Activity boundary. It runs only after the ProductionEngineFactory
// has constructed the engine and every client owned by that immutable
// snapshot. The builder may inspect the engine/client bundle and return a
// runtime that owns no independent copy of those resources; the production
// client set closes them when the snapshot drains.
//
// A nil builder intentionally leaves the v1 runtime unconfigured. The
// process-level runtime then fails closed before Temporal polling. In
// particular, this seam never adapts the legacy llm.Engine.Generate method or
// fabricates Compact/Query responses.
type V1RuntimeBuilder func(context.Context, *config.Snapshot, llm.Engine, app.ClientSet) (activity.V1Runtime, error)

// ProductionFactoryOptions provides explicit seams for tests and deployment
// composition. If a client/factory is omitted, the official SDK defaults are
// used only for the corresponding documented authentication mode; unsupported
// modes fail closed with ErrUnsupportedProviderAuth.
type ProductionFactoryOptions struct {
	Resolver       secrets.Resolver
	SnapshotLoader SnapshotLoader
	Profiles       map[string]EndpointProfile
	HTTPClient     *http.Client
	EgressResolver ProviderEgressResolver
	EgressDial     ProviderEgressDialContext
	Clock          func() time.Time
	Planner        routing.Planner

	RedisKeySecret  []byte
	RedisClient     redis.UniversalClient
	RedisFactory    RedisFactory
	PostgresFactory PostgresFactory
	BlobStore       blob.Store
	BlobFactory     BlobFactory
	BlobRefResolver BlobRefResolver

	AWSConfigFactory          AWSConfigFactory
	AzureCredentialFactory    AzureCredentialFactory
	AnthropicAWSClientFactory AnthropicAWSClientFactory
	AzureAPIVersions          map[string]string
	// V1RuntimeBuilder is optional until the durable phase ports are composed.
	// When supplied, it is invoked independently for every immutable config
	// snapshot, including reloads.
	V1RuntimeBuilder V1RuntimeBuilder
	// QueryServiceBuilder is optional by design. A deployment must provide
	// authorization, cursor-key, and audit composition explicitly before the
	// production factory exposes persisted control-plane queries.
	QueryServiceBuilder QueryServiceBuilder
}

// ProductionEngineFactory composes the full provider-neutral engine from one
// immutable config snapshot. It owns the SDK/state clients returned in the
// ClientSet and never places resolved secrets into the engine snapshot.
type ProductionEngineFactory struct {
	options ProductionFactoryOptions
}

// productionClientSet owns the SDK clients built for one immutable snapshot
// and exposes only the runtime's narrow dependency probe capability. The app
// package still sees it solely as a ClientSet.
type productionClientSet struct {
	close           func(context.Context) error
	probes          []DependencyProbe
	providerControl engine.ProviderStatusRecorder
	queryRepos      PostgresQueryRepositories
	queryService    activity.QueryService
	checkpoints     CheckpointCapabilities
	journal         durablestore.Journal
	v1Capabilities  V1RuntimeCapabilities
	v1Runtime       activity.V1Runtime
}

var _ V1RuntimeSource = (*productionClientSet)(nil)
var _ CheckpointCapabilitiesSource = (*productionClientSet)(nil)
var _ JournalSource = (*productionClientSet)(nil)
var _ V1RuntimeCapabilitiesSource = (*productionClientSet)(nil)

func (set *productionClientSet) Close(ctx context.Context) error {
	if set == nil || set.close == nil {
		return nil
	}
	return set.close(ctx)
}

func (set *productionClientSet) DependencyProbes() []DependencyProbe {
	if set == nil {
		return nil
	}
	return append([]DependencyProbe(nil), set.probes...)
}

// ProviderStatusRecorder exposes the snapshot-scoped recorder for adjacent
// runtime/query composition without exposing PostgreSQL credentials or pools.
func (set *productionClientSet) ProviderStatusRecorder() engine.ProviderStatusRecorder {
	if set == nil {
		return nil
	}
	return set.providerControl
}

// QueryRepositories returns the immutable read-side repositories constructed
// for this configuration snapshot. Nil entries are intentional capabilities:
// callers must fail closed when a query family has not been provisioned.
func (set *productionClientSet) QueryRepositories() PostgresQueryRepositories {
	if set == nil {
		return PostgresQueryRepositories{}
	}
	return set.queryRepos
}

// QueryService returns the optional typed control-plane service supplied by
// the same snapshot-scoped PostgreSQL closer. It is nil until authorization,
// cursor signing, and concrete handlers are explicitly configured.
func (set *productionClientSet) QueryService() activity.QueryService {
	if set == nil {
		return nil
	}
	return set.queryService
}

// CheckpointCapabilities returns the typed durable checkpoint bundle owned by
// this immutable snapshot. A nil repository, blob reader, or handle
// materializer is an explicit unconfigured capability; callers must not reach
// into PostgreSQL or invent a process-local substitute.
func (set *productionClientSet) CheckpointCapabilities() CheckpointCapabilities {
	if set == nil {
		return CheckpointCapabilities{}
	}
	return set.checkpoints
}

// Journal returns the optional write-only PostgreSQL budget journal owned by
// this immutable snapshot. It is not active-budget state and must not be used
// for normal admission reads. A nil value is an explicit unconfigured seam.
func (set *productionClientSet) Journal() durablestore.Journal {
	if set == nil {
		return nil
	}
	return set.journal
}

// V1RuntimeCapabilities returns the preparatory typed dependency bundle owned
// by this immutable snapshot. The zero value is intentionally unconfigured; no
// legacy engine or process-global dependency is synthesized here.
func (set *productionClientSet) V1RuntimeCapabilities() V1RuntimeCapabilities {
	if set == nil {
		return V1RuntimeCapabilities{}
	}
	return set.v1Capabilities
}

// V1Runtime returns the durable one-shot implementation composed for this
// immutable snapshot. A nil value is an explicit fail-closed capability, not
// a request to adapt the legacy engine.
func (set *productionClientSet) V1Runtime() activity.V1Runtime {
	if set == nil {
		return nil
	}
	return set.v1Runtime
}

func NewProductionEngineFactory(options ProductionFactoryOptions) (*ProductionEngineFactory, error) {
	if options.SnapshotLoader == nil {
		return nil, fmt.Errorf("%w: snapshot loader is required", ErrProductionFactoryInvalid)
	}
	if options.Resolver == nil {
		return nil, fmt.Errorf("%w: secret resolver is required", ErrProductionFactoryInvalid)
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.HTTPClient == nil {
		options.HTTPClient = &http.Client{}
	}
	if options.RedisClient == nil && options.RedisFactory == nil {
		options.RedisFactory = defaultRedisFactory
	}
	if options.PostgresFactory == nil {
		options.PostgresFactory = defaultPostgresFactory
	}
	if options.BlobStore == nil && options.BlobFactory == nil {
		options.BlobFactory = defaultBlobFactory
	}
	if options.AWSConfigFactory == nil {
		options.AWSConfigFactory = defaultAWSConfigFactory
	}
	if options.AzureCredentialFactory == nil {
		options.AzureCredentialFactory = defaultAzureCredentialFactory
	}
	if options.AnthropicAWSClientFactory == nil {
		options.AnthropicAWSClientFactory = anthropicmessages.NewAWSGatewayClient
	}
	return &ProductionEngineFactory{options: options}, nil
}

var _ EngineFactory = (*ProductionEngineFactory)(nil)

func (factory *ProductionEngineFactory) Build(ctx context.Context, snapshot *config.Snapshot) (llm.Engine, app.ClientSet, error) {
	if factory == nil {
		return nil, nil, fmt.Errorf("%w: factory is nil", ErrProductionFactoryInvalid)
	}
	if snapshot == nil {
		return nil, nil, fmt.Errorf("%w: configuration snapshot is required", ErrProductionFactoryInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	value := snapshot.Config()
	engineSnapshot, err := factory.options.SnapshotLoader.Load(ctx, snapshot)
	if err != nil {
		return nil, nil, fmt.Errorf("load engine snapshot: %w", err)
	}
	adapters, err := factory.buildAdapters(ctx, value, engineSnapshot)
	if err != nil {
		return nil, nil, err
	}
	if value.State.Kind == config.StateKindMemory {
		engineValue, clients, err := factory.buildMemory(ctx, value, engineSnapshot, adapters)
		if err != nil {
			return nil, nil, err
		}
		set, ok := clients.(*productionClientSet)
		if !ok {
			return nil, nil, fmt.Errorf("%w: memory factory returned an unsupported client set", ErrProductionFactoryInvalid)
		}
		return factory.attachV1Runtime(ctx, snapshot, engineValue, set)
	}
	redisClient, redisOwned, err := factory.buildRedis(ctx, value)
	if err != nil {
		return nil, nil, err
	}
	closeOwned := func() {
		if redisOwned && redisClient != nil {
			_ = redisClient.Close()
		}
	}
	keySecret, err := factory.redisKeySecret(ctx, value)
	if err != nil {
		closeOwned()
		return nil, nil, err
	}
	keyOptions, err := redisKeyOptions(value, keySecret)
	if err != nil {
		closeOwned()
		return nil, nil, fmt.Errorf("construct Redis key namespace: %w", err)
	}
	postgresProbe, postgresCloser, err := factory.buildPostgres(ctx, value)
	if err != nil {
		closeOwned()
		return nil, nil, err
	}
	var providerControl engine.ProviderStatusRecorder
	if source, ok := postgresCloser.(providerStatusRepositorySource); ok {
		providerControl = newPostgresProviderStatusRecorder(source)
	}
	queryRepos := queryRepositoriesFromCloser(postgresCloser)
	var queryService activity.QueryService
	if source, ok := postgresCloser.(queryServiceSource); ok {
		queryService = source.QueryService()
	}
	clock := factory.options.Clock
	admissionStore, err := redisstore.NewAdmissionStore(redisstore.AdmissionOptions{Client: redisClient, Mode: redisstore.AdmissionMode(value.State.Redis.AdmissionMode), FunctionVersion: value.State.Redis.AdmissionVersion, Keys: keyOptions, Clock: clock, MaxRecordBytes: value.Limits.RequestBytes})
	if err != nil {
		if postgresCloser != nil {
			_ = postgresCloser.Close()
		}
		closeOwned()
		return nil, nil, fmt.Errorf("construct Redis admission store: %w", err)
	}
	var generationPort redisstore.BudgetGenerationPort
	streamEnabled := value.State.Redis.CoordinationStreamEnabled != nil && *value.State.Redis.CoordinationStreamEnabled
	needBudgetKeys := streamEnabled || (value.Environment == "production" && value.State.Kind == config.StateKindDurable)
	var budgetKeys redisstore.BudgetKeySpace
	if needBudgetKeys {
		budgetKeys, err = redisstore.NewBudgetKeySpace(keyOptions)
		if err != nil {
			if postgresCloser != nil {
				_ = postgresCloser.Close()
			}
			closeOwned()
			return nil, nil, fmt.Errorf("construct Redis budget key namespace: %w", err)
		}
	}
	var streamKey string
	if streamEnabled {
		streamKey = budgetKeys.EventsKey()
	}
	if value.Environment == "production" && value.State.Kind == config.StateKindDurable {
		generationPort, err = redisstore.NewRedisBudgetGenerationPort(redisClient, budgetKeys)
		if err != nil {
			if postgresCloser != nil {
				_ = postgresCloser.Close()
			}
			closeOwned()
			return nil, nil, fmt.Errorf("construct Redis budget generation port: %w", err)
		}
	}
	var redisProbe DependencyProbe
	switch {
	case generationPort != nil && streamEnabled:
		redisProbe, err = NewRedisDependencyProbeWithBudgetGenerationAndStream(redisClient, value.State.Redis, generationPort, streamKey)
	case generationPort != nil:
		redisProbe, err = NewRedisDependencyProbeWithBudgetGeneration(redisClient, value.State.Redis, generationPort)
	case streamEnabled:
		redisProbe, err = NewRedisDependencyProbeWithStream(redisClient, value.State.Redis, streamKey)
	default:
		redisProbe, err = NewRedisDependencyProbe(redisClient, value.State.Redis)
	}
	if err != nil {
		if postgresCloser != nil {
			_ = postgresCloser.Close()
		}
		closeOwned()
		return nil, nil, fmt.Errorf("construct Redis readiness probe: %w", err)
	}
	keyring, err := factory.continuationKeyring(ctx, value)
	if err != nil {
		if postgresCloser != nil {
			_ = postgresCloser.Close()
		}
		closeOwned()
		return nil, nil, err
	}
	continuationStore, err := redisstore.NewContinuationStore(redisstore.ContinuationOptions{Client: redisClient, Keys: keyOptions, Keyring: keyring, Clock: clock, MaxBytes: value.Limits.RequestBytes, MaxDepth: value.Limits.ContinuationDepth})
	if err != nil {
		if postgresCloser != nil {
			_ = postgresCloser.Close()
		}
		closeOwned()
		return nil, nil, fmt.Errorf("construct Redis continuation store: %w", err)
	}
	blobStore, blobCloser, err := factory.buildBlob(ctx, value)
	if err != nil {
		if postgresCloser != nil {
			_ = postgresCloser.Close()
		}
		closeOwned()
		return nil, nil, err
	}
	closeAll := func() {
		if blobCloser != nil {
			_ = blobCloser.Close()
		}
		if postgresCloser != nil {
			postgresCloser.Close()
		}
		closeOwned()
	}
	if factory.options.QueryServiceBuilder != nil {
		queryService, err = factory.options.QueryServiceBuilder(ctx, snapshot, queryRepos)
		if err != nil {
			closeAll()
			return nil, nil, fmt.Errorf("construct query service: %w", err)
		}
	}
	bucket, ok := blobStore.(interface {
		ProbeBucket(context.Context) error
	})
	if !ok {
		closeAll()
		return nil, nil, fmt.Errorf("construct blob readiness probe: blob store does not support bucket probes")
	}
	blobProbe, err := NewBlobDependencyProbe(bucket)
	if err != nil {
		closeAll()
		return nil, nil, fmt.Errorf("construct blob readiness probe: %w", err)
	}
	refResolver := factory.options.BlobRefResolver
	if refResolver == nil {
		switch value.BlobStore.Kind {
		case "s3":
			refResolver, err = NewContentAddressedBlobRefResolver("s3", value.BlobStore.S3.Prefix)
		case "file":
			refResolver, err = NewContentAddressedBlobRefResolver("file", "")
		default:
			err = fmt.Errorf("unsupported blob store kind %q", value.BlobStore.Kind)
		}
		if err != nil {
			closeAll()
			return nil, nil, fmt.Errorf("construct blob reference resolver: %w", err)
		}
	}
	results, err := NewBlobResultStore(blobStore, admissionStore, refResolver, clock)
	if err != nil {
		closeAll()
		return nil, nil, err
	}
	estimator, err := buildEstimator(value)
	if err != nil {
		closeAll()
		return nil, nil, err
	}
	planner := factory.options.Planner
	if planner == nil {
		planner = routing.DeterministicPlanner{MaxRejections: value.Limits.RouteAttempts * 64}
	}
	snapshotSource := engine.StaticSnapshot{Value: engineSnapshot}
	adapterRegistry := engine.AdapterMap(adapters)
	capabilityAdapterRegistry := newSnapshotAdapterRegistry(adapters)
	engineValue, err := engine.New(engine.Dependencies{
		Snapshots: snapshotSource, Planner: planner, Adapters: adapterRegistry, Admission: admissionStore, Continuations: continuationStore, Results: results,
		Clock: clock, Estimator: estimator, MaxAttempts: value.Limits.RouteAttempts, FinalizationTimeout: time.Duration(value.Server.FinalizationTimeout), ProviderControl: providerControl,
	})
	if err != nil {
		closeAll()
		return nil, nil, fmt.Errorf("construct engine: %w", err)
	}
	probes := []DependencyProbe{redisProbe, blobProbe}
	if postgresProbe != nil {
		probes = append(probes, postgresProbe)
	}
	checkpointCapabilities := checkpointCapabilitiesFromCloser(postgresCloser)
	journal := journalFromCloser(postgresCloser)
	clients := &productionClientSet{
		probes:          probes,
		providerControl: providerControl,
		queryRepos:      queryRepos,
		queryService:    queryService,
		checkpoints:     checkpointCapabilities,
		journal:         journal,
		v1Capabilities: V1RuntimeCapabilities{
			Snapshot:               snapshotSource,
			Planner:                planner,
			Adapters:               capabilityAdapterRegistry,
			Checkpoints:            checkpointCapabilities,
			Journal:                journal,
			ProviderStatusRecorder: providerControl,
			Clock:                  clock,
		},
		close: func(closeContext context.Context) error {
			if closeContext == nil {
				closeContext = context.Background()
			}
			closeAll()
			return nil
		},
	}
	return factory.attachV1Runtime(ctx, snapshot, engineValue, clients)
}

// attachV1Runtime invokes the optional snapshot-bound builder only after the
// complete client set has been assembled. A failed builder closes that set so
// a rejected reload cannot leak provider/state credentials or connections.
func (factory *ProductionEngineFactory) attachV1Runtime(ctx context.Context, snapshot *config.Snapshot, engineValue llm.Engine, clients *productionClientSet) (llm.Engine, app.ClientSet, error) {
	if factory == nil || clients == nil {
		return nil, nil, fmt.Errorf("%w: v1 runtime composition requires factory and clients", ErrProductionFactoryInvalid)
	}
	builder := factory.options.V1RuntimeBuilder
	if builder == nil {
		return engineValue, clients, nil
	}
	v1Runtime, err := builder(ctx, snapshot, engineValue, clients)
	if err != nil {
		_ = clients.Close(context.Background())
		return nil, nil, fmt.Errorf("construct durable v1 runtime: %w", err)
	}
	clients.v1Runtime = v1Runtime
	return engineValue, clients, nil
}

// buildMemory composes the explicitly development-only, single-process state
// mode. It deliberately does not construct Redis, PostgreSQL, or an external
// blob store; all state is held by the process-local implementations and is
// lost on restart. Provider adapters are still built normally because memory
// mode changes state durability, not the provider contract.
func (factory *ProductionEngineFactory) buildMemory(ctx context.Context, value config.Config, engineSnapshot engine.Snapshot, adapters map[string]provider.Adapter) (llm.Engine, app.ClientSet, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	clock := factory.options.Clock
	if clock == nil {
		clock = time.Now
	}
	keyring, err := factory.continuationKeyring(ctx, value)
	if err != nil {
		return nil, nil, err
	}
	admissionStore := memorystore.NewAdmissionStore(memorystore.AdmissionOptions{Clock: clock})
	continuationStore, err := memorystore.NewContinuationStore(memorystore.ContinuationOptions{Keyring: keyring, Clock: clock, MaxDepth: value.Limits.ContinuationDepth})
	if err != nil {
		return nil, nil, fmt.Errorf("construct memory continuation store: %w", err)
	}
	var blobStore blob.Store = factory.options.BlobStore
	if blobStore != nil {
		if _, ok := blobStore.(*memorystore.BlobStore); !ok {
			return nil, nil, fmt.Errorf("memory state requires the process-local memory blob store")
		}
	} else {
		blobStore, err = memorystore.NewBlobStore(memorystore.BlobOptions{MaxBytes: int64(value.BlobStore.InlineBytes), Clock: clock})
		if err != nil {
			return nil, nil, fmt.Errorf("construct memory blob store: %w", err)
		}
	}
	results, err := NewBlobResultStore(blobStore, admissionStore, nil, clock)
	if err != nil {
		return nil, nil, err
	}
	estimator, err := buildEstimator(value)
	if err != nil {
		return nil, nil, err
	}
	planner := factory.options.Planner
	if planner == nil {
		planner = routing.DeterministicPlanner{MaxRejections: value.Limits.RouteAttempts * 64}
	}
	snapshotSource := engine.StaticSnapshot{Value: engineSnapshot}
	adapterRegistry := engine.AdapterMap(adapters)
	capabilityAdapterRegistry := newSnapshotAdapterRegistry(adapters)
	engineValue, err := engine.New(engine.Dependencies{
		Snapshots: snapshotSource, Planner: planner, Adapters: adapterRegistry, Admission: admissionStore, Continuations: continuationStore, Results: results,
		Clock: clock, Estimator: estimator, MaxAttempts: value.Limits.RouteAttempts, FinalizationTimeout: time.Duration(value.Server.FinalizationTimeout),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("construct memory engine: %w", err)
	}
	return engineValue, &productionClientSet{
		v1Capabilities: V1RuntimeCapabilities{
			Snapshot: snapshotSource,
			Planner:  planner,
			Adapters: capabilityAdapterRegistry,
			Clock:    clock,
		},
		close: func(context.Context) error { return nil },
	}, nil
}

func (factory *ProductionEngineFactory) buildPostgres(ctx context.Context, value config.Config) (DependencyProbe, io.Closer, error) {
	if value.State.Kind != config.StateKindDurable {
		return nil, nil, nil
	}
	if factory.options.PostgresFactory == nil {
		return nil, nil, fmt.Errorf("%w: PostgreSQL factory is unavailable", ErrDependencyUnavailable)
	}
	if factory.options.Resolver == nil {
		return nil, nil, fmt.Errorf("%w: secret resolver is unavailable", ErrDependencyUnavailable)
	}
	namespace, err := postgresstore.NewNamespace(value.State.Postgres.Database, value.State.Postgres.Schema, value.State.Postgres.TablePrefix)
	if err != nil {
		return nil, nil, fmt.Errorf("construct PostgreSQL namespace: %w", err)
	}
	username, err := factory.options.Resolver.Resolve(ctx, value.State.Postgres.Username)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve PostgreSQL username: %w", err)
	}
	password, err := factory.options.Resolver.Resolve(ctx, value.State.Postgres.Password)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve PostgreSQL password: %w", err)
	}
	probe, closer, err := factory.options.PostgresFactory(ctx, value.State.Postgres, namespace, string(username), string(password))
	if err != nil {
		if closer != nil {
			_ = closer.Close()
		}
		return nil, nil, fmt.Errorf("construct PostgreSQL durable-state dependency: %w", err)
	}
	if probe == nil {
		if closer != nil {
			_ = closer.Close()
		}
		return nil, nil, fmt.Errorf("%w: PostgreSQL factory returned no readiness probe", ErrDependencyUnavailable)
	}
	return probe, closer, nil
}

func redisKeyOptions(value config.Config, keySecret []byte) (redisstore.KeyOptions, error) {
	return redisstore.NewKeyOptions(value.State.Redis.KeyPrefix, value.State.Redis.AdmissionHashTag, keySecret)
}

func buildEstimator(value config.Config) (budget.Estimator, error) {
	ratio, ok := new(big.Rat).SetString(value.Limits.TokenEstimateSafetyRatio)
	if !ok || ratio.Sign() <= 0 {
		return budget.Estimator{}, fmt.Errorf("invalid token estimate safety ratio")
	}
	return budget.Estimator{SafetyRatio: ratio, MaxOutput: int64(value.Limits.MaxOutputTokens)}, nil
}

func (factory *ProductionEngineFactory) buildRedis(ctx context.Context, value config.Config) (redis.UniversalClient, bool, error) {
	if factory.options.RedisClient != nil {
		return factory.options.RedisClient, false, nil
	}
	if factory.options.RedisFactory == nil {
		return nil, false, fmt.Errorf("%w: Redis factory is unavailable", ErrDependencyUnavailable)
	}
	username, err := factory.resolveAuthSecret(ctx, config.AuthConfig{Kind: "bearer_env", Name: ""}, value.State.Redis.Username)
	if err != nil {
		return nil, false, fmt.Errorf("resolve Redis username: %w", err)
	}
	password, err := factory.resolveAuthSecret(ctx, config.AuthConfig{Kind: "bearer_env", Name: ""}, value.State.Redis.Password)
	if err != nil {
		return nil, false, fmt.Errorf("resolve Redis password: %w", err)
	}
	client, err := factory.options.RedisFactory(ctx, value.State.Redis, string(username), string(password))
	if err != nil {
		return nil, false, fmt.Errorf("construct Redis client: %w", err)
	}
	if client == nil {
		return nil, false, fmt.Errorf("%w: Redis factory returned nil client", ErrDependencyUnavailable)
	}
	return client, true, nil
}

func (factory *ProductionEngineFactory) redisKeySecret(ctx context.Context, value config.Config) ([]byte, error) {
	if len(factory.options.RedisKeySecret) >= 32 {
		return append([]byte(nil), factory.options.RedisKeySecret...), nil
	}
	password, err := factory.options.Resolver.Resolve(ctx, value.State.Redis.Password)
	if err != nil {
		return nil, fmt.Errorf("resolve Redis key secret: %w", err)
	}
	if len(password) == 0 {
		return nil, fmt.Errorf("%w: Redis key secret is empty", ErrDependencyUnavailable)
	}
	digest := sha256.Sum256(append([]byte("llmtw:redis-key-v1:"), password...))
	return digest[:], nil
}

func (factory *ProductionEngineFactory) continuationKeyring(ctx context.Context, value config.Config) (*state.Keyring, error) {
	keys := make([]state.Key, 0, len(value.Continuation.HandleKeys))
	for _, key := range value.Continuation.HandleKeys {
		secret, err := factory.options.Resolver.Resolve(ctx, key.Secret)
		if err != nil {
			return nil, fmt.Errorf("resolve continuation key %q: %w", key.ID, err)
		}
		keys = append(keys, state.Key{ID: key.ID, Secret: secret, Primary: key.Primary})
	}
	keyring, err := state.NewKeyring(keys, nil)
	if err != nil {
		return nil, fmt.Errorf("construct continuation keyring: %w", err)
	}
	return keyring, nil
}

func (factory *ProductionEngineFactory) buildBlob(ctx context.Context, value config.Config) (blob.Store, io.Closer, error) {
	if factory.options.BlobStore != nil {
		return factory.options.BlobStore, nil, nil
	}
	if factory.options.BlobFactory == nil {
		return nil, nil, fmt.Errorf("%w: blob factory is unavailable", ErrDependencyUnavailable)
	}
	store, closer, err := factory.options.BlobFactory(ctx, value)
	if err != nil {
		return nil, nil, fmt.Errorf("construct blob store: %w", err)
	}
	if store == nil {
		if closer != nil {
			_ = closer.Close()
		}
		return nil, nil, fmt.Errorf("%w: blob factory returned nil store", ErrDependencyUnavailable)
	}
	return store, closer, nil
}

func (factory *ProductionEngineFactory) buildAdapters(ctx context.Context, value config.Config, snapshot engine.Snapshot) (map[string]provider.Adapter, error) {
	ids := make([]string, 0, len(value.Endpoints))
	for id := range value.Endpoints {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	adapters := make(map[string]provider.Adapter, len(ids))
	for _, endpointID := range ids {
		adapter, err := factory.buildAdapter(ctx, value, snapshot, endpointID)
		if err != nil {
			return nil, err
		}
		adapters[endpointID] = adapter
	}
	return adapters, nil
}

func (factory *ProductionEngineFactory) buildAdapter(ctx context.Context, value config.Config, snapshot engine.Snapshot, endpointID string) (provider.Adapter, error) {
	endpoint := value.Endpoints[endpointID]
	capabilities, err := endpointCapabilities(snapshot, endpointID)
	if err != nil {
		return nil, err
	}
	client, err := newProviderEgressHTTPClient(factory.options.HTTPClient, endpoint, factory.options.EgressResolver, factory.options.EgressDial)
	if err != nil {
		return nil, fmt.Errorf("endpoint %q: provider egress policy: %w", endpointID, err)
	}
	profile := factory.options.Profiles[endpointID]
	switch endpoint.Family {
	case "openai_responses":
		key, err := factory.providerSecret(ctx, endpoint.Auth, endpointID)
		if err != nil {
			return nil, err
		}
		openaiClient, err := openairesponses.NewClient(openairesponses.ClientConfig{BaseURL: endpoint.BaseURL, APIKey: string(key), HTTPClient: client})
		if err != nil {
			return nil, fmt.Errorf("endpoint %q: %w", endpointID, err)
		}
		return openairesponses.NewAdapter(openaiClient, endpointID, capabilities.Version)
	case "azure_openai_responses":
		apiVersion := factory.azureAPIVersion(endpointID, endpoint)
		if apiVersion == "" {
			return nil, fmt.Errorf("endpoint %q: Azure API version is required", endpointID)
		}
		switch endpoint.Auth.Kind {
		case "azure_default_credential":
			credential, err := factory.options.AzureCredentialFactory(ctx, endpoint)
			if err != nil {
				return nil, fmt.Errorf("endpoint %q: create Azure credential: %w", endpointID, err)
			}
			azureClient, err := openairesponses.NewAzureTokenClient(openairesponses.AzureTokenClientConfig{Endpoint: endpoint.BaseURL, APIVersion: apiVersion, TokenCredential: credential, HTTPClient: client})
			if err != nil {
				return nil, fmt.Errorf("endpoint %q: %w", endpointID, err)
			}
			return openairesponses.NewAdapter(azureClient, endpointID, capabilities.Version)
		case "bearer_env", "header_env":
			key, err := factory.providerSecret(ctx, endpoint.Auth, endpointID)
			if err != nil {
				return nil, err
			}
			azureClient, err := openairesponses.NewAzureClient(openairesponses.AzureClientConfig{Endpoint: endpoint.BaseURL, APIVersion: apiVersion, APIKey: string(key), HTTPClient: client})
			if err != nil {
				return nil, fmt.Errorf("endpoint %q: %w", endpointID, err)
			}
			return openairesponses.NewAdapter(azureClient, endpointID, capabilities.Version)
		default:
			return nil, factory.unsupportedAuth(endpointID, endpoint.Auth.Kind)
		}
	case "azure_openai_chat":
		apiVersion := factory.azureAPIVersion(endpointID, endpoint)
		if apiVersion == "" {
			return nil, fmt.Errorf("endpoint %q: Azure API version is required", endpointID)
		}
		deployment, err := azureDeployment(endpoint)
		if err != nil {
			return nil, fmt.Errorf("endpoint %q: %w", endpointID, err)
		}
		if endpoint.Auth.Kind != "header_env" {
			return nil, factory.unsupportedAuth(endpointID, endpoint.Auth.Kind)
		}
		key, err := factory.providerSecret(ctx, endpoint.Auth, endpointID)
		if err != nil {
			return nil, err
		}
		azureClient, err := openaichat.NewAzureClient(openaichat.AzureClientConfig{Endpoint: endpoint.BaseURL, APIVersion: apiVersion, APIKey: string(key), HTTPClient: client})
		if err != nil {
			return nil, fmt.Errorf("endpoint %q: %w", endpointID, err)
		}
		tiers, actual := endpointTiers(endpoint)
		adapter, err := openaichat.NewAzureAdapter(azureClient, endpointID, openaichat.AzureProfileConfig{ID: endpointID, CapabilityVersion: capabilities.Version, BaseURL: endpoint.BaseURL, Deployment: deployment, Capabilities: capabilities, ServiceTiers: tiers, ActualServiceClasses: actual})
		if err != nil {
			return nil, fmt.Errorf("endpoint %q: %w", endpointID, err)
		}
		return adapter, nil
	case "openai_chat":
		chatProfile, err := factory.chatProfile(endpointID, endpoint, capabilities, profile)
		if err != nil {
			return nil, err
		}
		key, err := factory.providerSecret(ctx, endpoint.Auth, endpointID)
		if err != nil {
			return nil, err
		}
		switch profile.ChatDialect {
		case ChatDialectOpenRouter:
			openrouterClient, err := openaichat.NewOpenRouterClient(openaichat.OpenRouterClientConfig{BaseURL: endpoint.BaseURL, APIKey: string(key), HTTPClient: client})
			if err != nil {
				return nil, fmt.Errorf("endpoint %q: %w", endpointID, err)
			}
			return openaichat.NewAdapter(openrouterClient, endpointID, *chatProfile)
		case ChatDialectExa:
			exaClient, err := openaichat.NewExaClient(openaichat.ExaClientConfig{BaseURL: endpoint.BaseURL, APIKey: string(key), HTTPClient: client})
			if err != nil {
				return nil, fmt.Errorf("endpoint %q: %w", endpointID, err)
			}
			return openaichat.NewAdapter(exaClient, endpointID, *chatProfile)
		default:
			chatClient, err := openaichat.NewClient(openaichat.ClientConfig{BaseURL: endpoint.BaseURL, APIKey: string(key), HTTPClient: client})
			if err != nil {
				return nil, fmt.Errorf("endpoint %q: %w", endpointID, err)
			}
			return openaichat.NewAdapter(chatClient, endpointID, *chatProfile)
		}
	case "anthropic_messages":
		anthropicProfile, err := factory.anthropicProfile(endpointID, endpoint, capabilities, profile)
		if err != nil {
			return nil, err
		}
		key, err := factory.providerSecret(ctx, endpoint.Auth, endpointID)
		if err != nil {
			return nil, err
		}
		anthropicClient, err := anthropicmessages.NewClient(anthropicmessages.ClientConfig{BaseURL: endpoint.BaseURL, APIKey: string(key), HTTPClient: client})
		if err != nil {
			return nil, fmt.Errorf("endpoint %q: %w", endpointID, err)
		}
		return anthropicmessages.NewAdapter(anthropicClient, endpointID, *anthropicProfile)
	case "anthropic_aws_messages":
		if endpoint.Auth.Kind != "aws_default_chain" {
			return nil, factory.unsupportedAuth(endpointID, endpoint.Auth.Kind)
		}
		anthropicProfile, err := factory.anthropicProfile(endpointID, endpoint, capabilities, profile)
		if err != nil {
			return nil, err
		}
		if factory.options.AnthropicAWSClientFactory == nil {
			return nil, fmt.Errorf("%w: Anthropic AWS client factory is unavailable", ErrDependencyUnavailable)
		}
		anthropicClient, err := factory.options.AnthropicAWSClientFactory(ctx, anthropicmessages.AWSClientConfig{
			BaseURL:    endpoint.BaseURL,
			HTTPClient: client,
			AWSConfig: anthropicaws.ClientConfig{
				AWSRegion:   endpoint.Region,
				WorkspaceID: endpoint.AWSWorkspaceID,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("endpoint %q: %w", endpointID, err)
		}
		return anthropicmessages.NewAdapter(anthropicClient, endpointID, *anthropicProfile)
	case "bedrock_anthropic_messages":
		if endpoint.Auth.Kind != "aws_default_chain" {
			return nil, factory.unsupportedAuth(endpointID, endpoint.Auth.Kind)
		}
		bedrockProfile, err := factory.bedrockProfile(endpointID, endpoint, capabilities, profile)
		if err != nil {
			return nil, err
		}
		awsValue, err := factory.options.AWSConfigFactory(ctx, endpoint.Region)
		if err != nil {
			return nil, fmt.Errorf("endpoint %q: create AWS config: %w", endpointID, err)
		}
		bedrockClient, err := bedrockmessages.NewClient(ctx, bedrockmessages.ClientConfig{BaseURL: endpoint.BaseURL, HTTPClient: client, AWSConfig: awsValue})
		if err != nil {
			return nil, fmt.Errorf("endpoint %q: %w", endpointID, err)
		}
		return bedrockmessages.NewAdapter(bedrockClient, endpointID, *bedrockProfile)
	default:
		return nil, fmt.Errorf("endpoint %q: unsupported provider family %q", endpointID, endpoint.Family)
	}
}

func endpointCapabilities(snapshot engine.Snapshot, endpointID string) (provider.CapabilitySet, error) {
	found := provider.CapabilitySet{Features: make(map[provider.Feature]provider.Capability)}
	for _, model := range snapshot.Routes.Models {
		for _, route := range model.Routes {
			if route.EndpointID != endpointID {
				continue
			}
			capabilities := providerCapabilities(route.Capabilities)
			if found.Version == "" {
				found.Version = capabilities.Version
				found.Features = capabilities.Features
				continue
			}
			if found.Version != capabilities.Version {
				return provider.CapabilitySet{}, fmt.Errorf("endpoint %q has conflicting capability versions", endpointID)
			}
		}
	}
	if found.Version == "" {
		return provider.CapabilitySet{}, fmt.Errorf("endpoint %q has no route capability profile", endpointID)
	}
	return completeCapabilities(found), nil
}

func providerCapabilities(value routing.CapabilitySet) provider.CapabilitySet {
	result := provider.CapabilitySet{Version: value.Version, Features: make(map[provider.Feature]provider.Capability)}
	for source, target := range map[routing.Feature]provider.Feature{
		routing.FeatureText:             provider.FeatureText,
		routing.FeatureToolCall:         provider.FeatureToolCall,
		routing.FeatureStructuredOutput: provider.FeatureStructuredOutput,
		routing.FeatureReasoning:        provider.FeatureReasoning,
		routing.FeatureContinuation:     provider.FeatureContinuation,
	} {
		if capability, ok := value.Features[source]; ok {
			result.Features[target] = provider.Capability{State: provider.CapabilityState(capability.State), Transform: capability.Transform, Reason: capability.Reason}
		}
	}
	return result
}

func completeCapabilities(value provider.CapabilitySet) provider.CapabilitySet {
	copy := provider.CapabilitySet{Version: value.Version, Features: make(map[provider.Feature]provider.Capability, len(allProviderFeatures))}
	for _, feature := range allProviderFeatures {
		copy.Features[feature] = provider.Capability{State: provider.CapabilityUnknown, Reason: "catalog did not declare this capability"}
	}
	for feature, capability := range value.Features {
		copy.Features[feature] = capability
	}
	return copy
}

var allProviderFeatures = []provider.Feature{provider.FeatureText, provider.FeatureImage, provider.FeatureDocument, provider.FeatureToolCall, provider.FeatureStructuredOutput, provider.FeatureReasoning, provider.FeatureContinuation, provider.FeatureStreaming, provider.FeatureUsage}

func (factory *ProductionEngineFactory) chatProfile(endpointID string, endpoint config.EndpointConfig, capabilities provider.CapabilitySet, supplied EndpointProfile) (*openaichat.Profile, error) {
	if supplied.Chat != nil {
		copy := *supplied.Chat
		return &copy, nil
	}
	tiers, actual := endpointTiers(endpoint)
	allowed := extensionSpecs(endpoint)
	base := strings.TrimRight(endpoint.BaseURL, "/")
	dialect := supplied.ChatDialect
	if dialect == "" {
		_, openRouterExtension := endpoint.Extensions["openrouter"]
		_, exaExtension := endpoint.Extensions["exa"]
		switch {
		case openRouterExtension && exaExtension:
			return nil, fmt.Errorf("endpoint %q: chat dialect markers are ambiguous", endpointID)
		case openRouterExtension:
			dialect = ChatDialectOpenRouter
		case exaExtension:
			dialect = ChatDialectExa
		case base == "https://openrouter.ai/api/v1" || base == "https://api.exa.ai":
			return nil, fmt.Errorf("endpoint %q: specialized chat dialect must be explicit", endpointID)
		default:
			dialect = ChatDialectGeneric
		}
	}
	switch dialect {
	case ChatDialectOpenRouter:
		order, err := stringSlice(extensionValue(endpoint, "openrouter", "provider_order"))
		if err != nil {
			return nil, fmt.Errorf("endpoint %q: %w", endpointID, err)
		}
		value, err := openaichat.NewOpenRouterProfile(openaichat.OpenRouterProfileConfig{ID: endpointID, CapabilityVersion: capabilities.Version, BaseURL: endpoint.BaseURL, Capabilities: capabilities, ServiceTiers: tiers, ActualServiceClasses: actual, ProviderOrder: order, AllowFallbacks: false, RequireParameters: true})
		if err != nil {
			return nil, fmt.Errorf("endpoint %q: %w", endpointID, err)
		}
		return &value, nil
	case ChatDialectExa:
		value, err := openaichat.NewExaProfile(openaichat.ExaProfileConfig{ID: endpointID, CapabilityVersion: capabilities.Version, BaseURL: endpoint.BaseURL, Capabilities: capabilities, ServiceTiers: tiers, ActualServiceClasses: actual})
		if err != nil {
			return nil, fmt.Errorf("endpoint %q: %w", endpointID, err)
		}
		return &value, nil
	case ChatDialectGeneric:
		value, err := openaichat.NewProfile(openaichat.Profile{ID: endpointID, CapabilityVersion: capabilities.Version, Capabilities: capabilities, ServiceTiers: tiers, ActualServiceClasses: actual, AllowedExtensions: allowed, ExpectedBaseURL: endpoint.BaseURL})
		if err != nil {
			return nil, fmt.Errorf("endpoint %q: %w", endpointID, err)
		}
		return &value, nil
	default:
		return nil, fmt.Errorf("endpoint %q: unsupported chat dialect %q", endpointID, dialect)
	}
}

func (factory *ProductionEngineFactory) anthropicProfile(endpointID string, endpoint config.EndpointConfig, capabilities provider.CapabilitySet, supplied EndpointProfile) (*anthropicmessages.Profile, error) {
	if supplied.Anthropic != nil {
		copy := *supplied.Anthropic
		return &copy, nil
	}
	tiers, actual := endpointTiers(endpoint)
	priority := endpoint.ServiceClasses[llm.ServiceClassPriority]
	value, err := anthropicmessages.NewProfile(anthropicmessages.Profile{ID: endpointID, CapabilityVersion: capabilities.Version, Capabilities: capabilities, ServiceTiers: tiers, ActualServiceClasses: actual, AllowedExtensions: anthropicExtensionSpecs(endpoint), ExpectedBaseURL: endpoint.BaseURL, PriorityCapacity: priority.ProviderValue == "auto" || priority.RequiresCapability != ""})
	if err != nil {
		return nil, fmt.Errorf("endpoint %q: %w", endpointID, err)
	}
	return &value, nil
}

func (factory *ProductionEngineFactory) bedrockProfile(endpointID string, endpoint config.EndpointConfig, capabilities provider.CapabilitySet, supplied EndpointProfile) (*bedrockmessages.Profile, error) {
	if supplied.Bedrock != nil {
		copy := *supplied.Bedrock
		return &copy, nil
	}
	tiers, actual := endpointTiers(endpoint)
	value, err := bedrockmessages.NewProfile(bedrockmessages.Profile{ID: endpointID, CapabilityVersion: capabilities.Version, Capabilities: capabilities, ServiceTiers: tiers, ActualServiceClasses: actual, ExpectedBaseURL: endpoint.BaseURL})
	if err != nil {
		return nil, fmt.Errorf("endpoint %q: %w", endpointID, err)
	}
	return &value, nil
}

func endpointTiers(endpoint config.EndpointConfig) (map[llm.ServiceClass]string, map[string]llm.ServiceClass) {
	classes := map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "", llm.ServiceClassPriority: ""}
	actual := make(map[string]llm.ServiceClass)
	for class := range classes {
		if value, ok := endpoint.ServiceClasses[class]; ok {
			classes[class] = value.ProviderValue
			if value.ProviderValue != "" {
				actual[value.ProviderValue] = class
			}
		}
	}
	return classes, actual
}

func extensionSpecs(endpoint config.EndpointConfig) map[string]openaichat.ExtensionSpec {
	result := make(map[string]openaichat.ExtensionSpec, len(endpoint.Extensions))
	for namespace, fields := range endpoint.Extensions {
		mapped := make(map[string]string, len(fields))
		for field := range fields {
			mapped[field] = field
		}
		result[namespace] = openaichat.ExtensionSpec{Fields: mapped}
	}
	return result
}

func anthropicExtensionSpecs(endpoint config.EndpointConfig) map[string]anthropicmessages.ExtensionSpec {
	result := make(map[string]anthropicmessages.ExtensionSpec, len(endpoint.Extensions))
	for namespace, fields := range endpoint.Extensions {
		mapped := make(map[string]string, len(fields))
		for field := range fields {
			mapped[field] = field
		}
		result[namespace] = anthropicmessages.ExtensionSpec{Fields: mapped}
	}
	return result
}

func extensionValue(endpoint config.EndpointConfig, namespace, field string) any {
	if values, ok := endpoint.Extensions[namespace]; ok {
		return values[field]
	}
	return nil
}

func azureDeployment(endpoint config.EndpointConfig) (string, error) {
	value, ok := extensionValue(endpoint, "azure", "deployment").(string)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("Azure deployment is required")
	}
	return value, nil
}

func stringSlice(value any) ([]string, error) {
	values, ok := value.([]any)
	if !ok {
		if stringsValue, ok := value.([]string); ok {
			return append([]string(nil), stringsValue...), nil
		}
		return nil, fmt.Errorf("extension provider_order must be an array of strings")
	}
	result := make([]string, len(values))
	for index, item := range values {
		text, ok := item.(string)
		if !ok || strings.TrimSpace(text) == "" {
			return nil, fmt.Errorf("extension provider_order entry %d must be a non-empty string", index)
		}
		result[index] = text
	}
	return result, nil
}

func (factory *ProductionEngineFactory) providerSecret(ctx context.Context, auth config.AuthConfig, endpointID string) ([]byte, error) {
	switch auth.Kind {
	case "bearer_env", "header_env":
		return factory.options.Resolver.Resolve(ctx, config.SecretRef{Kind: config.SecretEnv, Name: auth.Name})
	default:
		return nil, factory.unsupportedAuth(endpointID, auth.Kind)
	}
}

func (factory *ProductionEngineFactory) resolveAuthSecret(ctx context.Context, _ config.AuthConfig, ref config.SecretRef) ([]byte, error) {
	return factory.options.Resolver.Resolve(ctx, ref)
}

func (factory *ProductionEngineFactory) unsupportedAuth(endpointID, kind string) error {
	return fmt.Errorf("endpoint %q: %w: %q", endpointID, ErrUnsupportedProviderAuth, kind)
}

func (factory *ProductionEngineFactory) azureAPIVersion(endpointID string, endpoint config.EndpointConfig) string {
	if value := factory.options.AzureAPIVersions[endpointID]; strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	if value, ok := endpoint.Extensions["azure"]["api_version"].(string); ok {
		return strings.TrimSpace(value)
	}
	return ""
}

func defaultRedisFactory(_ context.Context, value config.RedisConfig, username, password string) (redis.UniversalClient, error) {
	tlsConfig, err := loadRedisTLS(value.TLS)
	if err != nil {
		return nil, err
	}
	// go-redis uses -1, rather than zero, to disable retries. Redis admission
	// mutations are not safely replayable after an ambiguous transport failure.
	return redis.NewUniversalClient(&redis.UniversalOptions{Addrs: append([]string(nil), value.Addresses...), Username: username, Password: password, DialTimeout: time.Duration(value.DialTimeout), ReadTimeout: time.Duration(value.OperationTimeout), WriteTimeout: time.Duration(value.OperationTimeout), PoolSize: value.MaxConnections, MaxRetries: -1, TLSConfig: tlsConfig}), nil
}

func defaultPostgresFactory(ctx context.Context, value config.PostgresConfig, namespace postgresstore.Namespace, username, password string) (DependencyProbe, io.Closer, error) {
	pool, err := postgresstore.NewPool(ctx, postgresPoolOptions(value, namespace, username, password))
	if err != nil {
		return nil, nil, err
	}
	client := &postgresProbeClient{pool: pool}
	probe, err := NewPostgresDependencyProbe(client, namespace)
	if err != nil {
		pool.Close()
		return nil, nil, err
	}
	return probe, postgresPoolCloser{pool: pool, namespace: namespace}, nil
}

// postgresPoolOptions is the one configuration-to-driver projection for the
// durable PostgreSQL dependency. Keeping it separate makes it possible to
// test that every bounded pool and session setting is carried through without
// opening a live database.
func postgresPoolOptions(value config.PostgresConfig, namespace postgresstore.Namespace, username, password string) postgresstore.PoolOptions {
	return postgresstore.PoolOptions{
		Namespace:        namespace,
		Addresses:        append([]string(nil), value.Addresses...),
		Username:         username,
		Password:         password,
		TLS:              postgresstore.TLSOptions{Enabled: value.TLS.Enabled, ServerName: value.TLS.ServerName, CAFile: value.TLS.CAFile},
		MaxConnections:   int32(value.MaxConnections),
		MinConnections:   int32(value.MinConnections),
		DialTimeout:      time.Duration(value.DialTimeout),
		StatementTimeout: time.Duration(value.StatementTimeout),
		LockTimeout:      time.Duration(value.LockTimeout),
		IdleTxTimeout:    time.Duration(value.IdleTransactionTimeout),
		ApplicationName:  "llm-temporal-worker",
	}
}

type postgresProbeClient struct{ pool *pgxpool.Pool }

type postgresPoolCloser struct {
	pool      *pgxpool.Pool
	namespace postgresstore.Namespace
}

func (closer postgresPoolCloser) ProviderStatusRepository() postgresstore.ProviderStatusRepository {
	return postgresstore.DefaultProviderStatusRepository(closer.pool, closer.namespace)
}

func (closer postgresPoolCloser) QueryRepositories() PostgresQueryRepositories {
	repository := closer.ProviderStatusRepository()
	spend := postgresstore.SpendSummaryRepository{Pool: closer.pool, Namespace: closer.namespace}
	return PostgresQueryRepositories{ProviderStatus: &repository, SpendSummary: &spend}
}

// Journal exposes only the write-only durable journal contract. The runtime
// wraps it before storing it on the snapshot client set so callers cannot
// recover this concrete repository or its pool through a type assertion.
func (closer postgresPoolCloser) Journal() durablestore.Journal {
	return &postgresstore.BudgetJournalRepository{Pool: closer.pool, Namespace: closer.namespace}
}

// CheckpointRepository exposes only the storage-neutral repository contract.
// The default factory cannot construct a blob reader because PostgreSQL
// locator encryption keys and the object-store binding are deployment-owned;
// custom closers may implement CheckpointBlobReader as well.
func (closer postgresPoolCloser) CheckpointRepository() state.CheckpointRepository {
	return snapshotCheckpointRepository{delegate: postgresstore.DurableCheckpointRepository{Pool: closer.pool, Namespace: closer.namespace}}
}

func (closer postgresPoolCloser) CheckpointBlobReader() state.CheckpointBlobReader {
	return nil
}

func (closer postgresPoolCloser) Close() error {
	if closer.pool != nil {
		closer.pool.Close()
	}
	return nil
}

func (client *postgresProbeClient) Health(ctx context.Context, namespace postgresstore.Namespace) error {
	return postgresstore.Health(ctx, client.pool, namespace)
}

func (client *postgresProbeClient) Verify(ctx context.Context, namespace postgresstore.Namespace) error {
	return postgresstore.Verify(ctx, client.pool, namespace)
}

func defaultBlobFactory(ctx context.Context, value config.Config) (blob.Store, io.Closer, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if value.BlobStore.Kind == "file" {
		store, err := fileblob.New(fileblob.Options{Root: value.BlobStore.File.Root, MaxBytes: int64(value.Limits.RequestBytes)})
		if err != nil {
			return nil, nil, err
		}
		return store, nil, nil
	}
	if value.BlobStore.S3.Auth.Kind != "aws_default_chain" {
		return nil, nil, fmt.Errorf("%w: S3 auth %q", ErrUnsupportedProviderAuth, value.BlobStore.S3.Auth.Kind)
	}
	awsValue, err := defaultAWSConfigFactory(ctx, value.BlobStore.S3.Region)
	if err != nil {
		return nil, nil, err
	}
	client := s3.NewFromConfig(awsValue)
	store, err := s3blob.New(s3blob.Options{Client: client, Bucket: value.BlobStore.S3.Bucket, Prefix: value.BlobStore.S3.Prefix, MaxBytes: int64(value.Limits.RequestBytes)})
	if err != nil {
		return nil, nil, err
	}
	return store, nil, nil
}

func defaultAWSConfigFactory(ctx context.Context, region string) (aws.Config, error) {
	value, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return aws.Config{}, err
	}
	return value, nil
}

func defaultAzureCredentialFactory(ctx context.Context, _ config.EndpointConfig) (azcore.TokenCredential, error) {
	return azidentity.NewDefaultAzureCredential(nil)
}

func loadRedisTLS(value config.TLSConfig) (*tls.Config, error) {
	if !value.Enabled {
		return nil, nil
	}
	if value.CAFile == "" {
		return nil, fmt.Errorf("Redis TLS CA file is required")
	}
	data, err := os.ReadFile(value.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read Redis TLS CA certificate")
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("Redis TLS CA certificate is invalid")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, ServerName: value.ServerName, RootCAs: pool}, nil
}
