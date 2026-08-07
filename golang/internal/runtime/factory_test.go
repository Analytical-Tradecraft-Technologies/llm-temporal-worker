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
	"github.com/mfow/llm-temporal-worker/golang/budget"
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
	durablestore "github.com/mfow/llm-temporal-worker/golang/storage/durable"
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
		MinConnections: 3, MaxConnections: 12, IdleTransactionTimeout: config.Duration(17 * time.Second),
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
	if gotNamespace.String() != "worker_db/worker_state/tenant_" || got.Database != "worker_db" || got.MinConnections != 3 || got.MaxConnections != 12 || time.Duration(got.IdleTransactionTimeout) != 17*time.Second {
		t.Fatalf("Postgres namespace/config = %s/%#v", gotNamespace, got)
	}
}

func TestPostgresPoolOptionsCarryConfiguredBoundsAndTimeouts(t *testing.T) {
	namespace, err := postgresstore.NewNamespace("worker_db", "worker_state", "tenant_")
	if err != nil {
		t.Fatal(err)
	}
	value := config.PostgresConfig{
		Addresses:      []string{"postgres:5432"},
		TLS:            config.TLSConfig{Enabled: true, ServerName: "postgres.example.internal", CAFile: "/var/run/ca/postgres.pem"},
		MinConnections: 3, MaxConnections: 12,
		DialTimeout: config.Duration(2 * time.Second), StatementTimeout: config.Duration(30 * time.Second),
		LockTimeout: config.Duration(2 * time.Second), IdleTransactionTimeout: config.Duration(17 * time.Second),
	}
	options := postgresPoolOptions(value, namespace, "worker", "password")
	if options.Namespace != namespace || options.Username != "worker" || options.Password != "password" {
		t.Fatalf("pool identity = %#v", options)
	}
	if options.MinConnections != 3 || options.MaxConnections != 12 || options.DialTimeout != 2*time.Second || options.StatementTimeout != 30*time.Second || options.LockTimeout != 2*time.Second || options.IdleTxTimeout != 17*time.Second {
		t.Fatalf("pool bounds/timeouts = %#v", options)
	}
	if !options.TLS.Enabled || options.TLS.ServerName != "postgres.example.internal" || options.TLS.CAFile != "/var/run/ca/postgres.pem" {
		t.Fatalf("pool TLS = %#v", options.TLS)
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
	checkpoints := checkpointCapabilitiesFromCloser(closer)
	checkpointRepository, ok := checkpoints.Repository.(snapshotCheckpointRepository)
	if !ok {
		t.Fatalf("checkpoint repository = %T, want snapshot-scoped repository wrapper", checkpoints.Repository)
	}
	if checkpointRepository.delegate == nil {
		t.Fatal("snapshot checkpoint repository wrapper omitted delegate")
	}
	if checkpoints.Blobs != nil {
		t.Fatal("default closer exposed an unconfigured checkpoint blob reader")
	}
	if err := checkpoints.Validate(); err != nil {
		t.Fatalf("partial checkpoint capabilities failed optional validation: %v", err)
	}
	if err := checkpoints.RequireMaterializer(); err == nil {
		t.Fatal("partial checkpoint capabilities unexpectedly satisfied materializer requirement")
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

type checkpointCompositionCloser struct {
	postgresPoolCloser
	blobs        state.CheckpointBlobReader
	materializer state.CheckpointHandleMaterializer
}

func (closer checkpointCompositionCloser) CheckpointBlobReader() state.CheckpointBlobReader {
	return closer.blobs
}

func (closer checkpointCompositionCloser) CheckpointMaterializer() state.CheckpointHandleMaterializer {
	return closer.materializer
}

func TestCheckpointCapabilitiesCopyTypedBundleFromPostgresCloser(t *testing.T) {
	reader := checkpointBlobReaderStub{}
	closer := checkpointCompositionCloser{blobs: reader}
	capabilities := checkpointCapabilitiesFromCloser(closer)
	if capabilities.Repository == nil {
		t.Fatal("checkpoint capability bundle omitted repository")
	}
	wrappedReader, ok := capabilities.Blobs.(snapshotCheckpointBlobReader)
	if !ok {
		t.Fatalf("checkpoint blob reader = %T, want snapshot-scoped reader wrapper", capabilities.Blobs)
	}
	if wrappedReader.delegate != reader {
		t.Fatalf("checkpoint blob reader delegate = %T, want supplied reader", wrappedReader.delegate)
	}
	if got := (&productionClientSet{checkpoints: capabilities}).CheckpointCapabilities(); got.Repository == nil || got.Blobs == nil {
		t.Fatalf("snapshot client set checkpoint capabilities = %#v, want copied bundle", got)
	}
	if got := (&productionClientSet{}).CheckpointCapabilities(); got.Repository != nil || got.Blobs != nil {
		t.Fatalf("empty snapshot client set checkpoint capabilities = %#v", got)
	}
}

type checkpointCapabilitiesSourceStub struct {
	repository   state.CheckpointRepository
	blobs        state.CheckpointBlobReader
	materializer state.CheckpointHandleMaterializer
}

func (source checkpointCapabilitiesSourceStub) CheckpointRepository() state.CheckpointRepository {
	return source.repository
}

func (source checkpointCapabilitiesSourceStub) CheckpointBlobReader() state.CheckpointBlobReader {
	return source.blobs
}

func (source checkpointCapabilitiesSourceStub) CheckpointMaterializer() state.CheckpointHandleMaterializer {
	return source.materializer
}

func (source checkpointCapabilitiesSourceStub) Close() error { return nil }

func TestCheckpointMaterializerCapabilityRequiresCompleteDependencies(t *testing.T) {
	reader := checkpointBlobReaderStub{}
	materializer := &checkpointMaterializerStub{}
	base := postgresPoolCloser{}
	complete := checkpointCompositionCloser{postgresPoolCloser: base, blobs: reader, materializer: materializer}
	capabilities := checkpointCapabilitiesFromCloser(complete)
	wrapped, ok := capabilities.Materializer.(snapshotCheckpointMaterializer)
	if !ok {
		t.Fatalf("checkpoint materializer = %T, want private snapshot wrapper", capabilities.Materializer)
	}
	if wrapped.delegate != materializer {
		t.Fatalf("checkpoint materializer delegate = %T, want supplied materializer", wrapped.delegate)
	}
	wrappedReader, ok := capabilities.Blobs.(snapshotCheckpointBlobReader)
	if capabilities.Repository == nil || !ok || wrappedReader.delegate != reader {
		t.Fatalf("complete checkpoint capabilities = %#v, want repository and blob reader", capabilities)
	}
	if err := capabilities.RequireMaterializer(); err != nil {
		t.Fatalf("complete checkpoint capabilities failed validation: %v", err)
	}
	if got := (&productionClientSet{checkpoints: capabilities, v1Capabilities: V1RuntimeCapabilities{Checkpoints: capabilities}}).CheckpointCapabilities().Materializer; got == nil {
		t.Fatal("snapshot client set omitted complete checkpoint materializer")
	}

	missingBlobs := checkpointCompositionCloser{postgresPoolCloser: base, materializer: materializer}
	if got := checkpointCapabilitiesFromCloser(missingBlobs).Materializer; got != nil {
		t.Fatalf("materializer with missing blob reader = %T, want nil", got)
	}
	missingRepository := checkpointCapabilitiesSourceStub{blobs: reader, materializer: materializer}
	if got := checkpointCapabilitiesFromCloser(missingRepository).Materializer; got != nil {
		t.Fatalf("materializer with missing repository = %T, want nil", got)
	}
	missingMaterializer := checkpointCompositionCloser{postgresPoolCloser: base, blobs: reader}
	if got := checkpointCapabilitiesFromCloser(missingMaterializer).Materializer; got != nil {
		t.Fatalf("nil supplied materializer = %T, want nil", got)
	}
	if got := checkpointCapabilitiesFromCloser(nil); got.Repository != nil || got.Blobs != nil || got.Materializer != nil {
		t.Fatalf("nil closer capabilities = %#v, want zero value", got)
	}
}

func TestCheckpointCapabilitiesRejectsIncompleteMaterializerBundles(t *testing.T) {
	materializer := &checkpointMaterializerStub{}
	reader := checkpointBlobReaderStub{}
	repository := checkpointCompositionCloser{}.CheckpointRepository()
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

type checkpointMaterializerClosingCloser struct {
	postgresPoolCloser
	blobs        state.CheckpointBlobReader
	materializer state.CheckpointHandleMaterializer
	closed       bool
}

func (closer *checkpointMaterializerClosingCloser) CheckpointBlobReader() state.CheckpointBlobReader {
	return closer.blobs
}

func (closer *checkpointMaterializerClosingCloser) CheckpointMaterializer() state.CheckpointHandleMaterializer {
	return closer.materializer
}

func (closer *checkpointMaterializerClosingCloser) Close() error {
	closer.closed = true
	return nil
}

func TestCheckpointMaterializerCapabilityKeepsSnapshotOwnerLifecycle(t *testing.T) {
	closer := &checkpointMaterializerClosingCloser{
		blobs:        checkpointBlobReaderStub{},
		materializer: &checkpointMaterializerStub{},
	}
	capabilities := checkpointCapabilitiesFromCloser(closer)
	if capabilities.Materializer == nil {
		t.Fatal("complete checkpoint materializer capability is nil")
	}
	set := &productionClientSet{
		checkpoints: capabilities,
		close:       func(context.Context) error { return closer.Close() },
	}
	if err := set.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !closer.closed {
		t.Fatal("snapshot client close did not close checkpoint materializer owner")
	}
	if set.CheckpointCapabilities().Materializer == nil {
		t.Fatal("snapshot checkpoint materializer capability changed after owner close")
	}
}

func TestCheckpointCapabilitiesBindSnapshotBlobReaderAndHandleKeyring(t *testing.T) {
	keyring, err := state.NewKeyring([]state.Key{{ID: "checkpoint-v1", Secret: []byte("01234567890123456789012345678901"), Primary: true}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	reader := checkpointBlobReaderStub{}
	capabilities := checkpointCapabilitiesFromCloserWithBindings(postgresPoolCloser{}, reader, keyring, nowFunc(time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC)))
	if err := capabilities.RequireMaterializer(); err != nil {
		t.Fatalf("bound checkpoint capabilities failed validation: %v", err)
	}
	wrappedReader, ok := capabilities.Blobs.(snapshotCheckpointBlobReader)
	if !ok || wrappedReader.delegate != reader {
		t.Fatalf("bound blob reader = %#v, want private snapshot wrapper around supplied reader", capabilities.Blobs)
	}
	wrappedMaterializer, ok := capabilities.Materializer.(snapshotCheckpointMaterializer)
	if !ok {
		t.Fatalf("bound materializer = %T, want private snapshot wrapper", capabilities.Materializer)
	}
	durable, ok := wrappedMaterializer.delegate.(*state.DurableCheckpointMaterializer)
	if !ok {
		t.Fatalf("bound materializer delegate = %T, want state.DurableCheckpointMaterializer", wrappedMaterializer.delegate)
	}
	if _, ok := durable.Repository.(snapshotCheckpointRepository); !ok {
		t.Fatalf("durable materializer repository = %T, want snapshot repository wrapper", durable.Repository)
	}
	if _, ok := durable.Blobs.(snapshotCheckpointBlobReader); !ok {
		t.Fatalf("durable materializer blob reader = %T, want snapshot blob-reader wrapper", durable.Blobs)
	}
	if durable.HandleVerifier != keyring {
		t.Fatalf("durable materializer verifier = %T, want snapshot keyring", durable.HandleVerifier)
	}

	missingReader := checkpointCapabilitiesFromCloserWithBindings(postgresPoolCloser{}, nil, keyring, nil)
	if missingReader.Materializer != nil {
		t.Fatal("checkpoint materializer published without a blob reader")
	}
	missingVerifier := checkpointCapabilitiesFromCloserWithBindings(postgresPoolCloser{}, reader, nil, nil)
	if missingVerifier.Materializer != nil {
		t.Fatal("checkpoint materializer published without an opaque-handle verifier")
	}
}

func TestPostgresCloserExposesPrivateWriteOnlyJournal(t *testing.T) {
	namespace, err := postgresstore.NewNamespace("worker", "state", "tenant_")
	if err != nil {
		t.Fatal(err)
	}
	closer := postgresPoolCloser{namespace: namespace}
	raw := closer.Journal()
	repository, ok := raw.(*postgresstore.BudgetJournalRepository)
	if !ok {
		t.Fatalf("postgres closer journal = %T, want concrete repository before wrapping", raw)
	}
	if repository.Pool != closer.pool || repository.Namespace != namespace {
		t.Fatalf("postgres journal repository = %#v, want same pool/namespace", repository)
	}

	journal := journalFromCloser(closer)
	wrapped, ok := journal.(snapshotJournal)
	if !ok {
		t.Fatalf("snapshot journal = %T, want private wrapper", journal)
	}
	delegate, ok := wrapped.delegate.(*postgresstore.BudgetJournalRepository)
	if !ok || delegate.Pool != closer.pool || delegate.Namespace != namespace {
		t.Fatalf("snapshot journal delegate = %#v, want same pool/namespace", wrapped.delegate)
	}
	if _, leaked := journal.(*postgresstore.BudgetJournalRepository); leaked {
		t.Fatal("snapshot client journal leaked concrete PostgreSQL repository")
	}

	set := &productionClientSet{journal: journal, v1Capabilities: V1RuntimeCapabilities{Journal: journal}}
	if set.Journal() != journal {
		t.Fatal("snapshot client set did not retain journal capability")
	}
	if set.V1RuntimeCapabilities().Journal != journal {
		t.Fatal("snapshot v1 capability bundle did not retain journal capability")
	}
	if (&productionClientSet{}).Journal() != nil || (*productionClientSet)(nil).Journal() != nil {
		t.Fatal("empty snapshot client set exposed a journal capability")
	}
}

type journalStub struct{}

func (journalStub) AppendReservation(context.Context, budget.ReservationEvent) (postgresstore.JournalRecord, error) {
	return postgresstore.JournalRecord{}, nil
}

func (journalStub) AppendCompletion(context.Context, budget.CompletionEvent) (postgresstore.JournalRecord, error) {
	return postgresstore.JournalRecord{}, nil
}

type journalClosingCloser struct {
	journal durablestore.Journal
	closed  bool
}

func (closer *journalClosingCloser) Journal() durablestore.Journal { return closer.journal }

func (closer *journalClosingCloser) Close() error {
	closer.closed = true
	return nil
}

func TestJournalCapabilityClosesWithSnapshotClientSet(t *testing.T) {
	closer := &journalClosingCloser{journal: journalStub{}}
	journal := journalFromCloser(closer)
	if journal == nil {
		t.Fatal("journal source returned nil capability")
	}
	set := &productionClientSet{
		journal: journal,
		close:   func(context.Context) error { return closer.Close() },
	}
	if err := set.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !closer.closed {
		t.Fatal("snapshot client close did not close journal owner")
	}
	if set.Journal() != journal {
		t.Fatal("snapshot client journal capability changed after owner close")
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
	if empty.Snapshot != nil || empty.Planner != nil || empty.Adapters != nil || empty.Journal != nil || empty.ProviderStatusRecorder != nil || empty.Clock != nil {
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
	engineValue, clients, err := factory.buildMemory(context.Background(), value, engine.Snapshot{}, nil, nil)
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
	if capabilities.Journal != nil {
		t.Fatal("memory composition exposed a PostgreSQL journal capability")
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
