package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/control"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	durablestore "github.com/mfow/llm-temporal-worker/golang/storage/durable"
	postgresstore "github.com/mfow/llm-temporal-worker/golang/storage/postgres"
)

// providerStatusRepositorySource is implemented by a PostgreSQL client set
// that owns the same pool used for durable state. Keeping this optional avoids
// changing the public PostgresFactory signature or opening a second pool.
type providerStatusRepositorySource interface {
	ProviderStatusRepository() postgresstore.ProviderStatusRepository
}

// PostgresQueryRepositories is the optional query bundle owned by one durable
// PostgreSQL pool. Repositories are pointers because deployments may roll out
// only the query slices whose key material and schema are available. A missing
// repository must remain an explicit fail-closed result rather than being
// replaced with an in-memory answer.
type PostgresQueryRepositories struct {
	ProviderStatus *postgresstore.ProviderStatusRepository
	Inventory      *postgresstore.InventoryRepository
	SpendSummary   *postgresstore.SpendSummaryRepository
	QueryAudit     *postgresstore.QueryExecutionRepository
	ScopeResolver  QueryScopeResolver
}

// PostgresQueryRepositoriesSource is implemented by PostgreSQL closers that
// construct snapshot-owned query repositories and supporting capabilities
// alongside their pool. The runtime copies this bundle into the immutable
// production client set so a reload cannot accidentally point a query Activity
// at a closed or newer pool.
type PostgresQueryRepositoriesSource interface {
	QueryRepositories() PostgresQueryRepositories
}

// CheckpointCapabilities is the storage-neutral checkpoint bundle owned by
// one immutable runtime snapshot. The interfaces deliberately hide pgx pools,
// encryption keys, and object-store locators from Activity composition.
// Blobs and Materializer may be nil when a deployment has not supplied the
// complete scoped replay binding; callers must fail closed rather than
// substituting an in-memory reader or verifier.
type CheckpointCapabilities struct {
	Repository   state.CheckpointRepository
	Blobs        state.CheckpointBlobReader
	Materializer state.CheckpointHandleMaterializer
}

// Validate checks the optional checkpoint bundle's capability relationships.
// A repository or blob reader may be supplied ahead of the complete replay
// binding, but a materializer is never valid without both of those inputs.
// This lets a builder distinguish an intentionally partial rollout from an
// accidentally unsafe capability without inspecting concrete storage types.
func (capabilities CheckpointCapabilities) Validate() error {
	if isNilCapability(capabilities.Repository) {
		capabilities.Repository = nil
	}
	if isNilCapability(capabilities.Blobs) {
		capabilities.Blobs = nil
	}
	if isNilCapability(capabilities.Materializer) {
		capabilities.Materializer = nil
	}
	if capabilities.Materializer != nil && capabilities.Repository == nil {
		return errors.New("checkpoint materializer requires a repository capability")
	}
	if capabilities.Materializer != nil && capabilities.Blobs == nil {
		return errors.New("checkpoint materializer requires a blob-reader capability")
	}
	return nil
}

// RequireMaterializer is the fail-closed check used by a builder that needs
// durable replay. The default PostgreSQL factory intentionally returns a
// partial bundle until deployment supplies the scoped blob and handle
// bindings, so callers must opt into this stronger requirement explicitly.
func (capabilities CheckpointCapabilities) RequireMaterializer() error {
	if err := capabilities.Validate(); err != nil {
		return err
	}
	if isNilCapability(capabilities.Repository) || isNilCapability(capabilities.Blobs) || isNilCapability(capabilities.Materializer) {
		return errors.New("complete checkpoint materializer capability is not configured")
	}
	return nil
}

func isNilCapability(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

// PostgresCheckpointCapabilitiesSource is implemented by PostgreSQL client
// closers that construct checkpoint capabilities alongside their pool. The
// runtime copies the typed bundle into the snapshot client set so reloads
// cannot accidentally use a closed or newer checkpoint dependency.
type PostgresCheckpointCapabilitiesSource interface {
	CheckpointRepository() state.CheckpointRepository
	CheckpointBlobReader() state.CheckpointBlobReader
}

// PostgresCheckpointMaterializerSource is an optional deployment-owned
// binding for the complete checkpoint replay capability. The default factory
// deliberately does not implement it: constructing a materializer requires
// the scoped blob locator, encryption/key binding, and opaque-handle verifier
// owned by the deployment. A source must return nil until all of those
// dependencies are present; the runtime never fabricates an in-memory or
// unscoped substitute.
type PostgresCheckpointMaterializerSource interface {
	CheckpointMaterializer() state.CheckpointHandleMaterializer
}

// CheckpointCapabilitiesSource is the client-set capability exposed to a
// V1RuntimeBuilder through app.ClientSet. It intentionally has one aggregate
// method so builders never need to know whether the snapshot uses PostgreSQL
// or another durable repository implementation.
type CheckpointCapabilitiesSource interface {
	CheckpointCapabilities() CheckpointCapabilities
}

// PostgresJournalSource is implemented by a PostgreSQL client closer that
// exposes only the write-only durable budget journal. It deliberately returns
// the storage-neutral durable.Journal contract instead of a pool or concrete
// repository so a future V1 composition cannot read budget projections during
// normal admission.
type PostgresJournalSource interface {
	Journal() durablestore.Journal
}

// JournalSource is the optional snapshot-client capability consumed by a
// future durable V1 builder. The journal is owned by the same immutable client
// set and is closed with that set; nil means PostgreSQL journal composition is
// not configured.
type JournalSource interface {
	Journal() durablestore.Journal
}

// DurableCompositionFactory constructs the complete storage-neutral
// Composition for one immutable runtime snapshot. Implementations must close
// over only the supplied snapshot capability bundle and must return before
// any provider dispatch; the factory never fabricates missing stores or
// adapts the legacy engine.
type DurableCompositionFactory func(context.Context, V1RuntimeCapabilities) (durablestore.Composition, error)

// V1RuntimeCapabilities is the preparatory storage- and provider-neutral
// dependency bundle owned by one immutable configuration snapshot. It exposes
// only the snapshot/planning/adapter contracts, checkpoint capability, and
// optional write-only journal that are safe to carry into the future durable
// composition. It deliberately
// omits the legacy Redis admission, continuation, and blob-result stores:
// Task 19 must supply PostgreSQL durable operations, BudgetMaterializer/Journal,
// and the corresponding result/continuation ports before the V1 runtime can
// compose them. Adapters own provider credentials. A nil optional recorder or
// clock is an unconfigured capability; callers must not fall back to a legacy
// engine or a process-global dependency.
type V1RuntimeCapabilities struct {
	Snapshot    engine.SnapshotSource
	Planner     routing.Planner
	Adapters    engine.AdapterRegistry
	Checkpoints CheckpointCapabilities
	// Journal is the optional write-only PostgreSQL budget journal. It is
	// preparatory input for Task 19 and does not activate V1 composition.
	Journal durablestore.Journal
	// CompositionFactory is the Task 19 seam for the snapshot-owned
	// PostgreSQL/Redis responsibility split. It is optional while deployments
	// still use the preparatory phase factories; when selected, callers must
	// invoke BuildDurableComposition and fail closed on a missing factory or
	// incomplete returned Composition.
	CompositionFactory     DurableCompositionFactory
	ProviderStatusRecorder engine.ProviderStatusRecorder
	Clock                  func() time.Time
	// GeneratePortsFactory is a per-snapshot constructor for the storage-
	// neutral durable Generate phase. It must close over only the immutable
	// adapters and stores represented by this capability bundle; a nil value is
	// deliberately unconfigured and never falls back to the legacy engine.
	GeneratePortsFactory GeneratePortsFactory
	// CompactPortsFactory is the per-snapshot constructor for the storage-
	// neutral durable Compact phase. It is intentionally independent from
	// GeneratePortsFactory so composition can validate each phase without
	// adapting the legacy engine or fabricating the other phase. The
	// production builder must not expose Compact until a deployment supplies
	// every callback behind this factory.
	CompactPortsFactory CompactPortsFactory
}

// BuildDurableComposition invokes and validates the optional snapshot-owned
// composition factory. Validation is intentionally completed after the
// factory returns and before any caller can attach the value to an Activity.
// The method performs no fallback to Redis-only or in-memory durable state.
func (capabilities V1RuntimeCapabilities) BuildDurableComposition(ctx context.Context) (durablestore.Composition, error) {
	var zero durablestore.Composition
	if ctx == nil {
		return zero, errors.New("durable composition context is nil")
	}
	if capabilities.CompositionFactory == nil {
		return zero, errors.New("durable composition factory is not configured")
	}
	composition, err := capabilities.CompositionFactory(ctx, capabilities)
	if err != nil {
		return zero, fmt.Errorf("construct durable composition: %w", err)
	}
	if err := composition.Validate(); err != nil {
		return zero, fmt.Errorf("validate durable composition: %w", err)
	}
	return composition, nil
}

// GeneratePortsFactory constructs the complete durable Generate port set for
// one immutable runtime snapshot. The capability bundle is passed by value so
// the constructor cannot accidentally observe a later reload. Implementations
// must return ports backed by the supplied snapshot-owned capabilities; they
// must not create process-global clients or fabricate a missing phase.
type GeneratePortsFactory func(context.Context, V1RuntimeCapabilities) (durablestore.GeneratePorts, error)

// CompactPortsFactory constructs the complete durable Compact port set for
// one immutable runtime snapshot. Implementations must use only the supplied
// snapshot-owned capabilities and must fail closed when a required dependency
// is absent. A nil value keeps Compact fail-closed.
type CompactPortsFactory func(context.Context, V1RuntimeCapabilities) (durablestore.CompactPorts, error)

// ValidateGenerate checks every capability needed before the Generate-only
// runtime can be composed. It performs no I/O and intentionally treats a
// typed-nil interface as missing. Compact and Query have separate contracts
// and are not implied by this validation.
func (capabilities V1RuntimeCapabilities) ValidateGenerate() error {
	if isNilCapability(capabilities.Snapshot) {
		return errors.New("v1 Generate snapshot source is not configured")
	}
	if isNilCapability(capabilities.Planner) {
		return errors.New("v1 Generate planner is not configured")
	}
	if isNilCapability(capabilities.Adapters) {
		return errors.New("v1 Generate adapter registry is not configured")
	}
	if err := capabilities.Checkpoints.RequireMaterializer(); err != nil {
		return fmt.Errorf("v1 Generate checkpoint capabilities: %w", err)
	}
	if isNilCapability(capabilities.Journal) {
		return errors.New("v1 Generate PostgreSQL journal is not configured")
	}
	if capabilities.Clock == nil {
		return errors.New("v1 Generate clock is not configured")
	}
	if capabilities.GeneratePortsFactory == nil {
		return errors.New("v1 Generate ports factory is not configured")
	}
	return nil
}

// ValidateCompact checks every capability needed before a Compact-only
// runtime can be composed. It performs no I/O and does not imply that Generate
// or Query is configured; the two phase contracts remain independently
// validated, and no builder calls the factory until all callback ports are
// implemented.
func (capabilities V1RuntimeCapabilities) ValidateCompact() error {
	if isNilCapability(capabilities.Snapshot) {
		return errors.New("v1 Compact snapshot source is not configured")
	}
	if isNilCapability(capabilities.Planner) {
		return errors.New("v1 Compact planner is not configured")
	}
	if isNilCapability(capabilities.Adapters) {
		return errors.New("v1 Compact adapter registry is not configured")
	}
	if err := capabilities.Checkpoints.RequireMaterializer(); err != nil {
		return fmt.Errorf("v1 Compact checkpoint capabilities: %w", err)
	}
	if isNilCapability(capabilities.Journal) {
		return errors.New("v1 Compact PostgreSQL journal is not configured")
	}
	if capabilities.Clock == nil {
		return errors.New("v1 Compact clock is not configured")
	}
	if capabilities.CompactPortsFactory == nil {
		return errors.New("v1 Compact ports factory is not configured")
	}
	return nil
}

// snapshotAdapterRegistry owns a private copy of the endpoint map for one
// immutable snapshot. It intentionally does not expose engine.AdapterMap as
// its dynamic type, so a capability consumer cannot mutate the legacy engine's
// map through a type assertion or retain an alias to the factory input.
type snapshotAdapterRegistry struct {
	adapters map[string]provider.Adapter
}

func (registry snapshotAdapterRegistry) Adapter(ctx context.Context, candidate routing.Candidate) (provider.Adapter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	adapter := registry.adapters[candidate.EndpointID]
	if adapter == nil {
		return nil, fmt.Errorf("no adapter configured for endpoint %q", candidate.EndpointID)
	}
	return adapter, nil
}

func newSnapshotAdapterRegistry(adapters map[string]provider.Adapter) engine.AdapterRegistry {
	owned := make(map[string]provider.Adapter, len(adapters))
	for endpointID, adapter := range adapters {
		owned[endpointID] = adapter
	}
	return snapshotAdapterRegistry{adapters: owned}
}

// V1RuntimeCapabilitiesSource is the client-set seam consumed by a future
// V1RuntimeBuilder. The aggregate value is copied into each snapshot client
// set, so a reload cannot accidentally retain stores, adapters, or a clock
// from a previous snapshot.
type V1RuntimeCapabilitiesSource interface {
	V1RuntimeCapabilities() V1RuntimeCapabilities
}

// snapshotCheckpointRepository deliberately erases the concrete PostgreSQL
// repository type before it enters the client set. In particular, callers
// cannot type-assert the capability back to a value containing a pgx pool.
type snapshotCheckpointRepository struct {
	delegate state.CheckpointRepository
}

func (repository snapshotCheckpointRepository) Get(ctx context.Context, scopeID string, id state.CheckpointID) (state.DurableCheckpoint, error) {
	return repository.delegate.Get(ctx, scopeID, id)
}

func (repository snapshotCheckpointRepository) BeginCheckpoint(ctx context.Context) (state.CheckpointUnitOfWork, error) {
	return repository.delegate.BeginCheckpoint(ctx)
}

// snapshotCheckpointBlobReader prevents a snapshot capability consumer from
// type-asserting the deployment's concrete blob reader and reaching an object
// store, locator, or encryption binding outside the storage-neutral port.
type snapshotCheckpointBlobReader struct {
	delegate state.CheckpointBlobReader
}

var _ state.CheckpointBlobReader = snapshotCheckpointBlobReader{}

func (reader snapshotCheckpointBlobReader) Read(ctx context.Context, scopeID string, reference state.CheckpointBlobReference) ([]byte, error) {
	return reader.delegate.Read(ctx, scopeID, reference)
}

// snapshotCheckpointMaterializer erases deployment-specific repository,
// blob-store, keyring, and locator types before the capability enters an
// immutable snapshot client set. It preserves both the ID and opaque-handle
// materialization contracts needed by future V1 composition.
type snapshotCheckpointMaterializer struct {
	delegate state.CheckpointHandleMaterializer
}

var _ state.CheckpointHandleMaterializer = snapshotCheckpointMaterializer{}

func (materializer snapshotCheckpointMaterializer) Materialize(ctx context.Context, scopeID string, id state.CheckpointID, limits state.MaterializeLimits) (state.MaterializedState, error) {
	return materializer.delegate.Materialize(ctx, scopeID, id, limits)
}

func (materializer snapshotCheckpointMaterializer) MaterializeHandle(ctx context.Context, scopeID, handle string, limits state.MaterializeLimits) (state.MaterializedState, error) {
	return materializer.delegate.MaterializeHandle(ctx, scopeID, handle, limits)
}

// snapshotJournal erases the concrete PostgreSQL repository before it enters
// the client set. It preserves the write-only durable.Journal contract while
// preventing callers from reaching into a pgx pool through a type assertion.
type snapshotJournal struct {
	delegate durablestore.Journal
}

var _ durablestore.Journal = snapshotJournal{}

func (journal snapshotJournal) AppendReservation(ctx context.Context, event budget.ReservationEvent) (postgresstore.JournalRecord, error) {
	return journal.delegate.AppendReservation(ctx, event)
}

func (journal snapshotJournal) AppendCompletion(ctx context.Context, event budget.CompletionEvent) (postgresstore.JournalRecord, error) {
	return journal.delegate.AppendCompletion(ctx, event)
}

// queryServiceSource lets an embedding supply the typed control-plane query
// implementation from the same PostgreSQL pool. It is deliberately optional:
// until handlers for a query kind are composed, QueryService remains nil and
// the Activity fails closed.
type queryServiceSource interface {
	QueryService() activity.QueryService
}

func queryRepositoriesFromCloser(closer io.Closer) PostgresQueryRepositories {
	if source, ok := closer.(PostgresQueryRepositoriesSource); ok {
		return source.QueryRepositories()
	}
	if source, ok := closer.(providerStatusRepositorySource); ok {
		repository := source.ProviderStatusRepository()
		return PostgresQueryRepositories{ProviderStatus: &repository}
	}
	return PostgresQueryRepositories{}
}

func checkpointCapabilitiesFromCloser(closer io.Closer) CheckpointCapabilities {
	if closer == nil {
		return CheckpointCapabilities{}
	}
	if source, ok := closer.(PostgresCheckpointCapabilitiesSource); ok {
		capabilities := CheckpointCapabilities{}
		if repository := source.CheckpointRepository(); !isNilCapability(repository) {
			if wrapped, ok := repository.(snapshotCheckpointRepository); ok {
				capabilities.Repository = wrapped
			} else {
				capabilities.Repository = snapshotCheckpointRepository{delegate: repository}
			}
		}
		if blobs := source.CheckpointBlobReader(); !isNilCapability(blobs) {
			if wrapped, ok := blobs.(snapshotCheckpointBlobReader); ok {
				capabilities.Blobs = wrapped
			} else {
				capabilities.Blobs = snapshotCheckpointBlobReader{delegate: blobs}
			}
		}
		if materializerSource, ok := closer.(PostgresCheckpointMaterializerSource); ok {
			if materializer := materializerSource.CheckpointMaterializer(); !isNilCapability(materializer) && capabilities.Repository != nil && capabilities.Blobs != nil {
				capabilities.Materializer = snapshotCheckpointMaterializer{delegate: materializer}
			}
		}
		// Keep this guard adjacent to construction so a future source cannot
		// accidentally publish a materializer with incomplete dependencies.
		if err := capabilities.Validate(); err != nil {
			return CheckpointCapabilities{}
		}
		return capabilities
	}
	return CheckpointCapabilities{}
}

// checkpointCapabilitiesFromCloserWithBindings completes the optional
// PostgreSQL repository bundle with snapshot-owned blob and handle bindings.
// The bindings are passed in by the production factory only after it has
// constructed the immutable blob store and continuation keyring for the same
// configuration snapshot. Missing bindings preserve an explicitly partial
// bundle; they never result in an unscoped reader or an unsigned handle
// materializer.
func checkpointCapabilitiesFromCloserWithBindings(closer io.Closer, reader state.CheckpointBlobReader, verifier state.CheckpointHandleVerifier, now func() time.Time) CheckpointCapabilities {
	capabilities := checkpointCapabilitiesFromCloser(closer)
	if isNilCapability(capabilities.Blobs) && !isNilCapability(reader) {
		capabilities.Blobs = snapshotCheckpointBlobReader{delegate: reader}
	}
	if isNilCapability(capabilities.Materializer) && !isNilCapability(capabilities.Repository) && !isNilCapability(capabilities.Blobs) && !isNilCapability(verifier) {
		capabilities.Materializer = snapshotCheckpointMaterializer{delegate: &state.DurableCheckpointMaterializer{
			Repository:     capabilities.Repository,
			Blobs:          capabilities.Blobs,
			HandleVerifier: verifier,
			Now:            now,
		}}
	}
	if err := capabilities.Validate(); err != nil {
		return CheckpointCapabilities{}
	}
	return capabilities
}

func journalFromCloser(closer io.Closer) durablestore.Journal {
	if source, ok := closer.(PostgresJournalSource); ok {
		journal := source.Journal()
		if journal == nil {
			return nil
		}
		return snapshotJournal{delegate: journal}
	}
	return nil
}

type postgresProviderStatusRecorder struct {
	repository postgresstore.ProviderStatusRepository
}

var _ engine.ProviderStatusRecorder = (*postgresProviderStatusRecorder)(nil)

func newPostgresProviderStatusRecorder(source providerStatusRepositorySource) engine.ProviderStatusRecorder {
	if source == nil {
		return nil
	}
	repository := source.ProviderStatusRepository()
	return &postgresProviderStatusRecorder{repository: repository}
}

func (recorder *postgresProviderStatusRecorder) RecordProviderStatus(ctx context.Context, observation control.StatusObservation) error {
	if recorder == nil {
		return fmt.Errorf("provider status recorder is nil")
	}
	event, err := control.NewStatusEvent(observation)
	if err != nil {
		return err
	}
	_, err = recorder.repository.PersistStatusEvent(ctx, event)
	return err
}
