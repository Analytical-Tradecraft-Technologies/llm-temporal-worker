package runtime

import (
	"context"
	"fmt"
	"io"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/control"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/state"
	postgresstore "github.com/mfow/llm-temporal-worker/golang/storage/postgres"
)

// providerStatusRepositorySource is implemented by a PostgreSQL client set
// that owns the same pool used for durable state. Keeping this optional avoids
// changing the public PostgresFactory signature or opening a second pool.
type providerStatusRepositorySource interface {
	ProviderStatusRepository() postgresstore.ProviderStatusRepository
}

// PostgresQueryRepositories is the optional read-side bundle owned by one
// durable PostgreSQL pool. Repositories are pointers because deployments may
// roll out only the query slices whose key material and schema are available;
// a missing repository must remain an explicit fail-closed capability rather
// than being replaced with an in-memory answer.
type PostgresQueryRepositories struct {
	ProviderStatus *postgresstore.ProviderStatusRepository
	Inventory      *postgresstore.InventoryRepository
	SpendSummary   *postgresstore.SpendSummaryRepository
	QueryAudit     *postgresstore.QueryExecutionRepository
}

// PostgresQueryRepositoriesSource is implemented by PostgreSQL closers that
// construct read-side repositories alongside their pool. The runtime copies
// this bundle into the immutable production client set so a reload cannot
// accidentally point a query Activity at a closed or newer pool.
type PostgresQueryRepositoriesSource interface {
	QueryRepositories() PostgresQueryRepositories
}

// CheckpointCapabilities is the storage-neutral checkpoint bundle owned by
// one immutable runtime snapshot. The interfaces deliberately hide pgx pools,
// encryption keys, and object-store locators from Activity composition.
// Blobs may be nil when a deployment has not supplied a scoped blob reader;
// callers must fail closed rather than substituting an in-memory reader.
type CheckpointCapabilities struct {
	Repository state.CheckpointRepository
	Blobs      state.CheckpointBlobReader
}

// PostgresCheckpointCapabilitiesSource is implemented by PostgreSQL client
// closers that construct checkpoint capabilities alongside their pool. The
// runtime copies the typed bundle into the snapshot client set so reloads
// cannot accidentally use a closed or newer checkpoint dependency.
type PostgresCheckpointCapabilitiesSource interface {
	CheckpointRepository() state.CheckpointRepository
	CheckpointBlobReader() state.CheckpointBlobReader
}

// CheckpointCapabilitiesSource is the client-set capability exposed to a
// V1RuntimeBuilder through app.ClientSet. It intentionally has one aggregate
// method so builders never need to know whether the snapshot uses PostgreSQL
// or another durable repository implementation.
type CheckpointCapabilitiesSource interface {
	CheckpointCapabilities() CheckpointCapabilities
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
	if source, ok := closer.(PostgresCheckpointCapabilitiesSource); ok {
		return CheckpointCapabilities{
			Repository: source.CheckpointRepository(),
			Blobs:      source.CheckpointBlobReader(),
		}
	}
	return CheckpointCapabilities{}
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
