package runtime

import (
	"context"
	"fmt"
	"io"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/control"
	"github.com/mfow/llm-temporal-worker/golang/engine"
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
