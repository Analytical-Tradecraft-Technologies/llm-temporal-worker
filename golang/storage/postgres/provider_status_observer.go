package postgres

import (
	"context"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
)

// ProviderStatusObserver receives the bounded PostgreSQL lock signal emitted
// by provider-status persistence. Implementations must not attach request,
// tenant, route, or database identifiers to the metric.
type ProviderStatusObserver interface {
	RecordPostgresLatency(kind string, duration time.Duration)
}

// observeProviderStatusLock records the duration of the actual transaction
// advisory-lock boundary. Activity contexts carry the process metrics
// collector in production; Observer remains an explicit test/composition seam
// for callers that persist status outside an Activity.
func (repository ProviderStatusRepository) observeProviderStatusLock(ctx context.Context, started time.Time) {
	observer := repository.Observer
	if observer == nil && ctx != nil {
		observer = observability.MetricsFromContext(ctx)
	}
	if observer == nil {
		return
	}
	observer.RecordPostgresLatency("lock", time.Since(started))
}
