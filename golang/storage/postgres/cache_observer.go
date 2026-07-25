package postgres

import "time"

// CacheObserver receives bounded cache lifecycle signals without making the
// PostgreSQL adapter depend on a concrete metrics exporter. Implementations
// must keep event labels fixed; the adapter never supplies tenant, operation,
// or physical relation names.
type CacheObserver interface {
	RecordCache(event string)
	RecordPostgresLatency(kind string, duration time.Duration)
}

func (repository ResponseCacheRepository) observeCache(event string, started time.Time, err error) {
	if repository.Observer == nil {
		return
	}
	switch event {
	case "hit", "use", "miss", "fill", "fill_existing", "fill_busy", "fill_failed":
	default:
		event = "error"
	}
	if err != nil {
		event = "error"
	}
	repository.Observer.RecordCache(event)
	repository.Observer.RecordPostgresLatency("query", time.Since(started))
}
