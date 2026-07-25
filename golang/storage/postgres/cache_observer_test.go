package postgres

import (
	"testing"
	"time"
)

type cacheObserverRecorder struct {
	events []string
	kinds  []string
}

func (recorder *cacheObserverRecorder) RecordCache(event string) {
	recorder.events = append(recorder.events, event)
}

func (recorder *cacheObserverRecorder) RecordPostgresLatency(kind string, _ time.Duration) {
	recorder.kinds = append(recorder.kinds, kind)
}

func TestCacheObserverUsesBoundedEventsAndLatency(t *testing.T) {
	recorder := &cacheObserverRecorder{}
	repository := ResponseCacheRepository{Observer: recorder}
	repository.observeCache("hit", time.Now().Add(-time.Second), nil)
	repository.observeCache("unexpected-secret", time.Now(), nil)
	repository.observeCache("fill", time.Now(), testingError{})

	if len(recorder.events) != 3 || recorder.events[0] != "hit" || recorder.events[1] != "error" || recorder.events[2] != "error" {
		t.Fatalf("observer events = %#v", recorder.events)
	}
	if len(recorder.kinds) != 3 || recorder.kinds[0] != "query" || recorder.kinds[1] != "query" || recorder.kinds[2] != "query" {
		t.Fatalf("observer latency kinds = %#v", recorder.kinds)
	}
}

// testingError avoids coupling this unit test to storage error types; the
// observer only needs to distinguish a failed boundary from a successful one.
type testingError struct{}

func (testingError) Error() string { return "test" }
