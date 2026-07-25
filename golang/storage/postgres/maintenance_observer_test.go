package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/maintenance"
)

type maintenanceObserverRecorder struct {
	rows      []string
	failures  []string
	pool      bool
	latencies []string
	tuples    []string
}

func (recorder *maintenanceObserverRecorder) RecordMaintenance(resource, outcome string, rows int, _ time.Duration) {
	recorder.rows = append(recorder.rows, resource+":"+outcome)
}

func (recorder *maintenanceObserverRecorder) RecordMaintenanceFailure(resource string) {
	recorder.failures = append(recorder.failures, resource)
}

func (recorder *maintenanceObserverRecorder) RecordPostgresPool(int32, int32, int32, int32) {
	recorder.pool = true
}

func (recorder *maintenanceObserverRecorder) RecordPostgresLatency(kind string, _ time.Duration) {
	recorder.latencies = append(recorder.latencies, kind)
}

func (recorder *maintenanceObserverRecorder) RecordPostgresTableTuples(resource string, _, _ int64) {
	recorder.tuples = append(recorder.tuples, resource)
}

func TestMaintenanceObserverRecordsBoundedProgressWithoutPool(t *testing.T) {
	recorder := &maintenanceObserverRecorder{}
	repository := MaintenanceRepository{Observer: recorder}
	repository.observeMaintenance(context.Background(), "cache", time.Now().Add(-time.Second), maintenance.RetentionResult{Examined: 3, Tombstoned: 1, Deleted: 1, Skipped: 1}, nil)
	repository.observeBlobMaintenance(context.Background(), "blob", time.Now(), BlobGCResult{Examined: 2, Eligible: 1, Skipped: 1}, testingError{})

	if len(recorder.rows) != 7 {
		t.Fatalf("row observations = %d, want 7", len(recorder.rows))
	}
	if len(recorder.failures) != 1 || recorder.failures[0] != "blob" {
		t.Fatalf("failure observations = %#v", recorder.failures)
	}
	if len(recorder.latencies) != 2 || recorder.latencies[0] != "maintenance" || recorder.latencies[1] != "maintenance" {
		t.Fatalf("latency observations = %#v", recorder.latencies)
	}
}
