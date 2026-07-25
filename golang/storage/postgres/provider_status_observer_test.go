package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
)

type providerStatusObserverRecorder struct {
	kinds     []string
	durations []time.Duration
}

func (recorder *providerStatusObserverRecorder) RecordPostgresLatency(kind string, duration time.Duration) {
	recorder.kinds = append(recorder.kinds, kind)
	recorder.durations = append(recorder.durations, duration)
}

func TestProviderStatusObserverRecordsAdvisoryLockBoundary(t *testing.T) {
	recorder := &providerStatusObserverRecorder{}
	repository := ProviderStatusRepository{Observer: recorder}
	repository.observeProviderStatusLock(context.Background(), time.Now().Add(-time.Second))
	if len(recorder.kinds) != 1 || recorder.kinds[0] != "lock" {
		t.Fatalf("lock observations = %#v, want one bounded lock observation", recorder.kinds)
	}
	if recorder.durations[0] < 0 {
		t.Fatalf("lock duration = %s, want non-negative", recorder.durations[0])
	}
}

func TestProviderStatusObserverUsesActivityMetricsContext(t *testing.T) {
	metrics, err := observability.NewMetrics(observability.AllowedValues{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := observability.WithMetrics(context.Background(), metrics)
	ProviderStatusRepository{}.observeProviderStatusLock(ctx, time.Now())
	families, err := metrics.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "llmtw_postgres_latency_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "kind" && label.GetValue() == "lock" {
					if metric.GetHistogram().GetSampleCount() != 1 {
						t.Fatalf("lock histogram sample count = %d, want 1", metric.GetHistogram().GetSampleCount())
					}
					return
				}
			}
		}
	}
	t.Fatal("lock latency histogram was not recorded")
}
