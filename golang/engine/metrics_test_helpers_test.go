package engine

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
)

func assertMetricCounter(t *testing.T, metrics *observability.Metrics, name string, labels map[string]string, want float64) {
	t.Helper()
	got, found := metricValue(t, metrics, name, labels, false)
	if !found && want == 0 {
		return
	}
	if !found || got != want {
		t.Fatalf("%s%v = %v, found=%v; want %v", name, labels, got, found, want)
	}
}

func assertMetricHistogramCount(t *testing.T, metrics *observability.Metrics, name string, labels map[string]string, want uint64) {
	t.Helper()
	got, found := metricValue(t, metrics, name, labels, true)
	if !found || uint64(got) != want {
		t.Fatalf("%s%v count = %v, found=%v; want %v", name, labels, got, found, want)
	}
}

func metricValue(t *testing.T, metrics *observability.Metrics, name string, labels map[string]string, histogram bool) (float64, bool) {
	t.Helper()
	families, err := metrics.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			matches := len(metric.Label) == len(labels)
			for _, label := range metric.Label {
				if labels[label.GetName()] != label.GetValue() {
					matches = false
					break
				}
			}
			if matches {
				if histogram {
					return float64(metric.GetHistogram().GetSampleCount()), true
				}
				return metric.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}
