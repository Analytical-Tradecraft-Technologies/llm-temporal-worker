package testkit

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// MetricValue returns the value of the series name, among those metrics
// gathers, whose labels are exactly labels: a counter's value, or a
// histogram's sample count when histogram is set. found reports whether the
// series exists.
func MetricValue(t testing.TB, metrics prometheus.Gatherer, name string, labels map[string]string, histogram bool) (value float64, found bool) {
	t.Helper()
	families, err := metrics.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
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

// MetricCounter fails the test immediately unless the counter series name
// with exactly labels holds want. A series that does not exist counts as
// zero.
func MetricCounter(t testing.TB, metrics prometheus.Gatherer, name string, labels map[string]string, want float64) {
	t.Helper()
	got, found := MetricValue(t, metrics, name, labels, false)
	if got != want {
		t.Fatalf("%s%v = %v, found=%v; want %v", name, labels, got, found, want)
	}
}

// MetricHistogramCount fails the test immediately unless the histogram series
// name with exactly labels exists and holds want samples.
func MetricHistogramCount(t testing.TB, metrics prometheus.Gatherer, name string, labels map[string]string, want uint64) {
	t.Helper()
	got, found := MetricValue(t, metrics, name, labels, true)
	if !found || uint64(got) != want {
		t.Fatalf("%s%v count = %v, found=%v; want %v", name, labels, got, found, want)
	}
}
