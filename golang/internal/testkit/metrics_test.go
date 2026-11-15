package testkit

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricAssertions(t *testing.T) {
	registry := prometheus.NewRegistry()
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "requests_total"}, []string{"status"})
	histogram := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "latency_seconds"}, []string{"status"})
	registry.MustRegister(counter, histogram)
	counter.WithLabelValues("ok").Add(2)
	histogram.WithLabelValues("ok").Observe(1)
	ok := map[string]string{"status": "ok"}

	if got, found := MetricValue(t, registry, "requests_total", ok, false); !found || got != 2 {
		t.Fatalf("MetricValue(counter) = %v, %v; want 2, true", got, found)
	}
	if got, found := MetricValue(t, registry, "latency_seconds", ok, true); !found || got != 1 {
		t.Fatalf("MetricValue(histogram) = %v, %v; want 1, true", got, found)
	}
	if _, found := MetricValue(t, registry, "requests_total", map[string]string{"status": "ok", "extra": "x"}, false); found {
		t.Fatal("MetricValue matched a series with other labels")
	}
	failed := map[string]string{"status": "failed"}
	for _, test := range []struct {
		name        string
		assert      func(testing.TB)
		wantMessage string // "" when the assertion must pass
	}{
		{name: "counter", assert: func(t testing.TB) { MetricCounter(t, registry, "requests_total", ok, 2) }},
		{name: "absent counter is zero", assert: func(t testing.TB) { MetricCounter(t, registry, "requests_total", failed, 0) }},
		{name: "wrong counter", assert: func(t testing.TB) { MetricCounter(t, registry, "requests_total", ok, 3) }, wantMessage: "requests_totalmap[status:ok] = 2, found=true; want 3"},
		{name: "histogram", assert: func(t testing.TB) { MetricHistogramCount(t, registry, "latency_seconds", ok, 1) }},
		{name: "absent histogram", assert: func(t testing.TB) { MetricHistogramCount(t, registry, "latency_seconds", failed, 0) }, wantMessage: "latency_secondsmap[status:failed] count = 0, found=false; want 0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &recorder{}
			test.assert(r)
			if r.message != test.wantMessage {
				t.Fatalf("message = %q, want %q", r.message, test.wantMessage)
			}
		})
	}
}
