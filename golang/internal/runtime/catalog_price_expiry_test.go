package runtime

import (
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
)

// metricsText is the Prometheus exposition a scrape of metrics returns.
func metricsText(t *testing.T, metrics *observability.Metrics) string {
	t.Helper()
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", response.Code)
	}
	return response.Body.String()
}

func priceExpiryGauge(t *testing.T, metrics *observability.Metrics) float64 {
	t.Helper()
	families, err := metrics.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "llmtw_catalog_price_expiry_timestamp_seconds" {
			return family.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatal("llmtw_catalog_price_expiry_timestamp_seconds was not gathered")
	return 0
}

func TestCatalogPriceExpiryIsTheEarliestEndOfCurrentPricedCoverage(t *testing.T) {
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	window := func(from, until time.Duration) routing.PriceWindow {
		w := routing.PriceWindow{}
		if from != 0 {
			w.From = now.Add(from)
		}
		if until != 0 {
			w.Until = now.Add(until)
		}
		return w
	}
	catalog := func(windows ...[]routing.PriceWindow) routing.Catalog {
		routes := make([]routing.Route, 0, len(windows))
		for _, w := range windows {
			routes = append(routes, routing.Route{PricedWindows: w})
		}
		return routing.Catalog{Models: map[string]routing.Model{"model": {Routes: routes}}}
	}
	day := 24 * time.Hour
	for _, test := range []struct {
		name    string
		catalog routing.Catalog
		want    time.Time
	}{
		{"no routes", routing.Catalog{}, time.Time{}},
		{"unbounded price and legacy route without windows", catalog([]routing.PriceWindow{window(0, 0)}, nil), time.Time{}},
		{"earliest current window wins", catalog([]routing.PriceWindow{window(-day, 7*day)}, []routing.PriceWindow{window(0, 3*day)}), now.Add(3 * day)},
		// A later window that is not contiguous does not extend coverage.
		{"gap ends coverage", catalog([]routing.PriceWindow{window(-day, day), window(2*day, 0)}), now.Add(day)},
		// A lapsed route still reports its past expiry after a restart or reload.
		{"lapsed route", catalog([]routing.PriceWindow{window(-2*day, -day)}, []routing.PriceWindow{window(0, 3*day)}), now.Add(-day)},
		{"route priced only in future has no expiry", catalog([]routing.PriceWindow{window(day, 2*day)}, []routing.PriceWindow{}), time.Time{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := catalogPriceExpiry(test.catalog, now); !got.Equal(test.want) {
				t.Fatalf("expiry = %v, want %v", got, test.want)
			}
		})
	}
}

func TestPublishCatalogPriceExpirySetsGaugeFromPublishedSnapshot(t *testing.T) {
	metrics, err := observability.NewMetrics(observability.AllowedValues{})
	if err != nil {
		t.Fatal(err)
	}
	if got := priceExpiryGauge(t, metrics); !math.IsInf(got, 1) {
		t.Fatalf("initial expiry = %v, want +Inf", got)
	}
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	until := now.Add(36 * time.Hour)
	set := &productionClientSet{routes: routing.Catalog{Models: map[string]routing.Model{"model": {Routes: []routing.Route{{PricedWindows: []routing.PriceWindow{{Until: until}}}}}}}}
	snapshot := &app.RuntimeSnapshot{Clients: &snapshotClients{clients: set}}
	publishCatalogPriceExpiry(metrics, snapshot, now)
	if got := priceExpiryGauge(t, metrics); got != float64(until.Unix()) {
		t.Fatalf("expiry = %v, want %v", got, until.Unix())
	}
	// A snapshot whose clients cannot report an expiry resets the gauge.
	publishCatalogPriceExpiry(metrics, &app.RuntimeSnapshot{Clients: &snapshotClients{}}, now)
	if got := priceExpiryGauge(t, metrics); !math.IsInf(got, 1) {
		t.Fatalf("expiry without a catalog = %v, want +Inf", got)
	}
	publishCatalogPriceExpiry(nil, snapshot, now)
	publishCatalogPriceExpiry(metrics, nil, now)
}
