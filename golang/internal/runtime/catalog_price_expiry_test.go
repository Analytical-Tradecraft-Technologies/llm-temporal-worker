package runtime

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
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
		// Once coverage resumes after a gap, the resumed window's end counts.
		{"coverage resumed after gap", catalog([]routing.PriceWindow{window(-3*day, -2*day), window(-day, 4*day)}), now.Add(4 * day)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := catalogPriceExpiry(test.catalog, now); !got.Equal(test.want) {
				t.Fatalf("expiry = %v, want %v", got, test.want)
			}
		})
	}
}

// The gauge is evaluated at scrape time, so a priced window that opens or
// closes between reloads changes it without a reload.
func TestCatalogPriceExpiryFollowsWindowsOpeningWithoutReload(t *testing.T) {
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour
	future := routing.PriceWindow{From: now.Add(day), Until: now.Add(3 * day)}
	resumed := []routing.PriceWindow{{From: now.Add(-day), Until: now.Add(2 * day)}, {From: now.Add(4 * day), Until: now.Add(9 * day)}}
	set := &productionClientSet{routes: routing.Catalog{Models: map[string]routing.Model{
		"future":  {Routes: []routing.Route{{PricedWindows: []routing.PriceWindow{future}}}},
		"resumed": {Routes: []routing.Route{{PricedWindows: resumed}}},
	}}}
	snapshot := &app.RuntimeSnapshot{Clients: &snapshotClients{clients: set}}
	for _, step := range []struct {
		at   time.Time
		want time.Time
	}{
		{now, now.Add(2 * day)},
		// The future route opens; the resumed route still ends first.
		{now.Add(day), now.Add(2 * day)},
		{now.Add(day + day/2), now.Add(2 * day)},
		// The resumed route's first window has lapsed, and the future route
		// is still priced; the lapse is the earliest end.
		{now.Add(2*day + time.Hour), now.Add(2 * day)},
		// Coverage resumes, and the future route has lapsed.
		{now.Add(5 * day), now.Add(3 * day)},
	} {
		if got := snapshotCatalogPriceExpiry(snapshot, step.at); !got.Equal(step.want) {
			t.Fatalf("expiry at %v = %v, want %v", step.at, got, step.want)
		}
	}
	// Only the future route: nothing expires until its window opens.
	futureOnly := &app.RuntimeSnapshot{Clients: &snapshotClients{clients: &productionClientSet{routes: routing.Catalog{Models: map[string]routing.Model{
		"future": {Routes: []routing.Route{{PricedWindows: []routing.PriceWindow{future}}}},
	}}}}}
	if got := snapshotCatalogPriceExpiry(futureOnly, now); !got.IsZero() {
		t.Fatalf("expiry before the window opens = %v, want none", got)
	}
	if got := snapshotCatalogPriceExpiry(futureOnly, now.Add(2*day)); !got.Equal(future.Until) {
		t.Fatalf("expiry once the window opens = %v, want %v", got, future.Until)
	}
}

// The gauge reads the snapshot the application serves at scrape time, so it
// follows reloads and the passage of time with no publish hook.
func TestCatalogPriceExpiryGaugeReadsCurrentSnapshotAtScrape(t *testing.T) {
	metrics, err := observability.NewMetrics(observability.AllowedValues{})
	if err != nil {
		t.Fatal(err)
	}
	if got := priceExpiryGauge(t, metrics); !math.IsInf(got, 1) {
		t.Fatalf("initial expiry = %v, want +Inf", got)
	}
	start := time.Now().Truncate(time.Second)
	first := start.Add(36 * time.Hour)
	second := start.Add(72 * time.Hour)
	untils := []time.Time{first, second}
	builds := 0
	application, err := app.New(context.Background(), app.Options{
		InitialConfig: localConfigWithBudgetWindows(t, budgetWindowYAML("", "24h", "1h", "100")),
		Builder:       app.SnapshotBuilder{},
		Clients: func(context.Context, *config.Snapshot) (app.ClientSet, error) {
			until := untils[builds]
			builds++
			return &productionClientSet{routes: routing.Catalog{Models: map[string]routing.Model{"model": {Routes: []routing.Route{{PricedWindows: []routing.PriceWindow{{Until: until}}}}}}}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = application.Close(context.Background()) })
	exportCatalogPriceExpiry(metrics, application)
	if got := priceExpiryGauge(t, metrics); got != float64(first.Unix()) {
		t.Fatalf("expiry = %v, want %v", got, first.Unix())
	}
	if err := application.Reload(context.Background(), localConfigWithBudgetWindows(t, budgetWindowYAML("", "24h", "1h", "200"))); err != nil {
		t.Fatal(err)
	}
	if got := priceExpiryGauge(t, metrics); got != float64(second.Unix()) {
		t.Fatalf("expiry after reload = %v, want %v", got, second.Unix())
	}
	// Clients without a route catalog report no expiry.
	if got := snapshotCatalogPriceExpiry(&app.RuntimeSnapshot{Clients: &snapshotClients{}}, start); !got.IsZero() {
		t.Fatalf("expiry without a catalog = %v, want none", got)
	}
	if got := snapshotCatalogPriceExpiry(nil, start); !got.IsZero() {
		t.Fatalf("expiry without a snapshot = %v, want none", got)
	}
	exportCatalogPriceExpiry(nil, application)
	exportCatalogPriceExpiry(metrics, nil)
}
