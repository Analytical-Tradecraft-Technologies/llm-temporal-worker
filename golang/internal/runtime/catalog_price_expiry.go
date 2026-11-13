package runtime

import (
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
)

// catalogPriceExpirySource reports when the earliest current priced window of
// a snapshot's routes ends. A zero time means no route's price expires.
type catalogPriceExpirySource interface {
	CatalogPriceExpiry(now time.Time) time.Time
}

func (set *productionClientSet) CatalogPriceExpiry(now time.Time) time.Time {
	if set == nil {
		return time.Time{}
	}
	return catalogPriceExpiry(set.routes, now)
}

func (clients *snapshotClients) CatalogPriceExpiry(now time.Time) time.Time {
	if clients == nil {
		return time.Time{}
	}
	if source, ok := clients.clients.(catalogPriceExpirySource); ok {
		return source.CatalogPriceExpiry(now)
	}
	return time.Time{}
}

// catalogPriceExpiry returns the earliest end of a route's priced coverage at
// now, or zero when no route's coverage ends. A route priced at now
// contributes the end of its current window (windows are already merged, so
// that is where its coverage stops). A route unpriced at now whose last window
// has ended contributes that past end: its price has lapsed, and the gauge
// must show it even after a restart or reload. A route that has never been
// priced, or only will be, has no expiry; it surfaces as no_route.
func catalogPriceExpiry(catalog routing.Catalog, now time.Time) time.Time {
	var earliest time.Time
	consider := func(until time.Time) {
		if !until.IsZero() && (earliest.IsZero() || until.Before(earliest)) {
			earliest = until
		}
	}
	for _, model := range catalog.Models {
		for _, route := range model.Routes {
			consider(routePriceExpiry(route.PricedWindows, now))
		}
	}
	return earliest
}

func routePriceExpiry(windows []routing.PriceWindow, now time.Time) time.Time {
	var lapsed time.Time
	for _, window := range windows {
		if window.Contains(now) {
			return window.Until
		}
		if !window.Until.IsZero() && !window.Until.After(now) && window.Until.After(lapsed) {
			lapsed = window.Until
		}
	}
	return lapsed
}

// snapshotCatalogPriceExpiry is the catalog expiry of a published snapshot at
// now, or zero when it has no route catalog or no route's price expires.
func snapshotCatalogPriceExpiry(snapshot *app.RuntimeSnapshot, now time.Time) time.Time {
	if snapshot == nil {
		return time.Time{}
	}
	if source, ok := snapshot.Clients.(catalogPriceExpirySource); ok {
		return source.CatalogPriceExpiry(now)
	}
	return time.Time{}
}

// exportCatalogPriceExpiry points the catalog expiry gauge at whichever
// snapshot the application is serving when Prometheus scrapes. Reading the
// current snapshot follows reloads without a hook, and evaluating at scrape
// time follows priced windows that open or close between reloads.
func exportCatalogPriceExpiry(metrics *observability.Metrics, application *app.App) {
	if metrics == nil || application == nil {
		return
	}
	metrics.SetCatalogPriceExpirySource(func(now time.Time) time.Time {
		return snapshotCatalogPriceExpiry(application.Current(), now)
	})
}
