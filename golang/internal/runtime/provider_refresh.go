package runtime

// This file implements the on-demand provider management refresh used by the
// persisted control-plane queries. Only model inventory has a reusable
// provider fetcher (the provider.ModelLister extension implemented by the
// direct OpenAI Chat and Responses adapters). No adapter exposes a provider
// status or credit/balance management API, so those refreshes stay explicitly
// unsupported instead of inventing state.

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/control"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

const (
	// DefaultProviderRefreshTimeout bounds one endpoint's complete provider
	// listing, including every page it follows.
	DefaultProviderRefreshTimeout = 10 * time.Second
	// DefaultProviderRefreshMaxEndpoints bounds how many endpoints one query may
	// refresh. Endpoints beyond the bound are not fetched and the response is
	// reported stale.
	DefaultProviderRefreshMaxEndpoints = 4
	// DefaultProviderRefreshMinInterval is the floor applied to a caller's
	// refresh_if_older_than_seconds, so an authorized caller cannot turn the
	// query into a high-rate provider management API client.
	DefaultProviderRefreshMinInterval = time.Minute
	// DefaultProviderInventoryValidity is how long a refreshed listing is
	// reported current before it becomes stale.
	DefaultProviderInventoryValidity = time.Hour
	providerRefreshListPageSize      = provider.ModelListMaxPageSize
)

// ProviderRefreshTarget is one configured provider endpoint/account identity
// whose inventory may be refreshed. A nil Lister marks the endpoint as
// explicitly unsupported for refresh.
type ProviderRefreshTarget struct {
	Provider            string
	EndpointID          string
	EndpointFamily      string
	Region              string
	EndpointAccountHMAC [32]byte
	Lister              provider.ModelLister
}

// ProviderRefresherOptions configures a snapshot-owned refresher.
type ProviderRefresherOptions struct {
	ConfigDigest [32]byte
	Store        control.InventoryStore
	Targets      []ProviderRefreshTarget
	Clock        func() time.Time
	// Zero values select the defaults above.
	Timeout      time.Duration
	MaxEndpoints int
	MinInterval  time.Duration
	Validity     time.Duration
}

// ProviderRefresher refreshes provider inventory for an authorized query and
// writes the result to Redis provider state. Concurrent refreshes of the same
// endpoint identity share one provider fetch.
type ProviderRefresher struct {
	configDigest [32]byte
	store        control.InventoryStore
	targets      []ProviderRefreshTarget
	clock        func() time.Time
	timeout      time.Duration
	maxEndpoints int
	minInterval  time.Duration
	validity     time.Duration
	flights      refreshFlightGroup
}

// NewProviderRefresher validates the refresher options.
func NewProviderRefresher(options ProviderRefresherOptions) (*ProviderRefresher, error) {
	if options.ConfigDigest == ([32]byte{}) {
		return nil, errors.New("provider refresher config digest is required")
	}
	if isNilCapability(options.Store) {
		return nil, errors.New("provider refresher inventory store is required")
	}
	refresher := &ProviderRefresher{
		configDigest: options.ConfigDigest, store: options.Store, clock: options.Clock,
		timeout: options.Timeout, maxEndpoints: options.MaxEndpoints, minInterval: options.MinInterval, validity: options.Validity,
	}
	if refresher.clock == nil {
		refresher.clock = time.Now
	}
	if refresher.timeout <= 0 {
		refresher.timeout = DefaultProviderRefreshTimeout
	}
	if refresher.maxEndpoints <= 0 {
		refresher.maxEndpoints = DefaultProviderRefreshMaxEndpoints
	}
	if refresher.minInterval <= 0 {
		refresher.minInterval = DefaultProviderRefreshMinInterval
	}
	if refresher.validity <= 0 {
		refresher.validity = DefaultProviderInventoryValidity
	}
	for _, target := range options.Targets {
		if target.EndpointID == "" {
			return nil, errors.New("provider refresh target endpoint is required")
		}
		if isNilCapability(target.Lister) {
			target.Lister = nil
		}
		refresher.targets = append(refresher.targets, target)
	}
	sort.Slice(refresher.targets, func(i, j int) bool {
		return refreshTargetKey(refresher.targets[i]) < refreshTargetKey(refresher.targets[j])
	})
	return refresher, nil
}

// ProviderRefreshReport summarizes one query's refresh. Failed counts
// endpoints whose fetch or write failed or timed out; their last persisted
// state is left untouched. Skipped counts endpoints beyond the per-query bound.
type ProviderRefreshReport struct {
	Refreshed int
	Fresh     int
	Failed    int
	Skipped   int
}

// Degraded reports whether any endpoint in scope could not be brought up to
// date, so the response must be reported stale.
func (report ProviderRefreshReport) Degraded() bool { return report.Failed > 0 || report.Skipped > 0 }

// errRefreshUnsupported marks a refresh that cannot be served for the
// requested scope. The handler converts it to the typed unsupported error.
type errRefreshUnsupported struct{ reason string }

func (err errRefreshUnsupported) Error() string { return err.reason }

// RefreshInventory refreshes every endpoint matching the provider/endpoint
// filter whose last listing is older than olderThan (or the minimum interval,
// whichever is larger). It returns an unsupported error before any fetch when
// no endpoint matches or any matching endpoint has no model-list fetcher.
func (refresher *ProviderRefresher) RefreshInventory(ctx context.Context, providerName, endpointID string, olderThan time.Duration) (ProviderRefreshReport, error) {
	var report ProviderRefreshReport
	if refresher == nil {
		return report, errRefreshUnsupported{"provider refresh is not configured"}
	}
	var targets []ProviderRefreshTarget
	for _, target := range refresher.targets {
		if (providerName != "" && target.Provider != providerName) || (endpointID != "" && target.EndpointID != endpointID) {
			continue
		}
		if target.Lister == nil {
			return report, errRefreshUnsupported{"model inventory refresh is unsupported for an endpoint in scope; filter by a supported endpoint"}
		}
		targets = append(targets, target)
	}
	if len(targets) == 0 {
		return report, errRefreshUnsupported{"no refreshable endpoint matches the query scope"}
	}
	if olderThan < refresher.minInterval {
		olderThan = refresher.minInterval
	}
	if len(targets) > refresher.maxEndpoints {
		report.Skipped = len(targets) - refresher.maxEndpoints
		targets = targets[:refresher.maxEndpoints]
	}
	type outcome struct {
		fresh bool
		err   error
	}
	results := make([]outcome, len(targets))
	var wg sync.WaitGroup
	for index, target := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fresh, err := refresher.refreshTarget(ctx, target, olderThan)
			results[index] = outcome{fresh: fresh, err: err}
		}()
	}
	wg.Wait()
	for _, result := range results {
		switch {
		case result.err != nil:
			// The caller's own cancellation is not a provider failure.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return report, ctxErr
			}
			var unavailable refreshStateUnavailable
			if errors.As(result.err, &unavailable) {
				return report, unavailable.err
			}
			report.Failed++
		case result.fresh:
			report.Fresh++
		default:
			report.Refreshed++
		}
	}
	return report, nil
}

// refreshStateUnavailable wraps a Redis read failure. The query cannot be
// answered from Redis in that case, so it propagates instead of being
// reported as a stale provider result.
type refreshStateUnavailable struct{ err error }

func (err refreshStateUnavailable) Error() string { return err.err.Error() }

func (refresher *ProviderRefresher) refreshTarget(ctx context.Context, target ProviderRefreshTarget, olderThan time.Duration) (bool, error) {
	existing, err := refresher.store.GetInventorySnapshot(ctx, refresher.configDigest, target.Provider, target.EndpointID, target.EndpointAccountHMAC)
	switch {
	case err == nil:
		if existing.Source == control.InventoryProviderAPI && refresher.clock().Sub(existing.ObservedAt) < olderThan {
			return true, nil
		}
	case errors.Is(err, control.ErrInventorySnapshotNotFound):
	default:
		return false, refreshStateUnavailable{err}
	}
	key := refreshTargetKey(target)
	return false, refresher.flights.do(ctx, key, func() error {
		// The shared fetch is detached from the first caller's cancellation so
		// one canceled caller cannot fail every collapsed waiter. The timeout
		// still bounds it.
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), refresher.timeout)
		defer cancel()
		models, err := provider.CollectModelInventory(fetchCtx, target.Lister, provider.ModelListQuery{EndpointID: target.EndpointID, Limit: providerRefreshListPageSize})
		if err != nil {
			return err
		}
		observed := refresher.clock().UTC()
		snapshot := control.InventorySnapshot{
			ConfigDigest: refresher.configDigest, Provider: target.Provider, EndpointID: target.EndpointID,
			EndpointAccountHMAC: target.EndpointAccountHMAC, EndpointFamily: target.EndpointFamily, Region: target.Region,
			Source: control.InventoryProviderAPI, ObservedAt: observed, ExpiresAt: observed.Add(refresher.validity), Complete: true,
			Models: make([]control.Model, 0, len(models)),
		}
		for _, model := range models {
			snapshot.Models = append(snapshot.Models, control.Model{
				ProviderModelID: model.ProviderModelID, DisplayName: model.DisplayName, OwnedBy: model.OwnedBy,
				CreatedAt: model.CreatedAt.UTC(), Lifecycle: control.Lifecycle(model.Lifecycle),
				CapabilityDigest: model.CapabilityDigest, SafeMetadata: copyStringMap(model.SafeMetadata),
			})
		}
		snapshot.InventoryDigest = control.InventoryDigest(snapshot.Models)
		if err := snapshot.Validate(); err != nil {
			return fmt.Errorf("refreshed inventory is invalid: %w", err)
		}
		_, err = refresher.store.PersistInventorySnapshot(fetchCtx, snapshot)
		return err
	})
}

func copyStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	copied := make(map[string]string, len(values))
	for key, value := range values {
		copied[key] = value
	}
	return copied
}

func refreshTargetKey(target ProviderRefreshTarget) string {
	return target.Provider + "\x00" + target.EndpointID + "\x00" + hex.EncodeToString(target.EndpointAccountHMAC[:])
}

// providerRefreshTargets derives one refresh target per configured endpoint
// identity from the routing catalog. An endpoint whose routes disagree on the
// provider, family, region or account identity is ambiguous and gets no
// lister, so a refresh that reaches it is reported unsupported rather than
// written under a guessed identity. Endpoints without a model-list adapter are
// likewise unsupported.
func providerRefreshTargets(catalog routing.Catalog, adapters map[string]provider.Adapter) []ProviderRefreshTarget {
	type identity struct {
		provider, family, region string
		account                  [32]byte
	}
	seen := make(map[string]map[identity]struct{})
	for _, model := range catalog.Models {
		for _, route := range model.Routes {
			if seen[route.EndpointID] == nil {
				seen[route.EndpointID] = make(map[identity]struct{})
			}
			seen[route.EndpointID][identity{route.Provider, route.Family, route.Region, route.EndpointAccountHMAC}] = struct{}{}
		}
	}
	var targets []ProviderRefreshTarget
	for endpointID, identities := range seen {
		var lister provider.ModelLister
		if adapter, ok := adapters[endpointID].(provider.ModelLister); ok && len(identities) == 1 {
			lister = adapter
		}
		for id := range identities {
			target := ProviderRefreshTarget{Provider: id.provider, EndpointID: endpointID, EndpointFamily: id.family, Region: id.region, EndpointAccountHMAC: id.account, Lister: lister}
			if id.provider == "" || id.family == "" || id.region == "" || id.account == ([32]byte{}) {
				target.Lister = nil
			}
			targets = append(targets, target)
		}
	}
	return targets
}

// refreshFlightGroup collapses concurrent calls with the same key into one
// execution. Unlike a plain mutex, a waiter can stop waiting when its own
// context ends without canceling the shared call.
type refreshFlightGroup struct {
	mu    sync.Mutex
	calls map[string]*refreshFlight
}

type refreshFlight struct {
	done    chan struct{}
	err     error
	waiters int
}

func (group *refreshFlightGroup) do(ctx context.Context, key string, fn func() error) error {
	group.mu.Lock()
	if group.calls == nil {
		group.calls = make(map[string]*refreshFlight)
	}
	if call, ok := group.calls[key]; ok {
		call.waiters++
		group.mu.Unlock()
		select {
		case <-call.done:
			return call.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	call := &refreshFlight{done: make(chan struct{})}
	group.calls[key] = call
	group.mu.Unlock()

	go func() {
		defer func() {
			group.mu.Lock()
			delete(group.calls, key)
			group.mu.Unlock()
			close(call.done)
		}()
		call.err = fn()
	}()
	select {
	case <-call.done:
		return call.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// waiting reports how many callers have joined an in-flight call. Tests use
// it to prove that concurrent refreshes collapse.
func (group *refreshFlightGroup) waiting(key string) int {
	group.mu.Lock()
	defer group.mu.Unlock()
	if call, ok := group.calls[key]; ok {
		return call.waiters
	}
	return 0
}
