package runtime

import (
	"context"
	"crypto/sha256"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/control"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

// memoryInventoryStore is a minimal control.InventoryStore with the same
// "newer observation wins" rule as the Redis store.
type memoryInventoryStore struct {
	mu        sync.Mutex
	snapshots map[string]control.InventorySnapshot
	persists  int
}

func newMemoryInventoryStore() *memoryInventoryStore {
	return &memoryInventoryStore{snapshots: make(map[string]control.InventorySnapshot)}
}

func (store *memoryInventoryStore) PersistInventorySnapshot(_ context.Context, snapshot control.InventorySnapshot) (bool, error) {
	if err := snapshot.Validate(); err != nil {
		return false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	key := snapshot.Provider + "\x00" + snapshot.EndpointID
	if prior, ok := store.snapshots[key]; ok && !snapshot.ObservedAt.After(prior.ObservedAt) {
		return false, nil
	}
	store.snapshots[key] = snapshot
	store.persists++
	return true, nil
}

func (store *memoryInventoryStore) GetInventorySnapshot(_ context.Context, _ [32]byte, providerName, endpoint string, _ [32]byte) (control.InventorySnapshot, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	snapshot, ok := store.snapshots[providerName+"\x00"+endpoint]
	if !ok {
		return control.InventorySnapshot{}, control.ErrInventorySnapshotNotFound
	}
	return snapshot, nil
}

func (store *memoryInventoryStore) ListInventoryModels(_ context.Context, options control.InventoryModelListOptions) (control.InventoryModelPage, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	page := control.InventoryModelPage{SnapshotHorizon: options.SnapshotHorizon}
	for _, snapshot := range store.snapshots {
		if (options.Provider != "" && options.Provider != snapshot.Provider) || (options.EndpointID != "" && options.EndpointID != snapshot.EndpointID) {
			continue
		}
		info := control.InventorySnapshotInfo{ID: uuid.NewSHA1(uuid.NameSpaceOID, []byte(snapshot.EndpointID)), Provider: snapshot.Provider, EndpointID: snapshot.EndpointID, Source: snapshot.Source, ObservedAt: snapshot.ObservedAt, ExpiresAt: snapshot.ExpiresAt, Complete: snapshot.Complete}
		for _, model := range snapshot.Models {
			page.Models = append(page.Models, control.InventoryModelRecord{Snapshot: info, Model: model})
		}
	}
	sort.Slice(page.Models, func(i, j int) bool {
		left, right := page.Models[i], page.Models[j]
		if left.Snapshot.EndpointID != right.Snapshot.EndpointID {
			return left.Snapshot.EndpointID < right.Snapshot.EndpointID
		}
		return left.Model.ProviderModelID < right.Model.ProviderModelID
	})
	return page, nil
}

// countingLister is a provider.ModelLister whose fetch can be held open.
type countingLister struct {
	provider.Adapter
	calls   atomic.Int32
	release chan struct{}
	models  []provider.Model
}

func (lister *countingLister) ListModels(ctx context.Context, query provider.ModelListQuery) (provider.ModelListPage, error) {
	lister.calls.Add(1)
	if lister.release != nil {
		select {
		case <-lister.release:
		case <-ctx.Done():
			return provider.ModelListPage{}, ctx.Err()
		}
	}
	return provider.ModelListPage{Models: lister.models, Complete: true}, nil
}

var refreshTestNow = time.Date(2026, time.October, 7, 12, 0, 0, 0, time.UTC)

func refreshTarget(endpoint string, lister provider.ModelLister) ProviderRefreshTarget {
	return ProviderRefreshTarget{Provider: "openai", EndpointID: endpoint, EndpointFamily: "openai_responses", Region: "global", EndpointAccountHMAC: sha256.Sum256([]byte(endpoint)), Lister: lister}
}

func refreshTestService(t *testing.T, store *memoryInventoryStore, authorize control.AuthorizeFunc, options ProviderRefresherOptions) (*control.QueryService, *ProviderRefresher) {
	t.Helper()
	options.ConfigDigest = sha256.Sum256([]byte("snapshot"))
	options.Store = store
	options.Clock = func() time.Time { return refreshTestNow }
	refresher, err := NewProviderRefresher(options)
	if err != nil {
		t.Fatal(err)
	}
	codec := &control.CursorCodec{Key: []byte("query-test-key"), TTL: time.Hour, MaxPosition: 512}
	handler := &persistedQueryHandler{configDigest: options.ConfigDigest, inventory: store, provider: &fakePersistedProvider{}, refresh: refresher, cursor: codec, clock: func() time.Time { return refreshTestNow }}
	if authorize == nil {
		authorize = func(context.Context, control.Authorization) error { return nil }
	}
	return &control.QueryService{TypedHandler: handler, Authorize: authorize, Audit: func(context.Context, control.QueryAuditRecord) error { return nil }, CursorCodec: codec, Clock: func() time.Time { return refreshTestNow }}, refresher
}

func inventoryRefreshRequest(t *testing.T, kind llm.QueryKind, endpoint string, refresh time.Duration) llm.QueryRequestV1 {
	t.Helper()
	var filter control.QueryFilter
	var endpointID *control.EndpointID
	if endpoint != "" {
		value := control.EndpointID(endpoint)
		endpointID = &value
	}
	switch kind {
	case llm.QueryModelInventory:
		filter = control.ModelInventoryQuery{Endpoint: endpointID, RefreshIfOlderThan: refresh}
	case llm.QueryProviderStatus:
		filter = control.ProviderStatusQuery{Endpoint: endpointID, RefreshIfOlderThan: refresh}
	case llm.QueryCreditStatus:
		filter = control.CreditStatusQuery{Endpoint: endpointID, RefreshIfOlderThan: refresh}
	}
	request, err := control.EncodeQueryRequest(control.QueryRequest{OperationKey: "refresh-op", Scope: control.QueryScope{Tenant: "tenant", Project: "project", Actor: "actor"}, Kind: kind, Filter: filter})
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func seedInventory(t *testing.T, store *memoryInventoryStore, endpoint string, observed time.Time, models ...string) {
	t.Helper()
	snapshot := control.InventorySnapshot{ConfigDigest: sha256.Sum256([]byte("snapshot")), Provider: "openai", EndpointID: endpoint, EndpointAccountHMAC: sha256.Sum256([]byte(endpoint)), EndpointFamily: "openai_responses", Region: "global", Source: control.InventoryProviderAPI, ObservedAt: observed, ExpiresAt: observed.Add(time.Hour), Complete: true}
	for _, model := range models {
		snapshot.Models = append(snapshot.Models, control.Model{ProviderModelID: model, Lifecycle: control.LifecycleAvailable})
	}
	snapshot.InventoryDigest = control.InventoryDigest(snapshot.Models)
	if _, err := store.PersistInventorySnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
}

func inventoryModelIDs(t *testing.T, response llm.QueryResponseV1) []string {
	t.Helper()
	decoded, err := control.DecodeQueryResponse(response)
	if err != nil {
		t.Fatal(err)
	}
	result, ok := decoded.Result.(control.ModelInventoryResult)
	if !ok {
		t.Fatalf("result type %T", decoded.Result)
	}
	ids := make([]string, 0, len(result.Models))
	for _, row := range result.Models {
		ids = append(ids, string(row.ProviderModelID))
	}
	return ids
}

func TestProviderRefreshWritesFreshInventory(t *testing.T) {
	store := newMemoryInventoryStore()
	seedInventory(t, store, "primary", refreshTestNow.Add(-2*time.Hour), "gpt-old")
	lister := &countingLister{models: []provider.Model{{ProviderModelID: "gpt-new", Lifecycle: provider.ModelAvailable}}}
	service, _ := refreshTestService(t, store, nil, ProviderRefresherOptions{Targets: []ProviderRefreshTarget{refreshTarget("primary", lister)}})
	response, err := service.Execute(context.Background(), inventoryRefreshRequest(t, llm.QueryModelInventory, "primary", time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if lister.calls.Load() != 1 || response.Source != string(control.QuerySourcePersistedRefreshed) || response.Freshness != string(control.QueryFreshCurrent) {
		t.Fatalf("calls=%d source=%s freshness=%s", lister.calls.Load(), response.Source, response.Freshness)
	}
	if ids := inventoryModelIDs(t, response); len(ids) != 1 || ids[0] != "gpt-new" {
		t.Fatalf("models = %v, want refreshed listing", ids)
	}
	// A second query inside the refresh window reuses the persisted listing.
	response, err = service.Execute(context.Background(), inventoryRefreshRequest(t, llm.QueryModelInventory, "primary", time.Minute))
	if err != nil || lister.calls.Load() != 1 || response.Source != string(control.QuerySourcePersisted) {
		t.Fatalf("fresh listing was refetched: calls=%d source=%s err=%v", lister.calls.Load(), response.Source, err)
	}
}

func TestProviderRefreshCollapsesConcurrentRefreshes(t *testing.T) {
	const callers = 8
	store := newMemoryInventoryStore()
	lister := &countingLister{release: make(chan struct{}), models: []provider.Model{{ProviderModelID: "gpt-new", Lifecycle: provider.ModelAvailable}}}
	target := refreshTarget("primary", lister)
	service, refresher := refreshTestService(t, store, nil, ProviderRefresherOptions{Targets: []ProviderRefreshTarget{target}})
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := service.Execute(context.Background(), inventoryRefreshRequest(t, llm.QueryModelInventory, "primary", time.Minute))
			errs <- err
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for refresher.flights.waiting(refreshTargetKey(target)) != callers-1 {
		if time.Now().After(deadline) {
			t.Fatalf("only %d callers joined the in-flight refresh", refresher.flights.waiting(refreshTargetKey(target)))
		}
		time.Sleep(time.Millisecond)
	}
	close(lister.release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if calls := lister.calls.Load(); calls != 1 {
		t.Fatalf("provider fetched %d times, want 1", calls)
	}
	if store.persists != 1 {
		t.Fatalf("inventory persisted %d times, want 1", store.persists)
	}
}

func TestProviderRefreshTimeoutKeepsExistingStateAndReportsStale(t *testing.T) {
	store := newMemoryInventoryStore()
	observed := refreshTestNow.Add(-2 * time.Minute)
	seedInventory(t, store, "primary", observed, "gpt-existing")
	before, _ := store.GetInventorySnapshot(context.Background(), [32]byte{}, "openai", "primary", [32]byte{})
	lister := &countingLister{release: make(chan struct{})} // never released
	service, _ := refreshTestService(t, store, nil, ProviderRefresherOptions{Targets: []ProviderRefreshTarget{refreshTarget("primary", lister)}, Timeout: 20 * time.Millisecond})
	response, err := service.Execute(context.Background(), inventoryRefreshRequest(t, llm.QueryModelInventory, "primary", time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if response.Freshness != string(control.QueryFreshStale) || response.Source != string(control.QuerySourcePersisted) {
		t.Fatalf("timeout reported source=%s freshness=%s, want persisted/stale", response.Source, response.Freshness)
	}
	if ids := inventoryModelIDs(t, response); len(ids) != 1 || ids[0] != "gpt-existing" {
		t.Fatalf("models = %v, want the preserved listing", ids)
	}
	after, _ := store.GetInventorySnapshot(context.Background(), [32]byte{}, "openai", "primary", [32]byte{})
	if !after.ObservedAt.Equal(before.ObservedAt) || after.InventoryDigest != before.InventoryDigest || store.persists != 1 {
		t.Fatalf("timed-out refresh changed persisted state: before=%+v after=%+v", before, after)
	}
}

func TestProviderRefreshUnsupportedProviderFailsBeforeFetch(t *testing.T) {
	store := newMemoryInventoryStore()
	lister := &countingLister{}
	service, _ := refreshTestService(t, store, nil, ProviderRefresherOptions{Targets: []ProviderRefreshTarget{refreshTarget("primary", lister), refreshTarget("compatible", nil)}})
	cases := []struct {
		kind     llm.QueryKind
		endpoint string
	}{
		{llm.QueryModelInventory, "compatible"},
		{llm.QueryModelInventory, ""}, // scope includes the unsupported endpoint
		{llm.QueryModelInventory, "missing"},
		{llm.QueryProviderStatus, "primary"},
		{llm.QueryCreditStatus, "primary"},
	}
	for _, test := range cases {
		_, err := service.Execute(context.Background(), inventoryRefreshRequest(t, test.kind, test.endpoint, time.Minute))
		var providerErr *provider.Error
		if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeUnsupportedCapability {
			t.Fatalf("%s/%q: err = %v, want unsupported capability", test.kind, test.endpoint, err)
		}
	}
	if lister.calls.Load() != 0 || store.persists != 0 {
		t.Fatalf("unsupported refresh fetched or wrote state: calls=%d persists=%d", lister.calls.Load(), store.persists)
	}
}

func TestProviderRefreshCrossTenantDeniedBeforeFetch(t *testing.T) {
	store := newMemoryInventoryStore()
	lister := &countingLister{}
	deny := func(_ context.Context, request control.Authorization) error {
		if request.Tenant != "other-tenant" {
			return control.ErrQueryAuthorization
		}
		return nil
	}
	service, _ := refreshTestService(t, store, deny, ProviderRefresherOptions{Targets: []ProviderRefreshTarget{refreshTarget("primary", lister)}})
	_, err := service.Execute(context.Background(), inventoryRefreshRequest(t, llm.QueryModelInventory, "primary", time.Minute))
	if !errors.Is(err, control.ErrQueryAuthorization) {
		t.Fatalf("err = %v, want authorization denial", err)
	}
	if lister.calls.Load() != 0 || store.persists != 0 {
		t.Fatalf("denied query fetched or wrote state: calls=%d persists=%d", lister.calls.Load(), store.persists)
	}
}

func TestProviderRefreshAbsentLeavesQueriesUnchanged(t *testing.T) {
	store := newMemoryInventoryStore()
	seedInventory(t, store, "primary", refreshTestNow.Add(-2*time.Hour), "gpt-old")
	lister := &countingLister{models: []provider.Model{{ProviderModelID: "gpt-new", Lifecycle: provider.ModelAvailable}}}
	service, _ := refreshTestService(t, store, nil, ProviderRefresherOptions{Targets: []ProviderRefreshTarget{refreshTarget("primary", lister)}})
	response, err := service.Execute(context.Background(), inventoryRefreshRequest(t, llm.QueryModelInventory, "primary", 0))
	if err != nil {
		t.Fatal(err)
	}
	if lister.calls.Load() != 0 || response.Source != string(control.QuerySourcePersisted) || response.Freshness != string(control.QueryFreshStale) {
		t.Fatalf("query without refresh: calls=%d source=%s freshness=%s", lister.calls.Load(), response.Source, response.Freshness)
	}
	if ids := inventoryModelIDs(t, response); len(ids) != 1 || ids[0] != "gpt-old" {
		t.Fatalf("models = %v, want persisted listing", ids)
	}

	// With refresh disabled (no refresher composed), a refresh request is
	// still the typed unsupported error it was before this change.
	service.TypedHandler.(*persistedQueryHandler).refresh = nil
	for _, kind := range []llm.QueryKind{llm.QueryModelInventory, llm.QueryProviderStatus, llm.QueryCreditStatus} {
		_, err := service.Execute(context.Background(), inventoryRefreshRequest(t, kind, "primary", time.Minute))
		var providerErr *provider.Error
		if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeUnsupportedCapability {
			t.Fatalf("%s: err = %v, want unsupported capability", kind, err)
		}
	}
	if lister.calls.Load() != 0 {
		t.Fatalf("disabled refresh fetched")
	}
}

func TestProviderRefreshBoundsEndpointsPerQuery(t *testing.T) {
	store := newMemoryInventoryStore()
	listers := []*countingLister{{}, {}, {}}
	targets := []ProviderRefreshTarget{refreshTarget("a", listers[0]), refreshTarget("b", listers[1]), refreshTarget("c", listers[2])}
	for _, lister := range listers {
		lister.models = []provider.Model{{ProviderModelID: "gpt", Lifecycle: provider.ModelAvailable}}
	}
	service, _ := refreshTestService(t, store, nil, ProviderRefresherOptions{Targets: targets, MaxEndpoints: 2})
	response, err := service.Execute(context.Background(), inventoryRefreshRequest(t, llm.QueryModelInventory, "", time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if listers[0].calls.Load() != 1 || listers[1].calls.Load() != 1 || listers[2].calls.Load() != 0 {
		t.Fatalf("fetch counts = %d,%d,%d, want 1,1,0", listers[0].calls.Load(), listers[1].calls.Load(), listers[2].calls.Load())
	}
	if response.Freshness != string(control.QueryFreshStale) || response.Source != string(control.QuerySourcePersistedRefreshed) {
		t.Fatalf("bounded refresh reported source=%s freshness=%s, want refreshed/stale", response.Source, response.Freshness)
	}
}

func TestProviderRefreshTargetsRequireListerAndUnambiguousIdentity(t *testing.T) {
	account := sha256.Sum256([]byte("account"))
	other := sha256.Sum256([]byte("other"))
	route := func(endpoint, region string, hmac [32]byte) routing.Route {
		return routing.Route{EndpointID: endpoint, Provider: "openai", Family: "openai_responses", Region: region, EndpointAccountHMAC: hmac}
	}
	catalog := routing.Catalog{Models: map[string]routing.Model{
		"m1": {Routes: []routing.Route{route("lister", "global", account), route("plain", "global", account), route("ambiguous", "global", account)}},
		"m2": {Routes: []routing.Route{route("lister", "global", account), route("ambiguous", "eu", other)}},
	}}
	lister := &countingLister{}
	adapters := map[string]provider.Adapter{"lister": lister, "plain": struct{ provider.Adapter }{}, "ambiguous": lister}
	supported := map[string]bool{}
	count := map[string]int{}
	for _, target := range providerRefreshTargets(catalog, adapters) {
		count[target.EndpointID]++
		supported[target.EndpointID] = supported[target.EndpointID] || target.Lister != nil
	}
	if count["lister"] != 1 || !supported["lister"] || count["plain"] != 1 || supported["plain"] || count["ambiguous"] != 2 || supported["ambiguous"] {
		t.Fatalf("targets: count=%v supported=%v", count, supported)
	}
}
