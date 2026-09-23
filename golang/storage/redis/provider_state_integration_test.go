//go:build integration

package redis

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/control"
	redisclient "github.com/redis/go-redis/v9"
)

func liveProviderStore(t *testing.T) (*ProviderStateStore, *redisclient.Client, time.Time) {
	t.Helper()
	client := openLiveRedis(t)
	keys := liveKeyOptions("provider-state")
	cleanupLivePrefix(t, client, keys.Prefix)
	now := time.Now().UTC()
	store, err := NewProviderStateStore(ProviderStateOptions{Client: client, Keys: keys, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	return store, client, now
}

func TestLiveRedisProviderStatusConcurrency(t *testing.T) {
	store, client, now := liveProviderStore(t)
	ctx := context.Background()
	// Distinct clients represent independently running workers sharing one keyspace.
	secondClient := redisclient.NewClient(client.Options())
	defer secondClient.Close()
	second, err := NewProviderStateStore(ProviderStateOptions{Client: secondClient, Keys: KeyOptions{Prefix: store.space.prefix, HashTag: store.space.tag, KeySecret: store.space.secret}})
	if err != nil {
		t.Fatal(err)
	}
	at := now.Add(-time.Minute)
	first := providerTestEvent(t, at, "route", nil)
	var applied atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			worker := store
			if i%2 == 0 {
				worker = second
			}
			ok, err := worker.PersistStatusEvent(ctx, first)
			if err != nil {
				t.Error(err)
			}
			if ok {
				applied.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if applied.Load() != 1 {
		t.Fatalf("duplicate applied %d times", applied.Load())
	}
	// Deliberately submit observations out of order. Latest wins regardless of
	// which worker acquires the CAS first, including the missing-field race.
	events := make([]control.StatusEvent, 64)
	for i := range events {
		events[i] = providerTestEvent(t, at.Add(time.Duration(i)*time.Millisecond), "new-route", nil)
	}
	for i := range events {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := second.PersistStatusEvent(ctx, events[i])
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	status, err := store.GetRouteStatus(ctx, first.ConfigDigest, "new-route")
	if err != nil || status.LastEventDigest != events[len(events)-1].EventDigest {
		t.Fatalf("latest event lost: %#v %v", status, err)
	}
	if ttl := client.TTL(ctx, store.key("status", first.ConfigDigest)).Val(); ttl != -1 {
		t.Fatalf("incident state has TTL %v", ttl)
	}
	// A new process can read the state; no in-process cache is the authority.
	status, err = second.GetRouteStatus(ctx, first.ConfigDigest, "route")
	if err != nil || status.LastEventDigest != first.EventDigest {
		t.Fatalf("second worker: %#v %v", status, err)
	}
}

func TestLiveRedisProviderStickyEvidenceAndIsolation(t *testing.T) {
	store, client, now := liveProviderStore(t)
	ctx := context.Background()
	at := now.Add(-time.Hour)
	incident := providerTestEvent(t, at, "route", func(o *control.StatusObservation) {
		o.Credit = control.CreditExhausted
		o.Billing = control.BillingIssue
		o.ProviderCode = "insufficient_quota"
	})
	if err := store.RecordProviderStatus(ctx, incident.StatusObservation); err != nil {
		t.Fatal(err)
	}
	success := providerTestEvent(t, at.Add(time.Minute), "route", nil)
	if err := store.RecordProviderStatus(ctx, success.StatusObservation); err != nil {
		t.Fatal(err)
	}
	page, err := store.ListCreditStatuses(ctx, control.CreditStatusListOptions{ConfigDigest: incident.ConfigDigest})
	if err != nil || len(page.Endpoints) != 1 {
		t.Fatalf("credit page: %#v %v", page, err)
	}
	if credit := page.Endpoints[0]; credit.Credit != control.CreditExhausted || credit.Billing != control.BillingIssue || credit.SafeEvidenceCode != "insufficient_quota" || !credit.ConfirmedAt.Equal(at) || !now.After(credit.StaleAfter) {
		t.Fatalf("sticky credit: %#v", credit)
	}
	otherDigest := sha256.Sum256([]byte("different-config"))
	if _, err := store.GetRouteStatus(ctx, otherDigest, "route"); !errors.Is(err, control.ErrProviderStatusNotFound) {
		t.Fatalf("config leak: %v", err)
	}
	keys := liveKeyOptions("provider-isolated")
	cleanupLivePrefix(t, client, keys.Prefix)
	other, err := NewProviderStateStore(ProviderStateOptions{Client: client, Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.GetRouteStatus(ctx, incident.ConfigDigest, "route"); !errors.Is(err, control.ErrProviderStatusNotFound) {
		t.Fatalf("prefix leak: %v", err)
	}
	// Changed epochs reset incidents; clearing one config cannot change another.
	reset := providerTestEvent(t, now.Add(-time.Minute), "route", func(o *control.StatusObservation) { o.ConfigEpoch = "epoch-2" })
	if ok, err := store.PersistStatusEvent(ctx, reset); err != nil || !ok {
		t.Fatalf("epoch reset: %v %v", ok, err)
	}
	status, err := store.GetRouteStatus(ctx, reset.ConfigDigest, "route")
	if err != nil || status.Credit != control.CreditOK || status.Billing != control.BillingOK {
		t.Fatalf("epoch projection: %#v %v", status, err)
	}
}

func TestLiveRedisProviderInventoryConcurrencyEncryptionAndStaleness(t *testing.T) {
	store, client, now := liveProviderStore(t)
	ctx := context.Background()
	snapshot := providerTestInventory(now.Add(-time.Hour))
	snapshot.Complete = false
	snapshot.NextCursor = "private-provider-cursor"
	var wg sync.WaitGroup
	var applied atomic.Int32
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := store.PersistInventorySnapshot(ctx, snapshot)
			if err != nil {
				t.Error(err)
			}
			if ok {
				applied.Add(1)
			}
		}()
	}
	wg.Wait()
	if applied.Load() != 1 {
		t.Fatalf("duplicate inventory applied %d times", applied.Load())
	}
	equivalent := snapshot
	equivalent.ObservedAt = equivalent.ObservedAt.In(time.FixedZone("other", 3600))
	equivalent.ExpiresAt = equivalent.ExpiresAt.In(time.FixedZone("other", 3600))
	if changed, err := store.PersistInventorySnapshot(ctx, equivalent); err != nil || changed {
		t.Fatalf("timezone replay changed snapshot: %v %v", changed, err)
	}
	raw, err := client.HGetAll(ctx, store.key("inventory", snapshot.ConfigDigest)).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range raw {
		if strings.Contains(value, snapshot.NextCursor) {
			t.Fatal("provider cursor stored in plaintext")
		}
	}
	loaded, err := store.GetInventorySnapshot(ctx, snapshot.ConfigDigest, snapshot.Provider, snapshot.EndpointID, snapshot.EndpointAccountHMAC)
	if err != nil || loaded.NextCursor != snapshot.NextCursor || loaded.ProvenanceAt(now) != control.ProvenanceStale {
		t.Fatalf("stale last-known listing: %#v %v", loaded, err)
	}
	// Same timestamp with different content is rejected, never silently replaced.
	conflict := snapshot
	conflict.ExpiresAt = conflict.ExpiresAt.Add(time.Hour)
	if _, err := store.PersistInventorySnapshot(ctx, conflict); err == nil {
		t.Fatal("conflicting observation accepted")
	}
	stale := snapshot
	stale.ObservedAt = stale.ObservedAt.Add(-time.Minute)
	if ok, err := store.PersistInventorySnapshot(ctx, stale); err != nil || ok {
		t.Fatalf("stale overwrite: %v %v", ok, err)
	}
	newer := snapshot
	newer.ObservedAt = now.Add(-time.Second)
	newer.ExpiresAt = now.Add(time.Minute)
	newer.Complete = true
	newer.NextCursor = ""
	if ok, err := store.PersistInventorySnapshot(ctx, newer); err != nil || !ok {
		t.Fatalf("refresh: %v %v", ok, err)
	}
	if _, err := store.GetInventorySnapshot(ctx, snapshot.ConfigDigest, snapshot.Provider, snapshot.EndpointID, sha256.Sum256([]byte("different-account"))); !errors.Is(err, control.ErrInventorySnapshotNotFound) {
		t.Fatalf("account leak: %v", err)
	}
	// Tampered encrypted cursor is not exposed as an empty/healthy inventory.
	field := store.space.digest("provider-inventory", inventoryIdentity(snapshot.Provider, snapshot.EndpointID, snapshot.EndpointAccountHMAC))
	var corrupted inventoryCacheRecord
	for k, value := range raw {
		if k != "__bytes" {
			if err := json.Unmarshal([]byte(value), &corrupted); err != nil {
				t.Fatal(err)
			}
		}
	}
	corrupted.Cursor = "broken"
	data, _ := json.Marshal(corrupted)
	if err := client.HSet(ctx, store.key("inventory", snapshot.ConfigDigest), field, data).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetInventorySnapshot(ctx, snapshot.ConfigDigest, snapshot.Provider, snapshot.EndpointID, snapshot.EndpointAccountHMAC); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("tampered cursor accepted: %v", err)
	}
}

func TestLiveRedisProviderPaginationSurvivesUpdatesAndExpires(t *testing.T) {
	store, client, now := liveProviderStore(t)
	ctx := context.Background()
	snapshot := providerTestInventory(now.Add(-time.Minute))
	if _, err := store.PersistInventorySnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	options := control.InventoryModelListOptions{ConfigDigest: snapshot.ConfigDigest, Limit: 1}
	first, err := store.ListInventoryModels(ctx, options)
	if err != nil || first.Next == nil || first.Models[0].Model.ProviderModelID != "model-a" {
		t.Fatalf("first: %#v %v", first, err)
	}
	newer := snapshot
	newer.ObservedAt = now
	newer.ExpiresAt = now.Add(time.Hour)
	newer.Models = []control.Model{{ProviderModelID: "replacement", Lifecycle: control.LifecycleAvailable}}
	newer.InventoryDigest = control.InventoryDigest(newer.Models)
	if _, err := store.PersistInventorySnapshot(ctx, newer); err != nil {
		t.Fatal(err)
	}
	// Signed cursor encodes seconds, so reproduce that round trip here.
	options.SnapshotHorizon = time.Unix(first.SnapshotHorizon.Unix(), 0).UTC()
	options.After = *first.Next
	second, err := store.ListInventoryModels(ctx, options)
	if err != nil || len(second.Models) != 1 || second.Models[0].Model.ProviderModelID != "model-b" || second.Next != nil {
		t.Fatalf("second changed after refresh: %#v %v", second, err)
	}
	for _, route := range []string{"a", "b"} {
		event := providerTestEvent(t, now.Add(-time.Minute), route, nil)
		if _, err := store.PersistStatusEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	statusOptions := control.ProviderStatusListOptions{ConfigDigest: snapshot.ConfigDigest, IncludeHealthy: true, SnapshotHorizon: now, Limit: 1}
	statusFirst, err := store.ListRouteStatuses(ctx, statusOptions)
	if err != nil || statusFirst.NextRouteID != "a" {
		t.Fatalf("status first: %#v %v", statusFirst, err)
	}
	event := providerTestEvent(t, now.Add(time.Second), "b", func(o *control.StatusObservation) { o.Availability = control.AvailabilityUnavailable })
	if _, err := store.PersistStatusEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	statusOptions.AfterRouteID = "a"
	statusOptions.SnapshotHorizon = time.Unix(now.Unix(), 0)
	statusSecond, err := store.ListRouteStatuses(ctx, statusOptions)
	if err != nil || len(statusSecond.Routes) != 1 || statusSecond.Routes[0].Availability != control.AvailabilityAvailable {
		t.Fatalf("status membership changed: %#v %v", statusSecond, err)
	}
	// Force expiry on only this test namespace's query views, never shared data.
	viewKeys, err := client.Keys(ctx, store.space.admissionPrefix()+"provider-view:*").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(viewKeys) != 2 {
		t.Fatalf("view keys: %v", viewKeys)
	}
	for _, key := range viewKeys {
		if ttl := client.TTL(ctx, key).Val(); ttl <= 0 || ttl > providerViewTTL {
			t.Fatalf("view TTL: %v", ttl)
		}
		if err := client.Expire(ctx, key, -time.Second).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.ListInventoryModels(ctx, options); !errors.Is(err, control.ErrProviderViewExpired) {
		t.Fatalf("expired inventory view: %v", err)
	}
	if _, err := store.ListRouteStatuses(ctx, statusOptions); !errors.Is(err, control.ErrProviderViewExpired) {
		t.Fatalf("expired status view: %v", err)
	}
	if _, err := store.GetRouteStatus(ctx, snapshot.ConfigDigest, "b"); err != nil {
		t.Fatalf("view expiry removed current state: %v", err)
	}
}

func TestLiveRedisProviderFilteringEndpointSelectionAndFailures(t *testing.T) {
	store, client, now := liveProviderStore(t)
	ctx := context.Background()
	digest := sha256.Sum256([]byte("provider-config"))
	for i := 0; i < 4; i++ {
		event := providerTestEvent(t, now.Add(time.Duration(i-10)*time.Second), fmt.Sprintf("route-%d", i), func(o *control.StatusObservation) {
			o.EndpointID = fmt.Sprintf("endpoint-%d", i/2)
			if i == 2 {
				o.Credit = control.CreditExhausted
				o.ProviderCode = "insufficient_quota"
			}
		})
		if _, err := store.PersistStatusEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	// Latest route per endpoint wins before healthy filtering, matching the
	// existing control query contract (an older route incident is not resurrected).
	credit, err := store.ListCreditStatuses(ctx, control.CreditStatusListOptions{ConfigDigest: digest})
	if err != nil || len(credit.Endpoints) != 0 {
		t.Fatalf("endpoint selection: %#v %v", credit, err)
	}
	credit, err = store.ListCreditStatuses(ctx, control.CreditStatusListOptions{ConfigDigest: digest, IncludeOK: true, Limit: 1, SnapshotHorizon: now})
	if err != nil || len(credit.Endpoints) != 1 || credit.NextEndpointKey == "" {
		t.Fatalf("credit page: %#v %v", credit, err)
	}
	next, err := store.ListCreditStatuses(ctx, control.CreditStatusListOptions{ConfigDigest: digest, IncludeOK: true, Limit: 1, SnapshotHorizon: now, AfterEndpointKey: credit.NextEndpointKey})
	if err != nil || len(next.Endpoints) != 1 || next.Endpoints[0].EndpointID != "endpoint-1" {
		t.Fatalf("credit second: %#v %v", next, err)
	}
	page, err := store.ListRouteStatuses(ctx, control.ProviderStatusListOptions{ConfigDigest: digest, EndpointID: "endpoint-1"})
	if err != nil || len(page.Routes) != 1 || page.Routes[0].RouteID != "route-2" {
		t.Fatalf("status filters: %#v %v", page, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.GetRouteStatus(canceled, digest, "route-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
	event := providerTestEvent(t, now, "capacity-route", nil)
	key := store.key("status", digest)
	if err := client.HSet(ctx, key, "__bytes", 32<<20).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PersistStatusEvent(ctx, event); err == nil {
		t.Fatal("capacity overflow accepted")
	}
	if _, err := store.GetRouteStatus(ctx, digest, "capacity-route"); !errors.Is(err, control.ErrProviderStatusNotFound) {
		t.Fatalf("partial write on capacity failure: %v", err)
	}
	if err := client.Del(ctx, key).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, key, "wrong-type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PersistStatusEvent(ctx, event); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("wrong type did not fail closed: %v", err)
	}
}
