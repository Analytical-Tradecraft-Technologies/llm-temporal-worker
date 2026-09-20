package postgres

import (
	"context"
	"crypto/sha256"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mfow/llm-temporal-worker/golang/control"
)

func providerControlIntegrationPool(t *testing.T, configure ...func(*PoolOptions)) (context.Context, Namespace, *pgxpool.Pool, func()) {
	t.Helper()
	addr := os.Getenv("LLMTW_POSTGRES_ADDR")
	if addr == "" {
		t.Skip("LLMTW_POSTGRES_ADDR is not configured; set it for PostgreSQL integration tests")
	}
	namespace, err := NewNamespace(valueOr("LLMTW_POSTGRES_DATABASE", "llm_worker"), valueOr("LLMTW_POSTGRES_SCHEMA", "llm_worker"), os.Getenv("LLMTW_POSTGRES_TABLE_PREFIX"))
	if err != nil {
		t.Fatal(err)
	}
	options := PoolOptions{
		Namespace:        namespace,
		Addresses:        []string{addr},
		Username:         valueOr("LLMTW_POSTGRES_USER", "llmtw"),
		Password:         valueOr("LLMTW_POSTGRES_PASSWORD", "llmtw"),
		MaxConnections:   4,
		MinConnections:   1,
		DialTimeout:      5 * time.Second,
		StatementTimeout: 5 * time.Second,
		LockTimeout:      time.Second,
		IdleTxTimeout:    5 * time.Second,
	}
	for _, apply := range configure {
		apply(&options)
	}
	pool, err := NewPool(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := Install(ctx, pool, namespace); err != nil {
		cancel()
		pool.Close()
		t.Fatalf("install schema: %v", err)
	}
	return ctx, namespace, pool, func() {
		cancel()
		pool.Close()
	}
}

func TestProviderControlPersistenceIntegration(t *testing.T) {
	ctx, namespace, pool, cleanup := providerControlIntegrationPool(t)
	defer cleanup()
	var err error

	configDigest := sha256.Sum256([]byte("provider-control-integration-config-" + time.Now().UTC().Format(time.RFC3339Nano)))
	configs, err := namespace.Render("configuration_snapshots")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO "+configs+" (config_digest, config_version, source_digest, sanitized_config) VALUES ($1,$2,$1,'{}'::jsonb)", configDigest[:], "provider-control-integration"); err != nil {
		t.Fatal(err)
	}

	observed := time.Now().UTC().Truncate(time.Microsecond)
	observation := control.StatusObservation{
		ConfigDigest:        configDigest,
		RouteID:             "provider-control-route",
		EndpointID:          "provider-control-endpoint",
		EndpointAccountHMAC: sha256.Sum256([]byte("provider-control-account")),
		Provider:            "provider-control-provider",
		EndpointFamily:      "chat",
		ObservedAt:          observed,
		Source:              control.SourceInference,
		Availability:        control.AvailabilityAvailable,
		Credit:              control.CreditOK,
		Billing:             control.BillingOK,
		EvidenceDigest:      sha256.Sum256([]byte("provider-control-evidence")),
		ConfigEpoch:         "provider-control-epoch-1",
		ExpiresAt:           observed.Add(time.Hour),
	}
	event, err := control.NewStatusEvent(observation)
	if err != nil {
		t.Fatal(err)
	}
	statusRepository := DefaultProviderStatusRepository(pool, namespace)
	applied, err := statusRepository.PersistStatusEvent(ctx, event)
	if err != nil || !applied {
		t.Fatalf("persist initial status: applied=%v err=%v", applied, err)
	}
	if duplicate, err := statusRepository.PersistStatusEvent(ctx, event); err != nil || duplicate {
		t.Fatalf("persist duplicate status: applied=%v err=%v", duplicate, err)
	}
	status, err := statusRepository.GetRouteStatus(ctx, configDigest, observation.RouteID)
	if err != nil {
		t.Fatal(err)
	}
	if status.LastEventDigest != event.EventDigest || status.Availability != control.AvailabilityAvailable || status.ConsecutiveDefiniteFailures != 0 {
		t.Fatalf("initial route projection = %#v", status)
	}

	inventory := providerControlSnapshot()
	inventory.ConfigDigest = configDigest
	inventory.EndpointID = observation.EndpointID
	inventory.EndpointAccountHMAC = observation.EndpointAccountHMAC
	inventory.ObservedAt = observed.Add(time.Minute)
	inventory.ExpiresAt = inventory.ObservedAt.Add(time.Hour)
	inventoryRepository := DefaultInventoryRepository(pool, namespace, Keyring{Active: "inventory-v1", Keys: map[string][]byte{"inventory-v1": []byte("01234567890123456789012345678901")}})
	record, err := inventoryRepository.PersistSnapshot(ctx, inventory)
	if err != nil {
		t.Fatalf("persist inventory: %v", err)
	}
	if cursor, err := inventoryRepository.OpenCursor(record); err != nil || cursor != inventory.NextCursor {
		t.Fatalf("open persisted inventory cursor = %q, %v", cursor, err)
	}
	reloaded, err := inventoryRepository.GetSnapshot(ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Snapshot.InventoryDigest != inventory.InventoryDigest || len(reloaded.Snapshot.Models) != 1 {
		t.Fatalf("reloaded inventory = %#v", reloaded.Snapshot)
	}
	if cursor, err := inventoryRepository.OpenCursor(reloaded); err != nil || cursor != inventory.NextCursor {
		t.Fatalf("open reloaded inventory cursor = %q, %v", cursor, err)
	}
}

func TestProviderControlFirstProjectionConcurrencyIntegration(t *testing.T) {
	ctx, namespace, pool, cleanup := providerControlIntegrationPool(t, func(options *PoolOptions) {
		// This test deliberately parks open transactions at barriers. Let its
		// context bound the wait instead of racing short server-side timeouts.
		options.StatementTimeout = 30 * time.Second
		options.LockTimeout = 30 * time.Second
		options.IdleTxTimeout = 30 * time.Second
	})
	defer cleanup()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	configDigest := sha256.Sum256([]byte("provider-control-first-projection-race-" + time.Now().UTC().Format(time.RFC3339Nano)))
	configs, err := namespace.Render("configuration_snapshots")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO "+configs+" (config_digest, config_version, source_digest, sanitized_config) VALUES ($1,$2,$1,'{}'::jsonb)", configDigest[:], "provider-control-race"); err != nil {
		t.Fatal(err)
	}
	observed := time.Now().UTC().Truncate(time.Microsecond)
	base := control.StatusObservation{
		ConfigDigest:        configDigest,
		RouteID:             "provider-control-first-projection-route",
		EndpointID:          "provider-control-first-projection-endpoint",
		EndpointAccountHMAC: sha256.Sum256([]byte("provider-control-first-projection-account")),
		Provider:            "provider-control-first-projection-provider",
		EndpointFamily:      "chat",
		ObservedAt:          observed,
		Source:              control.SourceInference,
		Availability:        control.AvailabilityAvailable,
		Credit:              control.CreditOK,
		Billing:             control.BillingOK,
		ConfigEpoch:         "epoch-a",
		ExpiresAt:           observed.Add(time.Hour),
	}
	firstObservation := base
	firstObservation.EvidenceDigest = sha256.Sum256([]byte("first-projection-first"))
	secondObservation := base
	secondObservation.ObservedAt = observed.Add(time.Second)
	secondObservation.EvidenceDigest = sha256.Sum256([]byte("first-projection-second"))
	first, err := control.NewStatusEvent(firstObservation)
	if err != nil {
		t.Fatal(err)
	}
	second, err := control.NewStatusEvent(secondObservation)
	if err != nil {
		t.Fatal(err)
	}
	repository := DefaultProviderStatusRepository(pool, namespace)
	ready := make(chan struct{}, 2)
	allowFirst := make(chan struct{})
	firstRead := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondRead := make(chan struct{})
	releaseSecond := make(chan struct{})
	hooks := providerStatusTestHooks{
		beforeRouteLock: func(event control.StatusEvent) {
			ready <- struct{}{}
			if event.EventDigest == first.EventDigest {
				select {
				case <-allowFirst:
				case <-ctx.Done():
				}
				return
			}
			select {
			case <-firstRead:
			case <-ctx.Done():
			}
		},
		afterProjectionRead: func(event control.StatusEvent, _ bool) {
			if event.EventDigest == first.EventDigest {
				close(firstRead)
				select {
				case <-releaseFirst:
				case <-ctx.Done():
				}
				return
			}
			close(secondRead)
			select {
			case <-releaseSecond:
			case <-ctx.Done():
			}
		},
	}
	errs := make(chan error, 2)
	var group sync.WaitGroup
	defer func() {
		cancel()
		group.Wait()
	}()
	wait := func(stage string, signal <-chan struct{}) {
		t.Helper()
		for {
			select {
			case <-signal:
				return
			case err := <-errs:
				if err != nil {
					t.Fatalf("%s: persist status event: %v", stage, err)
				}
			case <-ctx.Done():
				t.Fatalf("%s: %v", stage, ctx.Err())
			}
		}
	}
	group.Add(1)
	go func() {
		defer group.Done()
		_, err := repository.persistStatusEvent(ctx, first, hooks)
		errs <- err
	}()
	group.Add(1)
	go func() {
		defer group.Done()
		_, err := repository.persistStatusEvent(ctx, second, hooks)
		errs <- err
	}()
	wait("first worker ready", ready)
	wait("second worker ready", ready)
	close(allowFirst)
	wait("first projection read", firstRead)
	// Observe the second transaction waiting on the actual advisory lock.
	// Sleeping cannot prove it reached the lock, and can overshoot lock_timeout
	// when the runner is descheduled.
	lockKey := uint64(providerRouteAdvisoryLockKey(namespace, configDigest, base.RouteID))
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted
			AND classid::bigint = $1 AND objid::bigint = $2 AND objsubid = 1
		)`, int64(lockKey>>32), int64(lockKey&0xffffffff)).Scan(&waiting)
		if err != nil {
			t.Fatalf("observe second transaction waiting on route lock: %v", err)
		}
		if waiting {
			break
		}
		select {
		case <-secondRead:
			t.Fatal("second projection was read while first transaction held the route barrier")
		case err := <-errs:
			t.Fatalf("transaction exited before route lock contention was observed: %v", err)
		case <-ctx.Done():
			t.Fatalf("wait for route lock contention: %v", ctx.Err())
		case <-ticker.C:
		}
	}
	close(releaseFirst)
	wait("second projection read", secondRead)
	close(releaseSecond)
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	routes, err := namespace.Render("provider_route_status")
	if err != nil {
		t.Fatal(err)
	}
	var projectionVersion int64
	if err := pool.QueryRow(ctx, "SELECT projection_version FROM "+routes+" WHERE config_digest = $1 AND route_id = $2", configDigest[:], base.RouteID).Scan(&projectionVersion); err != nil {
		t.Fatal(err)
	}
	if projectionVersion != 2 {
		t.Fatalf("first projection version = %d, want 2 after two concurrent first events", projectionVersion)
	}
	events, err := namespace.Render("provider_status_events")
	if err != nil {
		t.Fatal(err)
	}
	var eventCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+events+" WHERE config_digest = $1 AND route_id = $2", configDigest[:], base.RouteID).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 2 {
		t.Fatalf("status event count = %d, want 2", eventCount)
	}
}
