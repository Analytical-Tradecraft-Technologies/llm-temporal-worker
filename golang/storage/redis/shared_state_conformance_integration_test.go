//go:build integration

package redis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/control"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/conformance"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
	redisclient "github.com/redis/go-redis/v9"
)

var liveConformanceSequence atomic.Uint64

func TestLiveRedisStoreFactoryConformance(t *testing.T) {
	client := openLiveRedis(t)
	conformance.Run(t, liveRedisStoreFactory(client))
}

// TestLiveRedisTwoPrefixConformanceAndIsolation exercises the complete public
// shared-state contract twice against one Redis server. The stores deliberately
// share a client (and therefore a daemon) but receive different key namespaces;
// this catches accidental process-local state and prefix omissions that a
// single-prefix conformance run cannot observe.
func TestLiveRedisTwoPrefixConformanceAndIsolation(t *testing.T) {
	client := openLiveRedis(t)
	left := liveKeyOptions("prefix-a")
	right := liveKeyOptions("prefix-b")
	for _, test := range []struct {
		name string
		keys KeyOptions
	}{
		{name: "left", keys: left},
		{name: "right", keys: right},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			conformance.Run(t, liveRedisStoreFactoryWithKeys(client, AdmissionModeFunction, test.keys))
		})
	}

	// The full suite above proves each namespace independently. Keep one
	// cross-namespace assertion here as a regression guard for replay,
	// continuation, throttle, and cleanup behavior on a shared daemon.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cleanupLivePrefix(t, client, left.Prefix)
	cleanupLivePrefix(t, client, right.Prefix)
	now := time.Now().UTC()
	leftAdmission := openLiveAdmissionStore(t, client, AdmissionModeFunction, left, now)
	rightAdmission := openLiveAdmissionStore(t, client, AdmissionModeFunction, right, now)
	request := liveBeginRequest("shared-prefix-operation", "shared-prefix-policy", 1, 10, now)
	testassert.NoError(t, testassert.Err(leftAdmission.Begin(ctx, request)), "left admission")
	testassert.NoError(t, testassert.Err(rightAdmission.Begin(ctx, request)), "right admission")

	keyring := testKeyring(t)
	leftContinuation := openLiveContinuationStore(t, client, left, keyring, now)
	rightContinuation := openLiveContinuationStore(t, client, right, keyring, now)
	root := testContinuation(t, now)
	root.ExpiresAt = now.Add(time.Hour)
	leftHandle, err := leftContinuation.CreateRoot(ctx, root)
	testassert.NoError(t, err, "left continuation")
	if _, err := rightContinuation.Get(ctx, leftHandle); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("right continuation read = %v, want not found", err)
	}
	rightHandle, err := rightContinuation.CreateRoot(ctx, root)
	testassert.NoError(t, err, "right continuation")
	if _, err := leftContinuation.Get(ctx, rightHandle); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("left continuation read = %v, want not found", err)
	}

	leftThrottle := mustCall(t, NewThrottleStore, ThrottleOptions{Client: client, Mode: AdmissionModeFunction, Keys: left})
	rightThrottle := mustCall(t, NewThrottleStore, ThrottleOptions{Client: client, Mode: AdmissionModeFunction, Keys: right})
	limits := []ThrottleLimit{{Kind: ThrottleRequests, Scope: "shared-prefix-scope", Amount: 1, Limit: 2, Window: time.Minute}}
	testassert.NoError(t, testassert.Err(leftThrottle.Acquire(ctx, "shared-prefix-throttle", limits)), "left throttle")
	if _, err := rightThrottle.Lookup(ctx, "shared-prefix-throttle"); !errors.Is(err, ErrThrottleNotFound) {
		t.Fatalf("right throttle read = %v, want not found", err)
	}

	testassert.NoError(t, deleteLivePrefix(ctx, client, right.Prefix), "delete right namespace")
	testassert.NoError(t, testassert.Err(leftAdmission.Get(ctx, request.ID)), "left admission after right cleanup")
	testassert.NoError(t, testassert.Err(leftContinuation.Get(ctx, leftHandle)), "left continuation after right cleanup")
	testassert.NoError(t, testassert.Err(leftThrottle.Lookup(ctx, "shared-prefix-throttle")), "left throttle after right cleanup")
}

func TestLiveRedisLuaStoreFactoryConformance(t *testing.T) {
	client := openLiveRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Lua is an explicit compatibility mode. Provision the exact immutable
	// script only in this isolated test dependency, then exercise the same
	// black-box StoreFactory contract used by Function mode. The worker never
	// performs this load at runtime.
	sha, err := client.ScriptLoad(ctx, AdmissionLuaSource()).Result()
	testassert.NoError(t, err, "provision Redis admission Lua script")
	if sha != AdmissionLuaSHA1() {
		t.Fatalf("provisioned Lua script SHA = %q, want %q", sha, AdmissionLuaSHA1())
	}
	t.Cleanup(func() {
		if !liveRedisLuaScriptCleanupAllowed() {
			return
		}
		flushContext, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer flushCancel()
		if err := client.ScriptFlush(flushContext).Err(); err != nil {
			t.Errorf("flush isolated Redis Lua script cache: %v", err)
		}
	})
	conformance.Run(t, liveRedisStoreFactoryWithMode(client, AdmissionModeLua))
}

func TestLiveRedisLuaScriptCleanupRequiresIsolatedProvisioning(t *testing.T) {
	tests := []struct {
		name      string
		provision string
		container string
		prefix    string
		wantAllow bool
	}{
		{name: "manual address", provision: "1", wantAllow: false},
		{name: "provisioning disabled", container: "llmtw-redis-integration-123", prefix: "llmtw-redis-integration", wantAllow: false},
		{name: "unrecognized container", provision: "1", container: "redis-shared-123", prefix: "llmtw-redis-integration", wantAllow: false},
		{name: "isolated provisioned container", provision: "1", container: "llmtw-redis-integration-123", prefix: "llmtw-redis-integration", wantAllow: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := liveRedisLuaScriptCleanupAllowedValues(test.provision, test.container, test.prefix); got != test.wantAllow {
				t.Fatalf("cleanup allowed = %t, want %t", got, test.wantAllow)
			}
		})
	}
}

func TestLiveRedisFunctionBeginDecodesTwoFieldDenial(t *testing.T) {
	client := openLiveRedis(t)
	now := time.Now().UTC()
	keys := liveKeyOptions("begin-denial")
	cleanupLivePrefix(t, client, keys.Prefix)
	store := openLiveAdmissionStore(t, client, AdmissionModeFunction, keys, now)
	first := liveBeginRequest("begin-denial-first", "begin-denial-policy", 6, 10, now)
	if result, err := store.Begin(context.Background(), first); err != nil || result.Denied != nil {
		t.Fatalf("first reservation = %#v, %v", result, err)
	}
	second := liveBeginRequest("begin-denial-second", "begin-denial-policy", 5, 10, now)
	result, err := store.Begin(context.Background(), second)
	testassert.NoError(t, err, "denied reservation")
	if result.Denied == nil {
		t.Fatalf("denied reservation = %#v, want a decoded denial", result)
	}
	if got, want := result.Denied.PolicyID, "begin-denial-policy"; got != want {
		t.Fatalf("denial policy = %q, want %q", got, want)
	}
	if got, want := result.Denied.WindowID, "hour"; got != want {
		t.Fatalf("denial window = %q, want %q", got, want)
	}
	if got, want := result.Denied.Limit, pricing.MicroUSD(10); got != want {
		t.Fatalf("denial limit = %d, want %d", got, want)
	}
	if got, want := result.Denied.Active, pricing.MicroUSD(6); got != want {
		t.Fatalf("denial active = %d, want %d", got, want)
	}
	if got, want := result.Denied.Requested, pricing.MicroUSD(5); got != want {
		t.Fatalf("denial requested = %d, want %d", got, want)
	}
}

func TestLiveRedisTimeoutAfterMutationResolvesByRead(t *testing.T) {
	client := openLiveRedis(t)
	now := time.Now().UTC()
	keys := liveKeyOptions("timeout")
	cleanupLivePrefix(t, client, keys.Prefix)
	invoker := &timeoutAfterMutationFunctionInvoker{
		inner: redisInvoker{client: client, mode: AdmissionModeFunction, version: AdmissionFunctionVersion},
	}
	store, err := NewAdmissionStore(AdmissionOptions{
		Client:  client,
		Reader:  redisReader{client: client},
		Invoker: invoker,
		Keys:    keys,
		Clock:   func() time.Time { return now },
	})
	testassert.NoError(t, err)
	request := liveBeginRequest("timeout-after-mutation", "timeout-policy", 1, 10, now)
	if _, err := store.Begin(context.Background(), request); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("timeout after mutation = %v, want unresolved shared state", err)
	}
	resolved, err := store.Get(context.Background(), request.ID)
	if err != nil || resolved.State != admission.StateReserved || resolved.ReservedMicroUSD != 1 {
		t.Fatalf("read resolution = %#v, %v", resolved, err)
	}
}

func TestLiveRedisFunctionAndLuaMismatchFailClosed(t *testing.T) {
	client := openLiveRedis(t)
	now := time.Now().UTC()
	keys := liveKeyOptions("mismatch")
	cleanupLivePrefix(t, client, keys.Prefix)
	request := liveBeginRequest("mismatch", "mismatch-policy", 1, 10, now)

	functionStore, err := NewAdmissionStore(AdmissionOptions{
		Client:          client,
		Mode:            AdmissionModeFunction,
		FunctionVersion: "admission_v999",
		Keys:            keys,
		Clock:           func() time.Time { return now },
	})
	testassert.NoError(t, err)
	if _, err := functionStore.Begin(context.Background(), request); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("function version mismatch = %v, want unresolved shared state", err)
	}

	luaStore := openLiveAdmissionStore(t, client, AdmissionModeLua, keys, now)
	if _, err := luaStore.Begin(context.Background(), request); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lua digest mismatch = %v, want unresolved shared state", err)
	}

	space := mustCall(t, newKeySpace, keys)
	for _, key := range []string{
		space.scopeKey(request.ScopeKey),
		space.operationIndexKey(request.ID),
		space.operationKey(request.ScopeKey, request.ID),
		space.budgetKey("mismatch-policy", "hour"),
		space.continuationIndexKey("opaque-handle"),
		space.continuationKey("tenant-a", "opaque-handle"),
	} {
		if !strings.HasPrefix(key, keys.Prefix+":{"+keys.HashTag+"}") {
			t.Fatalf("live mutation key escapes configured hash slot: %q", key)
		}
	}
}

func TestLiveRedisConfiguredPersistenceSurvivesRestart(t *testing.T) {
	runLiveRedisConfiguredPersistence(t, AdmissionModeFunction, false)
}

// Kill the isolated daemon without a shutdown/snapshot opportunity. Both
// adapters must recover the same persistent authority and paid accounting.
func TestLiveRedisConfiguredPersistenceSurvivesCrash(t *testing.T) {
	for _, mode := range []AdmissionMode{AdmissionModeFunction, AdmissionModeLua} {
		t.Run(string(mode), func(t *testing.T) {
			runLiveRedisConfiguredPersistence(t, mode, true)
		})
	}
}

func runLiveRedisConfiguredPersistence(t *testing.T, mode AdmissionMode, crash bool) {
	t.Helper()
	container := os.Getenv("LLMTW_REDIS_CONTAINER")
	if container == "" {
		t.Skip("make redis-integration supplies the isolated Redis container")
	}
	if os.Getenv("LLMTW_REDIS_TEST_PROVISION") != "1" {
		t.Skip("restart is restricted to make redis-integration's explicitly provisioned dependency")
	}
	configuredPrefix := os.Getenv("LLMTW_REDIS_CONTAINER_PREFIX")
	if !isLiveRedisPersistenceContainer(container, configuredPrefix) {
		t.Skip("restart is restricted to make redis-integration's configured local dependency")
	}
	client := openLiveRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if mode == AdmissionModeLua {
		testassert.NoError(t, client.ScriptLoad(ctx, AdmissionLuaSource()).Err())
	}
	settings, err := client.ConfigGet(ctx, "appendonly").Result()
	if err != nil || settings["appendonly"] != "yes" {
		t.Fatalf("Redis appendonly configuration is not enabled")
	}
	settings, err = client.ConfigGet(ctx, "appendfsync").Result()
	if err != nil || settings["appendfsync"] != "always" {
		t.Fatalf("Redis appendfsync configuration is not durable")
	}
	settings, err = client.ConfigGet(ctx, "save").Result()
	if err != nil || !strings.Contains(settings["save"], "60 1") {
		t.Fatalf("Redis snapshot persistence configuration is not present")
	}

	now := time.Now().UTC()
	keys := liveKeyOptions("restart")
	initializer := mustBudgetInitializer(t, client, keys, mode, AdmissionFunctionVersion)
	authority := budget.Initialization{Schema: budget.InitializationSchema, Identity: initializer.Identity(), Epoch: "00000000-0000-4000-8000-000000000001", CreatedAt: now}
	testassert.NoError(t, initializer.Initialize(ctx, budget.InitializationPreparation{Receipt: authority, Created: true}))
	authority.Ready = true
	store := openLiveAdmissionStore(t, client, mode, keys, now)
	request := liveBeginRequest("restart", "restart-policy", 1, 10, now)
	if result, err := store.Begin(ctx, request); err != nil || result.Denied != nil {
		t.Fatalf("write before Redis restart = %#v, %v", result, err)
	}

	// The Redis-only budget authority must survive the same durable restart.
	budgetOptions := RedisBudgetMaterializerOptions{Client: client, Mode: mode, Keys: keys, GenerationID: "restart-gen", IncarnationID: "restart-inc", Initialization: &authority, CoordinationStreamEnabled: true}
	budgets := mustCall(t, NewRedisBudgetMaterializer, budgetOptions)
	budgetRequest := durable.ReserveRequest{OperationID: "paid-before-restart", GenerationID: "restart-gen", Reservations: []admission.WindowReservation{liveMinuteReservation("restart-budget", now, "1", "1")}}
	accepted, err := budgets.Accept(ctx, budgetRequest)
	if err != nil || !accepted.Accepted {
		t.Fatalf("reserve before restart: %#v %v", accepted, err)
	}
	testassert.NoError(t, testassert.Err(budgets.Claim(ctx, claimOf(accepted))))
	completion := durable.ReconcileRequest{OperationID: accepted.OperationID, GenerationID: accepted.GenerationID, IncarnationID: accepted.IncarnationID, Events: []budget.CompletionEvent{exactSettledEvent(accepted, accepted.Events[0], "settled-before-restart", pricing.MustUSD("0.4"), now)}}
	testassert.NoError(t, budgets.Reconcile(ctx, completion))
	pendingRequest := budgetRequest
	pendingRequest.OperationID = "pending-before-restart"
	pendingRequest.Reservations = append([]admission.WindowReservation(nil), budgetRequest.Reservations...)
	pendingRequest.Reservations[0].AmountUSD = pricing.MustUSD("0.6")
	pending, err := budgets.Accept(ctx, pendingRequest)
	if err != nil || !pending.Accepted {
		t.Fatalf("pending before restart: %#v %v", pending, err)
	}
	pendingClaim := claimOf(pending)
	testassert.NoError(t, testassert.Err(budgets.Claim(ctx, pendingClaim)))

	providerOptions := ProviderStateOptions{Client: client, Keys: keys}
	providers := mustCall(t, NewProviderStateStore, providerOptions)
	incident := providerTestEvent(t, now, "restart-route", func(o *control.StatusObservation) {
		o.Credit = control.CreditExhausted
		o.ProviderCode = "insufficient_quota"
	})
	testassert.NoError(t, testassert.Err(providers.PersistStatusEvent(ctx, incident)))
	inventory := providerTestInventory(now)
	inventory.Complete, inventory.NextCursor = false, "restart-private-cursor"
	testassert.NoError(t, testassert.Err(providers.PersistInventorySnapshot(ctx, inventory)))

	eventsBefore, err := client.XRange(ctx, initializer.keys.EventsKey(), "-", "+").Result()
	testassert.NoError(t, err)
	if len(eventsBefore) != 6 { // initialization, reserve/claim/settle, reserve/claim
		t.Fatalf("budget transitions before restart: %d", len(eventsBefore))
	}
	if crash {
		runLiveRedisDocker(t, "kill", "--signal", "KILL", container)
		runLiveRedisDocker(t, "start", container)
	} else {
		runLiveRedisDocker(t, "restart", container)
	}
	restartedClient := reopenLiveRedisAfterRestart(t, container)
	cleanupLivePrefix(t, restartedClient, keys.Prefix)
	recoveryContext, recoveryCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer recoveryCancel()
	recoveredInitializer := mustBudgetInitializer(t, restartedClient, keys, mode, AdmissionFunctionVersion)
	if mode == AdmissionModeLua {
		// Redis does not persist EVALSHA's script cache. Missing code must
		// remain unavailable until the operator provisions the pinned script.
		if err := recoveredInitializer.Check(recoveryContext, authority); err == nil || !strings.HasPrefix(err.Error(), "NOSCRIPT") {
			t.Fatalf("Lua cache loss did not fail closed: %v", err)
		}
		testassert.NoError(t, restartedClient.ScriptLoad(recoveryContext, AdmissionLuaSource()).Err())
	}
	testassert.NoError(t, recoveredInitializer.Initialize(recoveryContext, budget.InitializationPreparation{Receipt: authority}), "adopt persisted ready authority")
	eventsAfter, err := restartedClient.XRange(recoveryContext, initializer.keys.EventsKey(), "-", "+").Result()
	if err != nil || !reflect.DeepEqual(eventsAfter, eventsBefore) {
		t.Fatalf("budget transitions did not survive restart unchanged: %v", err)
	}

	providerOptions.Client = restartedClient
	providers = mustCall(t, NewProviderStateStore, providerOptions)
	status, err := providers.GetRouteStatus(context.Background(), incident.ConfigDigest, incident.RouteID, incident.EndpointID, incident.Model, incident.ProviderTier)
	if err != nil || status.Credit != control.CreditExhausted {
		t.Fatalf("provider incident after restart: %#v %v", status, err)
	}
	listing, err := providers.GetInventorySnapshot(context.Background(), inventory.ConfigDigest, inventory.Provider, inventory.EndpointID, inventory.EndpointAccountHMAC)
	if err != nil || listing.NextCursor != inventory.NextCursor {
		t.Fatalf("inventory after restart: %#v %v", listing, err)
	}
	// Redis script caches may disappear on restart; the embedded script must
	// still accept a new observation without a SQL fallback or manual preload.
	updated := providerTestEvent(t, now.Add(time.Second), "restart-route", nil)
	testassert.NoError(t, testassert.Err(providers.PersistStatusEvent(context.Background(), updated)))
	status, err = providers.GetRouteStatus(context.Background(), incident.ConfigDigest, incident.RouteID, incident.EndpointID, incident.Model, incident.ProviderTier)
	if err != nil || status.Credit != control.CreditExhausted {
		t.Fatalf("sticky incident after restart/update: %#v %v", status, err)
	}

	budgetOptions.Client = restartedClient
	recovered := mustCall(t, NewRedisBudgetMaterializer, budgetOptions)
	testassert.ErrorIs(t, testassert.Err(recovered.Claim(context.Background(), pendingClaim)), durable.ErrAlreadyClaimed, "pending claim after restart")
	testassert.NoError(t, recovered.Reconcile(context.Background(), completion))
	if n := restartedClient.XLen(recoveryContext, initializer.keys.EventsKey()).Val(); n != int64(len(eventsBefore)) {
		t.Fatalf("authority adoption/paid replay duplicated budget hints: %d", n)
	}
	extra := budgetRequest
	extra.OperationID = "blocked-after-restart"
	denied, err := recovered.Accept(context.Background(), extra)
	if err != nil || denied.Accepted || denied.Denial == nil || denied.Denial.ActiveUSD.Cmp(pricing.MustUSD("1")) != 0 {
		t.Fatalf("budget accounting lost on restart: %#v %v", denied, err)
	}

	restartedStore := openLiveAdmissionStore(t, restartedClient, mode, keys, now)
	stored, err := restartedStore.Get(context.Background(), request.ID)
	if err != nil || stored.State != admission.StateReserved {
		t.Fatalf("persisted operation after Redis restart = %#v, %v", stored, err)
	}
	if result, err := restartedStore.Begin(context.Background(), liveBeginRequest("restart-after", "restart-policy", 1, 10, now)); err != nil || result.Denied != nil {
		t.Fatalf("Function after Redis restart = %#v, %v", result, err)
	}
	testassert.NoError(t, restartedClient.Del(recoveryContext, initializer.keys.AuthorityKey()).Err())
	testassert.ErrorIs(t, recoveredInitializer.Initialize(recoveryContext, budget.InitializationPreparation{Receipt: authority}), ErrBudgetAuthorityUnavailable, "ready receipt recreated lost authority after restart")
	testassert.ErrorIs(t, testassert.Err(recovered.Accept(recoveryContext, extra)), ErrBudgetAuthorityUnavailable, "admission reopened after authority loss")
	testassert.ErrorIs(t, testassert.Err(recovered.Claim(recoveryContext, pendingClaim)), ErrBudgetAuthorityUnavailable, "claim allowed after authority loss")
	testassert.ErrorIs(t, recovered.Reconcile(recoveryContext, completion), ErrBudgetAuthorityUnavailable, "settlement allowed after authority loss")
	if n := restartedClient.Exists(recoveryContext, initializer.keys.AuthorityKey()).Val(); n != 0 {
		t.Fatal("lost authority was recreated")
	}
}

func TestLiveRedisConfiguredPersistenceGateAllowsOverriddenPrefix(t *testing.T) {
	const prefix = "llmtw-redis-custom-ci"
	if !isLiveRedisPersistenceContainer(prefix+"-123-456", prefix) {
		t.Fatal("configured Redis container prefix should allow the persistence gate to run")
	}
}

func TestLiveRedisAddressForPublishedPort(t *testing.T) {
	address, err := liveRedisAddressForPublishedPort("32768")
	testassert.NoError(t, err)
	if want := "127.0.0.1:32768"; address != want {
		t.Fatalf("address = %q, want %q", address, want)
	}
	for _, port := range []string{"", "not-a-port", "0", "65536"} {
		if _, err := liveRedisAddressForPublishedPort(port); err == nil {
			t.Errorf("port %q was accepted", port)
		}
	}
}

func isLiveRedisPersistenceContainer(container, configuredPrefix string) bool {
	return configuredPrefix != "" && strings.HasPrefix(container, configuredPrefix+"-")
}

func liveRedisLuaScriptCleanupAllowed() bool {
	return liveRedisLuaScriptCleanupAllowedValues(
		os.Getenv("LLMTW_REDIS_TEST_PROVISION"),
		os.Getenv("LLMTW_REDIS_CONTAINER"),
		os.Getenv("LLMTW_REDIS_CONTAINER_PREFIX"),
	)
}

func liveRedisLuaScriptCleanupAllowedValues(provision, container, prefix string) bool {
	return provision == "1" && isLiveRedisPersistenceContainer(container, prefix)
}

func openLiveRedis(t *testing.T) *redisclient.Client {
	t.Helper()
	address := os.Getenv("LLMTW_REDIS_ADDR")
	// Docker may assign a different host port when an ephemeral published
	// port is remapped by `docker restart`. Prefer the container's current
	// mapping whenever the isolated integration harness provides its name;
	// LLMTW_REDIS_ADDR is retained for manually provisioned Redis instances.
	if container := os.Getenv("LLMTW_REDIS_CONTAINER"); container != "" {
		current, err := liveRedisAddressForContainer(container)
		testassert.NoError(t, err, "could not discover the current isolated Redis address")
		address = current
	}
	if address == "" {
		t.Skip("set LLMTW_REDIS_ADDR to run the pinned live Redis gate")
	}
	client := openLiveRedisAt(t, address)
	if os.Getenv("LLMTW_REDIS_TEST_PROVISION") == "1" {
		for _, source := range []string{AdmissionFunctionSource(), ThrottleFunctionSource()} {
			if err := client.FunctionLoad(context.Background(), source).Err(); err != nil && !strings.Contains(err.Error(), "already exists") {
				t.Fatal("could not provision the isolated Redis Function")
			}
		}
	}
	return client
}

func openLiveRedisAt(t *testing.T, address string) *redisclient.Client {
	t.Helper()
	client := redisclient.NewClient(&redisclient.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	waitForLiveRedis(t, client)
	return client
}

func reopenLiveRedisAfterRestart(t *testing.T, container string) *redisclient.Client {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		address, err := liveRedisAddressForContainer(container)
		if err == nil {
			client := redisclient.NewClient(&redisclient.Options{Addr: address})
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			err = client.Ping(ctx).Err()
			cancel()
			if err == nil {
				t.Cleanup(func() { _ = client.Close() })
				return client
			}
			_ = client.Close()
			lastErr = fmt.Errorf("ping Redis at %s: %w", address, err)
		} else {
			lastErr = err
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("isolated Redis did not become reachable after restart: %v", lastErr)
	return nil
}

func liveRedisAddressForContainer(container string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", "inspect", "--format", `{{if .State.Running}}{{(index (index .NetworkSettings.Ports "6379/tcp") 0).HostPort}}{{end}}`, container).Output()
	if err != nil {
		return "", fmt.Errorf("inspect restarted Redis container: %w", err)
	}
	return liveRedisAddressForPublishedPort(strings.TrimSpace(string(output)))
}

func liveRedisAddressForPublishedPort(port string) (string, error) {
	value, err := strconv.ParseUint(port, 10, 16)
	if err != nil || value == 0 {
		return "", fmt.Errorf("invalid Redis published port %q", port)
	}
	return net.JoinHostPort("127.0.0.1", port), nil
}

func waitForLiveRedis(t *testing.T, client *redisclient.Client) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		err := client.Ping(ctx).Err()
		cancel()
		if err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("isolated Redis did not become ready")
}

func liveRedisStoreFactory(client *redisclient.Client) conformance.StoreFactory {
	return liveRedisStoreFactoryWithMode(client, AdmissionModeFunction)
}

func liveRedisStoreFactoryWithMode(client *redisclient.Client, mode AdmissionMode) conformance.StoreFactory {
	return liveRedisStoreFactoryWithKeys(client, mode, KeyOptions{})
}

func liveRedisStoreFactoryWithKeys(client *redisclient.Client, mode AdmissionMode, configured KeyOptions) conformance.StoreFactory {
	return conformance.StoreFactory{
		Name: "redis-" + string(mode),
		New: func(t testing.TB) conformance.Stores {
			t.Helper()
			now := time.Now().UTC()
			keys := configured
			if keys.Prefix == "" {
				keys = liveKeyOptions("conformance")
			}
			cleanupLivePrefix(t, client, keys.Prefix)
			keyring, err := state.NewKeyring([]state.Key{{
				ID:      "conformance",
				Secret:  bytes.Repeat([]byte{2}, 32),
				Primary: true,
			}}, nil)
			testassert.NoError(t, err)
			admissions := openLiveAdmissionStore(t, client, mode, keys, now)
			continuations := openLiveContinuationStore(t, client, keys, keyring, now)
			return conformance.Stores{
				Admission:     admissions,
				Continuations: continuations,
				Now:           func() time.Time { return now },
			}
		},
	}
}

// openLiveAdmissionStore opens an admission store on client in mode under
// keys with a clock fixed at now.
func openLiveAdmissionStore(t testing.TB, client redisclient.Scripter, mode AdmissionMode, keys KeyOptions, now time.Time) *AdmissionStore {
	t.Helper()
	store := mustCall(t, NewAdmissionStore, AdmissionOptions{Client: client, Mode: mode, Keys: keys, Clock: func() time.Time { return now }})
	return store
}

// openLiveContinuationStore opens a continuation store on client under keys
// with a clock fixed at now.
func openLiveContinuationStore(t testing.TB, client redisclient.Scripter, keys KeyOptions, keyring *state.Keyring, now time.Time) *ContinuationStore {
	t.Helper()
	store := mustCall(t, NewContinuationStore, ContinuationOptions{Client: client, Keys: keys, Keyring: keyring, Clock: func() time.Time { return now }})
	return store
}

func liveKeyOptions(kind string) KeyOptions {
	sequence := liveConformanceSequence.Add(1)
	return KeyOptions{
		Prefix:    fmt.Sprintf("live-%s-%d-%d", kind, time.Now().UnixNano(), sequence),
		HashTag:   "admission",
		KeySecret: bytes.Repeat([]byte{3}, 32),
	}
}

func liveBeginRequest(id, policy string, amount, limit int, now time.Time) admission.BeginRequest {
	return admission.BeginRequest{
		ID:            id,
		ScopeKey:      "tenant/" + id,
		RequestDigest: admission.Digest([]byte("request/" + id)),
		Reservation:   pricing.MicroUSD(amount),
		Reservations: []admission.WindowReservation{{
			PolicyID:      policy,
			WindowID:      "hour",
			Bucket:        now.UnixNano() / int64(time.Hour),
			Amount:        pricing.MicroUSD(amount),
			Limit:         pricing.MicroUSD(limit),
			BucketNanos:   int64(time.Hour),
			DurationNanos: int64(2 * time.Hour),
		}},
		LeaseUntil: now.Add(time.Minute),
		ExpiresAt:  now.Add(2 * time.Hour),
	}
}

func cleanupLivePrefix(t testing.TB, client *redisclient.Client, prefix string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := deleteLivePrefix(ctx, client, prefix); err != nil {
			t.Error("could not remove isolated Redis test keys during cleanup")
		}
	})
}

func deleteLivePrefix(ctx context.Context, client redisclient.UniversalClient, prefix string) error {
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, prefix+":*", 100).Result()
		if err != nil {
			return err
		}
		if len(keys) != 0 {
			if err := client.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

func runLiveRedisDocker(t *testing.T, arguments ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := exec.CommandContext(ctx, "docker", arguments...).Run(); err != nil {
		t.Fatal("could not restart the isolated Redis dependency")
	}
}
