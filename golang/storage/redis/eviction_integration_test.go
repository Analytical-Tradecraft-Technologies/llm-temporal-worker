//go:build integration

package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

// This gate changes maxmemory and must only target a dedicated test instance.
// It does not flush Redis or change application key retention.
func TestLiveRedisVolatileEvictionProtectsAccounting(t *testing.T) {
	if os.Getenv("LLMTW_REDIS_EVICTION_TEST") != "1" {
		t.Skip("set LLMTW_REDIS_EVICTION_TEST=1 on an isolated Redis instance")
	}
	client := openLiveRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	settings := make(map[string]string)
	for _, name := range []string{"maxmemory", "maxmemory-policy", "save", "appendonly"} {
		values, err := client.ConfigGet(ctx, name).Result()
		if err != nil {
			t.Fatal(err)
		}
		settings[name] = values[name]
	}
	if settings["maxmemory-policy"] != "volatile-lru" || settings["save"] != "" || settings["appendonly"] != "no" {
		t.Fatal("requires isolated volatile-lru Redis with persistence disabled")
	}
	t.Cleanup(func() {
		if err := client.ConfigSet(context.Background(), "maxmemory", settings["maxmemory"]).Err(); err != nil {
			t.Error(err)
		}
	})
	keys := liveKeyOptions("eviction")
	cleanupLivePrefix(t, client, keys.Prefix)
	initializer, err := NewRedisBudgetInitializer(client, keys, AdmissionModeFunction, AdmissionFunctionVersion)
	if err != nil {
		t.Fatal(err)
	}
	receipt := budget.Initialization{Schema: budget.InitializationSchema, Identity: initializer.Identity(),
		Epoch: "00000000-0000-4000-8000-000000000001", CreatedAt: time.Now().UTC()}
	if err := initializer.Initialize(ctx, budget.InitializationPreparation{Receipt: receipt, Created: true}); err != nil {
		t.Fatal(err)
	}
	receipt.Ready = true
	m, err := NewRedisBudgetMaterializer(RedisBudgetMaterializerOptions{
		Client: client, Mode: AdmissionModeFunction, Keys: keys, GenerationID: "eviction-gen", IncarnationID: "eviction-inc",
		Initialization: &receipt, CoordinationStreamEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	request := durable.ReserveRequest{OperationID: "protected", GenerationID: "eviction-gen",
		Reservations: []admission.WindowReservation{{PolicyID: "tenant", WindowID: "hour", Bucket: now.Unix() / 60,
			BucketNanos: int64(time.Minute), DurationNanos: int64(time.Hour), AmountUSD: pricing.MustUSD("0.8"), LimitUSD: pricing.MustUSD("1")}}}
	accepted, err := m.Accept(ctx, request)
	if err != nil || !accepted.Accepted {
		t.Fatalf("accept = %#v, %v", accepted, err)
	}
	claim := durable.ClaimRequest{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: accepted.IncarnationID}
	if _, err := m.Claim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	pending := request
	pending.OperationID = "not-yet-dispatched"
	pending.Reservations = append([]admission.WindowReservation(nil), request.Reservations...)
	pending.Reservations[0].AmountUSD = pricing.MustUSD("0.1")
	if result, err := m.Accept(ctx, pending); err != nil || !result.Accepted {
		t.Fatalf("pending reservation = %#v, %v", result, err)
	}
	// Snapshot every accounting key, including authority, operation tombstone,
	// budget hash and logical-expiry index. Coordination hints are disposable.
	accounting, err := client.Keys(ctx, keys.Prefix+":*").Result()
	if err != nil || len(accounting) < 5 {
		t.Fatalf("accounting fixture: %d keys, %v", len(accounting), err)
	}
	before := make(map[string]string, len(accounting))
	space, err := NewBudgetKeySpace(keys)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range accounting {
		if key == space.EventsKey() {
			if ttl := client.TTL(ctx, key).Val(); ttl <= 0 || ttl > 24*time.Hour {
				t.Fatalf("coordination stream must be evictable: %v", ttl)
			}
			continue
		}
		if ttl := client.PTTL(ctx, key).Val(); ttl != -1 {
			t.Fatalf("accounting key can expire/evict: %s (%v)", key, ttl)
		}
		before[key] = client.Dump(ctx, key).Val()
	}
	stats, err := client.Info(ctx, "stats").Result()
	if err != nil {
		t.Fatal(err)
	}
	evictedBefore := evictionInfoNumber(t, stats, "evicted_keys")
	memory, err := client.Info(ctx, "memory").Result()
	if err != nil {
		t.Fatal(err)
	}
	limit := evictionInfoNumber(t, memory, "used_memory") + 512*1024
	if err := client.ConfigSet(ctx, "maxmemory", strconv.FormatInt(limit, 10)).Err(); err != nil {
		t.Fatal(err)
	}
	for i := range 256 {
		if err := client.Set(ctx, fmt.Sprintf("%s:cache:%d", keys.Prefix, i), strings.Repeat("x", 16*1024), time.Hour).Err(); err != nil {
			t.Fatal(err)
		}
	}
	stats, err = client.Info(ctx, "stats").Result()
	if err != nil || evictionInfoNumber(t, stats, "evicted_keys") <= evictedBefore {
		t.Fatalf("memory pressure did not evict disposable keys: %v", err)
	}
	for key, data := range before {
		if got, err := client.Dump(ctx, key).Result(); err != nil || got != data || client.PTTL(ctx, key).Val() != -1 {
			t.Fatalf("eviction modified accounting key %s: %v", key, err)
		}
	}
	if _, err := m.Claim(ctx, claim); !errors.Is(err, durable.ErrAlreadyClaimed) {
		t.Fatalf("eviction reopened provider dispatch: %v", err)
	}
	blocked := request
	blocked.OperationID = "still-over-limit"
	blocked.Reservations = append([]admission.WindowReservation(nil), request.Reservations...)
	blocked.Reservations[0].AmountUSD = pricing.MustUSD("0.3")
	result, err := m.Accept(ctx, blocked)
	if err != nil || result.Accepted {
		t.Fatalf("eviction reopened spending: %#v, %v", result, err)
	}
	for key := range before {
		before[key] = client.Dump(ctx, key).Val()
	}
	// If protected data itself fills RAM, fail closed instead of discarding it.
	if err := client.ConfigSet(ctx, "maxmemory", "1").Err(); err != nil {
		t.Fatal(err)
	}
	blocked.OperationID = "memory-exhausted"
	if result, err := m.Accept(ctx, blocked); err == nil || result.Accepted {
		t.Fatalf("protected-data exhaustion admitted spending: %#v, %v", result, err)
	}
	for key, data := range before {
		if got, err := client.Dump(ctx, key).Result(); err != nil || got != data {
			t.Fatalf("out-of-memory modified accounting key %s: %v", key, err)
		}
	}
}

func evictionInfoNumber(t *testing.T, info, name string) int64 {
	t.Helper()
	for _, line := range strings.Split(info, "\n") {
		if value, found := strings.CutPrefix(line, name+":"); found {
			result, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			if err == nil {
				return result
			}
		}
	}
	t.Fatalf("missing Redis INFO field %s", name)
	return 0
}
