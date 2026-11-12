//go:build integration

package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	durable "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
	redisclient "github.com/redis/go-redis/v9"
)

// Operation records expire operation_key_reuse_horizon after their last change
// once nothing can still settle them; claimed and ambiguous records stay
// persistent, and the budget:operations index mirrors each key's expiry (#1543).
func TestLiveRedisOperationRecordRetention(t *testing.T) {
	client := openLiveRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.ScriptLoad(ctx, AdmissionLuaSource()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if liveRedisLuaScriptCleanupAllowed() {
			_ = client.ScriptFlush(context.Background()).Err()
		}
	})
	const horizon = 48 * time.Hour
	for _, test := range []struct {
		mode    AdmissionMode
		horizon time.Duration
	}{{AdmissionModeFunction, horizon}, {AdmissionModeLua, horizon}, {AdmissionModeFunction, 0}} {
		t.Run(fmt.Sprintf("%s/%s", test.mode, test.horizon), func(t *testing.T) {
			keys := liveKeyOptions("operation-retention")
			cleanupLivePrefix(t, client, keys.Prefix)
			now := time.Now().UTC()
			materializer, err := NewRedisBudgetMaterializer(RedisBudgetMaterializerOptions{Client: client, Mode: test.mode, Keys: keys, GenerationID: "retention-gen", IncarnationID: "retention-inc", Clock: time.Now, CoordinationStreamEnabled: true, OperationKeyReuseHorizon: test.horizon})
			if err != nil {
				t.Fatal(err)
			}
			space, err := newKeySpace(keys)
			if err != nil {
				t.Fatal(err)
			}
			sampler, err := NewBudgetGrowthSampler(client, keys, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			index := space.admissionPrefix() + BudgetOperationsSuffix
			// check asserts the record's TTL and index score: persistent, or
			// expiring after about the horizon.
			check := func(id string, persistent bool) {
				t.Helper()
				key := space.durableBudgetOperationKey("retention-gen", id)
				ttl, err := client.TTL(ctx, key).Result()
				if err != nil {
					t.Fatal(err)
				}
				score, err := client.ZScore(ctx, index, key[len(key)-64:]).Result()
				if err != nil {
					t.Fatalf("%s index entry: %v", id, err)
				}
				if persistent || test.horizon == 0 {
					if ttl != -1 || score < 1e300 {
						t.Fatalf("%s ttl=%v score=%v, want persistent", id, ttl, score)
					}
					return
				}
				if ttl < test.horizon-time.Minute || ttl > test.horizon {
					t.Fatalf("%s ttl=%v, want about %v", id, ttl, test.horizon)
				}
				if expiry := time.UnixMilli(int64(score)); expiry.Before(now.Add(test.horizon-time.Minute)) || expiry.After(time.Now().Add(test.horizon+time.Minute)) {
					t.Fatalf("%s index expiry=%v", id, expiry)
				}
			}
			reserve := func(id string) durable.ReserveResult {
				t.Helper()
				result, err := materializer.Accept(ctx, durable.ReserveRequest{OperationID: durable.OperationID(id), GenerationID: "retention-gen", ExpiresAt: now.Add(time.Minute), Reservations: []admission.WindowReservation{{PolicyID: "policy", WindowID: "hour", Bucket: now.Unix() / 60, BucketNanos: int64(time.Minute), DurationNanos: int64(time.Hour), AmountUSD: pricing.MustUSD("0.10"), LimitUSD: pricing.MustUSD("10")}}})
				if err != nil || !result.Accepted {
					t.Fatalf("%s: %#v %v", id, result, err)
				}
				return result
			}
			settle := func(result durable.ReserveResult, kind budget.JournalEventKind) {
				t.Helper()
				event := result.Events[0]
				cost := pricing.MustUSD("0.05")
				completion := budget.CompletionEvent{EventID: string(result.OperationID) + "-" + string(kind), OperationID: string(result.OperationID), GenerationID: "retention-gen", WindowID: event.WindowID, BucketStart: event.BucketStart, ReservationRevision: 2, Kind: kind, OccurredAt: now}
				switch kind {
				case budget.JournalRetainAmbiguous:
					completion.CostStatus, completion.UnknownReasonCode = budget.CostUnknown, "provider_cost_unknown"
				case budget.JournalRelease:
					zero := pricing.USD{}
					completion.ReservedDecreaseUSD, completion.CostStatus, completion.ActualCostUSD = event.AmountUSD, budget.CostExact, &zero
				default:
					completion.ReservedDecreaseUSD, completion.AccountedIncreaseUSD, completion.CostStatus, completion.ActualCostUSD = event.AmountUSD, cost, budget.CostExact, &cost
				}
				if err := materializer.Reconcile(ctx, durable.ReconcileRequest{OperationID: result.OperationID, GenerationID: "retention-gen", IncarnationID: "retention-inc", Events: []budget.CompletionEvent{completion}}); err != nil {
					t.Fatalf("%s %s: %v", result.OperationID, kind, err)
				}
			}
			claim := func(result durable.ReserveResult) {
				t.Helper()
				if _, err := materializer.Claim(ctx, durable.ClaimRequest{OperationID: result.OperationID, GenerationID: "retention-gen", IncarnationID: "retention-inc"}); err != nil {
					t.Fatal(err)
				}
			}

			unclaimed := reserve("unclaimed")
			check("unclaimed", false)
			settle(unclaimed, budget.JournalRelease)
			check("unclaimed", false)

			settled := reserve("settled")
			claim(settled)
			check("settled", true)
			settle(settled, budget.JournalFinalizeExact)
			check("settled", false)
			// Record the measured footprint that the sizing formula in
			// redis-budget-leases.md is based on.
			recordBytes, _ := client.MemoryUsage(ctx, space.durableBudgetOperationKey("retention-gen", "settled")).Result()
			streamBytes, _ := client.MemoryUsage(ctx, space.admissionPrefix()+BudgetEventsSuffix).Result()
			streamLength, _ := client.XLen(ctx, space.admissionPrefix()+BudgetEventsSuffix).Result()
			t.Logf("settled one-window operation record: %d bytes; Stream: %d bytes for %d entries", recordBytes, streamBytes, streamLength)

			ambiguous := reserve("ambiguous")
			claim(ambiguous)
			settle(ambiguous, budget.JournalRetainAmbiguous)
			check("ambiguous", true)

			growth, err := sampler.Sample(ctx)
			if err != nil {
				t.Fatal(err)
			}
			wantPersistent := int64(1)
			if test.horizon == 0 {
				wantPersistent = 3
			}
			if growth.OperationRecords != 3 || growth.PersistentOperationRecords != wantPersistent || growth.StreamLength == 0 {
				t.Fatalf("growth = %+v, want 3 records (%d persistent) and a Stream", growth, wantPersistent)
			}
			// An index entry whose expiry has passed is pruned by the next
			// mutation and no longer counted.
			if err := client.ZAdd(ctx, index, redisclient.Z{Score: float64(time.Now().Add(-time.Second).UnixMilli()), Member: "stale"}).Err(); err != nil {
				t.Fatal(err)
			}
			if growth, err := sampler.Sample(ctx); err != nil || growth.OperationRecords != 3 {
				t.Fatalf("growth with a stale entry = %+v, %v", growth, err)
			}
			reserve("after-stale")
			if _, err := client.ZScore(ctx, index, "stale").Result(); err == nil {
				t.Fatal("expired index entry was not pruned")
			}
		})
	}
}

// Provider state for a configuration digest no worker has reported loaded
// within the retirement horizon is dropped; the current digest and recently
// reported ones are kept.
func TestLiveRedisProviderStateRetiresUnloadedConfigs(t *testing.T) {
	client := openLiveRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	keys := liveKeyOptions("provider-retirement")
	cleanupLivePrefix(t, client, keys.Prefix)
	now := time.Now().UTC()
	store, err := NewProviderStateStore(ProviderStateOptions{Client: client, Keys: keys, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	current, recent, retired := [32]byte{1}, [32]byte{2}, [32]byte{3}
	for _, digest := range [][32]byte{current, recent, retired} {
		if err := client.HSet(ctx, store.key("status", digest), "field", "value").Err(); err != nil {
			t.Fatal(err)
		}
		if err := client.HSet(ctx, store.key("inventory", digest), "field", "value").Err(); err != nil {
			t.Fatal(err)
		}
	}
	report := func(digest [32]byte, at time.Time) {
		t.Helper()
		now = at
		if err := store.MarkConfigLoaded(ctx, digest); err != nil {
			t.Fatal(err)
		}
	}
	start := now
	report(retired, start.Add(-8*24*time.Hour))
	report(current, start.Add(-9*24*time.Hour))
	report(recent, start.Add(-time.Hour))
	now = start
	dropped, err := store.DropRetiredConfigs(ctx, current, ProviderStateRetireAfter)
	if err != nil || dropped != 1 {
		t.Fatalf("dropped = %d, %v; want 1", dropped, err)
	}
	for digest, want := range map[[32]byte]int64{current: 1, recent: 1, retired: 0} {
		exists, err := client.Exists(ctx, store.key("status", digest), store.key("inventory", digest)).Result()
		if err != nil || exists != 2*want {
			t.Fatalf("digest %x: %d keys exist, %v; want %d", digest[:1], exists, err, 2*want)
		}
	}
	// A digest reported again after the scan observed it stale is kept.
	report(retired, start.Add(-8*24*time.Hour))
	if err := client.HSet(ctx, store.key("status", retired), "field", "value").Err(); err != nil {
		t.Fatal(err)
	}
	applied, err := providerRetireScript.Run(ctx, client, []string{store.loadedConfigsKey(), store.key("status", retired), store.key("inventory", retired)}, fmt.Sprintf("%x", retired[:]), "0").Int()
	if err != nil || applied != 0 {
		t.Fatalf("retire with a changed marker = %d, %v; want 0", applied, err)
	}
}
