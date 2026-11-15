//go:build integration

package redis

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
	redisclient "github.com/redis/go-redis/v9"
)

func TestLiveRedisBudgetInitialization(t *testing.T) {
	client, ctx := openLiveRedisWithAdmissionLua(t, time.Minute)
	for _, mode := range []AdmissionMode{AdmissionModeFunction, AdmissionModeLua} {
		t.Run(string(mode), func(t *testing.T) {
			fixture := func(t *testing.T) (*RedisBudgetInitializer, budget.Initialization) {
				t.Helper()
				keys := liveKeyOptions("budget-initialization")
				cleanupLivePrefix(t, client, keys.Prefix)
				initializer := mustBudgetInitializer(t, client, keys, mode, AdmissionFunctionVersion)
				value := budget.Initialization{Schema: budget.InitializationSchema, Identity: initializer.Identity(), Epoch: "00000000-0000-4000-8000-000000000001", CreatedAt: time.Now().UTC()}
				return initializer, value
			}
			t.Run("initialize and replay preserve existing spending", func(t *testing.T) {
				i, value := fixture(t)
				testassert.ErrorIs(t, i.Check(ctx, value), ErrBudgetAuthorityUnavailable, "uninitialized check =")
				testassert.NoError(t, i.Initialize(ctx, budget.InitializationPreparation{Receipt: value, Created: true}))
				value.Ready = true
				testassert.NoError(t, i.Check(ctx, value))
				spending := i.keys.space.durableBudgetKey("policy", "window")
				testassert.NoError(t, client.HSet(ctx, spending, "sum:42", "250000000").Err())
				for range 3 {
					testassert.NoError(t, i.Initialize(ctx, budget.InitializationPreparation{Receipt: value}))
				}
				if got := client.HGet(ctx, spending, "sum:42").Val(); got != "250000000" {
					t.Fatalf("replay reset spending: %q", got)
				}
				if got := client.XLen(ctx, i.keys.EventsKey()).Val(); got != 1 {
					t.Fatalf("replay duplicated initialization event: %d", got)
				}
				if ttl := client.PTTL(ctx, i.keys.AuthorityKey()).Val(); ttl != -1 {
					t.Fatalf("authority expires: %v", ttl)
				}
				port := mustBudgetEventPort(t, client, i.keys)
				rows, err := port.Read(ctx, "", 10)
				if err != nil || len(rows) != 1 || rows[0].Event.Kind != BudgetEventInitialize {
					t.Fatalf("initialization hint = %#v, %v", rows, err)
				}
			})
			t.Run("ready receipt never recreates lost authority", func(t *testing.T) {
				i, value := fixture(t)
				testassert.NoError(t, i.Initialize(ctx, budget.InitializationPreparation{Receipt: value, Created: true}))
				testassert.NoError(t, client.Del(ctx, i.keys.AuthorityKey(), i.keys.EventsKey()).Err())
				// Even a stale pending receipt has no creation permission on replay.
				for _, ready := range []bool{false, true} {
					value.Ready = ready
					testassert.ErrorIs(t, i.Initialize(ctx, budget.InitializationPreparation{Receipt: value}), ErrBudgetAuthorityUnavailable, "lost authority was recreated")
				}
				if n := client.Exists(ctx, i.keys.AuthorityKey(), i.keys.EventsKey()).Val(); n != 0 {
					t.Fatal("lost dataset received a new marker")
				}
			})
			t.Run("lost commit reply resumes without another event", func(t *testing.T) {
				i, value := fixture(t)
				i.invoke = &lostBudgetReply{delegate: i.invoke, action: "durable_authority_commit"}
				if err := i.Initialize(ctx, budget.InitializationPreparation{Receipt: value, Created: true}); err == nil {
					t.Fatal("expected lost reply")
				}
				testassert.NoError(t, i.Initialize(ctx, budget.InitializationPreparation{Receipt: value}))
				if n := client.XLen(ctx, i.keys.EventsKey()).Val(); n != 1 {
					t.Fatalf("lost reply duplicated events: %d", n)
				}
			})
			t.Run("concurrent initializer resumes one epoch", func(t *testing.T) {
				i, value := fixture(t)
				var wg sync.WaitGroup
				for n := range 16 {
					wg.Go(func() {
						err := i.Initialize(ctx, budget.InitializationPreparation{Receipt: value, Created: n == 0})
						if err != nil && !errors.Is(err, ErrBudgetAuthorityUnavailable) {
							t.Error(err)
						}
					})
				}
				wg.Wait()
				testassert.NoError(t, i.Initialize(ctx, budget.InitializationPreparation{Receipt: value}))
				if n := client.XLen(ctx, i.keys.EventsKey()).Val(); n != 1 {
					t.Fatalf("concurrent initialization created %d events", n)
				}
			})
			t.Run("existing accounting and foreign authority are never overwritten", func(t *testing.T) {
				for _, kind := range []string{"durable-budget", "operation", "budget:active-generation", "budget:authority"} {
					t.Run(kind, func(t *testing.T) {
						i, value := fixture(t)
						key := i.keys.space.admissionPrefix() + kind
						testassert.NoError(t, client.Set(ctx, key, "preserve-me", 0).Err())
						testassert.ErrorIs(t, i.Initialize(ctx, budget.InitializationPreparation{Receipt: value, Created: true}), ErrBudgetAuthorityUnavailable, "initialized occupied namespace")
						if got := client.Get(ctx, key).Val(); got != "preserve-me" {
							t.Fatal("existing state overwritten")
						}
						if n := client.Exists(ctx, i.keys.EventsKey()).Val(); n != 0 {
							t.Fatal("failed initialization published success")
						}
					})
				}
			})
			for _, stream := range []bool{false, true} {
				for _, action := range []string{"reserve", "claim", "reconcile"} {
					for _, failure := range []string{"missing", "preparing", "other-epoch", "expiring", "wrong-type"} {
						name := action + "/" + failure
						if stream {
							name += "/stream"
						}
						t.Run(name, func(t *testing.T) {
							i, value := fixture(t)
							testassert.NoError(t, i.Initialize(ctx, budget.InitializationPreparation{Receipt: value, Created: true}))
							value.Ready = true
							m := mustCall(t, NewRedisBudgetMaterializer, RedisBudgetMaterializerOptions{Client: client, Mode: mode, Keys: KeyOptions{Prefix: i.keys.space.prefix, HashTag: i.keys.space.tag, KeySecret: i.keys.space.secret}, GenerationID: "redis-budget-v1", IncarnationID: "redis-budget-v1", Initialization: &value, CoordinationStreamEnabled: stream})
							req := durableTestRequest(time.Now().UTC())
							req.GenerationID = "redis-budget-v1"
							var reserved durable.ReserveResult
							var err error
							if action != "reserve" {
								reserved, err = m.Accept(ctx, req)
								if err != nil || !reserved.Accepted {
									t.Fatalf("prepare reservation: %#v, %v", reserved, err)
								}
							}
							claim := durable.ClaimRequest{OperationID: req.OperationID, GenerationID: req.GenerationID, IncarnationID: "redis-budget-v1"}
							if action == "reconcile" {
								testassert.NoError(t, testassert.Err(m.Claim(ctx, claim)))
							}
							snapshot := func() []string {
								t.Helper()
								var result []string
								for _, key := range []string{m.space.durableBudgetOperationKey(string(req.GenerationID), string(req.OperationID)), m.space.durableBudgetKey(req.Reservations[0].PolicyID, req.Reservations[0].WindowID), m.space.durableBudgetExpiryKey(req.Reservations[0].PolicyID, req.Reservations[0].WindowID), i.keys.EventsKey()} {
									data, err := client.Dump(ctx, key).Result()
									if err != nil && !errors.Is(err, redisclient.Nil) {
										t.Fatal(err)
									}
									result = append(result, data)
								}
								return result
							}
							before := snapshot()
							key := i.keys.AuthorityKey()
							testassert.NoError(t, client.Del(ctx, key).Err())
							switch failure {
							case "preparing":
								err = client.Set(ctx, key, "preparing:"+value.Marker(), 0).Err()
							case "other-epoch":
								other := value
								other.Epoch = "00000000-0000-4000-8000-000000000002"
								err = client.Set(ctx, key, "ready:"+other.Marker(), 0).Err()
							case "expiring":
								err = client.Set(ctx, key, "ready:"+value.Marker(), time.Hour).Err()
							case "wrong-type":
								err = client.HSet(ctx, key, "ready", value.Marker()).Err()
							}
							testassert.NoError(t, err)
							switch action {
							case "reserve":
								_, err = m.Accept(ctx, req)
							case "claim":
								_, err = m.Claim(ctx, claim)
							case "reconcile":
								event := exactSettledEvent(reserved, reserved.Events[0], "complete", pricing.MustUSD("0.004"), time.Now().UTC())
								err = m.Reconcile(ctx, durable.ReconcileRequest{OperationID: req.OperationID, GenerationID: req.GenerationID, IncarnationID: "redis-budget-v1", Events: []budget.CompletionEvent{event}})
							}
							if !errors.Is(err, ErrBudgetAuthorityUnavailable) {
								t.Fatalf("unfenced %s = %v", action, err)
							}
							if after := snapshot(); !reflect.DeepEqual(before, after) {
								t.Fatal("authority failure changed accounting or published an event")
							}
						})
					}
				}
			}
		})
	}
}
