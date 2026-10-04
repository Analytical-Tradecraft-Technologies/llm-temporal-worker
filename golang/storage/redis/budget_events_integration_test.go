//go:build integration

package redis

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
	redisclient "github.com/redis/go-redis/v9"
)

func TestLiveRedisBudgetEvents(t *testing.T) {
	client := openLiveRedis(t)
	ctx := context.Background()
	if err := client.ScriptLoad(ctx, AdmissionLuaSource()).Err(); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []AdmissionMode{AdmissionModeFunction, AdmissionModeLua} {
		t.Run(string(mode), func(t *testing.T) {
			fixture := func(t *testing.T) (*RedisBudgetMaterializer, BudgetKeySpace, durable.ReserveRequest) {
				t.Helper()
				keys := liveKeyOptions("budget-events")
				cleanupLivePrefix(t, client, keys.Prefix)
				space, err := NewBudgetKeySpace(keys)
				if err != nil {
					t.Fatal(err)
				}
				m, err := NewRedisBudgetMaterializer(RedisBudgetMaterializerOptions{
					Client: client, Mode: mode, Keys: keys,
					GenerationID: "generation-1", IncarnationID: "incarnation-1",
					CoordinationStreamEnabled: true,
				})
				if err != nil {
					t.Fatal(err)
				}
				return m, space, durableTestRequest(time.Now().UTC())
			}
			read := func(t *testing.T, space BudgetKeySpace, cursor string) []BudgetStreamRecord {
				t.Helper()
				port, err := NewRedisBudgetEventPort(client, space)
				if err != nil {
					t.Fatal(err)
				}
				rows, err := port.Read(ctx, cursor, 100)
				if err != nil {
					t.Fatal(err)
				}
				return rows
			}
			acquire := func(t *testing.T, m *RedisBudgetMaterializer, req durable.ReserveRequest) durable.ReserveResult {
				t.Helper()
				result, err := m.Accept(ctx, req)
				if err != nil || !result.Accepted {
					t.Fatalf("acquire = %#v, %v", result, err)
				}
				return result
			}
			claim := func(r durable.ReserveResult) durable.ClaimRequest {
				return durable.ClaimRequest{OperationID: r.OperationID, GenerationID: r.GenerationID, IncarnationID: r.IncarnationID}
			}
			settle := func(r durable.ReserveResult, amount string) durable.ReconcileRequest {
				cost := pricing.MustUSD(amount)
				e := r.Events[0]
				return durable.ReconcileRequest{
					OperationID: r.OperationID, GenerationID: r.GenerationID, IncarnationID: r.IncarnationID,
					Events: []budget.CompletionEvent{{
						EventID: "completion", GenerationID: string(r.GenerationID), OperationID: string(r.OperationID),
						WindowID: e.WindowID, BucketStart: e.BucketStart, ReservationRevision: 2,
						Kind: budget.JournalFinalizeExact, ReservedDecreaseUSD: e.AmountUSD,
						AccountedIncreaseUSD: cost, ActualCostUSD: &cost, CostStatus: budget.CostExact,
						OccurredAt: time.Now().UTC(),
					}},
				}
			}
			t.Run("lost replies and independent readers", func(t *testing.T) {
				m, space, req := fixture(t)
				m.invoke = &lostBudgetReply{delegate: m.invoke, action: "durable_reserve"}
				if _, err := m.Accept(ctx, req); err == nil {
					t.Fatal("expected lost acquisition reply")
				}
				r := acquire(t, m, req)
				m.invoke = &lostBudgetReply{delegate: m.invoke, action: "durable_claim"}
				if _, err := m.Claim(ctx, claim(r)); err == nil {
					t.Fatal("expected lost claim reply")
				}
				if _, err := m.Claim(ctx, claim(r)); !errors.Is(err, durable.ErrAlreadyClaimed) {
					t.Fatalf("claim replay = %v", err)
				}
				m.invoke = &lostBudgetReply{delegate: m.invoke, action: "durable_reconcile"}
				completed := settle(r, "0.004")
				if err := m.Reconcile(ctx, completed); err == nil {
					t.Fatal("expected lost settlement reply")
				}
				if err := m.Reconcile(ctx, completed); err != nil {
					t.Fatal(err)
				}
				rows := read(t, space, "")
				if len(rows) != 3 {
					t.Fatalf("expected one event per transition, got %#v", rows)
				}
				if other := read(t, space, ""); !reflect.DeepEqual(rows, other) {
					t.Fatalf("independent worker missed events: %#v", other)
				}
				kinds := []BudgetStreamEventKind{BudgetEventReserve, BudgetEventClaim, BudgetEventReconcile}
				deltas := []int64{10_000_000, 0, 6_000_000}
				for i, row := range rows {
					if row.Event.Kind != kinds[i] || row.Event.NanoDelta != deltas[i] ||
						row.Event.GenerationID != BudgetGenerationID(req.GenerationID) ||
						row.Event.OperationHash != m.space.digest("durable-budget-operation", string(req.GenerationID), string(req.OperationID)) {
						t.Fatalf("event %d = %#v", i, row.Event)
					}
					wire, err := row.Event.Marshal()
					if err != nil {
						t.Fatal(err)
					}
					for _, secret := range []string{string(req.OperationID), req.Reservations[0].PolicyID, req.Reservations[0].WindowID} {
						if strings.Contains(string(wire), "\""+secret+"\"") {
							t.Fatalf("raw application identifier in event: %s", wire)
						}
					}
				}
				if remaining := read(t, space, rows[0].ID); !reflect.DeepEqual(rows[1:], remaining) {
					t.Fatalf("cursor replay = %#v", remaining)
				}
			})
			t.Run("concurrent replay publishes once", func(t *testing.T) {
				m, space, req := fixture(t)
				var workers sync.WaitGroup
				for range 16 {
					workers.Go(func() {
						result, err := m.Accept(ctx, req)
						if err != nil || !result.Accepted {
							t.Errorf("acquire = %#v, %v", result, err)
						}
					})
				}
				workers.Wait()
				if rows := read(t, space, ""); len(rows) != 1 || rows[0].Event.Kind != BudgetEventReserve {
					t.Fatalf("concurrent replay events = %#v", rows)
				}
			})
			t.Run("denials and release", func(t *testing.T) {
				m, space, req := fixture(t)
				r := acquire(t, m, req)
				blocked := req
				blocked.OperationID = "blocked"
				blocked.Reservations = append(blocked.Reservations[:0:0], req.Reservations...)
				blocked.Reservations[0].AmountUSD = blocked.Reservations[0].LimitUSD
				if result, err := m.Accept(ctx, blocked); err != nil || result.Accepted {
					t.Fatalf("denial = %#v, %v", result, err)
				}
				if _, err := m.Claim(ctx, claim(r)); err != nil {
					t.Fatal(err)
				}
				if err := m.Reconcile(ctx, settle(r, "0")); err != nil {
					t.Fatal(err)
				}
				rows := read(t, space, "")
				want := []BudgetStreamEventKind{BudgetEventReserve, BudgetEventDenial, BudgetEventClaim, BudgetEventRelease}
				if len(rows) != len(want) {
					t.Fatalf("events = %#v", rows)
				}
				for i, kind := range want {
					if rows[i].Event.Kind != kind {
						t.Fatalf("event %d = %#v", i, rows[i])
					}
				}
			})
			t.Run("expired reservation publishes cleanup", func(t *testing.T) {
				m, space, req := fixture(t)
				acquire(t, m, req)
				expiry := m.space.durableBudgetExpiryKey(req.Reservations[0].PolicyID, req.Reservations[0].WindowID)
				members, err := client.ZRange(ctx, expiry, 0, -1).Result()
				if err != nil || len(members) != 1 {
					t.Fatalf("expiry index = %#v, %v", members, err)
				}
				if err := client.ZAdd(ctx, expiry, redisclient.Z{Score: 1, Member: members[0]}).Err(); err != nil {
					t.Fatal(err)
				}
				req.OperationID = "after-expiry"
				acquire(t, m, req)
				rows := read(t, space, "")
				if len(rows) != 3 || rows[1].Event.Kind != BudgetEventExpire ||
					rows[1].Event.NanoDelta != 10_000_000 || rows[1].Event.OperationHash != "" ||
					rows[1].Event.MemberHash != rows[0].Event.MemberHash {
					t.Fatalf("expiry events = %#v", rows)
				}
			})
			t.Run("stream failures precede accounting writes", func(t *testing.T) {
				for _, failure := range []string{"wrong-type", "full-id", "acl"} {
					t.Run(failure, func(t *testing.T) {
						m, space, req := fixture(t)
						switch failure {
						case "wrong-type":
							if err := client.Set(ctx, space.EventsKey(), "not-a-stream", 0).Err(); err != nil {
								t.Fatal(err)
							}
						case "full-id":
							if err := client.XAdd(ctx, &redisclient.XAddArgs{Stream: space.EventsKey(), ID: "18446744073709551615-18446744073709551615", Values: map[string]any{"event": "{}"}}).Err(); err != nil {
								t.Fatal(err)
							}
						case "acl":
							username := m.space.prefix
							password := m.space.prefix
							if err := client.Do(ctx, "ACL", "SETUSER", username, "on", ">"+password, "~"+m.space.prefix+":*", "+@all", "-xadd").Err(); err != nil {
								t.Fatal(err)
							}
							t.Cleanup(func() { client.Do(ctx, "ACL", "DELUSER", username) })
							options := *client.Options()
							options.Username, options.Password = username, password
							restricted := redisclient.NewClient(&options)
							t.Cleanup(func() { _ = restricted.Close() })
							if got, err := restricted.Do(ctx, "ACL", "WHOAMI").Text(); err != nil || got != username {
								t.Fatalf("ACL fixture authenticated as %q: %v", got, err)
							}
							m.invoke = redisInvoker{client: restricted, mode: mode}
						}
						if _, err := m.Accept(ctx, req); err == nil {
							t.Fatal("broken Stream accepted a reservation")
						}
						count, err := client.Exists(ctx,
							m.space.durableBudgetOperationKey(string(req.GenerationID), string(req.OperationID)),
							m.space.durableBudgetKey(req.Reservations[0].PolicyID, req.Reservations[0].WindowID),
						).Result()
						if err != nil || count != 0 {
							t.Fatalf("accounting changed before Stream failure: %d, %v", count, err)
						}
					})
				}
			})
			t.Run("rejected reconciliation emits no success", func(t *testing.T) {
				m, space, req := fixture(t)
				r := acquire(t, m, req)
				completed := settle(r, "0.004")
				if err := m.Reconcile(ctx, completed); !errors.Is(err, durable.ErrClaimRequired) {
					t.Fatalf("unclaimed settlement = %v", err)
				}
				if rows := read(t, space, ""); len(rows) != 1 {
					t.Fatalf("rejected settlement published: %#v", rows)
				}
				if _, err := m.Claim(ctx, claim(r)); err != nil {
					t.Fatal(err)
				}
				key := m.space.durableBudgetOperationKey(string(req.GenerationID), string(req.OperationID))
				before, err := client.Get(ctx, key).Result()
				if err != nil {
					t.Fatal(err)
				}
				if err := client.Del(ctx, space.EventsKey()).Err(); err != nil {
					t.Fatal(err)
				}
				if err := client.Set(ctx, space.EventsKey(), "blocked", 0).Err(); err != nil {
					t.Fatal(err)
				}
				if err := m.Reconcile(ctx, completed); err == nil {
					t.Fatal("settlement accepted a broken Stream")
				}
				after, err := client.Get(ctx, key).Result()
				if err != nil || before != after || !json.Valid([]byte(after)) {
					t.Fatalf("rejected settlement changed its record: %v", err)
				}
			})
		})
	}
}
