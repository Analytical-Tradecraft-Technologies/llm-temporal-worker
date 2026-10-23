//go:build integration

package redis

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
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
			t.Run("persistent history becomes bounded and evictable", func(t *testing.T) {
				m, space, req := fixture(t)
				pipe := client.Pipeline()
				for range 12000 {
					pipe.XAdd(ctx, &redisclient.XAddArgs{Stream: space.EventsKey(), Values: map[string]any{"event": "old-hint"}})
				}
				if _, err := pipe.Exec(ctx); err != nil {
					t.Fatal(err)
				}
				oldID := client.XInfoStream(ctx, space.EventsKey()).Val().FirstEntry.ID
				r := acquire(t, m, req)
				if n := client.XLen(ctx, space.EventsKey()).Val(); n > 10100 {
					t.Fatalf("unbounded hints: %d", n)
				}
				if ttl := client.TTL(ctx, space.EventsKey()).Val(); ttl <= 0 || ttl > 24*time.Hour {
					t.Fatalf("hint TTL = %v", ttl)
				}
				port, err := NewRedisBudgetEventPort(client, space)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := port.Read(ctx, oldID, 10); !errors.Is(err, ErrBudgetStreamGap) {
					t.Fatalf("trim recovery: %v", err)
				}
				if err := client.PExpire(ctx, space.EventsKey(), time.Millisecond).Err(); err != nil {
					t.Fatal(err)
				}
				deadline := time.Now().Add(time.Second)
				for client.Exists(ctx, space.EventsKey()).Val() != 0 {
					if time.Now().After(deadline) {
						t.Fatal("stream did not expire")
					}
					time.Sleep(time.Millisecond)
				}
				cursor, err := port.CurrentCursor(ctx)
				if err != nil || cursor != "0-0" {
					t.Fatalf("expired stream cursor: %q, %v", cursor, err)
				}
				if _, err := m.Claim(ctx, claim(r)); err != nil {
					t.Fatalf("claim after hint expiry: %v", err)
				}
				if _, err := m.Claim(ctx, claim(r)); !errors.Is(err, durable.ErrAlreadyClaimed) {
					t.Fatalf("hint expiry reopened dispatch: %v", err)
				}
				rows, err := port.Read(ctx, cursor, 10)
				if err != nil || len(rows) != 1 || rows[0].Event.Kind != BudgetEventClaim {
					t.Fatalf("recreated hints: %#v, %v", rows, err)
				}
				if client.TTL(ctx, space.EventsKey()).Val() <= 0 {
					t.Fatal("recreated stream is persistent")
				}
			})
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
			t.Run("wildcard IDs carry an exhausted sequence into the next millisecond", func(t *testing.T) {
				m, space, req := fixture(t)
				future := time.Now().Add(time.Hour).UnixMilli()
				seed := strconv.FormatInt(future, 10) + "-18446744073709551615"
				if err := client.XAdd(ctx, &redisclient.XAddArgs{Stream: space.EventsKey(), ID: seed, Values: map[string]any{"event": "{}"}}).Err(); err != nil {
					t.Fatal(err)
				}
				// XADD * can advance the millisecond component. It differs from
				// XADD <milliseconds>-*, which must fail when its sequence fills.
				r := acquire(t, m, req)
				if _, err := m.Claim(ctx, claim(r)); err != nil {
					t.Fatal(err)
				}
				if err := m.Reconcile(ctx, settle(r, "0.004")); err != nil {
					t.Fatal(err)
				}
				rows, err := client.XRange(ctx, space.EventsKey(), "("+seed, "+").Result()
				if err != nil || len(rows) != 3 {
					t.Fatalf("accounting events after sequence rollover: %#v, %v", rows, err)
				}
				for i, kind := range []BudgetStreamEventKind{BudgetEventReserve, BudgetEventClaim, BudgetEventReconcile} {
					var event BudgetStreamEvent
					wire, ok := rows[i].Values["event"].(string)
					wantID := strconv.FormatInt(future+1, 10) + "-" + strconv.Itoa(i)
					if !ok || json.Unmarshal([]byte(wire), &event) != nil || event.Validate() != nil || event.Kind != kind || rows[i].ID != wantID {
						t.Fatalf("event %d after sequence rollover = %#v", i, rows[i])
					}
				}
			})
			t.Run("stream failures precede accounting writes", func(t *testing.T) {
				for _, failure := range []string{"wrong-type", "full-id", "acl", "expire-acl"} {
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
						case "acl", "expire-acl":
							username := m.space.prefix
							password := m.space.prefix
							denied := "-xadd"
							if failure == "expire-acl" {
								denied = "-expire"
							}
							if err := client.Do(ctx, "ACL", "SETUSER", username, "on", ">"+password, "~"+m.space.prefix+":*", "+@all", denied).Err(); err != nil {
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

func TestLiveRedisBudgetHintPortRetention(t *testing.T) {
	client := openLiveRedis(t)
	ctx := context.Background()
	keys := liveKeyOptions("hint-port-retention")
	cleanupLivePrefix(t, client, keys.Prefix)
	space, err := NewBudgetKeySpace(keys)
	if err != nil {
		t.Fatal(err)
	}
	port, err := NewRedisBudgetEventPort(client, space)
	if err != nil {
		t.Fatal(err)
	}
	pipe := client.Pipeline()
	for range 12000 {
		pipe.XAdd(ctx, &redisclient.XAddArgs{Stream: space.EventsKey(), Values: map[string]any{"event": "old-hint"}})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	event := BudgetStreamEvent{Schema: budgetStreamEventSchema, Kind: BudgetEventPolicyRefresh,
		GenerationID: "generation", Revision: 1, OccurredAt: time.Now().UTC()}
	if _, err := port.Append(ctx, event); err != nil {
		t.Fatal(err)
	}
	if n := client.XLen(ctx, space.EventsKey()).Val(); n > 10100 {
		t.Fatalf("unbounded standalone hints: %d", n)
	}
	if ttl := client.TTL(ctx, space.EventsKey()).Val(); ttl <= 0 || ttl > 24*time.Hour {
		t.Fatalf("standalone hint TTL = %v", ttl)
	}
	oldCursor, err := port.CurrentCursor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate losing the optional hint key while durable state survives.
	if err := client.Del(ctx, space.EventsKey()).Err(); err != nil {
		t.Fatal(err)
	}
	tailer, err := NewBudgetStreamTailer(BudgetStreamTailerOptions{
		Port: port, Enabled: true, BatchSize: 10,
		Initial: BudgetStreamCursorState{GenerationID: "generation", Cursor: oldCursor},
		Reload: func(context.Context) (BudgetStreamCursorState, error) {
			// A publication racing the authoritative reload must remain visible.
			if _, err := port.Append(ctx, event); err != nil {
				return BudgetStreamCursorState{}, err
			}
			return BudgetStreamCursorState{GenerationID: "generation", Cursor: oldCursor}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := tailer.Poll(ctx); err != nil || !result.Reloaded || result.State.Cursor != "0-0" {
		t.Fatalf("reload after eviction = %#v, %v", result, err)
	}
	if result, err := tailer.Poll(ctx); err != nil || result.Records != 1 {
		t.Fatalf("publication during reload lost = %#v, %v", result, err)
	}
	// Lose and recreate the stream again, then exceed retention between the
	// authoritative reload and the next poll from its recovered zero cursor.
	if err := client.Del(ctx, space.EventsKey()).Err(); err != nil {
		t.Fatal(err)
	}
	if result, err := tailer.Poll(ctx); err != nil || !result.Reloaded || result.State.Cursor != "0-0" {
		t.Fatalf("second eviction recovery = %#v, %v", result, err)
	}
	payload, err := event.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	pipe = client.Pipeline()
	for range 12000 {
		pipe.XAdd(ctx, &redisclient.XAddArgs{Stream: space.EventsKey(), MaxLen: 10000, Approx: true,
			Values: map[string]any{"event": string(payload)}})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if result, err := tailer.Poll(ctx); err != nil || !result.Reloaded || result.Records != 0 || result.State.Cursor == "0-0" {
		t.Fatalf("zero cursor silently skipped discarded hints = %#v, %v", result, err)
	}
	if result, err := tailer.Poll(ctx); err != nil || result.Records != 1 {
		t.Fatalf("post-trim recovery missed concurrent publication = %#v, %v", result, err)
	}
}
