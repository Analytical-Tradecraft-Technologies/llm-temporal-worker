//go:build integration

package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

// Drop the response only after the real Redis mutation succeeded.
type lostBudgetReply struct {
	delegate FunctionInvoker
	action   string
	lost     bool
}

func (f *lostBudgetReply) Run(ctx context.Context, name string, keys []string, args ...string) ([]any, error) {
	result, err := f.delegate.Run(ctx, name, keys, args...)
	if err == nil && !f.lost && args[0] == f.action {
		f.lost = true
		return nil, errors.New("simulated lost Redis reply")
	}
	return result, err
}

func TestLiveRedisBudgetLease(t *testing.T) {
	client := openLiveRedis(t)
	ctx := context.Background()
	if err := client.ScriptLoad(ctx, AdmissionLuaSource()).Err(); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []AdmissionMode{AdmissionModeFunction, AdmissionModeLua} {
		t.Run(string(mode), func(t *testing.T) {
			newMaterializer := func(t *testing.T) *RedisBudgetMaterializer {
				t.Helper()
				keys := liveKeyOptions("budget-lease")
				cleanupLivePrefix(t, client, keys.Prefix)
				m, err := NewRedisBudgetMaterializer(RedisBudgetMaterializerOptions{
					Client: client, Mode: mode, Keys: keys, GenerationID: "lease-gen", IncarnationID: "lease-inc",
					CoordinationStreamEnabled: true,
				})
				if err != nil {
					t.Fatal(err)
				}
				return m
			}
			request := func(id, amount string) durable.ReserveRequest {
				now := time.Now().UTC()
				return durable.ReserveRequest{OperationID: durable.OperationID(id), GenerationID: "lease-gen", Reservations: []admission.WindowReservation{{PolicyID: "tenant", WindowID: "hour", Bucket: now.Unix() / 60, BucketNanos: int64(time.Minute), DurationNanos: int64(time.Hour), AmountUSD: pricing.MustUSD(amount), LimitUSD: pricing.MustUSD("1")}}}
			}
			acquireClaimed := func(t *testing.T, m *RedisBudgetMaterializer, req durable.ReserveRequest) durable.ReserveResult {
				t.Helper()
				result := acquireLive(t, m, req)
				if _, err := m.Claim(ctx, claimOf(result)); err != nil {
					t.Fatal(err)
				}
				return result
			}
			// deny requires req to be refused without an error.
			deny := func(t *testing.T, m *RedisBudgetMaterializer, req durable.ReserveRequest, what string) {
				t.Helper()
				result, err := m.Accept(ctx, req)
				if err != nil || result.Accepted {
					t.Fatalf("%s: %#v %v", what, result, err)
				}
			}
			settle := func(r durable.ReserveResult, amount string) durable.ReconcileRequest {
				cost := pricing.MustUSD(amount)
				result := durable.ReconcileRequest{OperationID: r.OperationID, GenerationID: r.GenerationID, IncarnationID: r.IncarnationID}
				for i, e := range r.Events {
					result.Events = append(result.Events, budget.CompletionEvent{EventID: fmt.Sprintf("%s-complete-%d", r.OperationID, i), GenerationID: string(r.GenerationID), OperationID: string(r.OperationID), WindowID: e.WindowID, BucketStart: e.BucketStart, ReservationRevision: 2, Kind: budget.JournalFinalizeExact, ReservedDecreaseUSD: e.AmountUSD, AccountedIncreaseUSD: cost, ActualCostUSD: &cost, CostStatus: budget.CostExact, OccurredAt: time.Now().UTC()})
				}
				return result
			}
			t.Run("lost replies cannot duplicate debits, starts or refunds", func(t *testing.T) {
				m := newMaterializer(t)
				req := request("lost", "0.8")
				m.invoke = &lostBudgetReply{delegate: m.invoke, action: "durable_reserve"}
				if _, err := m.Accept(ctx, req); err == nil {
					t.Fatal("expected lost reserve reply")
				}
				r := acquireLive(t, m, req)
				deny(t, m, request("blocked", "0.3"), "reservation lost after reply")
				m.invoke = &lostBudgetReply{delegate: m.invoke, action: "durable_claim"}
				if _, err := m.Claim(ctx, claimOf(r)); err == nil {
					t.Fatal("expected lost claim reply")
				}
				if _, err := m.Claim(ctx, claimOf(r)); !errors.Is(err, durable.ErrAlreadyClaimed) {
					t.Fatalf("claim replay = %v", err)
				}
				m.invoke = &lostBudgetReply{delegate: m.invoke, action: "durable_reconcile"}
				completed := settle(r, "0.3")
				if err := m.Reconcile(ctx, completed); err == nil {
					t.Fatal("expected lost settlement reply")
				}
				if err := m.Reconcile(ctx, completed); err != nil {
					t.Fatal(err)
				}
				acquireLive(t, m, request("remaining", "0.7"))
				if err := m.Reconcile(ctx, completed); err != nil {
					t.Fatal(err)
				}
				deny(t, m, request("no-double-refund", "0.01"), "double refund")
				key := m.space.durableBudgetOperationKey("lease-gen", "lost")
				if ttl := client.TTL(ctx, key).Val(); ttl != -1 {
					t.Fatalf("lease tombstone TTL = %v", ttl)
				}
			})

			t.Run("Redis caps the start deadline and replay never renews it", func(t *testing.T) {
				for _, explicit := range []bool{false, true} {
					m := newMaterializer(t)
					// Even a fast worker clock cannot extend Redis's authorization deadline.
					m.clock = func() time.Time { return time.Now().Add(time.Hour) }
					req := request("deadline", "0.1")
					if explicit {
						req.ExpiresAt = time.Now().Add(24 * time.Hour)
					}
					before := time.Now()
					acquireLive(t, m, req)
					after := time.Now()
					key := m.space.durableBudgetOperationKey("lease-gen", "deadline")
					raw, err := client.Get(ctx, key).Result()
					if err != nil {
						t.Fatal(err)
					}
					var record durableOperation
					if err := json.Unmarshal([]byte(raw), &record); err != nil {
						t.Fatal(err)
					}
					deadline := time.UnixMilli(record.StartByMillis)
					if deadline.Before(before.Add(durable.BudgetStartLease-time.Second)) || deadline.After(after.Add(durable.BudgetStartLease+time.Second)) {
						t.Fatalf("deadline = %v", deadline)
					}
					m.clock = func() time.Time { return time.Now().Add(2 * time.Hour) }
					acquireLive(t, m, req)
					replay, err := client.Get(ctx, key).Result()
					if err != nil {
						t.Fatal(err)
					}
					if replay != raw {
						t.Fatal("replay changed or extended the lease")
					}
				}
			})
			t.Run("unused release is idempotent and allows a waiting request", func(t *testing.T) {
				m := newMaterializer(t)
				original := request("unused-release", "1")
				r := acquireLive(t, m, original)
				waiting := request("waiting-release", "1")
				deny(t, m, waiting, "waiting request")
				released := settle(r, "0")
				released.Events[0].Kind = budget.JournalRelease
				if err := m.Reconcile(ctx, released); err != nil {
					t.Fatal(err)
				}
				if err := m.Reconcile(ctx, released); err != nil {
					t.Fatal(err)
				}
				acquireLive(t, m, waiting)
				if _, err := m.Claim(ctx, claimOf(r)); !errors.Is(err, ErrRedisBudgetReservationFinalized) {
					t.Fatalf("released claim: %v", err)
				}
				acquireLive(t, m, original)
				if err := m.Reconcile(ctx, released); err != nil {
					t.Fatal(err)
				}
				deny(t, m, request("no-extra-refund", "0.1"), "release replay refunded another lease")
			})
			t.Run("concurrent settlements and conflicting retries cannot double refund", func(t *testing.T) {
				m := newMaterializer(t)
				r := acquireClaimed(t, m, request("settlement-race", "1"))
				completed := settle(r, "0.6")
				var wg sync.WaitGroup
				for i := 0; i < 24; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						if err := m.Reconcile(ctx, completed); err != nil {
							t.Errorf("settlement: %v", err)
						}
					}()
				}
				wg.Wait()
				changed := settle(r, "0.1")
				if err := m.Reconcile(ctx, changed); !errors.Is(err, ErrRedisBudgetConflict) {
					t.Fatalf("changed event accepted: %v", err)
				}
				acquireLive(t, m, request("remaining-after-race", "0.4"))
				deny(t, m, request("no-double-settlement", "0.01"), "double settlement")
			})
			t.Run("malformed second window cannot partially reserve", func(t *testing.T) {
				m := newMaterializer(t)
				req := request("wrong-type", "0.8")
				second := req.Reservations[0]
				second.WindowID = "z-second"
				req.Reservations = append(req.Reservations, second)
				corruptKey := m.space.durableBudgetExpiryKey("tenant", "z-second")
				if err := client.Set(ctx, corruptKey, "unexpected string", 0).Err(); err != nil {
					t.Fatal(err)
				}
				if _, err := m.Accept(ctx, req); !errors.Is(err, ErrUnavailable) {
					t.Fatalf("wrong Redis key type: %v", err)
				}
				acquireLive(t, m, request("unaffected-first-window", "1"))
			})
			t.Run("only one worker may claim", func(t *testing.T) {
				m := newMaterializer(t)
				r := acquireLive(t, m, request("concurrent", "1"))
				var wg sync.WaitGroup
				var starts atomic.Int64
				for i := 0; i < 24; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						_, err := m.Claim(ctx, claimOf(r))
						if err == nil {
							starts.Add(1)
						} else if !errors.Is(err, durable.ErrAlreadyClaimed) {
							t.Errorf("claim: %v", err)
						}
					}()
				}
				wg.Wait()
				if starts.Load() != 1 {
					t.Fatalf("starts = %d", starts.Load())
				}
			})
			t.Run("expiry refunds unused leases but retains started work and cost", func(t *testing.T) {
				m := newMaterializer(t)
				unusedReq := request("unused", "0.4")
				unusedReq.ExpiresAt = time.Now().Add(2 * time.Second)
				usedReq := request("started", "0.6")
				usedReq.ExpiresAt = unusedReq.ExpiresAt
				unused := acquireLive(t, m, unusedReq)
				used := acquireClaimed(t, m, usedReq)
				time.Sleep(time.Until(unusedReq.ExpiresAt) + 20*time.Millisecond)
				if _, err := m.Claim(ctx, claimOf(unused)); !errors.Is(err, durable.ErrLeaseExpired) {
					t.Fatalf("expired claim: %v", err)
				}
				// Reusing the same waiting operation succeeds once capacity is reclaimed.
				acquireLive(t, m, request("replacement", "0.4"))
				release := settle(unused, "0")
				release.Events[0].Kind = budget.JournalRelease
				if err := m.Reconcile(ctx, release); !errors.Is(err, ErrRedisBudgetReservationNotFound) {
					t.Fatalf("expired release: %v", err)
				}
				deny(t, m, request("cannot-refund-paid", "0.01"), "paid work expired")
				completed := settle(used, "0.6")
				if err := m.Reconcile(ctx, completed); err != nil {
					t.Fatal(err)
				}
				deny(t, m, request("cost-still-counts", "0.01"), "paid cost expired with start lease")
				// Retrying expired acquisition returns the old lease, never a new authorization.
				acquireLive(t, m, unusedReq)
				if _, err := m.Claim(ctx, claimOf(unused)); !errors.Is(err, durable.ErrLeaseExpired) {
					t.Fatalf("resurrected lease: %v", err)
				}
			})
			t.Run("replay decides a claim's outcome after its deadline", func(t *testing.T) {
				// A worker resolves an uncertain claim by replaying it (#1312).
				// durable_claim reports already_claimed before it checks the
				// deadline, so a lost claim reply stays provable, and expired
				// proves the lease was never claimed.
				m := newMaterializer(t)
				short := func(req durable.ReserveRequest, now time.Time) durable.ReserveRequest {
					req.Reservations[0].BucketNanos = int64(time.Second)
					req.Reservations[0].DurationNanos = int64(2 * time.Second)
					req.Reservations[0].Bucket = now.Unix()
					req.ExpiresAt = now.Add(2 * time.Second)
					return req
				}
				// Both leases share one deadline so the sleep below cannot end
				// before the later one expires, however slow the acquire round
				// trips are (#1524).
				now := time.Now()
				lostReq := short(request("lost-claim", "0.6"), now)
				unclaimedReq := short(request("unclaimed", "0.4"), now)
				lost := acquireLive(t, m, lostReq)
				unclaimed := acquireLive(t, m, unclaimedReq)
				invoke := m.invoke
				m.invoke = &lostBudgetReply{delegate: invoke, action: "durable_claim"}
				if _, err := m.Claim(ctx, claimOf(lost)); err == nil {
					t.Fatal("expected lost claim reply")
				}
				m.invoke = invoke
				time.Sleep(time.Until(unclaimedReq.ExpiresAt) + 250*time.Millisecond)
				if _, err := m.Claim(ctx, claimOf(lost)); !errors.Is(err, durable.ErrAlreadyClaimed) {
					t.Fatalf("replayed applied claim: %v", err)
				}
				for range 2 {
					if _, err := m.Claim(ctx, claimOf(unclaimed)); !errors.Is(err, durable.ErrLeaseExpired) {
						t.Fatalf("replayed unclaimed lease: %v", err)
					}
				}
				// The applied claim holds its reservation until it is settled;
				// the expired, unclaimed lease does not.
				deny(t, m, short(request("held", "0.5"), time.Now()), "lost claim released before settlement")
				// Charged at its full reservation, the claim ages out with its window.
				unknown := settle(lost, "0.6")
				event := &unknown.Events[0]
				event.Kind, event.CostStatus, event.ActualCostUSD, event.UnknownReasonCode = budget.JournalFinalizeUnknown, budget.CostUnknown, nil, "provider_cost_unknown"
				if err := m.Reconcile(ctx, unknown); err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Until(time.Unix(lostReq.Reservations[0].Bucket+3, 0)) + 20*time.Millisecond)
				acquireLive(t, m, request("after-rollover", "1"))
			})
			t.Run("paid work survives window rollover until settlement", func(t *testing.T) {
				m := newMaterializer(t)
				req := request("slow-provider", "0.8")
				req.Reservations[0].BucketNanos = int64(time.Second)
				req.Reservations[0].DurationNanos = int64(2 * time.Second)
				req.Reservations[0].Bucket = time.Now().Unix()
				r := acquireClaimed(t, m, req)
				windowEnd := time.Unix(req.Reservations[0].Bucket+3, 0)
				time.Sleep(time.Until(windowEnd) + 20*time.Millisecond)
				deny(t, m, request("still-paid", "0.3"), "window rollover abandoned paid work")
				completed := settle(r, "0.8")
				if err := m.Reconcile(ctx, completed); err != nil {
					t.Fatal(err)
				}
				// A known cost belongs to the original window, which has now expired.
				acquireLive(t, m, request("after-cost-expiry", "1"))
				if err := m.Reconcile(ctx, completed); err != nil {
					t.Fatal(err)
				}
				deny(t, m, request("no-expired-refund", "0.01"), "late settlement refunded another request")
			})
			t.Run("wait can retry and policy reload preserves spend", func(t *testing.T) {
				m := newMaterializer(t)
				r := acquireClaimed(t, m, request("existing", "0.8"))
				if err := m.Reconcile(ctx, settle(r, "0.8")); err != nil {
					t.Fatal(err)
				}
				// New process-local materializer, unchanged Redis identity/keyspace.
				reloaded := *m
				retry := request("waiting", "0.3")
				retry.Reservations[0].LimitUSD = pricing.MustUSD("0.5")
				result, err := reloaded.Accept(ctx, retry)
				if err != nil || result.Accepted || result.RetryAfter <= 0 {
					t.Fatalf("lowered policy: %#v %v", result, err)
				}
				retry.Reservations[0].LimitUSD = pricing.MustUSD("1.1")
				acquireLive(t, &reloaded, retry)
				tooLarge := request("over-limit", "2")
				result, err = m.Accept(ctx, tooLarge)
				if err != nil || result.Accepted || result.RetryAfter <= 0 {
					t.Fatalf("over-limit must wait: %#v %v", result, err)
				}
			})
			t.Run("ambiguous paid attempt requires another reservation", func(t *testing.T) {
				m := newMaterializer(t)
				r := acquireClaimed(t, m, request("unknown", "0.8"))
				retained := settle(r, "0")
				e := &retained.Events[0]
				e.Kind = budget.JournalRetainAmbiguous
				e.ReservedDecreaseUSD = pricing.MustUSD("0")
				e.ActualCostUSD = nil
				e.CostStatus = budget.CostUnknown
				e.UnknownReasonCode = "provider_reply_lost"
				if err := m.Reconcile(ctx, retained); err != nil {
					t.Fatal(err)
				}
				if err := m.Reconcile(ctx, retained); err != nil {
					t.Fatal(err)
				}
				deny(t, m, request("paid-retry", "0.8"), "retry reused ambiguous reservation")
				corrected := settle(r, "0.1")
				corrected.Events[0].EventID = "unknown-exact-correction"
				corrected.Events[0].Kind = budget.JournalResolveUnknownExact
				corrected.Events[0].ReservationRevision = 3
				if err := m.Reconcile(ctx, corrected); err != nil {
					t.Fatal(err)
				}
				acquireLive(t, m, request("paid-retry", "0.8"))
			})
			t.Run("conflicting multiwindow settlement changes no window", func(t *testing.T) {
				m := newMaterializer(t)
				req := request("multi", "0.8")
				second := req.Reservations[0]
				second.WindowID = "day"
				second.DurationNanos = int64(24 * time.Hour)
				req.Reservations = append(req.Reservations, second)
				r := acquireLive(t, m, req)
				if err := m.Reconcile(ctx, settle(r, "0.2")); !errors.Is(err, durable.ErrClaimRequired) {
					t.Fatalf("unclaimed settlement: %v", err)
				}
				if _, err := m.Claim(ctx, claimOf(r)); err != nil {
					t.Fatal(err)
				}
				completed := settle(r, "0.2")
				bad := completed
				bad.Events = append([]budget.CompletionEvent(nil), completed.Events...)
				bad.Events[1].ReservedDecreaseUSD = pricing.MustUSD("0.9")
				if err := m.Reconcile(ctx, bad); err == nil {
					t.Fatal("accepted conflicting batch")
				}
				partial := completed
				partial.Events = append([]budget.CompletionEvent(nil), completed.Events...)
				partial.Events[1].ReservedDecreaseUSD = pricing.MustUSD("0.1")
				if err := m.Reconcile(ctx, partial); !errors.Is(err, ErrRedisBudgetConflict) {
					t.Fatalf("partial reservation removal accepted: %v", err)
				}
				duplicate := completed
				duplicate.Events = []budget.CompletionEvent{completed.Events[0], completed.Events[0]}
				if err := m.Reconcile(ctx, duplicate); err == nil {
					t.Fatal("duplicate batch event accepted")
				}
				for _, reservation := range req.Reservations {
					test := request("blocked-"+reservation.WindowID, "0.3")
					test.Reservations[0].WindowID = reservation.WindowID
					deny(t, m, test, "partial settlement")
				}
				if err := m.Reconcile(ctx, completed); err != nil {
					t.Fatal(err)
				}
				if err := m.Reconcile(ctx, completed); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}
