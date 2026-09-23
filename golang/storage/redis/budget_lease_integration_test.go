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

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
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
				m, err := NewRedisBudgetMaterializer(RedisBudgetMaterializerOptions{Client: client, Mode: mode, Keys: keys, GenerationID: "lease-gen", IncarnationID: "lease-inc"})
				if err != nil {
					t.Fatal(err)
				}
				return m
			}
			request := func(id, amount string) durable.ReserveRequest {
				now := time.Now().UTC()
				return durable.ReserveRequest{OperationID: durable.OperationID(id), GenerationID: "lease-gen", Reservations: []admission.WindowReservation{{PolicyID: "tenant", WindowID: "hour", Bucket: now.Unix() / 60, BucketNanos: int64(time.Minute), DurationNanos: int64(time.Hour), AmountUSD: pricing.MustUSD(amount), LimitUSD: pricing.MustUSD("1")}}}
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
				r := acquire(t, m, req)
				blocked, err := m.Accept(ctx, request("blocked", "0.3"))
				if err != nil || blocked.Accepted {
					t.Fatalf("reservation lost after reply: %#v %v", blocked, err)
				}
				m.invoke = &lostBudgetReply{delegate: m.invoke, action: "durable_claim"}
				if _, err := m.Claim(ctx, claim(r)); err == nil {
					t.Fatal("expected lost claim reply")
				}
				if _, err := m.Claim(ctx, claim(r)); !errors.Is(err, durable.ErrAlreadyClaimed) {
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
				acquire(t, m, request("remaining", "0.7"))
				if err := m.Reconcile(ctx, completed); err != nil {
					t.Fatal(err)
				}
				blocked, err = m.Accept(ctx, request("no-double-refund", "0.01"))
				if err != nil || blocked.Accepted {
					t.Fatalf("double refund: %#v %v", blocked, err)
				}
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
					acquire(t, m, req)
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
					acquire(t, m, req)
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
				r := acquire(t, m, original)
				waiting := request("waiting-release", "1")
				result, err := m.Accept(ctx, waiting)
				if err != nil || result.Accepted {
					t.Fatalf("waiting request: %#v %v", result, err)
				}
				released := settle(r, "0")
				released.Events[0].Kind = budget.JournalRelease
				if err := m.Reconcile(ctx, released); err != nil {
					t.Fatal(err)
				}
				if err := m.Reconcile(ctx, released); err != nil {
					t.Fatal(err)
				}
				acquire(t, m, waiting)
				if _, err := m.Claim(ctx, claim(r)); !errors.Is(err, ErrRedisBudgetReservationFinalized) {
					t.Fatalf("released claim: %v", err)
				}
				acquire(t, m, original)
				if err := m.Reconcile(ctx, released); err != nil {
					t.Fatal(err)
				}
				result, err = m.Accept(ctx, request("no-extra-refund", "0.1"))
				if err != nil || result.Accepted {
					t.Fatalf("release replay refunded another lease: %#v %v", result, err)
				}
			})
			t.Run("concurrent settlements and conflicting retries cannot double refund", func(t *testing.T) {
				m := newMaterializer(t)
				r := acquire(t, m, request("settlement-race", "1"))
				if _, err := m.Claim(ctx, claim(r)); err != nil {
					t.Fatal(err)
				}
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
				acquire(t, m, request("remaining-after-race", "0.4"))
				result, err := m.Accept(ctx, request("no-double-settlement", "0.01"))
				if err != nil || result.Accepted {
					t.Fatalf("double settlement: %#v %v", result, err)
				}
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
				acquire(t, m, request("unaffected-first-window", "1"))
			})
			t.Run("only one worker may claim", func(t *testing.T) {
				m := newMaterializer(t)
				r := acquire(t, m, request("concurrent", "1"))
				var wg sync.WaitGroup
				var starts atomic.Int64
				for i := 0; i < 24; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						_, err := m.Claim(ctx, claim(r))
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
				unused := acquire(t, m, unusedReq)
				used := acquire(t, m, usedReq)
				if _, err := m.Claim(ctx, claim(used)); err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Until(unusedReq.ExpiresAt) + 20*time.Millisecond)
				if _, err := m.Claim(ctx, claim(unused)); !errors.Is(err, durable.ErrLeaseExpired) {
					t.Fatalf("expired claim: %v", err)
				}
				// Reusing the same waiting operation succeeds once capacity is reclaimed.
				acquire(t, m, request("replacement", "0.4"))
				release := settle(unused, "0")
				release.Events[0].Kind = budget.JournalRelease
				if err := m.Reconcile(ctx, release); !errors.Is(err, ErrRedisBudgetReservationNotFound) {
					t.Fatalf("expired release: %v", err)
				}
				denied, err := m.Accept(ctx, request("cannot-refund-paid", "0.01"))
				if err != nil || denied.Accepted {
					t.Fatalf("paid work expired: %#v %v", denied, err)
				}
				completed := settle(used, "0.6")
				if err := m.Reconcile(ctx, completed); err != nil {
					t.Fatal(err)
				}
				denied, err = m.Accept(ctx, request("cost-still-counts", "0.01"))
				if err != nil || denied.Accepted {
					t.Fatalf("paid cost expired with start lease: %#v %v", denied, err)
				}
				// Retrying expired acquisition returns the old lease, never a new authorization.
				acquire(t, m, unusedReq)
				if _, err := m.Claim(ctx, claim(unused)); !errors.Is(err, durable.ErrLeaseExpired) {
					t.Fatalf("resurrected lease: %v", err)
				}
			})
			t.Run("paid work survives window rollover until settlement", func(t *testing.T) {
				m := newMaterializer(t)
				req := request("slow-provider", "0.8")
				req.Reservations[0].BucketNanos = int64(time.Second)
				req.Reservations[0].DurationNanos = int64(2 * time.Second)
				req.Reservations[0].Bucket = time.Now().Unix()
				r := acquire(t, m, req)
				if _, err := m.Claim(ctx, claim(r)); err != nil {
					t.Fatal(err)
				}
				windowEnd := time.Unix(req.Reservations[0].Bucket+3, 0)
				time.Sleep(time.Until(windowEnd) + 20*time.Millisecond)
				result, err := m.Accept(ctx, request("still-paid", "0.3"))
				if err != nil || result.Accepted {
					t.Fatalf("window rollover abandoned paid work: %#v %v", result, err)
				}
				completed := settle(r, "0.8")
				if err := m.Reconcile(ctx, completed); err != nil {
					t.Fatal(err)
				}
				// A known cost belongs to the original window, which has now expired.
				acquire(t, m, request("after-cost-expiry", "1"))
				if err := m.Reconcile(ctx, completed); err != nil {
					t.Fatal(err)
				}
				result, err = m.Accept(ctx, request("no-expired-refund", "0.01"))
				if err != nil || result.Accepted {
					t.Fatalf("late settlement refunded another request: %#v %v", result, err)
				}
			})
			t.Run("wait can retry and policy reload preserves spend", func(t *testing.T) {
				m := newMaterializer(t)
				r := acquire(t, m, request("existing", "0.8"))
				if _, err := m.Claim(ctx, claim(r)); err != nil {
					t.Fatal(err)
				}
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
				acquire(t, &reloaded, retry)
				tooLarge := request("over-limit", "2")
				result, err = m.Accept(ctx, tooLarge)
				if err != nil || result.Accepted || result.RetryAfter <= 0 {
					t.Fatalf("over-limit must wait: %#v %v", result, err)
				}
			})
			t.Run("ambiguous paid attempt requires another reservation", func(t *testing.T) {
				m := newMaterializer(t)
				r := acquire(t, m, request("unknown", "0.8"))
				if _, err := m.Claim(ctx, claim(r)); err != nil {
					t.Fatal(err)
				}
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
				retry, err := m.Accept(ctx, request("paid-retry", "0.8"))
				if err != nil || retry.Accepted {
					t.Fatalf("retry reused ambiguous reservation: %#v %v", retry, err)
				}
				corrected := settle(r, "0.1")
				corrected.Events[0].EventID = "unknown-exact-correction"
				corrected.Events[0].Kind = budget.JournalResolveUnknownExact
				corrected.Events[0].ReservationRevision = 3
				if err := m.Reconcile(ctx, corrected); err != nil {
					t.Fatal(err)
				}
				acquire(t, m, request("paid-retry", "0.8"))
			})
			t.Run("conflicting multiwindow settlement changes no window", func(t *testing.T) {
				m := newMaterializer(t)
				req := request("multi", "0.8")
				second := req.Reservations[0]
				second.WindowID = "day"
				second.DurationNanos = int64(24 * time.Hour)
				req.Reservations = append(req.Reservations, second)
				r := acquire(t, m, req)
				if err := m.Reconcile(ctx, settle(r, "0.2")); !errors.Is(err, durable.ErrClaimRequired) {
					t.Fatalf("unclaimed settlement: %v", err)
				}
				if _, err := m.Claim(ctx, claim(r)); err != nil {
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
					result, err := m.Accept(ctx, test)
					if err != nil || result.Accepted {
						t.Fatalf("partial settlement: %#v %v", result, err)
					}
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
