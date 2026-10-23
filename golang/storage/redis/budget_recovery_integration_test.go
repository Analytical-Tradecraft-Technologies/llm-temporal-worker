//go:build integration

package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
	redisclient "github.com/redis/go-redis/v9"
)

func TestLiveRedisBudgetRecovery(t *testing.T) {
	client := openLiveRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := client.ScriptLoad(ctx, AdmissionLuaSource()).Err(); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []AdmissionMode{AdmissionModeFunction, AdmissionModeLua} {
		t.Run(string(mode), func(t *testing.T) {
			type fixture struct {
				keys        KeyOptions
				initializer *RedisBudgetInitializer
				receipt     budget.Initialization
				m           *RedisBudgetMaterializer
				// durable mirrors the cloud execution records of every attempt.
				durable map[string]*BudgetRecoveryOperation
			}
			setup := func(t *testing.T) *fixture {
				t.Helper()
				keys := liveKeyOptions("budget-recovery")
				cleanupLivePrefix(t, client, keys.Prefix)
				initializer, err := NewRedisBudgetInitializer(client, keys, mode, AdmissionFunctionVersion)
				if err != nil {
					t.Fatal(err)
				}
				receipt := budget.Initialization{Schema: budget.InitializationSchema, Identity: initializer.Identity(), Epoch: "00000000-0000-4000-8000-000000000002", CreatedAt: time.Now().UTC()}
				if err := initializer.Initialize(ctx, budget.InitializationPreparation{Receipt: receipt, Created: true}); err != nil {
					t.Fatal(err)
				}
				receipt.Ready = true
				m, err := NewRedisBudgetMaterializer(RedisBudgetMaterializerOptions{
					Client: client, Mode: mode, Keys: keys, GenerationID: "recovery-gen", IncarnationID: "recovery-inc",
					CoordinationStreamEnabled: true, Initialization: &receipt,
				})
				if err != nil {
					t.Fatal(err)
				}
				return &fixture{keys: keys, initializer: initializer, receipt: receipt, m: m, durable: map[string]*BudgetRecoveryOperation{}}
			}
			request := func(id, amount string) durable.ReserveRequest {
				now := time.Now().UTC()
				return durable.ReserveRequest{OperationID: durable.OperationID(id), GenerationID: "recovery-gen", Reservations: []admission.WindowReservation{{PolicyID: "tenant", WindowID: "hour", Bucket: now.Unix() / 60, BucketNanos: int64(time.Minute), DurationNanos: int64(time.Hour), AmountUSD: pricing.MustUSD(amount), LimitUSD: pricing.MustUSD("1")}}}
			}
			settlement := func(r durable.ReserveResult, amount string) *durable.ReconcileRequest {
				cost := pricing.MustUSD(amount)
				result := &durable.ReconcileRequest{OperationID: r.OperationID, GenerationID: r.GenerationID, IncarnationID: r.IncarnationID}
				for i, e := range r.Events {
					result.Events = append(result.Events, budget.CompletionEvent{EventID: fmt.Sprintf("%s-complete-%d", r.OperationID, i), GenerationID: string(r.GenerationID), OperationID: string(r.OperationID), WindowID: e.WindowID, BucketStart: e.BucketStart, ReservationRevision: 2, Kind: budget.JournalFinalizeExact, ReservedDecreaseUSD: e.AmountUSD, AccountedIncreaseUSD: cost, ActualCostUSD: &cost, CostStatus: budget.CostExact, OccurredAt: time.Now().UTC()})
				}
				return result
			}
			// accept, claim and settle in the order production saves them:
			// the durable record precedes each Redis claim and settlement.
			accept := func(t *testing.T, f *fixture, id, amount string) durable.ReserveResult {
				t.Helper()
				req := request(id, amount)
				result, err := f.m.Accept(ctx, req)
				if err != nil || !result.Accepted {
					t.Fatalf("accept %s = %#v, %v", id, result, err)
				}
				if len(result.WindowExpiries) != len(result.Events) {
					t.Fatalf("acceptance expiry not recorded: %#v", result.WindowExpiries)
				}
				started := time.Now().UTC()
				f.durable[id] = &BudgetRecoveryOperation{Request: req, Reservation: result, Claimed: true, StartedAt: started, StartBy: started.Add(durable.BudgetStartLease + BudgetRecoveryClockSkew)}
				return result
			}
			unclaimed := func(id string, f *fixture) { f.durable[id].Claimed = false }
			claim := func(t *testing.T, f *fixture, r durable.ReserveResult) {
				t.Helper()
				if _, err := f.m.Claim(ctx, durable.ClaimRequest{OperationID: r.OperationID, GenerationID: r.GenerationID, IncarnationID: r.IncarnationID}); err != nil {
					t.Fatal(err)
				}
			}
			settle := func(t *testing.T, f *fixture, r durable.ReserveResult, amount string) {
				t.Helper()
				s := settlement(r, amount)
				f.durable[string(r.OperationID)].Settlement = s
				if err := f.m.Reconcile(ctx, *s); err != nil {
					t.Fatal(err)
				}
			}
			// Spend 0.3 settled, 0.2 claimed and unsettled, 0.1 unclaimed.
			populate := func(t *testing.T, f *fixture) map[string]durable.ReserveResult {
				t.Helper()
				settled := accept(t, f, "settled", "0.4")
				claim(t, f, settled)
				settle(t, f, settled, "0.3")
				claimed := accept(t, f, "claimed", "0.2")
				claim(t, f, claimed)
				open := accept(t, f, "open", "0.1")
				unclaimed("open", f)
				return map[string]durable.ReserveResult{"settled": settled, "claimed": claimed, "open": open}
			}
			recoverAll := func(t *testing.T, f *fixture, force bool) (restored int) {
				t.Helper()
				required, err := f.initializer.BeginRecovery(ctx, f.receipt, force)
				if err != nil || !required {
					t.Fatalf("begin recovery = %v, %v", required, err)
				}
				if _, _, err := f.initializer.HoldRecoveryLease(ctx, f.receipt, "token-a", 5*time.Second, ""); err != nil {
					t.Fatal(err)
				}
				for _, operation := range f.durable {
					changed, err := f.initializer.RecoverOperation(ctx, f.receipt, "token-a", *operation)
					if err != nil {
						t.Fatal(err)
					}
					if changed {
						restored++
					}
				}
				if err := f.initializer.CommitRecovery(ctx, f.receipt, time.Now()); err != nil {
					t.Fatal(err)
				}
				if err := f.initializer.Check(ctx, f.receipt); err != nil {
					t.Fatalf("authority not ready after recovery: %v", err)
				}
				return restored
			}
			// Remaining capacity of the 1 USD window must be exactly 0.4.
			requireRemaining := func(t *testing.T, f *fixture, label string) {
				t.Helper()
				denied, err := f.m.Accept(ctx, request(label+"-over", "0.41"))
				if err != nil || denied.Accepted {
					t.Fatalf("%s replenished budget: %#v, %v", label, denied, err)
				}
				fits, err := f.m.Accept(ctx, request(label+"-fits", "0.39"))
				if err != nil || !fits.Accepted {
					t.Fatalf("%s lost capacity: %#v, %v", label, fits, err)
				}
				if full, err := f.m.Accept(ctx, request(label+"-full", "0.02")); err != nil || full.Accepted {
					t.Fatalf("%s exceeded the limit: %#v, %v", label, full, err)
				}
			}

			t.Run("complete loss restores spend, claims and settlement fences", func(t *testing.T) {
				f := setup(t)
				results := populate(t, f)
				if err := deleteLivePrefix(ctx, client, f.keys.Prefix); err != nil {
					t.Fatal(err)
				}
				if err := f.initializer.Check(ctx, f.receipt); !errors.Is(err, ErrBudgetAuthorityUnavailable) {
					t.Fatalf("lost authority check = %v", err)
				}
				if required, err := f.initializer.BeginRecovery(ctx, f.receipt, false); err != nil || !required {
					t.Fatalf("begin = %v, %v", required, err)
				}
				if _, err := f.m.Accept(ctx, request("during", "0.01")); !errors.Is(err, ErrBudgetAuthorityUnavailable) {
					t.Fatalf("partially restored state served admission: %v", err)
				}
				if restored := recoverAll(t, f, false); restored != 3 {
					t.Fatalf("restored %d operations", restored)
				}
				// The settled event is deduplicated: no double charge or refund.
				if err := f.m.Reconcile(ctx, *f.durable["settled"].Settlement); err != nil {
					t.Fatalf("settlement replay = %v", err)
				}
				if _, err := f.m.Claim(ctx, durable.ClaimRequest{OperationID: "claimed", GenerationID: "recovery-gen", IncarnationID: "recovery-inc"}); !errors.Is(err, durable.ErrAlreadyClaimed) {
					t.Fatalf("restored claim reusable: %v", err)
				}
				if _, err := f.m.Claim(ctx, durable.ClaimRequest{OperationID: "open", GenerationID: "recovery-gen", IncarnationID: "recovery-inc"}); err != nil {
					t.Fatalf("unclaimed reservation not restored: %v", err)
				}
				changed := settlement(results["settled"], "0.1")
				if err := f.m.Reconcile(ctx, *changed); err == nil {
					t.Fatal("changed settlement accepted after recovery")
				}
				requireRemaining(t, f, "complete")
				if got := client.TTL(ctx, f.m.space.durableBudgetOperationKey("recovery-gen", "settled")).Val(); got != -1 {
					t.Fatalf("restored operation record expires: %v", got)
				}
				if got := client.TTL(ctx, f.initializer.keys.EventsKey()).Val(); got <= 0 {
					t.Fatalf("recovered stream is not evictable: %v", got)
				}
			})

			t.Run("marker-only loss and repeated recovery never double count", func(t *testing.T) {
				f := setup(t)
				populate(t, f)
				if err := client.Del(ctx, f.initializer.keys.AuthorityKey()).Err(); err != nil {
					t.Fatal(err)
				}
				if restored := recoverAll(t, f, false); restored != 0 {
					t.Fatalf("surviving records rewritten: %d", restored)
				}
				if restored := recoverAll(t, f, true); restored != 0 {
					t.Fatalf("forced replay rewrote records: %d", restored)
				}
				requireRemaining(t, f, "marker")
			})

			t.Run("failover that lost recent writes is repaired by forced recovery", func(t *testing.T) {
				f := setup(t)
				settled := accept(t, f, "settled", "0.4")
				claim(t, f, settled)
				snapshot := dumpLivePrefix(t, client, f.keys.Prefix)
				// Acknowledged on the old primary, lost by the failover.
				settle(t, f, settled, "0.3")
				claimed := accept(t, f, "claimed", "0.2")
				claim(t, f, claimed)
				accept(t, f, "open", "0.1")
				unclaimed("open", f)
				restoreLivePrefix(t, client, f.keys.Prefix, snapshot)
				if err := f.initializer.Check(ctx, f.receipt); err != nil {
					t.Fatalf("surviving marker = %v", err)
				}
				if required, err := f.initializer.BeginRecovery(ctx, f.receipt, false); err != nil || required {
					t.Fatalf("unforced recovery fenced a ready marker: %v, %v", required, err)
				}
				if restored := recoverAll(t, f, true); restored != 3 {
					t.Fatalf("restored %d operations", restored)
				}
				requireRemaining(t, f, "failover")
			})

			t.Run("interrupted recovery resumes behind the fence", func(t *testing.T) {
				f := setup(t)
				populate(t, f)
				if err := deleteLivePrefix(ctx, client, f.keys.Prefix); err != nil {
					t.Fatal(err)
				}
				if _, err := f.initializer.BeginRecovery(ctx, f.receipt, false); err != nil {
					t.Fatal(err)
				}
				if _, _, err := f.initializer.HoldRecoveryLease(ctx, f.receipt, "crashed", time.Second, `{"shard":3}`); err != nil {
					t.Fatal(err)
				}
				if _, err := f.initializer.RecoverOperation(ctx, f.receipt, "crashed", *f.durable["claimed"]); err != nil {
					t.Fatal(err)
				}
				// The crashed process never commits. Its fence survives.
				if err := f.initializer.Check(ctx, f.receipt); !errors.Is(err, ErrBudgetAuthorityUnavailable) {
					t.Fatalf("interrupted recovery published readiness: %v", err)
				}
				if _, _, err := f.initializer.HoldRecoveryLease(ctx, f.receipt, "successor", 5*time.Second, ""); !errors.Is(err, ErrBudgetRecoveryBusy) {
					t.Fatalf("lease stolen from a live holder: %v", err)
				}
				if _, err := f.initializer.RecoverOperation(ctx, f.receipt, "successor", *f.durable["open"]); !errors.Is(err, ErrBudgetRecoveryBusy) {
					t.Fatalf("non-holder merged: %v", err)
				}
				time.Sleep(1100 * time.Millisecond)
				if required, err := f.initializer.BeginRecovery(ctx, f.receipt, false); err != nil || !required {
					t.Fatalf("resume = %v, %v", required, err)
				}
				progress, _, err := f.initializer.HoldRecoveryLease(ctx, f.receipt, "successor", 5*time.Second, "")
				if err != nil || progress != `{"shard":3}` {
					t.Fatalf("progress = %q, %v", progress, err)
				}
				if err := f.initializer.ReleaseRecoveryLease(ctx, f.receipt, "successor"); err != nil {
					t.Fatal(err)
				}
				if restored := recoverAll(t, f, false); restored != 2 {
					t.Fatalf("resume restored %d operations", restored)
				}
				if got := client.Exists(ctx, f.initializer.keys.RecoveryProgressKey()).Val(); got != 0 {
					t.Fatal("recovery progress survived commit")
				}
				requireRemaining(t, f, "interrupted")
			})

			t.Run("concurrent startups elect one recoverer", func(t *testing.T) {
				f := setup(t)
				populate(t, f)
				if err := deleteLivePrefix(ctx, client, f.keys.Prefix); err != nil {
					t.Fatal(err)
				}
				var wg sync.WaitGroup
				var mu sync.Mutex
				holders := 0
				for worker := range 8 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						token := fmt.Sprintf("worker-%d", worker)
						if _, err := f.initializer.BeginRecovery(ctx, f.receipt, false); err != nil {
							t.Error(err)
							return
						}
						if _, ready, err := f.initializer.HoldRecoveryLease(ctx, f.receipt, token, 10*time.Second, ""); errors.Is(err, ErrBudgetRecoveryBusy) || ready {
							return
						} else if err != nil {
							t.Error(err)
							return
						}
						mu.Lock()
						holders++
						mu.Unlock()
						for _, operation := range f.durable {
							if _, err := f.initializer.RecoverOperation(ctx, f.receipt, token, *operation); err != nil {
								t.Error(err)
								return
							}
						}
						if err := f.initializer.CommitRecovery(ctx, f.receipt, time.Now()); err != nil {
							t.Error(err)
						}
					}()
				}
				wg.Wait()
				if holders != 1 {
					t.Fatalf("%d recoverers held the lease", holders)
				}
				if required, err := f.initializer.BeginRecovery(ctx, f.receipt, false); err != nil || required {
					t.Fatalf("after concurrent recovery = %v, %v", required, err)
				}
				requireRemaining(t, f, "concurrent")
			})

			t.Run("foreign or preparing markers are never recovered", func(t *testing.T) {
				f := setup(t)
				other := f.receipt
				other.Epoch = "00000000-0000-4000-8000-000000000003"
				if err := client.Set(ctx, f.initializer.keys.AuthorityKey(), "ready:"+other.Marker(), 0).Err(); err != nil {
					t.Fatal(err)
				}
				if _, err := f.initializer.BeginRecovery(ctx, f.receipt, true); !errors.Is(err, ErrBudgetAuthorityUnavailable) {
					t.Fatalf("foreign marker = %v", err)
				}
				if err := client.Set(ctx, f.initializer.keys.AuthorityKey(), "preparing:"+f.receipt.Marker(), 0).Err(); err != nil {
					t.Fatal(err)
				}
				if _, err := f.initializer.BeginRecovery(ctx, f.receipt, false); !errors.Is(err, ErrBudgetAuthorityUnavailable) {
					t.Fatalf("preparing marker = %v", err)
				}
			})
		})
	}
}

func dumpLivePrefix(t *testing.T, client *redisclient.Client, prefix string) map[string]string {
	t.Helper()
	ctx := context.Background()
	snapshot := map[string]string{}
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, prefix+"*", 256).Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			value, err := client.Dump(ctx, key).Result()
			if err != nil {
				t.Fatal(err)
			}
			snapshot[key] = value
		}
		if next == 0 {
			return snapshot
		}
		cursor = next
	}
}

func restoreLivePrefix(t *testing.T, client *redisclient.Client, prefix string, snapshot map[string]string) {
	t.Helper()
	ctx := context.Background()
	if err := deleteLivePrefix(ctx, client, prefix); err != nil {
		t.Fatal(err)
	}
	for key, value := range snapshot {
		if err := client.Restore(ctx, key, 0, value).Err(); err != nil {
			t.Fatal(err)
		}
	}
}
