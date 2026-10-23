package conformance

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
)

// RunDurableRollingBudget exercises identical rolling-window behavior in the
// reference model and live Redis. Each factory call must return isolated state.
// It is the conformance suite the production Redis Function and the reference
// model share (#1498), so a lifecycle rule tested for only one of them belongs
// here.
func RunDurableRollingBudget(t *testing.T, newMaterializer func(*testing.T) (durable.BudgetMaterializer, func() time.Time, func(time.Duration))) {
	for _, scenario := range []string{"reserved", "accounted", "batch", "concurrent", "concurrent-overlapping", "isolation", "expiry", "claim-expiry", "finalize-unknown", "duplicate-settlement"} {
		t.Run(scenario, func(t *testing.T) {
			m, clock, advance := newMaterializer(t)
			ctx := context.Background()
			now := clock()
			reservation := func(offset int64, amount string) admission.WindowReservation {
				return admission.WindowReservation{PolicyID: "policy", WindowID: "hour", Bucket: now.Unix()/60 + offset, BucketNanos: int64(time.Minute), DurationNanos: int64(time.Hour), AmountUSD: pricing.MustUSD(amount), LimitUSD: pricing.MustUSD("1")}
			}
			accept := func(id string, want bool, reservations ...admission.WindowReservation) durable.ReserveResult {
				t.Helper()
				result, err := m.Accept(ctx, durable.ReserveRequest{OperationID: durable.OperationID(id), GenerationID: "rolling-gen", Reservations: reservations})
				if err != nil || result.Accepted != want {
					t.Fatalf("%s: accepted=%v, want %v; error=%v", id, result.Accepted, want, err)
				}
				return result
			}
			claim := func(result durable.ReserveResult) error {
				t.Helper()
				_, err := m.(durable.BudgetLeaser).Claim(ctx, durable.ClaimRequest{OperationID: result.OperationID, GenerationID: "rolling-gen", IncarnationID: "rolling-inc"})
				return err
			}
			// settle reconciles every window of result with one event kind,
			// releasing the reservation and accounting amount in its place.
			settle := func(result durable.ReserveResult, kind budget.JournalEventKind, amount string) durable.ReconcileRequest {
				cost := pricing.MustUSD(amount)
				request := durable.ReconcileRequest{OperationID: result.OperationID, GenerationID: "rolling-gen", IncarnationID: "rolling-inc"}
				for index, event := range result.Events {
					completion := budget.CompletionEvent{EventID: fmt.Sprintf("%s-settle-%d", result.OperationID, index), OperationID: string(result.OperationID), GenerationID: "rolling-gen", WindowID: event.WindowID, BucketStart: event.BucketStart, ReservationRevision: event.ReservationRevision + 1, Kind: kind, ReservedDecreaseUSD: event.AmountUSD, AccountedIncreaseUSD: cost, OccurredAt: now}
					if kind == budget.JournalFinalizeUnknown {
						completion.CostStatus, completion.UnknownReasonCode = budget.CostUnknown, "provider_cost_unknown"
					} else {
						completion.CostStatus, completion.ActualCostUSD = budget.CostExact, &cost
					}
					request.Events = append(request.Events, completion)
				}
				return request
			}
			reconcile := func(request durable.ReconcileRequest) {
				t.Helper()
				if err := m.Reconcile(ctx, request); err != nil {
					t.Fatalf("reconcile %s: %v", request.OperationID, err)
				}
			}
			switch scenario {
			case "reserved", "accounted":
				first := accept("first", true, reservation(-1, "0.60"))
				if scenario == "accounted" {
					if _, err := m.(durable.BudgetLeaser).Claim(ctx, durable.ClaimRequest{OperationID: "first", GenerationID: "rolling-gen", IncarnationID: "rolling-inc"}); err != nil {
						t.Fatal(err)
					}
					event := first.Events[0]
					cost := pricing.MustUSD("0.60")
					err := m.Reconcile(ctx, durable.ReconcileRequest{OperationID: "first", GenerationID: "rolling-gen", IncarnationID: "rolling-inc", Events: []budget.CompletionEvent{{EventID: "completion", OperationID: "first", GenerationID: "rolling-gen", WindowID: event.WindowID, BucketStart: event.BucketStart, ReservationRevision: event.ReservationRevision + 1, Kind: budget.JournalFinalizeExact, ReservedDecreaseUSD: cost, AccountedIncreaseUSD: cost, ActualCostUSD: &cost, CostStatus: budget.CostExact, OccurredAt: now}}})
					if err != nil {
						t.Fatal(err)
					}
				}
				denied := accept("denied", false, reservation(0, "0.60"))
				if denied.Denial == nil || denied.Denial.ActiveUSD.Cmp(pricing.MustUSD("0.60")) != 0 {
					t.Fatalf("wrong aggregate denial: %#v", denied.Denial)
				}
				accept("denied", false, reservation(0, "0.60"))
				accept("boundary", true, reservation(0, "0.40"))
			case "batch":
				accept("batch", false, reservation(-1, "0.60"), reservation(0, "0.60"))
				accept("after-batch", true, reservation(0, "1"))
			case "concurrent":
				const count = 20
				results := make(chan durable.ReserveResult, count)
				errors := make(chan error, count)
				start := make(chan struct{})
				var wg sync.WaitGroup
				for i := 0; i < count; i++ {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						<-start
						result, err := m.Accept(ctx, durable.ReserveRequest{OperationID: durable.OperationID(fmt.Sprintf("race-%d", i)), GenerationID: "rolling-gen", Reservations: []admission.WindowReservation{reservation(-int64(i%2), "0.60")}})
						results <- result
						errors <- err
					}(i)
				}
				close(start)
				wg.Wait()
				close(results)
				close(errors)
				for err := range errors {
					if err != nil {
						t.Fatal(err)
					}
				}
				accepted := 0
				for result := range results {
					if result.Accepted {
						accepted++
					}
				}
				if accepted != 1 {
					t.Fatalf("accepted %d concurrent cross-bucket requests, want 1", accepted)
				}
			case "concurrent-overlapping":
				// Every racer reserves in a tight hour window and a looser day
				// window at once. Exactly the hour's capacity is accepted, and a
				// racer the hour denies leaves nothing behind in the day.
				const count = 20
				day := func(amount string) admission.WindowReservation {
					reservation := reservation(0, amount)
					reservation.WindowID, reservation.DurationNanos, reservation.LimitUSD = "day", int64(24*time.Hour), pricing.MustUSD("3")
					return reservation
				}
				results := make(chan durable.ReserveResult, count)
				errors := make(chan error, count)
				start := make(chan struct{})
				var wg sync.WaitGroup
				for i := 0; i < count; i++ {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						<-start
						result, err := m.Accept(ctx, durable.ReserveRequest{OperationID: durable.OperationID(fmt.Sprintf("overlap-%d", i)), GenerationID: "rolling-gen", Reservations: []admission.WindowReservation{reservation(0, "0.60"), day("0.60")}})
						results <- result
						errors <- err
					}(i)
				}
				close(start)
				wg.Wait()
				close(results)
				close(errors)
				for err := range errors {
					if err != nil {
						t.Fatal(err)
					}
				}
				accepted := 0
				for result := range results {
					if result.Accepted {
						accepted++
					}
				}
				if accepted != 1 {
					t.Fatalf("accepted %d overlapping-window racers, want the hour's capacity of 1", accepted)
				}
				accept("day-remainder", true, day("2.40"))
				accept("day-full", false, day("0.01"))
				accept("hour-remainder", true, reservation(0, "0.40"))
				accept("hour-full", false, reservation(0, "0.01"))
			case "isolation":
				accept("first", true, reservation(-1, "1"))
				otherPolicy := reservation(0, "1")
				otherPolicy.PolicyID = "other-policy"
				accept("other-policy", true, otherPolicy)
				otherWindow := reservation(0, "1")
				otherWindow.WindowID = "other-window"
				accept("other-window", true, otherWindow)
			case "expiry":
				result, err := m.Accept(ctx, durable.ReserveRequest{OperationID: "expiring", GenerationID: "rolling-gen", ExpiresAt: now.Add(time.Second), Reservations: []admission.WindowReservation{reservation(-1, "0.60")}})
				if err != nil || !result.Accepted {
					t.Fatalf("expiring reservation: %#v, %v", result, err)
				}
				accept("before-expiry", false, reservation(0, "0.60"))
				advance(1100 * time.Millisecond)
				accept("after-expiry", true, reservation(0, "1"))
			case "claim-expiry":
				// An unclaimed lease past its deadline can no longer start and
				// gives its capacity back; a claimed one keeps holding it.
				lease := func(id, amount string) durable.ReserveResult {
					t.Helper()
					result, err := m.Accept(ctx, durable.ReserveRequest{OperationID: durable.OperationID(id), GenerationID: "rolling-gen", ExpiresAt: now.Add(time.Second), Reservations: []admission.WindowReservation{reservation(0, amount)}})
					if err != nil || !result.Accepted {
						t.Fatalf("%s: %#v, %v", id, result, err)
					}
					return result
				}
				unused := lease("unused", "0.40")
				started := lease("started", "0.60")
				if err := claim(started); err != nil {
					t.Fatal(err)
				}
				advance(1100 * time.Millisecond)
				if err := claim(unused); !errorsIs(err, durable.ErrLeaseExpired) {
					t.Fatalf("claim after the deadline = %v, want ErrLeaseExpired", err)
				}
				accept("replacement", true, reservation(0, "0.40"))
				accept("started-still-held", false, reservation(0, "0.01"))
			case "finalize-unknown":
				// Unknown cost is charged at the full reservation, once.
				first := accept("unknown", true, reservation(0, "0.60"))
				if err := claim(first); err != nil {
					t.Fatal(err)
				}
				unknown := settle(first, budget.JournalFinalizeUnknown, "0.60")
				reconcile(unknown)
				reconcile(unknown)
				accept("charged", false, reservation(0, "0.60"))
				accept("remainder", true, reservation(0, "0.40"))
				accept("full", false, reservation(0, "0.01"))
			case "duplicate-settlement":
				// A replayed settlement neither refunds nor charges twice, and a
				// batch naming one event twice is refused.
				first := accept("settled", true, reservation(0, "0.80"))
				if err := claim(first); err != nil {
					t.Fatal(err)
				}
				completed := settle(first, budget.JournalFinalizeExact, "0.30")
				duplicate := completed
				duplicate.Events = []budget.CompletionEvent{completed.Events[0], completed.Events[0]}
				if err := m.Reconcile(ctx, duplicate); err == nil {
					t.Fatal("a batch naming one event twice was accepted")
				}
				reconcile(completed)
				reconcile(completed)
				accept("remainder", true, reservation(0, "0.70"))
				reconcile(completed)
				accept("no-double-refund", false, reservation(0, "0.01"))
			}
		})
	}
}

// errorsIs is errors.Is; the concurrent scenarios shadow the package name.
func errorsIs(err, target error) bool { return errors.Is(err, target) }
