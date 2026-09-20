package conformance

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// RunDurableRollingBudget exercises identical rolling-window behavior in the
// reference model and live Redis. Each factory call must return isolated state.
func RunDurableRollingBudget(t *testing.T, newMaterializer func(*testing.T) (durable.BudgetMaterializer, func() time.Time, func(time.Duration))) {
	for _, scenario := range []string{"reserved", "accounted", "batch", "concurrent", "isolation", "expiry"} {
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
			switch scenario {
			case "reserved", "accounted":
				first := accept("first", true, reservation(-1, "0.60"))
				if scenario == "accounted" {
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
			}
		})
	}
}
