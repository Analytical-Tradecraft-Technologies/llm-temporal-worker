package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

// TestBudgetWaitedSpendCountsFromAcceptance quotes a request while a 10-minute
// window is saturated, lets it wait 12 minutes for capacity, and then requires
// its paid spend to count against the window from acceptance rather than from
// the quote-time bucket, which has already left the window.
func TestBudgetWaitedSpendCountsFromAcceptance(t *testing.T) {
	ctx := context.Background()
	var reserved pricing.USD
	f := boundedCloud(t, false, func(b *budgetPlanningFixture) {
		b.source.value.BudgetPolicies[0].Windows = b.source.value.BudgetPolicies[0].Windows[:1]
		b.source.value.BudgetPolicies[0].Windows[0].Duration, b.source.value.BudgetPolicies[0].Windows[0].Bucket = 10*time.Minute, time.Minute
	})
	f.cap.Budgets = &admissionLeaser{BudgetLeaser: f.cap.Budgets}
	leaser := f.cap.Budgets.(*admissionLeaser)
	leaser.accept = func(ctx context.Context, request durable.ReserveRequest) (durable.ReserveResult, error) {
		reserved = request.Reservations[0].AmountUSD
		return leaser.BudgetLeaser.Accept(ctx, request)
	}
	f.restart(t)
	f.finish(t)
	if reserved.IsZero() {
		t.Fatal("no reservation was taken against the window")
	}

	// The limit now fits exactly one request: the paid first request fills the
	// window until its bucket ages out at the start of minute 11.
	f.cap.Snapshot.(*planningSource).value.BudgetPolicies[0].Windows[0].LimitUSD = reserved
	f.now = f.now.Add(30 * time.Second)
	f.restart(t)
	f.request.OperationKey = "waiter"
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	waiter := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	v, err = f.runtime.AcquireBudgetV1(ctx, waiter)
	boundedState(t, v, err, llm.ExecutionBudgetWait)

	// Twelve minutes later the first request's spend has left the window, so
	// the waiter is admitted on its persisted quote and pays.
	f.now = f.now.Add(12 * time.Minute)
	v, err = f.runtime.AcquireBudgetV1(ctx, waiter)
	boundedState(t, v, err, llm.ExecutionAcquired)
	f.restart(t)
	v, err = f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	v, err = f.runtime.CompleteExecutionV1(ctx, waiter)
	boundedState(t, v, err, llm.ExecutionCompleted)
	if f.submits.Load() != 2 {
		t.Fatalf("submissions = %d, want 2", f.submits.Load())
	}

	// A request arriving seconds after the waiter paid must wait: the waiter's
	// spend was accepted and paid inside this window, not twelve minutes ago.
	f.now = f.now.Add(5 * time.Second)
	f.request.OperationKey = "after-waiter"
	v, err = f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionBudgetWait)
	if f.submits.Load() != 2 {
		t.Fatal("request dispatched while the waiter's spend should still count against the window")
	}
}
