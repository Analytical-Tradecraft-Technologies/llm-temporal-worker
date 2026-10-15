package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

func TestCloudProviderDeadlineMatchesActivityWindow(t *testing.T) {
	// A live one-shot call and its outcome saves must finish before another
	// observer is allowed to classify submission as unrecoverable.
	if config.ActivityStartToClose+executionSaveBudget >= durable.BudgetStartLease {
		t.Fatal("provider Activity overlaps uncertain-submission recovery")
	}
	for _, shorterParent := range []bool{false, true} {
		for _, async := range []bool{false, true} {
			t.Run(map[bool]string{false: "activity", true: "parent"}[shorterParent]+map[bool]string{false: "/sync", true: "/async"}[async], func(t *testing.T) {
				f := newProviderExecutionFixture(t, "generate", async)
				ctx := context.Background()
				if shorterParent {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, time.Minute)
					defer cancel()
				}
				observed := 0
				checkDeadline := func(callCtx context.Context) {
					t.Helper()
					observed++
					deadline, ok := callCtx.Deadline()
					if !ok {
						t.Fatal("provider call is unbounded")
					}
					if parentDeadline, ok := ctx.Deadline(); ok {
						if !deadline.Equal(parentDeadline) {
							t.Fatal("provider call extended its parent deadline")
						}
					} else if remaining := time.Until(deadline); remaining < 9*time.Minute || remaining > config.ActivityStartToClose {
						t.Fatalf("provider deadline = %s; need the longer Activity window", remaining)
					}
				}
				invoke, submit, poll := f.adapter.invoke, f.adapter.submit, f.adapter.poll
				f.adapter.invoke = func(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
					checkDeadline(ctx)
					return invoke(ctx, call, observer)
				}
				f.adapter.submit = func(ctx context.Context, call provider.Call, observer provider.Observer) (provider.ResumableResult, error) {
					checkDeadline(ctx)
					return submit(ctx, call, observer)
				}
				f.adapter.poll = func(ctx context.Context, call provider.Call, id string, observer provider.Observer) (provider.ResumableResult, error) {
					checkDeadline(ctx)
					return poll(ctx, call, id, observer)
				}
				result, err := f.submit(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if async {
					f.now = f.now.Add(3 * time.Second)
					result, err = f.resume(ctx)
					if err != nil {
						t.Fatal(err)
					}
				}
				wantObserved := 1
				if async {
					wantObserved = 2
				}
				if observed != wantObserved || result.Saved.Execution.Stage != cloudstate.ExecutionSucceeded || f.submits.Load() != 1 || f.settlements.Load() != 1 {
					t.Fatalf("completion did not retain one attempt and settlement: observed=%d result=%+v", observed, result)
				}
			})
		}
	}
}
