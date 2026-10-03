package cloudstate

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

func failedRequestFixture(t *testing.T, retryable bool) (*Repository, *memoryTable, *memoryBlobs, Record, RequestAttempt, llm.ExecutionResultV1, time.Time) {
	t.Helper()
	r, table, blobs, root, plan := requestAttemptFixture(t)
	a := initialRequestAttempt(t, r, root)
	child, err := r.Read(context.Background(), root.Request.Scope, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	plan.Mode = BudgetUnmatched
	plan.Route.OperationID = durable.OperationID(a.ID)
	plan.QuotedAt = a.CreatedAt
	plan.Reservation = durable.ReserveRequest{OperationID: plan.Route.OperationID, GenerationID: plan.Route.GenerationID, ExpiresAt: plan.QuotedAt.Add(durable.BudgetStartLease)}
	if err := r.SaveBudgetPlan(context.Background(), root.Request.Scope, a.ID, plan, plan.QuotedAt); err != nil {
		t.Fatal(err)
	}
	saved := startExecution(t, r, child, plan, durable.ReserveResult{})
	saved = advanceExecution(t, r, child, saved, func(e *ProviderExecution) {
		e.Stage = ExecutionFailed
		e.CompletedAt = e.UpdatedAt.Add(time.Second)
		e.Settled = true
		e.Failure = &ExecutionFailure{Code: provider.CodeProviderUnavailable, Dispatch: provider.DispatchRejected, Retryable: retryable}
		if retryable {
			e.Failure.RetryNotBefore = e.CompletedAt.Add(time.Second)
		}
	})
	failure := llm.ExecutionResultV1{RequestID: string(root.Request.ID), Kind: root.Request.Kind, State: llm.ExecutionFailed, FailureCode: "provider_error", Retryable: retryable}
	return r, table, blobs, root, a, failure, saved.Execution.UpdatedAt.Add(time.Second)
}

func TestRequestFailureRepairsLostAcknowledgementsAndIndexWrites(t *testing.T) {
	for _, event := range []bool{false, true} {
		for _, target := range []int{1, 2} {
			t.Run(map[bool]string{false: "index", true: "event"}[event]+string(rune('0'+target)), func(t *testing.T) {
				r, table, blobs, root, a, failure, now := failedRequestFixture(t, false)
				calls := 0
				table.hook = func(action string, item kv.KeyValueItem) (error, error) {
					match := !event && action == "replace" && strings.Contains(item.PartitionKey, "/pending/") || event && action == "create" && strings.Contains(item.PartitionKey, "/request/")
					if match {
						calls++
						if calls == target {
							if event {
								return nil, contracts.ErrOutcomeUnknown
							}
							return contracts.ErrUnavailable, nil
						}
					}
					return nil, nil
				}
				ctx := context.Background()
				if err := r.FinishRequestFailure(ctx, root.Request.Scope, root.Request.ID, a.ID, failure, now); err == nil {
					t.Fatal("fault ignored")
				}
				table.hook = nil
				r = reopen(t, table, blobs)
				if err := r.FinishRequestFailure(ctx, root.Request.Scope, root.Request.ID, a.ID, failure, now.Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				for _, id := range []RequestID{root.Request.ID, a.ID} {
					record, err := r.Read(ctx, root.Request.Scope, id)
					if err != nil || record.Status != StatusFailed {
						t.Fatal("failure not terminal", err)
					}
					shard, _ := PendingShard(id)
					page, err := r.ListPending(ctx, shard, 100, "")
					if err != nil {
						t.Fatal(err)
					}
					for _, pending := range page.Requests {
						if pending.ID == id {
							t.Fatal("terminal request still discoverable")
						}
					}
				}
				table.hook = func(string, kv.KeyValueItem) (error, error) { t.Error("healthy replay wrote table"); return nil, nil }
				if err := r.FinishRequestFailure(ctx, root.Request.Scope, root.Request.ID, a.ID, failure, now); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestRequestFailureRenewalRequiresSettledRetryableChild(t *testing.T) {
	for _, retryable := range []bool{false, true} {
		r, _, _, root, a, failure, now := failedRequestFixture(t, retryable)
		ctx, scope := context.Background(), root.Request.Scope
		// A provider result alone cannot bypass failure finalization.
		if _, err := r.BeginRequestAttempt(ctx, scope, root.Request.ID, a.ID, now); !errors.Is(err, contracts.ErrConflict) {
			t.Fatal("unclosed child renewed", err)
		}
		changed := failure
		changed.Retryable = !retryable
		if err := r.FinishRequestFailure(ctx, scope, root.Request.ID, a.ID, changed, now); err == nil {
			t.Fatal("changed retry classification accepted")
		}
		if err := r.FinishRequestFailure(ctx, scope, root.Request.ID, a.ID, failure, now); err != nil {
			t.Fatal(err)
		}
		next, err := r.BeginRequestAttempt(ctx, scope, root.Request.ID, a.ID, now)
		if retryable {
			if err != nil || next.Number != 2 || next.PreviousID != a.ID {
				t.Fatal("retry could not renew", err)
			}
		} else if !errors.Is(err, contracts.ErrConflict) {
			t.Fatal("permanent failure renewed", err)
		}
	}
}
