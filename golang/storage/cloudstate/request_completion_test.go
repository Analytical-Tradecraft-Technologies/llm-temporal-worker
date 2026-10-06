package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

func completionAttemptFixture(t *testing.T, mode string) (*Repository, *memoryTable, *memoryBlobs, Record, RequestAttempt, BudgetPlan, state.DurableCheckpoint, FinalizationHandoff) {
	t.Helper()
	r, table, blobs, root, checkpoint, handoff := handoffFixture(t)
	_, _, _, _, template := budgetPlanFixture(t)
	preparation := RequestPreparation{Version: 1, ConfigDigest: template.ConfigDigest, CheckpointScope: checkpoint.ScopeID, PreparedAt: root.Request.CreatedAt}
	if err := r.SaveRequestPreparation(context.Background(), root.Request.Scope, root.Request.ID, preparation); err != nil {
		t.Fatal(err)
	}
	a := initialRequestAttempt(t, r, root)
	handoff.Mode = mode
	if mode == "cache" {
		checkpoint.Kind = state.CheckpointCacheReplay
		entry := state.CacheEntryID("origin-entry")
		checkpoint.OriginCacheEntryID = &entry
	}
	return r, table, blobs, root, a, template, checkpoint, handoff
}

func commitCompletionHandoff(t *testing.T, r *Repository, root Record, checkpoint state.DurableCheckpoint, handoff FinalizationHandoff, now time.Time) {
	t.Helper()
	ctx := context.Background()
	if err := publishCheckpoint(ctx, r.Checkpoints(), checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := r.SaveFinalizationHandoff(ctx, root.Request.Scope, root.Request.ID, handoff, now); err != nil {
		t.Fatal(err)
	}
}

func TestRequestCompletionRepairsChildAndRootLostWrites(t *testing.T) {
	for _, mode := range []string{"cache", "provider"} {
		for _, event := range []bool{false, true} {
			for _, target := range []int{1, 2} {
				t.Run(fmt.Sprintf("%s/event-%t/write-%d", mode, event, target), func(t *testing.T) {
					r, table, blobs, root, a, plan, checkpoint, handoff := completionAttemptFixture(t, mode)
					ctx, scope := context.Background(), root.Request.Scope
					now := a.CreatedAt.Add(time.Minute)
					if mode == "provider" {
						child, err := r.Read(ctx, scope, a.ID)
						if err != nil {
							t.Fatal(err)
						}
						plan.Mode, plan.Route.OperationID, plan.QuotedAt = BudgetUnmatched, durable.OperationID(a.ID), a.CreatedAt
						plan.Reservation = durable.ReserveRequest{OperationID: plan.Route.OperationID, GenerationID: plan.Route.GenerationID, ExpiresAt: plan.QuotedAt.Add(durable.BudgetStartLease)}
						if err := r.SaveBudgetPlan(ctx, scope, a.ID, plan, plan.QuotedAt); err != nil {
							t.Fatal(err)
						}
						saved := startExecution(t, r, child, plan, durable.ReserveResult{})
						saved = advanceExecution(t, r, child, saved, func(e *ProviderExecution) { e.Stage = ExecutionSubmitting })
						advanceExecution(t, r, child, saved, func(e *ProviderExecution) {
							e.Stage, e.Settled, e.CompletedAt = ExecutionSucceeded, true, e.UpdatedAt.Add(time.Second)
							e.Response = &llm.Response{OperationKey: "paid", OperationID: string(a.ID), Status: llm.ResponseStatusCompleted}
						})
					}
					commitCompletionHandoff(t, r, root, checkpoint, handoff, now)
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
					result := json.RawMessage(`{"version":1,"response":{}}`)
					if _, err := r.CompleteOperation(ctx, scope, root.Request.ID, result, now); err == nil || calls < target {
						t.Fatal("fault was not exercised")
					}
					table.hook = nil
					r = reopen(t, table, blobs)
					if _, err := r.CompleteOperation(ctx, scope, root.Request.ID, result, now.Add(time.Second)); err != nil {
						t.Fatal(err)
					}
					for _, id := range []RequestID{root.Request.ID, a.ID} {
						record, err := r.Read(ctx, scope, id)
						if err != nil || record.Status != StatusCompleted {
							t.Fatal("request did not finish", err)
						}
					}
					for shard := range PendingShards {
						page, err := r.ListPending(ctx, shard, 100, "")
						if err != nil || len(page.Requests) != 0 {
							t.Fatal("completed work remained pending", err)
						}
					}
					table.hook = func(string, kv.KeyValueItem) (error, error) {
						t.Error("healthy completion replay wrote table")
						return nil, nil
					}
					if _, err := r.CompleteOperation(ctx, scope, root.Request.ID, result, now); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func TestRequestCompletionCannotHideUnfinishedPaidChild(t *testing.T) {
	for _, mode := range []string{"cache", "provider"} {
		t.Run(mode, func(t *testing.T) {
			r, _, _, root, a, template, checkpoint, handoff := completionAttemptFixture(t, mode)
			child, plan, budget := saveAttemptPlan(t, r, root.Request.Scope, a, template)
			startExecution(t, r, child, plan, budget)
			now := plan.QuotedAt.Add(time.Minute)
			commitCompletionHandoff(t, r, root, checkpoint, handoff, now)
			if _, err := r.CompleteOperation(context.Background(), root.Request.Scope, root.Request.ID, json.RawMessage(`{"version":1,"response":{}}`), now); !errors.Is(err, contracts.ErrConflict) {
				t.Fatal("unfinished paid child was hidden", err)
			}
			for _, id := range []RequestID{root.Request.ID, a.ID} {
				shard, _ := PendingShard(id)
				page, err := r.ListPending(context.Background(), shard, 100, "")
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, pending := range page.Requests {
					found = found || pending.ID == id
				}
				if !found {
					t.Fatal("pending work disappeared")
				}
			}
		})
	}
}

func TestCacheCompletionPreservesPreviousUnknownPaidChild(t *testing.T) {
	r, _, _, root, first, template, checkpoint, handoff := completionAttemptFixture(t, "cache")
	ctx, scope := context.Background(), root.Request.Scope
	child, plan, budget := saveAttemptPlan(t, r, scope, first, template)
	saved := startExecution(t, r, child, plan, budget)
	saved = advanceExecution(t, r, child, saved, func(e *ProviderExecution) {
		e.Stage = ExecutionUnknown
		e.Failure = &ExecutionFailure{Code: provider.CodeAmbiguousDispatch, Dispatch: provider.DispatchAmbiguous}
	})
	now := saved.Execution.RecoverAfter
	next, err := r.BeginRequestAttempt(ctx, scope, root.Request.ID, first.ID, now)
	if err != nil {
		t.Fatal(err)
	}
	commitCompletionHandoff(t, r, root, checkpoint, handoff, now)
	if _, err := r.CompleteOperation(ctx, scope, root.Request.ID, json.RawMessage(`{"version":1,"response":{}}`), now); err != nil {
		t.Fatal(err)
	}
	count := 0
	for shard := range PendingShards {
		page, err := r.ListPending(ctx, shard, 100, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, pending := range page.Requests {
			if pending.ID != first.ID || pending.Status != StatusOutcomeUnknown {
				t.Fatal("unexpected pending request")
			}
			count++
		}
	}
	if count != 1 {
		t.Fatal("unknown paid work was hidden")
	}
	finished, err := r.Read(ctx, scope, next.ID)
	if err != nil || finished.Status != StatusCompleted {
		t.Fatal("unused cache attempt did not finish", err)
	}
}
