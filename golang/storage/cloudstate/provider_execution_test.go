package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

func executionFixture(t *testing.T, modes ...BudgetMode) (*Repository, *memoryTable, *memoryBlobs, Record, BudgetPlan, durable.ReserveResult) {
	t.Helper()
	r, table, blobs, record, plan := budgetPlanFixture(t)
	ctx := context.Background()
	if len(modes) > 0 {
		plan.Mode = modes[0]
		plan.Reservation.Reservations = nil
	}
	if err := r.SaveBudgetPlan(ctx, record.Request.Scope, record.Request.ID, plan, plan.QuotedAt); err != nil {
		t.Fatal(err)
	}
	if !plan.RequiresReservation() {
		return r, table, blobs, record, plan, durable.ReserveResult{}
	}
	leaser, err := durable.NewReferenceBudgetMaterializer(plan.Route.GenerationID, "incarnation", func() time.Time { return plan.QuotedAt })
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := leaser.Accept(ctx, plan.Reservation)
	if err != nil {
		t.Fatal(err)
	}
	return r, table, blobs, record, plan, reservation
}

func startExecution(t *testing.T, r *Repository, record Record, plan BudgetPlan, reservation durable.ReserveResult) SavedProviderExecution {
	t.Helper()
	saved, fresh, err := r.BeginProviderExecution(context.Background(), record.Request.Scope, record.Request.ID, reservation, plan.QuotedAt)
	if err != nil || !fresh {
		t.Fatalf("begin: %t %v", fresh, err)
	}
	return saved
}

func advanceExecution(t *testing.T, r *Repository, record Record, saved SavedProviderExecution, update func(*ProviderExecution)) SavedProviderExecution {
	t.Helper()
	next := saved.Execution
	update(&next)
	next.Revision++
	next.UpdatedAt = next.UpdatedAt.Add(time.Second)
	if err := r.SaveProviderExecution(context.Background(), record.Request.Scope, record.Request.ID, saved.Execution.Revision, next); err != nil {
		t.Fatal(err)
	}
	saved.Execution = next
	return saved
}

func executionClaim(reservation durable.ReserveResult) *durable.ClaimReceipt {
	return &durable.ClaimReceipt{OperationID: reservation.OperationID, GenerationID: reservation.GenerationID, IncarnationID: reservation.IncarnationID}
}

func TestProviderExecutionFencesAdmissionAndRecoversEncryptedJob(t *testing.T) {
	r, table, blobs, record, plan, reservation := executionFixture(t)
	saved := startExecution(t, r, record, plan, reservation)
	ctx := context.Background()
	if _, err := r.LoadBudgetPlan(ctx, record.Request.Scope, record.Request.ID); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("execution did not fence admission", err)
	}
	if err := r.SaveBudgetPlan(ctx, record.Request.Scope, record.Request.ID, plan, plan.QuotedAt); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("execution allowed initial replanning", err)
	}
	saved = advanceExecution(t, r, record, saved, func(e *ProviderExecution) { e.Stage = ExecutionSubmitting; e.Claim = executionClaim(reservation) })
	saved = advanceExecution(t, r, record, saved, func(e *ProviderExecution) {
		e.Stage = ExecutionPending
		e.ProviderOperationID = "sensitive-provider-job"
		e.PollAfter = plan.QuotedAt.Add(time.Minute)
	})
	restarted := reopen(t, table, blobs)
	loaded, err := restarted.LoadProviderExecution(ctx, record.Request.Scope, record.Request.ID)
	if err != nil || !equalExecution(loaded.Execution, saved.Execution) {
		t.Fatal("restart lost provider job", err)
	}
	for _, value := range blobs.values {
		if strings.Contains(string(value), "sensitive-provider-job") {
			t.Fatal("provider ID leaked in blob ciphertext")
		}
	}
	for _, row := range table.rows {
		data, _ := json.Marshal(row.Item)
		if strings.Contains(string(data), "sensitive-provider-job") {
			t.Fatal("provider ID leaked into table")
		}
	}
	wrong := record.Request.Scope
	wrong.Project = "different"
	if _, err := restarted.LoadProviderExecution(ctx, wrong, record.Request.ID); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatal("scope leaked", err)
	}
	// Even a replacement reservation or changed time cannot grant a second start.
	_, fresh, err := restarted.BeginProviderExecution(ctx, record.Request.Scope, record.Request.ID, durable.ReserveResult{}, plan.QuotedAt.Add(time.Hour))
	if err != nil || fresh {
		t.Fatal("replay authorized submission", err)
	}
	saved = advanceExecution(t, restarted, record, saved, func(e *ProviderExecution) {
		e.Stage = ExecutionSucceeded
		e.CompletedAt = e.UpdatedAt.Add(time.Second)
		e.PollAfter = time.Time{}
		e.Response = &llm.Response{OperationKey: "operation", Status: llm.ResponseStatusToolCalls}
	})
	current, err := restarted.Read(ctx, record.Request.Scope, record.Request.ID)
	if err != nil || current.Status != StatusRunning {
		t.Fatal("result is not ready for checkpoint finalization", err)
	}
	if _, err := restarted.LoadBudgetPlan(ctx, record.Request.Scope, record.Request.ID); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("saved result reopened admission", err)
	}
	// Terminal provider output cannot be replaced, even before checkpoint publication.
	next := saved.Execution
	next.Revision++
	next.Response = &llm.Response{OperationKey: "replacement", Status: llm.ResponseStatusCompleted}
	if err := restarted.SaveProviderExecution(ctx, record.Request.Scope, record.Request.ID, saved.Execution.Revision, next); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("terminal response replaced", err)
	}
}

func TestProviderExecutionConcurrentStartsAuthorizeOnlyOneWriter(t *testing.T) {
	r, table, blobs, record, plan, reservation := executionFixture(t)
	var group sync.WaitGroup
	var winners atomic.Int32
	for range 20 {
		group.Add(1)
		go func() {
			defer group.Done()
			_, fresh, err := reopen(t, table, blobs).BeginProviderExecution(context.Background(), record.Request.Scope, record.Request.ID, reservation, plan.QuotedAt)
			if err != nil {
				t.Error(err)
			}
			if fresh {
				winners.Add(1)
			}
		}()
	}
	group.Wait()
	if winners.Load() != 1 {
		t.Fatal("multiple fresh start permissions", winners.Load())
	}
	if _, err := r.LoadProviderExecution(context.Background(), record.Request.Scope, record.Request.ID); err != nil {
		t.Fatal(err)
	}
}

func TestProviderExecutionLostStartReplyNeverAuthorizesReplay(t *testing.T) {
	for _, faultAt := range []string{"event", "index"} {
		t.Run(faultAt, func(t *testing.T) {
			r, table, blobs, record, plan, reservation := executionFixture(t)
			fired := false
			table.hook = func(action string, item kv.KeyValueItem) (error, error) {
				if !fired && (faultAt == "event" && action == "create" && strings.Contains(item.PartitionKey, "/request/") || faultAt == "index" && action == "replace" && strings.Contains(item.PartitionKey, "/pending/")) {
					fired = true
					if faultAt == "index" {
						return contracts.ErrUnavailable, nil
					}
					return nil, contracts.ErrOutcomeUnknown
				}
				return nil, nil
			}
			_, fresh, err := r.BeginProviderExecution(context.Background(), record.Request.Scope, record.Request.ID, reservation, plan.QuotedAt)
			if err == nil || fresh || !fired {
				t.Fatalf("uncertain start authorized dispatch: %t %v", fresh, err)
			}
			table.hook = nil
			loaded, fresh, err := reopen(t, table, blobs).BeginProviderExecution(context.Background(), record.Request.Scope, record.Request.ID, reservation, plan.QuotedAt.Add(time.Second))
			if err != nil || fresh || loaded.Execution.Stage != ExecutionClaiming {
				t.Fatalf("replay authorized dispatch: %t %v", fresh, err)
			}
		})
	}
}

func TestProviderExecutionRejectsChangedLeaseAndStalePoll(t *testing.T) {
	r, _, _, record, plan, reservation := executionFixture(t)
	bad := reservation
	bad.Events = append(bad.Events[:0:0], bad.Events...)
	bad.Events[1] = bad.Events[0]
	bad.Events[1].EventID = "another"
	if _, _, err := r.BeginProviderExecution(context.Background(), record.Request.Scope, record.Request.ID, bad, plan.QuotedAt); !errors.Is(err, ErrInvalid) {
		t.Fatal("missing window accepted", err)
	}
	saved := startExecution(t, r, record, plan, reservation)
	saved = advanceExecution(t, r, record, saved, func(e *ProviderExecution) { e.Stage = ExecutionSubmitting; e.Claim = executionClaim(reservation) })
	saved = advanceExecution(t, r, record, saved, func(e *ProviderExecution) {
		e.Stage = ExecutionPending
		e.ProviderOperationID = "job"
		e.PollAfter = plan.QuotedAt.Add(time.Minute)
	})
	for _, mutation := range []func(*ProviderExecution){
		func(e *ProviderExecution) { e.ProviderOperationID = "other-job" },
		func(e *ProviderExecution) {
			e.Reservation.IncarnationID = "other"
			e.Claim = executionClaim(e.Reservation)
		},
		func(e *ProviderExecution) {
			e.StartedAt = e.StartedAt.Add(time.Second)
			e.RecoverAfter = e.RecoverAfter.Add(time.Second)
		},
	} {
		next := saved.Execution
		next.Revision++
		mutation(&next)
		if err := r.SaveProviderExecution(context.Background(), record.Request.Scope, record.Request.ID, saved.Execution.Revision, next); !errors.Is(err, contracts.ErrConflict) {
			t.Fatal("changed binding accepted", err)
		}
	}
	old := saved
	saved = advanceExecution(t, r, record, saved, func(e *ProviderExecution) { e.PollAfter = e.PollAfter.Add(time.Second) })
	next := old.Execution
	next.Revision++
	next.Stage = ExecutionUnknown
	next.PollAfter = time.Time{}
	next.Failure = &ExecutionFailure{Code: provider.CodeAmbiguousDispatch, Dispatch: provider.DispatchAmbiguous}
	if err := r.SaveProviderExecution(context.Background(), record.Request.Scope, record.Request.ID, old.Execution.Revision, next); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("stale poll overwrote newer state", err)
	}
	// Positive recovery after an unknown outcome can retain the original paid job.
	saved = advanceExecution(t, r, record, saved, func(e *ProviderExecution) {
		e.Stage = ExecutionUnknown
		e.PollAfter = time.Time{}
		e.Failure = next.Failure
	})
	advanceExecution(t, r, record, saved, func(e *ProviderExecution) {
		e.Stage = ExecutionPending
		e.Failure = nil
		e.PollAfter = plan.QuotedAt.Add(time.Minute)
	})
}

func TestUnreservedProviderExecutionConcurrentAndLostStarts(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "concurrent", true: "lost-ack"}[lost], func(t *testing.T) {
			r, table, blobs, record, plan, reservation := executionFixture(t, BudgetUnmatched)
			if lost {
				table.hook = func(action string, item kv.KeyValueItem) (error, error) {
					if action == "create" && strings.Contains(item.PartitionKey, "/request/") {
						return nil, contracts.ErrOutcomeUnknown
					}
					return nil, nil
				}
				_, fresh, err := r.BeginProviderExecution(context.Background(), record.Request.Scope, record.Request.ID, reservation, plan.QuotedAt)
				if err == nil || fresh {
					t.Fatal("uncertain write authorized submission")
				}
				table.hook = nil
			}
			var group sync.WaitGroup
			var winners atomic.Int32
			for range 12 {
				group.Add(1)
				go func() {
					defer group.Done()
					_, fresh, err := reopen(t, table, blobs).BeginProviderExecution(context.Background(), record.Request.Scope, record.Request.ID, reservation, plan.QuotedAt)
					if err != nil {
						t.Error(err)
					}
					if fresh {
						winners.Add(1)
					}
				}()
			}
			group.Wait()
			expected := int32(1)
			if lost {
				expected = 0
			}
			if winners.Load() != expected {
				t.Fatal("duplicate start permission", winners.Load())
			}
		})
	}
}
