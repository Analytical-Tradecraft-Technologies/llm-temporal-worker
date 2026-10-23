package cloudstate

import (
	"context"
	"errors"
	"testing"
	"time"
)

func listAllBudgetRecovery(t *testing.T, r *Repository, notBefore time.Time) ([]SavedProviderExecution, int) {
	t.Helper()
	var executions []SavedProviderExecution
	scanned := 0
	for shard := range PendingShards {
		token := ""
		for {
			page, err := r.ListBudgetRecovery(context.Background(), shard, 1, token, notBefore)
			if err != nil {
				t.Fatal(err)
			}
			executions = append(executions, page.Executions...)
			scanned += page.Scanned
			if token = page.NextPageToken; token == "" {
				break
			}
		}
	}
	return executions, scanned
}

func TestListBudgetRecoveryReturnsDurablyRecordedReservations(t *testing.T) {
	r, _, _, record, plan, reservation := executionFixture(t)
	if executions, scanned := listAllBudgetRecovery(t, r, time.Time{}); len(executions) != 0 || scanned != 1 {
		t.Fatalf("a plan without an execution never reached Redis: %d of %d", len(executions), scanned)
	}
	saved := startExecution(t, r, record, plan, reservation)
	saved = advanceExecution(t, r, record, saved, func(next *ProviderExecution) {
		next.Stage, next.Claim = ExecutionSubmitting, executionClaim(reservation)
	})
	executions, _ := listAllBudgetRecovery(t, r, time.Time{})
	if len(executions) != 1 || executions[0].Execution.Claim == nil || executions[0].Plan.Reservation.OperationID != plan.Reservation.OperationID ||
		executions[0].Execution.Revision != saved.Execution.Revision {
		t.Fatalf("recovery records = %#v", executions)
	}
	// Non-terminal requests are always read, whatever the horizon.
	if executions, _ := listAllBudgetRecovery(t, r, time.Now().Add(time.Hour)); len(executions) != 1 {
		t.Fatalf("horizon skipped an unfinished paid attempt: %d", len(executions))
	}
}

func TestListBudgetRecoveryRejectsInvalidShards(t *testing.T) {
	r, _, _, _, _, _ := executionFixture(t)
	for _, shard := range []int{-1, PendingShards} {
		if _, err := r.ListBudgetRecovery(context.Background(), shard, 1, "", time.Time{}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("shard %d = %v", shard, err)
		}
	}
}

func TestListBudgetRecoveryUnpaidPlansNeverRestoreBudget(t *testing.T) {
	r, _, _, record, plan, reservation := executionFixture(t, BudgetUnmatched)
	startExecution(t, r, record, plan, reservation)
	if executions, _ := listAllBudgetRecovery(t, r, time.Time{}); len(executions) != 0 {
		t.Fatalf("unpaid execution restored: %#v", executions)
	}
}
