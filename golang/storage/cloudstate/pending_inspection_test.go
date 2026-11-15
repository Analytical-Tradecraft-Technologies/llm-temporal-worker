package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// forbidInspectionWrites reports any table or blob write as a test error
// until the returned function restores the stores.
func forbidInspectionWrites(t *testing.T, table *memoryTable, blobs *memoryBlobs) func() {
	t.Helper()
	forbidWrites(t, table, blobs, "inspection")
	return func() { table.hook, blobs.hook = nil, nil }
}

func inspect(t *testing.T, r *Repository, id RequestID) PendingInspection {
	t.Helper()
	inspection, err := r.InspectPending(context.Background(), id)
	if err != nil {
		t.Fatalf("inspect %s: %v", id, err)
	}
	return inspection
}

// A terminated workflow leaves its root and its active attempt behind. The
// inspection names both, links them, and summarizes the attempt's accounting
// without a single write, so it can run beside live workers and budget
// recovery.
func TestInspectPendingLinksRootsAndAttemptsWithoutWriting(t *testing.T) {
	r, table, blobs, root, template := requestAttemptFixture(t)
	ctx, scope := context.Background(), root.Request.Scope
	first := initialRequestAttempt(t, r, root)
	child, plan, accepted := saveAttemptPlan(t, r, scope, first, template)
	saved := startExecution(t, r, child, plan, accepted)
	saved = advanceExecution(t, r, child, saved, func(e *ProviderExecution) { e.Stage = ExecutionSubmitting; e.Claim = executionClaim(accepted) })
	saved = advanceExecution(t, r, child, saved, func(e *ProviderExecution) {
		e.Stage = ExecutionUnknown
		e.Failure = &ExecutionFailure{Code: provider.CodeAmbiguousDispatch, Dispatch: provider.DispatchAmbiguous}
	})

	restore := forbidInspectionWrites(t, table, blobs)
	rootView := inspect(t, r, root.Request.ID)
	attemptView := inspect(t, r, first.ID)
	restore()
	if rootView.Role != PendingRoleRoot || rootView.Terminal() || rootView.ActiveAttemptID != first.ID || rootView.Kind != "generate" || rootView.Initializing {
		t.Fatalf("root inspection = %+v", rootView)
	}
	wantShard, _ := PendingShard(root.Request.ID)
	if rootView.Shard != wantShard || rootView.Revision == 0 || rootView.UpdatedAt.IsZero() {
		t.Fatalf("root discovery metadata = %+v", rootView)
	}
	want := PendingExecution{AttemptID: first.ID, Stage: ExecutionUnknown, Paid: true, Claimed: true, RecoverAfter: saved.Execution.RecoverAfter}
	if rootView.Execution == nil || *rootView.Execution != want {
		t.Fatalf("root's active execution = %+v, want %+v", rootView.Execution, want)
	}
	if attemptView.Role != PendingRoleAttempt || attemptView.RootID != root.Request.ID || attemptView.ActiveAttemptID != first.ID || attemptView.Retired ||
		attemptView.Status != StatusOutcomeUnknown || attemptView.Execution == nil || *attemptView.Execution != want {
		t.Fatalf("attempt inspection = %+v", attemptView)
	}

	// Replacing the unknown attempt retires it: no workflow will advance it
	// again, and the root's new active attempt has no execution yet.
	next, err := r.BeginRequestAttempt(ctx, scope, root.Request.ID, first.ID, saved.Execution.RecoverAfter)
	if err != nil {
		t.Fatal(err)
	}
	restore = forbidInspectionWrites(t, table, blobs)
	rootView = inspect(t, r, root.Request.ID)
	attemptView = inspect(t, r, first.ID)
	restore()
	if rootView.ActiveAttemptID != next.ID || rootView.Execution != nil {
		t.Fatalf("root after replacement = %+v", rootView)
	}
	if !attemptView.Retired || attemptView.ActiveAttemptID != next.ID || attemptView.Execution == nil || attemptView.Execution.Stage != ExecutionUnknown {
		t.Fatalf("retired attempt = %+v", attemptView)
	}

	// Completion drops the root's execution metadata; the older unknown
	// attempt remains independently discoverable and inspectable.
	if _, err := r.CompleteOperation(ctx, scope, root.Request.ID, json.RawMessage(`{"version":1,"response":{}}`), next.CreatedAt); err != nil {
		t.Fatal(err)
	}
	restore = forbidInspectionWrites(t, table, blobs)
	rootView = inspect(t, r, root.Request.ID)
	attemptView = inspect(t, r, first.ID)
	restore()
	if !rootView.Terminal() || rootView.Execution != nil || rootView.ActiveAttemptID != "" {
		t.Fatalf("completed root = %+v", rootView)
	}
	if !attemptView.Retired || attemptView.ActiveAttemptID != "" || attemptView.Terminal() {
		t.Fatalf("attempt of a completed root = %+v", attemptView)
	}
	// Budget recovery reads every discovery row, terminal ones included, and
	// still finds the unknown attempt's paid execution.
	executions, _ := listAllBudgetRecovery(t, r, time.Time{})
	found := false
	for _, execution := range executions {
		found = found || execution.Plan.Reservation.OperationID == plan.Reservation.OperationID
	}
	if !found {
		t.Fatal("budget recovery lost the inspected attempt")
	}
}

func TestInspectPendingInitializingAndUnpreparedRecords(t *testing.T) {
	r, table, blobs, request := fixture(t)
	ctx := context.Background()
	normalized, err := normalizeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ensureIndex(ctx, normalized); err != nil {
		t.Fatal(err)
	}
	restore := forbidInspectionWrites(t, table, blobs)
	view := inspect(t, r, request.ID)
	restore()
	if !view.Initializing || view.Role != "" || view.Status != StatusPending || view.Execution != nil {
		t.Fatalf("initializing row = %+v", view)
	}
	if _, err := r.Create(ctx, request); err != nil {
		t.Fatal(err)
	}
	restore = forbidInspectionWrites(t, table, blobs)
	view = inspect(t, r, request.ID)
	restore()
	if view.Initializing || view.Role != "" || view.Status != StatusPending || view.Revision != 1 || view.Kind != "generate" {
		t.Fatalf("unprepared record = %+v", view)
	}
}

func TestInspectPendingRejectsUnknownAndInvalidIDs(t *testing.T) {
	r, _, _, _ := fixture(t)
	missing, err := NewRequestID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.InspectPending(context.Background(), missing); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("missing request = %v", err)
	}
	if _, err := r.InspectPending(context.Background(), "not-an-id"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid request = %v", err)
	}
}
