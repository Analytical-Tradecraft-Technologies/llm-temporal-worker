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
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
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
	mustSaveBudgetPlan(t, r, record.Request.Scope, record.Request.ID, plan)
	if !plan.RequiresReservation() {
		return r, table, blobs, record, plan, durable.ReserveResult{}
	}
	leaser, err := durable.NewReferenceBudgetMaterializer(plan.Route.GenerationID, "incarnation", func() time.Time { return plan.QuotedAt })
	testassert.NoError(t, err)
	reservation, err := leaser.Accept(ctx, plan.Reservation)
	testassert.NoError(t, err)
	return r, table, blobs, record, plan, reservation
}

func startExecution(t *testing.T, r *Repository, record Record, plan BudgetPlan, reservation durable.ReserveResult) SavedProviderExecution {
	t.Helper()
	saved, fresh, err := r.BeginProviderExecution(context.Background(), record.Request.Scope, record.Request.ID, reservation, false, plan.QuotedAt)
	if err != nil || !fresh {
		t.Fatalf("begin: %t %v", fresh, err)
	}
	return saved
}

// closeInvalidResponseAttempt closes a paid attempt whose response was
// refused, proving the refusal left its revision usable.
func closeInvalidResponseAttempt(t *testing.T, r *Repository, record Record, saved SavedProviderExecution) {
	t.Helper()
	saved = advanceExecution(t, r, record, saved, func(e *ProviderExecution) {
		e.Stage = ExecutionFailed
		e.CompletedAt = e.UpdatedAt.Add(time.Second)
		e.Failure = &ExecutionFailure{Code: provider.CodeProviderInvalidResponse, Dispatch: provider.DispatchAccepted}
	})
	if saved.Execution.Stage != ExecutionFailed {
		t.Fatal("attempt not closed")
	}
}

// concurrentFreshStarts has callers independent repositories race to begin
// record's provider execution and returns how many were told to submit.
func concurrentFreshStarts(t *testing.T, table *memoryTable, blobs *memoryBlobs, record Record, plan BudgetPlan, reservation durable.ReserveResult, callers int) int32 {
	t.Helper()
	var group sync.WaitGroup
	var winners atomic.Int32
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			_, fresh, err := reopen(t, table, blobs).BeginProviderExecution(context.Background(), record.Request.Scope, record.Request.ID, reservation, false, plan.QuotedAt)
			if err != nil {
				t.Error(err)
			}
			if fresh {
				winners.Add(1)
			}
		}()
	}
	group.Wait()
	return winners.Load()
}

func advanceExecution(t *testing.T, r *Repository, record Record, saved SavedProviderExecution, update func(*ProviderExecution)) SavedProviderExecution {
	t.Helper()
	next := saved.Execution
	update(&next)
	next.Revision++
	next.UpdatedAt = next.UpdatedAt.Add(time.Second)
	testassert.NoError(t, r.SaveProviderExecution(context.Background(), record.Request.Scope, record.Request.ID, saved.Execution.Revision, next))
	saved.Execution = next
	return saved
}

// markExecutionUnknown records an ambiguous dispatch for saved.
func markExecutionUnknown(t *testing.T, r *Repository, record Record, saved SavedProviderExecution) SavedProviderExecution {
	t.Helper()
	return advanceExecution(t, r, record, saved, func(e *ProviderExecution) {
		e.Stage = ExecutionUnknown
		e.Failure = &ExecutionFailure{Code: provider.CodeAmbiguousDispatch, Dispatch: provider.DispatchAmbiguous}
	})
}

// markExecutionSubmitting records reservation's claim on saved.
func markExecutionSubmitting(t *testing.T, r *Repository, record Record, saved SavedProviderExecution, reservation durable.ReserveResult) SavedProviderExecution {
	t.Helper()
	return advanceExecution(t, r, record, saved, func(e *ProviderExecution) { e.Stage = ExecutionSubmitting; e.Claim = executionClaim(reservation) })
}

// markExecutionPending records provider job as pending until pollAfter.
func markExecutionPending(t *testing.T, r *Repository, record Record, saved SavedProviderExecution, job string, pollAfter time.Time) SavedProviderExecution {
	t.Helper()
	return advanceExecution(t, r, record, saved, func(e *ProviderExecution) {
		e.Stage = ExecutionPending
		e.ProviderOperationID = job
		e.PollAfter = pollAfter
	})
}

func executionClaim(reservation durable.ReserveResult) *durable.ClaimReceipt {
	return &durable.ClaimReceipt{OperationID: reservation.OperationID, GenerationID: reservation.GenerationID, IncarnationID: reservation.IncarnationID}
}

func TestProviderExecutionFencesAdmissionAndRecoversEncryptedJob(t *testing.T) {
	r, table, blobs, record, plan, reservation := executionFixture(t)
	saved := startExecution(t, r, record, plan, reservation)
	ctx := context.Background()
	testassert.ErrorIs(t, testassert.Err(r.LoadBudgetPlan(ctx, record.Request.Scope, record.Request.ID)), contracts.ErrConflict, "execution did not fence admission")
	testassert.ErrorIs(t, r.SaveBudgetPlan(ctx, record.Request.Scope, record.Request.ID, plan, plan.QuotedAt), contracts.ErrConflict, "execution allowed initial replanning")
	saved = markExecutionSubmitting(t, r, record, saved, reservation)
	saved = markExecutionPending(t, r, record, saved, "sensitive-provider-job", plan.QuotedAt.Add(time.Minute))
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
	testassert.ErrorIs(t, testassert.Err(restarted.LoadProviderExecution(ctx, wrong, record.Request.ID)), contracts.ErrNotFound, "scope leaked")
	// Even a replacement reservation or changed time cannot grant a second start.
	_, fresh, err := restarted.BeginProviderExecution(ctx, record.Request.Scope, record.Request.ID, durable.ReserveResult{}, false, plan.QuotedAt.Add(time.Hour))
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
	testassert.ErrorIs(t, testassert.Err(restarted.LoadBudgetPlan(ctx, record.Request.Scope, record.Request.ID)), contracts.ErrConflict, "saved result reopened admission")
	// Terminal provider output cannot be replaced, even before checkpoint publication.
	next := saved.Execution
	next.Revision++
	next.Response = &llm.Response{OperationKey: "replacement", Status: llm.ResponseStatusCompleted}
	testassert.ErrorIs(t, restarted.SaveProviderExecution(ctx, record.Request.Scope, record.Request.ID, saved.Execution.Revision, next), contracts.ErrConflict, "terminal response replaced")
}

func TestProviderExecutionOversizedResponseIsRefusedBySize(t *testing.T) {
	// "progress" exceeds the progress limit itself; "record" fits the progress
	// but not the whole encoded record with its request.
	for _, mode := range []string{"progress", "record"} {
		t.Run(mode, func(t *testing.T) {
			r, _, _, record, plan, reservation := executionFixture(t)
			ctx := context.Background()
			saved := startExecution(t, r, record, plan, reservation)
			saved = markExecutionSubmitting(t, r, record, saved, reservation)
			next := saved.Execution
			next.Revision++
			next.Stage = ExecutionSucceeded
			next.UpdatedAt = next.UpdatedAt.Add(time.Second)
			next.CompletedAt = next.UpdatedAt
			response := func(text string) *llm.Response {
				return &llm.Response{OperationKey: "operation", Status: llm.ResponseStatusCompleted, Output: []llm.Item{llm.ModelText(text)}}
			}
			size := maxPayloadBytes
			if mode == "record" {
				current := mustRead(t, r, record.Request.Scope, record.Request.ID)
				// The saved progress replaces the current execution entry.
				next.Response = response("")
				encoded, _ := json.Marshal(next)
				previous, _ := json.Marshal(saved.Execution)
				size = maxPayloadBytes - (len(current.Progress) - len(previous)) - len(encoded) - 64
			}
			next.Response = response(strings.Repeat("x", size))
			err := r.SaveProviderExecution(ctx, record.Request.Scope, record.Request.ID, saved.Execution.Revision, next)
			if !errors.Is(err, ErrRecordTooLarge) || !errors.Is(err, ErrInvalid) {
				t.Fatal("oversized response not refused by size", err)
			}
			// Nothing was written, so the same revision can still close the attempt.
			closeInvalidResponseAttempt(t, r, record, saved)
		})
	}
}

func TestProviderExecutionToolCallDepth(t *testing.T) {
	nested := func(depth int) json.RawMessage {
		return json.RawMessage(strings.Repeat(`{"a":`, depth-1) + `{}` + strings.Repeat(`}`, depth-1))
	}
	succeeded := func(saved SavedProviderExecution, depth int) ProviderExecution {
		next := saved.Execution
		next.Revision++
		next.Stage = ExecutionSucceeded
		next.UpdatedAt = next.UpdatedAt.Add(time.Second)
		next.CompletedAt = next.UpdatedAt
		next.Response = &llm.Response{OperationKey: "operation", Status: llm.ResponseStatusCompleted, Output: []llm.Item{llm.ToolCall{ID: "call", Name: "lookup", Arguments: nested(depth)}}}
		return next
	}
	// Every tool call an adapter accepts fits inside the saved progress.
	r, _, _, record, plan, reservation := executionFixture(t)
	ctx := context.Background()
	saved := startExecution(t, r, record, plan, reservation)
	saved = markExecutionSubmitting(t, r, record, saved, reservation)
	testassert.NoError(t, r.SaveProviderExecution(ctx, record.Request.Scope, record.Request.ID, saved.Execution.Revision, succeeded(saved, llm.MaxToolArgumentsDepth)), "tool call at the adapter depth limit not saved")

	// Anything deeper is refused as a deterministic record limit, like an
	// oversized response, so the paid attempt can still be closed.
	r, _, _, record, plan, reservation = executionFixture(t)
	saved = startExecution(t, r, record, plan, reservation)
	saved = markExecutionSubmitting(t, r, record, saved, reservation)
	err := r.SaveProviderExecution(ctx, record.Request.Scope, record.Request.ID, saved.Execution.Revision, succeeded(saved, llm.DefaultCanonicalMaxDepth))
	if !errors.Is(err, ErrRecordTooLarge) || !errors.Is(err, ErrInvalid) {
		t.Fatal("too-deep response not refused as a record limit", err)
	}
	closeInvalidResponseAttempt(t, r, record, saved)
}

func TestProviderExecutionConcurrentStartsAuthorizeOnlyOneWriter(t *testing.T) {
	r, table, blobs, record, plan, reservation := executionFixture(t)
	winners := concurrentFreshStarts(t, table, blobs, record, plan, reservation, 20)
	if winners != 1 {
		t.Fatal("multiple fresh start permissions", winners)
	}
	testassert.NoError(t, testassert.Err(r.LoadProviderExecution(context.Background(), record.Request.Scope, record.Request.ID)))
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
			_, fresh, err := r.BeginProviderExecution(context.Background(), record.Request.Scope, record.Request.ID, reservation, false, plan.QuotedAt)
			if err == nil || fresh || !fired {
				t.Fatalf("uncertain start authorized dispatch: %t %v", fresh, err)
			}
			table.hook = nil
			loaded, fresh, err := reopen(t, table, blobs).BeginProviderExecution(context.Background(), record.Request.Scope, record.Request.ID, reservation, false, plan.QuotedAt.Add(time.Second))
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
	if _, _, err := r.BeginProviderExecution(context.Background(), record.Request.Scope, record.Request.ID, bad, false, plan.QuotedAt); !errors.Is(err, ErrInvalid) {
		t.Fatal("missing window accepted", err)
	}
	saved := startExecution(t, r, record, plan, reservation)
	saved = markExecutionSubmitting(t, r, record, saved, reservation)
	saved = markExecutionPending(t, r, record, saved, "job", plan.QuotedAt.Add(time.Minute))
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
		testassert.ErrorIs(t, r.SaveProviderExecution(context.Background(), record.Request.Scope, record.Request.ID, saved.Execution.Revision, next), contracts.ErrConflict, "changed binding accepted")
	}
	old := saved
	saved = advanceExecution(t, r, record, saved, func(e *ProviderExecution) { e.PollAfter = e.PollAfter.Add(time.Second) })
	next := old.Execution
	next.Revision++
	next.Stage = ExecutionUnknown
	next.PollAfter = time.Time{}
	next.Failure = &ExecutionFailure{Code: provider.CodeAmbiguousDispatch, Dispatch: provider.DispatchAmbiguous}
	testassert.ErrorIs(t, r.SaveProviderExecution(context.Background(), record.Request.Scope, record.Request.ID, old.Execution.Revision, next), contracts.ErrConflict, "stale poll overwrote newer state")
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
				table.hook = failTableWrites("create", "/request/", nil, contracts.ErrOutcomeUnknown)
				_, fresh, err := r.BeginProviderExecution(context.Background(), record.Request.Scope, record.Request.ID, reservation, false, plan.QuotedAt)
				if err == nil || fresh {
					t.Fatal("uncertain write authorized submission")
				}
				table.hook = nil
			}
			winners := concurrentFreshStarts(t, table, blobs, record, plan, reservation, 12)
			expected := int32(1)
			if lost {
				expected = 0
			}
			if winners != expected {
				t.Fatal("duplicate start permission", winners)
			}
		})
	}
}

func TestProviderExecutionKeepsUnknownCostReasonOutsideTheResponse(t *testing.T) {
	execution := ProviderExecution{StartToken: "token", Version: 1, Stage: ExecutionSucceeded,
		Response: &llm.Response{APIVersion: llm.APIVersion, OperationKey: "op", Status: llm.ResponseStatusCompleted, Cost: llm.Cost{Status: llm.CostStatusUnknown, UnknownReason: llm.CostUnknownCatalogIncomplete}}}
	encoded, err := json.Marshal(execution)
	testassert.NoError(t, err)
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil || string(fields["cost_unknown_reason"]) != `"catalog_incomplete"` || fields["start_token"] == nil {
		t.Fatalf("encoded execution = %s, %v", encoded, err)
	}
	// An earlier worker decodes the response cost strictly, so the response
	// encoding must not change.
	if strings.Contains(string(fields["response"]), "catalog_incomplete") {
		t.Fatalf("response encoding carries the reason: %s", fields["response"])
	}
	var decoded ProviderExecution
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded.StartToken != "token" || decoded.Response == nil || decoded.Response.Cost.UnknownReason != llm.CostUnknownCatalogIncomplete {
		t.Fatalf("decoded = %#v, %v", decoded, err)
	}
	if !equalExecution(execution, decoded) {
		t.Fatal("round-tripped execution is not equal")
	}

	delete(fields, "cost_unknown_reason")
	stored, err := json.Marshal(fields)
	testassert.NoError(t, err)
	var legacy ProviderExecution
	if err := json.Unmarshal(stored, &legacy); err != nil || legacy.Response == nil || legacy.Response.Cost.Status != llm.CostStatusUnknown || legacy.Response.Cost.UnknownReason != "" {
		t.Fatalf("stored execution without a reason = %#v, %v", legacy, err)
	}
}

// An unreserved attempt has no claim, so only one closed before reaching the
// provider as an unclaimed non-dispatch renews without using up an attempt
// (#1454). Its other pre-dispatch failures still count.
func TestProviderExecutionUnclaimedUnreservedPlans(t *testing.T) {
	for _, mode := range []BudgetMode{BudgetReserved, BudgetUnmatched, BudgetFree} {
		plan := BudgetPlan{Mode: mode}
		for _, tc := range []struct {
			code     provider.Code
			dispatch provider.DispatchCertainty
			want     bool
		}{
			{provider.CodeBudgetDenied, provider.DispatchNotDispatched, true},
			{provider.CodeStateUnavailable, provider.DispatchNotDispatched, mode == BudgetReserved},
			{provider.CodeBudgetDenied, provider.DispatchAmbiguous, false},
		} {
			execution := ProviderExecution{Stage: ExecutionFailed, Failure: &ExecutionFailure{Code: tc.code, Dispatch: tc.dispatch}}
			if got := execution.Unclaimed(plan); got != tc.want {
				t.Fatalf("%s %s/%s unclaimed = %v", mode, tc.code, tc.dispatch, got)
			}
		}
	}
}
