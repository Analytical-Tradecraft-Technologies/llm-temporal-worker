package cloudstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

func requestAttemptFixture(t *testing.T) (*Repository, *memoryTable, *memoryBlobs, Record, BudgetPlan) {
	t.Helper()
	r, table, blobs, root, plan := budgetPlanFixture(t)
	p := RequestPreparation{Version: 1, ConfigDigest: plan.ConfigDigest, CheckpointScope: "opaque", PreparedAt: root.Request.CreatedAt.Add(time.Second)}
	mustSavePreparation(t, r, root.Request.Scope, root.Request.ID, p)
	return r, table, blobs, root, plan
}

func saveAttemptPlan(t *testing.T, r *Repository, scope Scope, attempt RequestAttempt, template BudgetPlan) (Record, BudgetPlan, durable.ReserveResult) {
	t.Helper()
	child := mustRead(t, r, scope, attempt.ID)
	plan := copyTestBudgetPlan(t, template)
	plan.Route.OperationID = durable.OperationID(child.Request.ID)
	plan.QuotedAt = child.Request.CreatedAt.Add(time.Second)
	plan.Reservation.OperationID = plan.Route.OperationID
	plan.Reservation.ExpiresAt = plan.QuotedAt.Add(durable.BudgetStartLease)
	for i := range plan.Reservation.Reservations {
		w := &plan.Reservation.Reservations[i]
		w.Bucket = plan.QuotedAt.UnixNano() / w.BucketNanos
	}
	mustSaveBudgetPlan(t, r, scope, attempt.ID, plan)
	leaser, err := durable.NewReferenceBudgetMaterializer(plan.Route.GenerationID, "incarnation", func() time.Time { return plan.QuotedAt })
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := leaser.Accept(context.Background(), plan.Reservation)
	if err != nil {
		t.Fatal(err)
	}
	return child, plan, accepted
}

func initialRequestAttempt(t *testing.T, r *Repository, root Record) RequestAttempt {
	t.Helper()
	a := mustBeginRequestAttempt(t, r, root.Request.Scope, root.Request.ID, "", root.Request.CreatedAt.Add(2*time.Second))
	return a
}

func TestRequestAttemptConcurrentInitializationAndReadOnlyReplay(t *testing.T) {
	r, table, blobs, root, _ := requestAttemptFixture(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	results := make(chan RequestAttempt, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			a, err := reopen(t, table, blobs).BeginRequestAttempt(ctx, root.Request.Scope, root.Request.ID, "", root.Request.CreatedAt.Add(time.Duration(i+2)*time.Second))
			if err != nil {
				t.Error(err)
				return
			}
			results <- a
		}(i)
	}
	wg.Wait()
	close(results)
	active, err := r.LoadRequestAttempt(ctx, root.Request.Scope, root.Request.ID)
	if err != nil {
		t.Fatal(err)
	}
	for a := range results {
		if !equalExecutionJSON(a, active) {
			t.Fatal("competing attempt identities")
		}
	}
	child, err := r.Read(ctx, root.Request.Scope, active.ID)
	if err != nil || child.Status != StatusRunning || active.ID == root.Request.ID || active.Number != 1 || active.PreviousID != "" {
		t.Fatal("invalid initial attempt", err)
	}
	if !bytes.Equal(child.Request.Manifest, root.Request.Manifest) {
		t.Fatal("changed original input")
	}
	if _, err := r.LoadRequestPreparation(ctx, root.Request.Scope, active.ID); err != nil {
		t.Fatal("preparation missing", err)
	}
	requireErrorIs(t, errOf(r.LoadRequestAttempt(ctx, Scope{Tenant: root.Request.Scope.Tenant, Project: "other"}, root.Request.ID)), contracts.ErrNotFound, "cross-scope lookup")
	forbidWrites(t, table, blobs, "healthy replay")
	again, err := r.BeginRequestAttempt(ctx, root.Request.Scope, root.Request.ID, "", root.Request.CreatedAt.Add(2*time.Second))
	if err != nil || !equalExecutionJSON(active, again) {
		t.Fatal("replay changed attempt", err)
	}
}

func TestRequestAttemptExpiredQuoteUsesNewBudgetAndFencesOldStart(t *testing.T) {
	r, _, _, root, template := requestAttemptFixture(t)
	ctx, scope := context.Background(), root.Request.Scope
	first := initialRequestAttempt(t, r, root)
	child, plan, accepted := saveAttemptPlan(t, r, scope, first, template)
	// An unused quote renews within the margin before it expires (#1360).
	renewAt := plan.Reservation.ExpiresAt.Add(-UnusedAttemptRenewMargin)
	requireErrorIs(t, errOf(r.BeginRequestAttempt(ctx, scope, root.Request.ID, first.ID, renewAt.Add(-time.Nanosecond))), contracts.ErrConflict, "renewed active quote")
	next, err := r.BeginRequestAttempt(ctx, scope, root.Request.ID, first.ID, renewAt)
	if err != nil || next.ID == first.ID || next.Number != 2 || next.PreviousID != first.ID {
		t.Fatal("renewal", err)
	}
	if len(next.PriorCandidates) != 0 {
		t.Fatal("unused quote counted as provider attempt")
	}
	retired, err := r.Read(ctx, scope, first.ID)
	if err != nil || retired.Status != StatusFailed {
		t.Fatal("old attempt was not fenced", err)
	}
	if _, fresh, err := r.BeginProviderExecution(ctx, scope, first.ID, accepted, false, plan.Reservation.ExpiresAt); err == nil || fresh {
		t.Fatal("retired attempt authorized dispatch")
	}
	newChild, newPlan, newBudget := saveAttemptPlan(t, r, scope, next, template)
	if newPlan.Route.OperationID == plan.Route.OperationID {
		t.Fatal("reused budget operation")
	}
	if _, fresh, err := r.BeginProviderExecution(ctx, scope, next.ID, accepted, false, newPlan.QuotedAt); err == nil || fresh {
		t.Fatal("accepted old budget for new attempt")
	}
	_ = startExecution(t, r, newChild, newPlan, newBudget)
	if !bytes.Equal(child.Request.Manifest, newChild.Request.Manifest) {
		t.Fatal("changed semantic request")
	}
	replay, err := r.BeginRequestAttempt(ctx, scope, root.Request.ID, first.ID, newPlan.QuotedAt)
	if err != nil || replay.ID != next.ID {
		t.Fatal("uncertain renewal replay advanced again", err)
	}
	requireErrorIs(t, errOf(r.BeginRequestAttempt(ctx, scope, root.Request.ID, "", newPlan.QuotedAt)), contracts.ErrConflict, "stale first initializer accepted")
}

func TestRequestAttemptUnknownPaidWorkRemainsDiscoverableAfterRootCompletion(t *testing.T) {
	r, table, blobs, root, template := requestAttemptFixture(t)
	ctx, scope := context.Background(), root.Request.Scope
	first := initialRequestAttempt(t, r, root)
	child, plan, budget := saveAttemptPlan(t, r, scope, first, template)
	saved := startExecution(t, r, child, plan, budget)
	saved = markExecutionUnknown(t, r, child, saved)
	requireErrorIs(t, errOf(r.BeginRequestAttempt(ctx, scope, root.Request.ID, first.ID, saved.Execution.RecoverAfter.Add(-time.Nanosecond))), contracts.ErrConflict, "replaced work inside recovery interval")
	next, err := reopen(t, table, blobs).BeginRequestAttempt(ctx, scope, root.Request.ID, first.ID, saved.Execution.RecoverAfter)
	if err != nil || next.Number != 2 {
		t.Fatal(err)
	}
	if len(next.PriorCandidates) != 1 || next.PriorCandidates[0] != plan.Estimate.CandidateID {
		t.Fatalf("unknown attempt history = %#v", next.PriorCandidates)
	}
	replayed, err := reopen(t, table, blobs).LoadRequestAttempt(ctx, scope, root.Request.ID)
	if err != nil || !equalExecutionJSON(replayed, next) {
		t.Fatal("history lost on reopen", err)
	}
	old, err := r.LoadProviderExecution(ctx, scope, first.ID)
	if err != nil || !equalExecutionJSON(saved, old) {
		t.Fatal("old paid receipt changed", err)
	}
	mustCompleteOperation(t, r, scope, root.Request.ID, json.RawMessage(`{"version":1,"response":{}}`), next.CreatedAt)
	page := firstPendingPage(t, r, first.ID)
	found := false
	for _, pending := range page.Requests {
		if pending.ID == first.ID {
			found = true
			if pending.Status != StatusOutcomeUnknown {
				t.Fatal("unknown status lost")
			}
		}
	}
	if !found {
		t.Fatal("root completion hid unknown paid work")
	}
	if _, fresh, err := r.BeginProviderExecution(ctx, scope, first.ID, budget, false, next.CreatedAt); err != nil || fresh {
		t.Fatal("unknown replay authorized resubmission", err)
	}
}

func TestRequestAttemptCannotReplaceKnownPendingOrSuccessfulWork(t *testing.T) {
	for _, stage := range []ExecutionStage{ExecutionClaiming, ExecutionSubmitting, ExecutionPending, ExecutionSucceeded} {
		t.Run(string(stage), func(t *testing.T) {
			r, _, _, root, template := requestAttemptFixture(t)
			a := initialRequestAttempt(t, r, root)
			child, plan, budget := saveAttemptPlan(t, r, root.Request.Scope, a, template)
			saved := startExecution(t, r, child, plan, budget)
			if stage != ExecutionClaiming {
				saved = markExecutionSubmitting(t, r, child, saved, budget)
			}
			if stage == ExecutionPending {
				saved = markExecutionPending(t, r, child, saved, "provider-job", saved.Execution.UpdatedAt.Add(time.Minute))
			}
			if stage == ExecutionSucceeded {
				saved = markExecutionUnknown(t, r, child, saved) // a known result must win over retirement
				saved = advanceExecution(t, r, child, saved, func(e *ProviderExecution) {
					e.Stage = ExecutionSucceeded
					e.Failure = nil
					e.Response = &llm.Response{OperationKey: "operation", Status: llm.ResponseStatusCompleted}
					e.CompletedAt = e.UpdatedAt.Add(time.Second)
				})
			}
			requireErrorIs(t, errOf(r.BeginRequestAttempt(context.Background(), root.Request.Scope, root.Request.ID, a.ID, saved.Execution.RecoverAfter.Add(time.Hour))), contracts.ErrConflict, "replaced known work")
		})
	}
}

func TestRequestAttemptLostAcknowledgementsRepairSameChild(t *testing.T) {
	for _, phase := range []string{"child-discovery", "child-event", "root-event", "root-index"} {
		t.Run(phase, func(t *testing.T) {
			r, table, blobs, root, _ := requestAttemptFixture(t)
			childID := r.requestAttemptID(root, 1)
			fired := false
			table.hook = func(action string, item kv.KeyValueItem) (error, error) {
				matches := phase == "child-discovery" && action == "create" && item.Key() == r.indexKey(childID) ||
					phase == "child-event" && action == "create" && strings.Contains(item.PartitionKey, r.stream(childID)) ||
					phase == "root-event" && action == "create" && strings.Contains(item.PartitionKey, r.stream(root.Request.ID)) ||
					phase == "root-index" && action == "replace" && item.Key() == r.indexKey(root.Request.ID)
				if !fired && matches {
					fired = true
					return nil, contracts.ErrOutcomeUnknown
				}
				return nil, nil
			}
			if _, err := r.BeginRequestAttempt(context.Background(), root.Request.Scope, root.Request.ID, "", root.Request.CreatedAt.Add(2*time.Second)); err == nil || !fired {
				t.Fatal("fault not surfaced", phase, err)
			}
			table.hook = nil
			next, err := reopen(t, table, blobs).BeginRequestAttempt(context.Background(), root.Request.Scope, root.Request.ID, "", root.Request.CreatedAt.Add(time.Minute))
			if err != nil || next.ID != childID || next.Number != 1 {
				t.Fatal("lost acknowledgement created another child", err)
			}
			loaded, err := r.LoadRequestAttempt(context.Background(), root.Request.Scope, root.Request.ID)
			if err != nil || !equalExecutionJSON(loaded, next) {
				t.Fatal("index recovery", err)
			}
		})
	}
}

func TestRequestAttemptRenewalRacesWithStart(t *testing.T) {
	for i := 0; i < 20; i++ {
		r, _, _, root, template := requestAttemptFixture(t)
		ctx, scope := context.Background(), root.Request.Scope
		first := initialRequestAttempt(t, r, root)
		_, plan, accepted := saveAttemptPlan(t, r, scope, first, template)
		gate := make(chan struct{})
		type result struct {
			won bool
			err error
		}
		start := make(chan result, 1)
		renew := make(chan result, 1)
		go func() {
			<-gate
			_, fresh, err := r.BeginProviderExecution(ctx, scope, first.ID, accepted, false, plan.Reservation.ExpiresAt)
			start <- result{fresh, err}
		}()
		go func() {
			<-gate
			_, err := r.BeginRequestAttempt(ctx, scope, root.Request.ID, first.ID, plan.Reservation.ExpiresAt)
			renew <- result{err == nil, err}
		}()
		close(gate)
		a, b := <-start, <-renew
		if a.won == b.won {
			t.Fatalf("start and retirement must have exactly one winner: %+v %+v", a, b)
		}
		for _, outcome := range []result{a, b} {
			if !outcome.won && !errors.Is(outcome.err, contracts.ErrConflict) {
				t.Fatal(outcome.err)
			}
		}
	}
}

func TestRequestAttemptRetiredUnknownCanRecoverLater(t *testing.T) {
	r, _, _, root, template := requestAttemptFixture(t)
	ctx, scope := context.Background(), root.Request.Scope
	first := initialRequestAttempt(t, r, root)
	child, plan, accepted := saveAttemptPlan(t, r, scope, first, template)
	saved := startExecution(t, r, child, plan, accepted)
	saved = markExecutionSubmitting(t, r, child, saved, accepted)
	saved = markExecutionUnknown(t, r, child, saved)
	next := mustBeginRequestAttempt(t, r, scope, root.Request.ID, first.ID, saved.Execution.RecoverAfter)
	recovered := saved.Execution
	recovered.Revision++
	recovered.Stage, recovered.Failure = ExecutionSucceeded, nil
	recovered.CompletedAt, recovered.UpdatedAt = next.CreatedAt.Add(time.Second), next.CreatedAt.Add(time.Second)
	recovered.Response = &llm.Response{OperationKey: "operation", Status: llm.ResponseStatusCompleted}
	if err := r.SaveProviderExecution(ctx, scope, first.ID, saved.Execution.Revision, recovered); err != nil {
		t.Fatal("retirement blocked eventual recovery", err)
	}
	active, err := r.LoadRequestAttempt(ctx, scope, root.Request.ID)
	if err != nil || active.ID != next.ID {
		t.Fatal("recovery replaced active attempt", err)
	}
	requireErrorIs(t, errOf(r.BeginRequestAttempt(ctx, scope, next.ID, "", next.CreatedAt)), contracts.ErrConflict, "allowed a nested paid attempt")
}

func TestRequestAttemptHistorySurvivesUnusedRenewalsAndReplay(t *testing.T) {
	r, table, blobs, root, template := requestAttemptFixture(t)
	template.Quote.Entry.EffectiveUntil = root.Request.CreatedAt.Add(24 * time.Hour)
	ctx, scope := context.Background(), root.Request.Scope
	active := initialRequestAttempt(t, r, root)
	for paid := 1; paid <= 3; paid++ {
		child, plan, reservation := saveAttemptPlan(t, r, scope, active, template)
		saved := startExecution(t, r, child, plan, reservation)
		saved = markExecutionUnknown(t, r, child, saved)
		next := mustBeginRequestAttempt(t, reopen(t, table, blobs), scope, root.Request.ID, active.ID, saved.Execution.RecoverAfter)
		if len(next.PriorCandidates) != paid {
			t.Fatalf("history=%v, paid attempts=%d", next.PriorCandidates, paid)
		}
		for _, candidate := range next.PriorCandidates {
			if candidate != plan.Estimate.CandidateID {
				t.Fatal("candidate identity lost")
			}
		}
		replay, err := r.BeginRequestAttempt(ctx, scope, root.Request.ID, active.ID, next.CreatedAt)
		if err != nil || !equalExecutionJSON(replay, next) {
			t.Fatal("retry duplicated history", err)
		}
		renewed := mustBeginRequestAttempt(t, r, scope, root.Request.ID, next.ID, next.CreatedAt.Add(durable.BudgetStartLease))
		if len(renewed.PriorCandidates) != paid {
			t.Fatal("unused quote renewal consumed provider attempt")
		}
		active = renewed
	}
}
