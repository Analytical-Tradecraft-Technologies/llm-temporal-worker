//go:build cloudworkflowintegration

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// Create an aged attempt through the production persistence path, leaving a
// few seconds of its real authorization deadline. Redis always uses its own
// wall clock; no record is edited and no production timeout is shortened.
func ageLiveCloudAttempt(t *testing.T, h *liveCloudWorkflow) {
	t.Helper()
	now, err := h.redis.Time(h.ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	created := now.Add(-cache.MaxFillLease + 5*time.Second)
	h.fixture.cap.Clock = func() time.Time { return created }
	h.fixture.restart(t)
}

func TestCloudWorkflowLiveExpiredAdmission(t *testing.T) {
	h := newLiveCloudWorkflow(t, false, false)
	f := h.fixture
	ageLiveCloudAttempt(t, h)
	prepared, err := f.runtime.PrepareExecutionV1(h.ctx, llm.PrepareExecutionV1{Generate: &f.request})
	boundedState(t, prepared, err, llm.ExecutionBudgetRequired)
	ref := llm.ExecutionReferenceV1{RequestID: prepared.RequestID, Context: f.request.Context}
	acquired, err := f.runtime.AcquireBudgetV1(h.ctx, ref)
	boundedState(t, acquired, err, llm.ExecutionAcquired)
	scope := cloudstate.Scope{Tenant: f.request.Context.Tenant, Project: f.request.Context.Project}
	first, err := f.repository.LoadRequestAttempt(h.ctx, scope, cloudstate.RequestID(ref.RequestID))
	if err != nil {
		t.Fatal(err)
	}
	leases := liveCloudLeases(t, h)
	old, ok := leases[string(first.ID)]
	if !ok || len(leases) != 1 || old.Claimed {
		t.Fatal("expected one unused Redis authorization")
	}
	// Wait for actual Redis time, then prove that this authorization cannot
	// dispatch before allowing a restarted worker to recover the request.
	waitLiveRedisDeadline(t, h, time.UnixMilli(old.StartByMillis))
	leaser := f.cap.Budgets.(durable.BudgetLeaser)
	if _, err := leaser.Claim(h.ctx, durable.ClaimRequest{OperationID: durable.OperationID(first.ID), GenerationID: f.options.BudgetGeneration, IncarnationID: durable.IncarnationID(old.IncarnationID)}); !errors.Is(err, durable.ErrLeaseExpired) {
		t.Fatalf("expired start permission: %v", err)
	}
	f.cap.Clock = time.Now
	h.startWorker(t)
	h.generate(t, f.request)
	next, leases := liveReplacementAttempt(t, h, scope, first)
	if f.submits.Load() != 1 {
		t.Fatal("expired admission did not become exactly one fresh attempt")
	}
	retired, err := f.repository.Read(h.ctx, scope, first.ID)
	if err != nil || retired.Status != cloudstate.StatusFailed {
		t.Fatal("unused attempt was not retired", err)
	}
	if len(leases) != 2 || leases[string(first.ID)].Claimed {
		t.Fatal("expired authorization was reused")
	}
	paid := requireLiveSettledLease(t, leases, string(next.ID))
	assertLiveBudgetTotals(t, h, liveLeaseTotals(t, paid, false))
	assertLivePending(t, h)
}

func TestCloudWorkflowLiveUnknownSubmission(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async-%t", async), func(t *testing.T) {
			h := newLiveCloudWorkflow(t, async, false)
			f := h.fixture
			f.request.Cache = &llm.CachePolicyV1{}
			ageLiveCloudAttempt(t, h)
			invoke, submit := f.adapter.invoke, f.adapter.submit
			lost := func(ctx context.Context, observer provider.Observer) error {
				f.submits.Add(1)
				if err := observer.BeforePossibleWrite(ctx); err != nil {
					return err
				}
				return errors.New("injected loss of accepted provider response")
			}
			f.adapter.invoke = func(ctx context.Context, _ provider.Call, observer provider.Observer) (provider.Result, error) {
				return provider.Result{}, lost(ctx, observer)
			}
			f.adapter.submit = func(ctx context.Context, _ provider.Call, observer provider.Observer) (provider.ResumableResult, error) {
				return provider.ResumableResult{}, lost(ctx, observer)
			}
			pending, err := f.runtime.GenerateStepV1(h.ctx, f.request)
			boundedState(t, pending, err, llm.ExecutionPending)
			scope := cloudstate.Scope{Tenant: f.request.Context.Tenant, Project: f.request.Context.Project}
			first, err := f.repository.LoadRequestAttempt(h.ctx, scope, cloudstate.RequestID(pending.RequestID))
			if err != nil {
				t.Fatal(err)
			}
			saved, err := f.repository.LoadProviderExecution(h.ctx, scope, first.ID)
			if err != nil || saved.Execution.Stage != cloudstate.ExecutionUnknown || saved.Execution.Claim == nil || saved.Execution.Settled {
				t.Fatal("lost provider response did not preserve the paid claim", err)
			}
			// A new Temporal caller and fresh workers recover only from the saved
			// records. Their clocks are normal; the original lease remains paid.
			f.adapter.invoke, f.adapter.submit = invoke, submit
			f.cap.Clock = time.Now
			h.complete.Store(true)
			h.startWorker(t)
			h.startWorker(t)
			h.generate(t, f.request)
			next, leases := liveReplacementAttempt(t, h, scope, first)
			if f.submits.Load() != 2 {
				t.Fatal("uncertain submission did not acquire one independent paid retry")
			}
			old, err := f.repository.Read(h.ctx, scope, first.ID)
			if err != nil || old.Status != cloudstate.StatusOutcomeUnknown {
				t.Fatal("original paid work was discarded", err)
			}
			held, ok := leases[string(first.ID)]
			if !ok || len(leases) != 2 || !held.Claimed || len(held.Reservations) != 2 {
				t.Fatal("old and new attempts did not pay independently")
			}
			paid := requireLiveSettledLease(t, leases, string(next.ID))
			totals := liveLeaseTotals(t, paid, false)
			for window, amount := range liveLeaseTotals(t, held, true) {
				totals[window] += amount
			}
			assertLiveBudgetTotals(t, h, totals)
			assertLivePending(t, h, first.ID)
		})
	}
}

type liveLostSettlement struct {
	durable.BudgetLeaser
	calls atomic.Int32
}

func (b *liveLostSettlement) Reconcile(ctx context.Context, request durable.ReconcileRequest) error {
	if err := b.BudgetLeaser.Reconcile(ctx, request); err != nil {
		return err
	}
	if b.calls.Add(1) == 1 {
		return errors.New("injected loss after Redis committed settlement")
	}
	return nil
}

func TestCloudWorkflowLiveLostSettlement(t *testing.T) {
	h := newLiveCloudWorkflow(t, false, false)
	f := h.fixture
	budgets := &liveLostSettlement{BudgetLeaser: f.cap.Budgets.(durable.BudgetLeaser)}
	f.cap.Budgets = budgets
	var err error
	f.cap.Finalizer, err = newCloudFinalizer(f.repository, f.cap.Responses, f.cap.ResponseFills, budgets, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	h.startWorker(t)
	h.generate(t, f.request)
	if budgets.calls.Load() < 2 || f.submits.Load() != 1 {
		t.Fatal("settlement replay did not recover without another paid call")
	}
	h.assertSettled(t, 1)
	leases := liveCloudLeases(t, h)
	for id := range leases {
		paid := requireLiveSettledLease(t, leases, id)
		assertLiveBudgetTotals(t, h, liveLeaseTotals(t, paid, false))
	}
	assertLivePending(t, h)
}

type liveCloudLease struct {
	OperationID   string `json:"operation_id"`
	IncarnationID string `json:"incarnation_id"`
	Status        string `json:"status"`
	Claimed       bool   `json:"claimed"`
	StartByMillis int64  `json:"start_by_millis"`
	Reservations  []struct {
		WindowID  string `json:"window_id"`
		Status    string `json:"status"`
		Reserved  string `json:"reserved_nano"`
		Accounted string `json:"accounted_nano"`
	} `json:"reservations"`
}

func liveCloudLeases(t *testing.T, h *liveCloudWorkflow) map[string]liveCloudLease {
	t.Helper()
	leases := map[string]liveCloudLease{}
	it := h.redis.Scan(h.ctx, 0, h.prefix+"*", 100).Iterator()
	for it.Next(h.ctx) {
		if h.redis.Type(h.ctx, it.Val()).Val() != "string" {
			continue
		}
		raw, err := h.redis.Get(h.ctx, it.Val()).Result()
		if err != nil {
			t.Fatal(err)
		}
		var record liveCloudLease
		if json.Unmarshal([]byte(raw), &record) != nil || record.OperationID == "" || record.Status != "accepted" {
			t.Fatal("invalid Redis authorization tombstone")
		}
		leases[record.OperationID] = record
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return leases
}

// Completion replaces the root's active-attempt pointer with its final result.
// Verify the retained child and its immutable parent link instead.
func liveReplacementAttempt(t *testing.T, h *liveCloudWorkflow, scope cloudstate.Scope, first cloudstate.RequestAttempt) (cloudstate.RequestAttempt, map[string]liveCloudLease) {
	t.Helper()
	leases := liveCloudLeases(t, h)
	if len(leases) != 2 {
		t.Fatal("expected exactly two Redis authorizations")
	}
	for id := range leases {
		if id == string(first.ID) {
			continue
		}
		record, err := h.fixture.repository.Read(h.ctx, scope, cloudstate.RequestID(id))
		if err != nil || record.Status != cloudstate.StatusCompleted {
			t.Fatal("replacement attempt was not completed", err)
		}
		link, err := cloudRequestAttempt(record)
		if err != nil || link == nil || link.RootID != first.RootID || link.PreviousID != first.ID || link.Number != first.Number+1 {
			t.Fatal("replacement attempt lost its lineage", err)
		}
		return *link, leases
	}
	t.Fatal("replacement authorization missing")
	return cloudstate.RequestAttempt{}, nil
}

func requireLiveSettledLease(t *testing.T, leases map[string]liveCloudLease, id string) liveCloudLease {
	t.Helper()
	record, ok := leases[id]
	if !ok || !record.Claimed || len(record.Reservations) != 2 {
		t.Fatal("missing paid Redis authorization")
	}
	_ = liveLeaseTotals(t, record, false)
	return record
}

func liveLeaseTotals(t *testing.T, record liveCloudLease, held bool) map[string]int64 {
	t.Helper()
	totals := map[string]int64{}
	for _, reservation := range record.Reservations {
		amount := reservation.Accounted
		if held {
			if reservation.Status != "reserved" || reservation.Accounted != "0" {
				t.Fatal("unknown paid work was released or settled")
			}
			amount = reservation.Reserved
		} else if reservation.Status != "finalized" || reservation.Reserved != "0" {
			t.Fatal("completed paid work was not settled")
		}
		n, err := strconv.ParseInt(amount, 10, 64)
		if err != nil || n <= 0 {
			t.Fatal("invalid nonzero budget amount", err)
		}
		totals[reservation.WindowID] += n
	}
	return totals
}

// Tombstones alone do not prove accounting: check the aggregate window hashes
// that Redis actually uses to accept or deny the next reservation.
func assertLiveBudgetTotals(t *testing.T, h *liveCloudWorkflow, windows map[string]int64) {
	t.Helper()
	var got, want []int64
	for _, amount := range windows {
		want = append(want, amount)
	}
	it := h.redis.Scan(h.ctx, 0, h.prefix+"*", 100).Iterator()
	for it.Next(h.ctx) {
		if h.redis.Type(h.ctx, it.Val()).Val() != "hash" {
			continue
		}
		fields, err := h.redis.HGetAll(h.ctx, it.Val()).Result()
		if err != nil {
			t.Fatal(err)
		}
		var total int64
		for field, value := range fields {
			if strings.HasPrefix(field, "sum:") {
				n, err := strconv.ParseInt(value, 10, 64)
				if err != nil || n < 0 {
					t.Fatal("invalid aggregate budget", err)
				}
				total += n
			}
		}
		got = append(got, total)
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("Redis active budget totals = %v; want %v", got, want)
	}
}

func waitLiveRedisDeadline(t *testing.T, h *liveCloudWorkflow, deadline time.Time) {
	t.Helper()
	for {
		now, err := h.redis.Time(h.ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		if !now.Before(deadline) {
			return
		}
		if deadline.Sub(now) > 10*time.Second {
			t.Fatal("test did not create an aged authorization")
		}
		timer := time.NewTimer(deadline.Sub(now) + time.Millisecond)
		select {
		case <-timer.C:
		case <-h.ctx.Done():
			timer.Stop()
			t.Fatal(h.ctx.Err())
		}
	}
}

func assertLivePending(t *testing.T, h *liveCloudWorkflow, want ...cloudstate.RequestID) {
	t.Helper()
	var got []cloudstate.RequestID
	for shard := range cloudstate.PendingShards {
		page, err := h.fixture.repository.ListPending(h.ctx, shard, 100, "")
		if err != nil || page.NextPageToken != "" {
			t.Fatal("unexpected pending page", err)
		}
		for _, request := range page.Requests {
			got = append(got, request.ID)
		}
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("pending request IDs = %v; want %v", got, want)
	}
}
