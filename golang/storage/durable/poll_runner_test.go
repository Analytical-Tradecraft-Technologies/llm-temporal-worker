package durable

import (
	"context"
	"errors"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"reflect"
	"testing"
	"time"
)

type pollAdapter struct {
	provider.Adapter
	outcome provider.ResumableResult
	err     error
	calls   int
}

func (a *pollAdapter) Submit(context.Context, provider.Call, provider.Observer) (provider.ResumableResult, error) {
	panic("poll must never submit")
}
func (a *pollAdapter) Poll(context.Context, provider.Call, string, provider.Observer) (provider.ResumableResult, error) {
	a.calls++
	return a.outcome, a.err
}
func pendingHandle(kind string) llm.PendingOperationV1 {
	return llm.PendingOperationV1{OperationID: "operation-id", Kind: kind, Provider: "provider", EndpointID: "endpoint-1", ProviderOperationID: "resp_1"}
}

type pollBudget struct {
	requests []ReconcileRequest
	err      error
}

func (b *pollBudget) Accept(context.Context, ReserveRequest) (ReserveResult, error) {
	panic("poll must never reserve")
}
func (b *pollBudget) Reconcile(_ context.Context, r ReconcileRequest) error {
	b.requests = append(b.requests, r)
	return b.err
}

func TestPollOnceAndRetrySettlement(t *testing.T) {
	ctx := context.Background()
	request := llm.PollRequestV1{Context: testGenerateRequest().Context, Pending: pendingHandle("generate")}
	adapter := &pollAdapter{outcome: provider.ResumableResult{State: provider.ResumablePending, ProviderOperationID: "resp_1", Dispatch: provider.DispatchAccepted}}
	budget := &pollBudget{}
	journalCalls := []string{}
	journal := &boundaryJournal{calls: &journalCalls}
	finalized := 0
	replay := PollReplay{Context: request.Context, Pending: request.Pending, Call: provider.Call{EndpointID: "endpoint-1", OperationKey: "operation-1"}, Adapter: adapter, Budget: budget, Journal: journal, Reservation: testReservation(testRoutePlan())}
	response := testFinalization(testGenerateRequest()).Response
	terminal := llm.PollResponseV1{Status: "completed", Pending: request.Pending, Generate: &response}
	finalization := PollFinalization{Response: terminal, FinalizedAt: time.Now().UTC()}
	replay.Finalize = func(context.Context, provider.ResumableResult) (PollFinalization, error) {
		finalized++
		replay.Completed = &finalization
		return finalization, nil
	}
	ports := PollPorts{Load: func(context.Context, llm.PollRequestV1) (PollReplay, error) { return replay, nil }}
	got, err := PollV1(ctx, request, ports)
	if err != nil || got.Status != "pending" || adapter.calls != 1 || finalized != 0 || len(budget.requests) != 0 {
		t.Fatalf("pending: %v %v %d %d", got, err, adapter.calls, finalized)
	}
	adapter.err = errors.New("network failure")
	if _, err = PollV1(ctx, request, ports); err == nil || len(budget.requests) != 0 {
		t.Fatal("transient error released budget")
	}
	adapter.err = nil
	adapter.outcome = provider.ResumableResult{State: provider.ResumableCompleted, ProviderOperationID: "resp_1", Dispatch: provider.DispatchAccepted, Result: provider.Result{Response: llm.Response{OperationKey: "operation-1", Status: llm.ResponseStatusCompleted}}}
	budget.err = errors.New("lost Redis acknowledgement")
	if _, err = PollV1(ctx, request, ports); !errors.Is(err, ErrReconcilePending) {
		t.Fatalf("settlement: %v", err)
	}
	budget.err = nil
	if _, err = PollV1(ctx, request, ports); err != nil {
		t.Fatal(err)
	}
	if adapter.calls != 3 || finalized != 1 || len(budget.requests) != 2 || !reflect.DeepEqual(budget.requests[0], budget.requests[1]) {
		t.Fatal("retry did not reuse terminal result and settlement")
	}
	request.Context.Tenant = "other"
	if _, err = PollV1(ctx, request, ports); err == nil || len(budget.requests) != 2 || adapter.calls != 3 {
		t.Fatal("scope mismatch reached provider/Redis")
	}
}

func TestPollSettlementDeterministicAndConservative(t *testing.T) {
	r := testReservation(testRoutePlan())
	actual := pricing.MustUSD("0.003")
	a, err := PollSettlement(r, &actual, r.Events[0].OccurredAt)
	if err != nil {
		t.Fatal(err)
	}
	b, err := PollSettlement(r, &actual, r.Events[0].OccurredAt)
	if err != nil || !reflect.DeepEqual(a, b) {
		t.Fatal("non-deterministic settlement")
	}
	e := a.Events[0]
	if e.ReservedDecreaseUSD.Cmp(pricing.MustUSD("0.01")) != 0 || e.AccountedIncreaseUSD.Cmp(pricing.MustUSD("0.003")) != 0 {
		t.Fatalf("incorrect settlement: %#v", e)
	}
	unknown, err := PollSettlement(r, nil, r.Events[0].OccurredAt)
	if err != nil {
		t.Fatal(err)
	}
	if !unknown.Events[0].ReservedDecreaseUSD.IsZero() || !unknown.Events[0].AccountedIncreaseUSD.IsZero() {
		t.Fatal("unknown cost released budget")
	}
	if unknown.Events[0].EventID != e.EventID {
		t.Fatal("conflicting costs must share identity so Redis rejects them")
	}
}

func TestGenerateAndCompactSuspendWithoutReconcile(t *testing.T) {
	ctx := context.Background()
	events := []string{}
	gp := testGeneratePorts(&events, "")
	h := pendingHandle("generate")
	saved := false
	gp.Dispatch = func(context.Context, llm.GenerateRequestV1, GenerateReplay, RoutePlan, JournalReceipt) (DispatchResult, error) {
		return DispatchResult{Pending: &h}, nil
	}
	gp.Suspend = func(context.Context, llm.GenerateRequestV1, GenerateReplay, RoutePlan, ReserveResult, DispatchResult) error {
		saved = true
		return nil
	}
	got, err := GenerateV1(ctx, testGenerateRequest(), gp)
	if err != nil || got.Pending == nil || !saved {
		t.Fatalf("Generate: %v %v", got, err)
	}
	for _, e := range events {
		if e == "finalize" || e == "reconcile" {
			t.Fatal("released pending budget")
		}
	}
	gp.Replay = func(context.Context, llm.GenerateRequestV1) (GenerateReplay, error) {
		return GenerateReplay{Pending: &h}, nil
	}
	events = nil
	if _, err = GenerateV1(ctx, testGenerateRequest(), gp); err != nil || len(events) != 0 {
		t.Fatal("pending replay re-dispatched")
	}
	events = nil
	cp := testCompactPorts(&events, "")
	h = pendingHandle("compact")
	h.EndpointID = "compact-endpoint"
	saved = false
	cp.Dispatch = func(context.Context, llm.CompactRequestV1, CompactReplay, RoutePlan, JournalReceipt) (CompactDispatchResult, error) {
		return CompactDispatchResult{Pending: &h}, nil
	}
	cp.Suspend = func(context.Context, llm.CompactRequestV1, CompactReplay, RoutePlan, ReserveResult, CompactDispatchResult) error {
		saved = true
		return nil
	}
	compact, err := CompactV1(ctx, testCompactRequest(), cp)
	if err != nil || compact.Pending == nil || !saved {
		t.Fatalf("Compact: %v %v", compact, err)
	}
	for _, e := range events {
		if e == "finalize" || e == "reconcile" {
			t.Fatal("released pending compact budget")
		}
	}
}

func TestPollFailureRetainsBudgetAndJournalsBeforeRedis(t *testing.T) {
	for _, state := range []provider.ResumableState{provider.ResumableFailed, provider.ResumableNotFound} {
		t.Run(string(state), func(t *testing.T) {
			request := llm.PollRequestV1{Context: testGenerateRequest().Context, Pending: pendingHandle("generate")}
			outcome := provider.ResumableResult{State: state, Dispatch: provider.DispatchAmbiguous}
			if state == provider.ResumableFailed {
				outcome.Dispatch = provider.DispatchAccepted
				outcome.ProviderOperationID = "resp_1"
				outcome.Failure = provider.NewError(provider.CodeProviderUnavailable, provider.PhasePoll, provider.DispatchAccepted, provider.RetryNever, "safe")
			}
			adapter := &pollAdapter{outcome: outcome}
			materializer := &pollBudget{}
			journalCalls := []string{}
			journal := &boundaryJournal{calls: &journalCalls, failComplete: 1}
			replay := PollReplay{Context: request.Context, Pending: request.Pending, Call: provider.Call{EndpointID: "endpoint-1", OperationKey: "operation-1"}, Adapter: adapter, Budget: materializer, Journal: journal, Reservation: testReservation(testRoutePlan())}
			replay.Finalize = func(_ context.Context, r provider.ResumableResult) (PollFinalization, error) {
				if r.State != provider.ResumableFailed {
					t.Fatal("missing result not classified terminal")
				}
				value := PollFinalization{Response: llm.PollResponseV1{Status: "failed", Pending: request.Pending, Failure: &llm.PollFailureV1{Code: "result_unavailable", CostUnknown: true}}, FinalizedAt: time.Now().UTC()}
				replay.Completed = &value
				return value, nil
			}
			ports := PollPorts{Load: func(context.Context, llm.PollRequestV1) (PollReplay, error) { return replay, nil }}
			if _, err := PollV1(context.Background(), request, ports); !errors.Is(err, ErrReconcilePending) || len(materializer.requests) != 0 {
				t.Fatalf("journal failure: %v", err)
			}
			journal.failComplete = 0
			got, err := PollV1(context.Background(), request, ports)
			if err != nil || got.Status != "failed" || adapter.calls != 1 {
				t.Fatalf("retry: %v %v", got, err)
			}
			event := materializer.requests[0].Events[0]
			if !event.ReservedDecreaseUSD.IsZero() || event.Kind != "retain_ambiguous" {
				t.Fatal("failed unknown request released funds")
			}
		})
	}
}

func TestBackgroundRouteRequiresPersistenceBeforeReserve(t *testing.T) {
	events := []string{}
	ports := testGeneratePorts(&events, "")
	ports.Route = func(context.Context, llm.GenerateRequestV1, GenerateReplay, CompactionDecision) (RoutePlan, error) {
		route := testRoutePlan()
		route.Background = true
		return route, nil
	}
	if _, err := GenerateV1(context.Background(), testGenerateRequest(), ports); err == nil {
		t.Fatal("missing suspend accepted")
	}
	for _, event := range events {
		if event == "reserve" || event == "dispatch" {
			t.Fatal("unrecoverable submission")
		}
	}
}
