package redis

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	durable "github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

type durableMaterializerInvoker struct {
	result []any
	err    error
	calls  int
	args   [][]string
}

type durableMaterializerReader struct {
	value string
	err   error
}

func (reader durableMaterializerReader) Get(context.Context, string) (string, error) {
	if reader.err != nil {
		return "", reader.err
	}
	return reader.value, nil
}

func (invoker *durableMaterializerInvoker) Run(_ context.Context, _ string, _ []string, args ...string) ([]any, error) {
	invoker.calls++
	invoker.args = append(invoker.args, append([]string(nil), args...))
	if invoker.err != nil {
		return nil, invoker.err
	}
	return append([]any(nil), invoker.result...), nil
}

func testRedisMaterializer(t *testing.T, invoker FunctionInvoker) *RedisBudgetMaterializer {
	t.Helper()
	now := time.Unix(1_700_000_000, 0).UTC()
	materializer, err := NewRedisBudgetMaterializer(RedisBudgetMaterializerOptions{
		Invoker:       invoker,
		Keys:          testKeyOptions(),
		GenerationID:  durable.GenerationID("generation-1"),
		IncarnationID: durable.IncarnationID("incarnation-1"),
		Clock:         func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return materializer
}

func durableTestReservation(now time.Time, amount, limit string) admission.WindowReservation {
	return admission.WindowReservation{
		PolicyID: "policy", WindowID: "window", Bucket: now.Unix() / 60,
		AmountUSD: pricing.MustUSD(amount), LimitUSD: pricing.MustUSD(limit),
		BucketNanos: int64(time.Minute), DurationNanos: int64(time.Hour),
	}
}

func durableTestRequest(now time.Time) durable.ReserveRequest {
	return durable.ReserveRequest{
		OperationID: "operation-1", GenerationID: "generation-1",
		ExpiresAt:    now.Add(10 * time.Minute),
		Reservations: []admission.WindowReservation{durableTestReservation(now, "0.01", "1")},
	}
}

func durableAcceptedRecord(t *testing.T, request durable.ReserveRequest, now time.Time) string {
	t.Helper()
	reservations, err := canonicalDurableReservations(request.OperationID, request.GenerationID, request.Reservations, request.ExpiresAt, now)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := durableRequestFingerprint(request, reservations)
	if err != nil {
		t.Fatal(err)
	}
	return mustJSON(t, durableOperation{
		Schema: "durable-budget/v1", OperationID: string(request.OperationID), GenerationID: string(request.GenerationID),
		IncarnationID: "incarnation-1", Fingerprint: fingerprint, Status: "accepted", OccurredAt: now,
		Reservations: reservations, Events: map[string]string{},
	})
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestRedisBudgetMaterializerAcceptsAndReturnsJournalEvents(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	request := durableTestRequest(now)
	invoker := &durableMaterializerInvoker{}
	invoker.result = []any{"created", durableAcceptedRecord(t, request, now)}
	materializer := testRedisMaterializer(t, invoker)
	result, err := materializer.Accept(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Accepted || len(result.Events) != 1 || result.Events[0].AmountUSD.Cmp(pricing.MustUSD("0.01")) != 0 {
		t.Fatalf("accepted result = %#v", result)
	}
	if invoker.calls != 1 || len(invoker.args[0]) != 8 || invoker.args[0][0] != "durable_reserve" {
		t.Fatalf("invocation = %#v", invoker.args)
	}
}

func TestRedisBudgetMaterializerDenialIsReplayable(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	request := durableTestRequest(now)
	reservations, err := canonicalDurableReservations(request.OperationID, request.GenerationID, request.Reservations, request.ExpiresAt, now)
	if err != nil {
		t.Fatal(err)
	}
	invoker := &durableMaterializerInvoker{result: []any{"existing", mustJSON(t, durableOperation{
		Schema: "durable-budget/v1", OperationID: string(request.OperationID), GenerationID: string(request.GenerationID),
		IncarnationID: "incarnation-1", Status: "denied", OccurredAt: now, Reservations: reservations,
		Denial: &durableDenial{PolicyID: "policy", WindowID: "window", LimitNano: "1000000000", ActiveNano: "1000000000", RequestedNano: "10000000"},
	})}}
	result, err := testRedisMaterializer(t, invoker).Accept(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Accepted || result.Denial == nil || result.Denial.PolicyID != "policy" {
		t.Fatalf("denied result = %#v", result)
	}
}

func TestRedisBudgetMaterializerRejectsSnapshotMismatchBeforeRedis(t *testing.T) {
	invoker := &durableMaterializerInvoker{result: []any{"invalid_request", ""}}
	materializer := testRedisMaterializer(t, invoker)
	now := time.Unix(1_700_000_000, 0).UTC()
	request := durableTestRequest(now)
	request.GenerationID = "different-generation"
	if _, err := materializer.Accept(context.Background(), request); !errors.Is(err, ErrRedisBudgetGenerationMismatch) {
		t.Fatalf("generation mismatch = %v", err)
	}
	if invoker.calls != 0 {
		t.Fatalf("generation mismatch invoked Redis %d times", invoker.calls)
	}
}

func TestRedisBudgetMaterializerReconcileDuplicateEventIsIdempotent(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	request := durableTestRequest(now)
	invoker := &durableMaterializerInvoker{result: []any{"ok", ""}}
	materializer := testRedisMaterializer(t, invoker)
	materializer.reader = durableMaterializerReader{value: durableAcceptedRecord(t, request, now)}
	event := budget.CompletionEvent{
		EventID: "event-1", GenerationID: string(request.GenerationID), OperationID: string(request.OperationID), WindowID: "window",
		BucketStart: time.Unix(0, int64(time.Minute)).UTC(), ReservationRevision: 2, Kind: budget.JournalFinalizeExact,
		ReservedDecreaseUSD: pricing.MustUSD("0.01"), AccountedIncreaseUSD: pricing.MustUSD("0.005"),
		ActualCostUSD: ptrUSD(pricing.MustUSD("0.005")), CostStatus: budget.CostExact, OccurredAt: now,
	}
	reconcile := durable.ReconcileRequest{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: "incarnation-1", Events: []budget.CompletionEvent{event}}
	if err := materializer.Reconcile(context.Background(), reconcile); err != nil {
		t.Fatal(err)
	}
	if err := materializer.Reconcile(context.Background(), reconcile); err != nil {
		t.Fatal(err)
	}
	if invoker.calls != 2 || invoker.args[0][0] != "durable_reconcile" {
		t.Fatalf("reconcile invocations = %#v", invoker.args)
	}
}

func TestRedisBudgetMaterializerTimeoutAfterMutationFailsClosed(t *testing.T) {
	invoker := &durableMaterializerInvoker{err: ErrUnavailable}
	materializer := testRedisMaterializer(t, invoker)
	_, err := materializer.Accept(context.Background(), durableTestRequest(time.Unix(1_700_000_000, 0).UTC()))
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("timeout-after-mutation = %v", err)
	}
}

func ptrUSD(value pricing.USD) *pricing.USD { return &value }
