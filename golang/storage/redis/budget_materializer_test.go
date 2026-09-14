package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	value  string
	values map[string]string
	err    error
	calls  int
	keys   []string
}

func (reader *durableMaterializerReader) Get(_ context.Context, key string) (string, error) {
	reader.calls++
	reader.keys = append(reader.keys, key)
	if reader.err != nil {
		return "", reader.err
	}
	if reader.values != nil {
		return reader.values[key], nil
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
		OperationID: "operation-1", GenerationID: "generation-1", IncarnationID: "incarnation-1",
		ExpiresAt: now.Add(10 * time.Minute), OccurredAt: now,
		LogicalCostUSD: pricing.MustUSD("0.01"),
		Route: durable.DispatchRouteFacts{
			RouteID: "route-1", EndpointID: "endpoint-1", Provider: "provider-1",
			ResolvedModel: "model-1", ServiceClass: "standard", PriceVersion: "prices-1",
		},
		Reservations: []admission.WindowReservation{durableTestReservation(now, "0.01", "1")},
	}
}

func durableTestCompletionEvent(now time.Time, operation durable.OperationID, generation durable.GenerationID) budget.CompletionEvent {
	return budget.CompletionEvent{
		EventID: "event-1", GenerationID: string(generation), OperationID: string(operation), WindowID: "window",
		BucketStart: time.Unix(0, int64(time.Minute)).UTC(), ReservationRevision: 2, Kind: budget.JournalFinalizeExact,
		ReservedDecreaseUSD: pricing.MustUSD("0.01"), AccountedIncreaseUSD: pricing.MustUSD("0.005"),
		ActualCostUSD: ptrUSD(pricing.MustUSD("0.005")), CostStatus: budget.CostExact, OccurredAt: now,
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
	logicalNano, err := durableLogicalCostNano(request.LogicalCostUSD, false)
	if err != nil {
		t.Fatal(err)
	}
	return mustJSON(t, durableOperation{
		Schema: "durable-budget/v1", OperationID: string(request.OperationID), GenerationID: string(request.GenerationID),
		IncarnationID: "incarnation-1", Fingerprint: fingerprint, Status: "accepted", OccurredAt: now,
		ExpiresAt: request.ExpiresAt, LogicalCostNano: logicalNano, RemainingEscrowNano: logicalNano,
		Route: request.Route, Bounds: request.Bounds, Reservations: reservations, Events: map[string]string{},
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
	materializer.reader = &durableMaterializerReader{value: durableAcceptedRecord(t, request, now)}
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
	var materializedEvents []durableEvent
	if err := json.Unmarshal([]byte(invoker.args[0][4]), &materializedEvents); err != nil {
		t.Fatalf("materialized completion payload = %v", err)
	}
	if len(materializedEvents) != 1 || materializedEvents[0].ReservationRevision != 2 {
		t.Fatalf("materialized completion revision = %#v, want 2", materializedEvents)
	}
}

func TestRedisBudgetMaterializerReconcilesRecordedGenerationAfterRotation(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	reservation := durableTestRequest(now)
	reader := &durableMaterializerReader{value: durableAcceptedRecord(t, reservation, now)}
	invoker := &durableMaterializerInvoker{result: []any{"ok", ""}}
	materializer := testRedisMaterializer(t, invoker)
	materializer.generation = "generation-2"
	materializer.incarnation = "incarnation-2"
	materializer.reader = reader

	reconcile := durable.ReconcileRequest{
		OperationID: reservation.OperationID, GenerationID: reservation.GenerationID, IncarnationID: "incarnation-1",
		Events: []budget.CompletionEvent{durableTestCompletionEvent(now, reservation.OperationID, reservation.GenerationID)},
	}
	if err := materializer.Reconcile(context.Background(), reconcile); err != nil {
		t.Fatal(err)
	}
	expectedKey := materializer.space.durableBudgetOperationKey(string(reservation.GenerationID), string(reservation.OperationID))
	if reader.calls != 1 || len(reader.keys) != 1 || reader.keys[0] != expectedKey {
		t.Fatalf("operation record reads = %#v, want %q", reader.keys, expectedKey)
	}
	if invoker.calls != 1 || len(invoker.args[0]) != 5 ||
		invoker.args[0][0] != "durable_reconcile" ||
		invoker.args[0][1] != string(reservation.GenerationID) ||
		invoker.args[0][2] != "incarnation-1" ||
		invoker.args[0][3] != string(reservation.OperationID) {
		t.Fatalf("reconcile invocation = %#v", invoker.args)
	}
}

func TestRedisBudgetMaterializerReconcileRejectsForgedStoredIdentityWithoutMutation(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	reservation := durableTestRequest(now)
	record := durableAcceptedRecord(t, reservation, now)
	tests := []struct {
		name        string
		operation   durable.OperationID
		generation  durable.GenerationID
		incarnation durable.IncarnationID
		want        error
	}{
		{name: "generation", operation: reservation.OperationID, generation: "generation-forged", incarnation: "incarnation-1", want: ErrRedisBudgetGenerationMismatch},
		{name: "incarnation", operation: reservation.OperationID, generation: reservation.GenerationID, incarnation: "incarnation-forged", want: ErrRedisBudgetIncarnationMismatch},
		{name: "operation", operation: "operation-forged", generation: reservation.GenerationID, incarnation: "incarnation-1", want: ErrRedisBudgetReservationNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := &durableMaterializerReader{value: record}
			invoker := &durableMaterializerInvoker{result: []any{"ok", ""}}
			materializer := testRedisMaterializer(t, invoker)
			materializer.generation = "generation-2"
			materializer.incarnation = "incarnation-2"
			materializer.reader = reader
			reconcile := durable.ReconcileRequest{
				OperationID: test.operation, GenerationID: test.generation, IncarnationID: test.incarnation,
				Events: []budget.CompletionEvent{durableTestCompletionEvent(now, test.operation, test.generation)},
			}
			err := materializer.Reconcile(context.Background(), reconcile)
			if !errors.Is(err, test.want) {
				t.Fatalf("reconcile error = %v, want %v", err, test.want)
			}
			expectedKey := materializer.space.durableBudgetOperationKey(string(test.generation), string(test.operation))
			if reader.calls != 1 || len(reader.keys) != 1 || reader.keys[0] != expectedKey {
				t.Fatalf("operation record reads = %#v, want %q", reader.keys, expectedKey)
			}
			if invoker.calls != 0 {
				t.Fatalf("forged identity invoked Redis mutation %d times", invoker.calls)
			}
		})
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

func TestRedisBudgetMaterializerRoundsSubsecondExpiryUp(t *testing.T) {
	now := time.Unix(1_700_000_000, 100_000_000).UTC()
	request := durableTestRequest(now)
	request.ExpiresAt = now.Add(500 * time.Millisecond)
	reservations, err := canonicalDurableReservations(request.OperationID, request.GenerationID, request.Reservations, request.ExpiresAt, now)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := reservations[0].ExpiresMillis, request.ExpiresAt.UnixMilli(); got != want {
		t.Fatalf("expiry milliseconds = %d, want %d", got, want)
	}
	if got := durableTTLSeconds(request.ExpiresAt, now, reservations); got != 1 {
		t.Fatalf("subsecond TTL = %d, want one second", got)
	}
}

func TestRedisBudgetMaterializerConfirmsExistingImmutableReservationWithoutMutation(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	request := durableTestRequest(now)
	reader := &durableMaterializerReader{value: durableAcceptedRecord(t, request, now)}
	invoker := &durableMaterializerInvoker{}
	materializer := testRedisMaterializer(t, invoker)
	materializer.reader = reader
	materializer.generation = "generation-2"
	materializer.incarnation = "incarnation-2"
	result, err := materializer.Confirm(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Accepted || reader.calls != 1 || invoker.calls != 0 {
		t.Fatalf("confirmation mutated Redis or lost acceptance: result=%#v reads=%d mutations=%d", result, reader.calls, invoker.calls)
	}
}

func TestRedisBudgetMaterializerBatchGrantUsesCanonicalOperationRecord(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	request := durableTestRequest(now)
	request.Bounds = durable.ReservationBounds{
		OperationSHA256:     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Model:               "model-1",
		MaxInputTokens:      4096,
		MaxOutputTokens:     1024,
		MaxReasoningTokens:  512,
		MaxCacheReadTokens:  2048,
		MaxCacheWriteTokens: 256,
	}
	var record durableOperation
	if err := json.Unmarshal([]byte(durableAcceptedRecord(t, request, now)), &record); err != nil {
		t.Fatal(err)
	}
	batchOperation := record
	batchOperation.Bounds = durable.ReservationBounds{}
	contentDigest := sha256.Sum256([]byte("batch-grant"))
	batchFingerprint := sha256.Sum256([]byte("batch-fingerprint"))
	batch := durableBatchRecord{
		ContentDigest: hex.EncodeToString(contentDigest[:]), Fingerprint: hex.EncodeToString(batchFingerprint[:]),
		Status: "accepted", Operations: []durableOperation{batchOperation},
	}
	invoker := &durableMaterializerInvoker{}
	materializer := testRedisMaterializer(t, invoker)
	reader := &durableMaterializerReader{values: map[string]string{
		materializer.space.durableBudgetBatchKey(string(request.GenerationID), batch.ContentDigest):             mustJSON(t, batch),
		materializer.space.durableBudgetOperationKey(string(request.GenerationID), string(request.OperationID)): mustJSON(t, record),
	}}
	materializer.reader = reader

	confirmedRequest, result, err := materializer.ConfirmBatchGrant(context.Background(), contentDigest, request.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if confirmedRequest.Bounds != request.Bounds || confirmedRequest.LogicalCostUSD.Cmp(request.LogicalCostUSD) != 0 || !result.Accepted {
		t.Fatalf("confirmed batch grant = request %#v result %#v", confirmedRequest, result)
	}
	if invoker.calls != 0 {
		t.Fatalf("batch grant confirmation mutated Redis %d times", invoker.calls)
	}
}

func TestRedisBudgetMaterializerConfirmationFailsClosedAtLeaseDeadline(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	request := durableTestRequest(now)
	request.ExpiresAt = now
	reader := &durableMaterializerReader{}
	materializer := testRedisMaterializer(t, &durableMaterializerInvoker{})
	materializer.reader = reader
	if _, err := materializer.Confirm(context.Background(), request); !errors.Is(err, ErrRedisBudgetReservationNotFound) {
		t.Fatalf("expired confirmation error = %v", err)
	}
	if reader.calls != 0 {
		t.Fatal("expired reservation performed a Redis read")
	}
}

func TestRedisBudgetMaterializerDispatchFenceInvokesAtomicImmutableFence(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	request := durableTestRequest(now)
	request.ExpiresAt = now.Add(time.Millisecond)
	retainUntil := now.Add(time.Hour)
	invoker := &durableMaterializerInvoker{result: []any{"fenced", ""}}
	materializer := testRedisMaterializer(t, invoker)
	materializer.generation = "generation-rotated"
	materializer.incarnation = "incarnation-rotated"
	err := materializer.FenceDispatch(context.Background(), durable.DispatchFenceRequest{
		Reservation: request, RetainUntil: retainUntil,
	})
	if err != nil {
		t.Fatalf("near-expiry fence = %v", err)
	}
	if invoker.calls != 1 || len(invoker.args[0]) != 8 || invoker.args[0][0] != "durable_fence" {
		t.Fatalf("fence invocation = %#v", invoker.args)
	}
	if got := invoker.args[0][5]; got != "1700003600000" {
		t.Fatalf("retention deadline = %q", got)
	}
	var route durable.DispatchRouteFacts
	if err := json.Unmarshal([]byte(invoker.args[0][6]), &route); err != nil || route != request.Route {
		t.Fatalf("fenced route = %#v, %v", route, err)
	}
}

func TestRedisBudgetMaterializerDispatchFenceStatusIsFailClosedAndRetrySafe(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	request := durable.DispatchFenceRequest{Reservation: durableTestRequest(now), RetainUntil: now.Add(time.Hour)}
	for _, test := range []struct {
		status  string
		wantErr error
	}{
		{status: "existing"},
		{status: "expired", wantErr: ErrRedisBudgetReservationNotFound},
		{status: "not_found", wantErr: ErrRedisBudgetReservationNotFound},
		{status: "conflict", wantErr: ErrRedisBudgetConflict},
		{status: "generation_mismatch", wantErr: ErrRedisBudgetGenerationMismatch},
		{status: "incarnation_mismatch", wantErr: ErrRedisBudgetIncarnationMismatch},
		{status: "state_unavailable", wantErr: ErrUnavailable},
	} {
		t.Run(test.status, func(t *testing.T) {
			invoker := &durableMaterializerInvoker{result: []any{test.status, ""}}
			err := testRedisMaterializer(t, invoker).FenceDispatch(context.Background(), request)
			if test.wantErr == nil && err != nil {
				t.Fatalf("idempotent fence retry = %v", err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("fence error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func ptrUSD(value pricing.USD) *pricing.USD { return &value }
