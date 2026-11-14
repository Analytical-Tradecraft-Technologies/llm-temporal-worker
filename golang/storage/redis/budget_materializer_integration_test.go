//go:build integration

package redis

import (
	"context"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/conformance"
	durable "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

// openLiveMaterializer clears prefix on the live Redis and returns a
// Function-mode materializer for generation and incarnation whose clock is
// fixed at the returned time, with a context that ends with the test or
// after ten seconds.
func openLiveMaterializer(t *testing.T, prefix string, generation durable.GenerationID, incarnation durable.IncarnationID) (context.Context, *RedisBudgetMaterializer, time.Time) {
	t.Helper()
	client := openLiveRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	keys := liveKeyOptions(prefix)
	cleanupLivePrefix(t, client, keys.Prefix)
	now := time.Now().UTC().Truncate(time.Second)
	materializer, err := NewRedisBudgetMaterializer(RedisBudgetMaterializerOptions{
		Client: client, Mode: AdmissionModeFunction, Keys: keys,
		GenerationID: generation, IncarnationID: incarnation,
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return ctx, materializer, now
}

// liveHourReservation reserves amount against limit in the current hour of
// a one-day rolling window.
func liveHourReservation(policy string, now time.Time, amount, limit string) admission.WindowReservation {
	return admission.WindowReservation{
		PolicyID: policy, WindowID: "hour", Bucket: now.Unix() / 3600,
		AmountUSD: pricing.MustUSD(amount), LimitUSD: pricing.MustUSD(limit),
		BucketNanos: int64(time.Hour), DurationNanos: int64(24 * time.Hour),
	}
}

// TestLiveRedisBudgetMaterializerContract exercises the active durable budget
// port against the same provisioned Redis Function used by production. It
// covers acceptance, idempotent denial, and duplicate completion reconciliation
// without depending on process-local state.
func TestLiveRedisBudgetMaterializerContract(t *testing.T) {
	ctx, materializer, now := openLiveMaterializer(t, "durable-materializer", "generation-live", "incarnation-live")
	reservation := liveHourReservation("durable-policy", now, "0.01", "0.02")
	request := durable.ReserveRequest{
		OperationID: "durable-operation-accepted", GenerationID: "generation-live",
		ExpiresAt: now.Add(time.Hour), Reservations: []admission.WindowReservation{reservation},
	}
	accepted, err := materializer.Accept(ctx, request)
	if err != nil {
		t.Fatalf("accepted reservation = %v", err)
	}
	if !accepted.Accepted || len(accepted.Events) != 1 {
		t.Fatalf("accepted reservation = %#v", accepted)
	}

	deniedRequest := request
	deniedRequest.OperationID = "durable-operation-denied"
	// The first reservation consumes exactly half of the two-cent limit. Keep
	// the denial assertion meaningful by requesting the full limit on the same
	// window; an exact-boundary request is valid and must remain accepted.
	deniedReservation := reservation
	deniedReservation.AmountUSD = pricing.MustUSD("0.02")
	deniedRequest.Reservations = []admission.WindowReservation{deniedReservation}
	denied, err := materializer.Accept(ctx, deniedRequest)
	if err != nil {
		t.Fatalf("denied reservation = %v", err)
	}
	if denied.Accepted || denied.Denial == nil {
		t.Fatalf("denied reservation = %#v", denied)
	}
	deniedReplay, err := materializer.Accept(ctx, deniedRequest)
	if err != nil {
		t.Fatalf("denied replay = %v", err)
	}
	if deniedReplay.Accepted || deniedReplay.Denial == nil || deniedReplay.Denial.PolicyID != denied.Denial.PolicyID {
		t.Fatalf("denied replay = %#v", deniedReplay)
	}

	if _, err := materializer.Claim(ctx, durable.ClaimRequest{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: accepted.IncarnationID}); err != nil {
		t.Fatal(err)
	}
	reservationEvent := accepted.Events[0]
	completion := budget.CompletionEvent{
		EventID: "durable-completion-1", GenerationID: string(request.GenerationID),
		OperationID: string(request.OperationID), WindowID: reservationEvent.WindowID,
		BucketStart: reservationEvent.BucketStart, ReservationRevision: reservationEvent.ReservationRevision + 1,
		Kind: budget.JournalFinalizeExact, ReservedDecreaseUSD: reservationEvent.AmountUSD,
		AccountedIncreaseUSD: reservationEvent.AmountUSD, ActualCostUSD: ptrUSD(reservationEvent.AmountUSD),
		CostStatus: budget.CostExact, OccurredAt: now,
	}
	reconcile := durable.ReconcileRequest{
		OperationID: request.OperationID, GenerationID: request.GenerationID,
		IncarnationID: "incarnation-live", Events: []budget.CompletionEvent{completion},
	}
	if err := materializer.Reconcile(ctx, reconcile); err != nil {
		t.Fatalf("completion reconciliation = %v", err)
	}
	if err := materializer.Reconcile(ctx, reconcile); err != nil {
		t.Fatalf("duplicate completion reconciliation = %v", err)
	}
}

func TestLiveRedisBudgetMaterializerRejectsMixedBatchAtomically(t *testing.T) {
	ctx, materializer, now := openLiveMaterializer(t, "durable-materializer-batch", "generation-batch", "incarnation-batch")
	reservation := liveHourReservation("batch-policy", now, "0.01", "0.02")
	request := durable.ReserveRequest{OperationID: "batch-operation", GenerationID: "generation-batch", ExpiresAt: now.Add(time.Hour), Reservations: []admission.WindowReservation{reservation}}
	accepted, err := materializer.Accept(ctx, request)
	if err != nil {
		t.Fatalf("accepted reservation = %v", err)
	}
	if _, err := materializer.Claim(ctx, durable.ClaimRequest{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: accepted.IncarnationID}); err != nil {
		t.Fatal(err)
	}
	base := accepted.Events[0]
	completion := func(id string) budget.CompletionEvent {
		return budget.CompletionEvent{EventID: id, GenerationID: string(request.GenerationID), OperationID: string(request.OperationID), WindowID: base.WindowID,
			BucketStart: base.BucketStart, ReservationRevision: base.ReservationRevision + 1, Kind: budget.JournalFinalizeExact,
			ReservedDecreaseUSD: base.AmountUSD, AccountedIncreaseUSD: base.AmountUSD, ActualCostUSD: ptrUSD(base.AmountUSD), CostStatus: budget.CostExact, OccurredAt: now}
	}
	first, second := completion("batch-first"), completion("batch-second")
	if err := materializer.Reconcile(ctx, durable.ReconcileRequest{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: "incarnation-batch", Events: []budget.CompletionEvent{first, second}}); err == nil {
		t.Fatalf("mixed completion batch = %v, want conflict", err)
	}
	if err := materializer.Reconcile(ctx, durable.ReconcileRequest{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: "incarnation-batch", Events: []budget.CompletionEvent{first}}); err != nil {
		t.Fatalf("first completion after rejected batch = %v", err)
	}
}

func TestLiveRedisRollingBudget(t *testing.T) {
	client := openLiveRedis(t)
	if err := client.ScriptLoad(context.Background(), AdmissionLuaSource()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if liveRedisLuaScriptCleanupAllowed() {
			if err := client.ScriptFlush(context.Background()).Err(); err != nil {
				t.Errorf("flush isolated Redis Lua script cache: %v", err)
			}
		}
	})
	for _, mode := range []AdmissionMode{AdmissionModeFunction, AdmissionModeLua} {
		t.Run(string(mode), func(t *testing.T) {
			conformance.RunDurableRollingBudget(t, func(t *testing.T) (durable.BudgetMaterializer, func() time.Time, func(time.Duration)) {
				keys := liveKeyOptions("rolling-budget")
				cleanupLivePrefix(t, client, keys.Prefix)
				m, err := NewRedisBudgetMaterializer(RedisBudgetMaterializerOptions{Client: client, Mode: mode, Keys: keys, GenerationID: "rolling-gen", IncarnationID: "rolling-inc", Clock: time.Now})
				if err != nil {
					t.Fatal(err)
				}
				return m, time.Now, time.Sleep
			})
		})
	}
}

// TestLiveRedisBudgetMaterializerDenialDecodesAtLargeActiveTotal reproduces
// the window total crossing 1e14 nano-USD (USD 100,000). Redis' Lua 5.1
// tostring() renders such a total in scientific notation, which the Go decoder
// rejects, so the denial must be formatted as a plain decimal integer.
func TestLiveRedisBudgetMaterializerDenialDecodesAtLargeActiveTotal(t *testing.T) {
	ctx, materializer, now := openLiveMaterializer(t, "durable-large-total", "generation-large", "incarnation-large")
	reservation := liveHourReservation("large-policy", now, "40000.01", "150000")
	request := func(id string) durable.ReserveRequest {
		return durable.ReserveRequest{OperationID: durable.OperationID(id), GenerationID: "generation-large", ExpiresAt: now.Add(time.Hour), Reservations: []admission.WindowReservation{reservation}}
	}
	for _, id := range []string{"large-operation-1", "large-operation-2", "large-operation-3"} {
		accepted, err := materializer.Accept(ctx, request(id))
		if err != nil || !accepted.Accepted {
			t.Fatalf("%s = %#v, %v; want accepted", id, accepted, err)
		}
	}
	denied, err := materializer.Accept(ctx, request("large-operation-4"))
	if err != nil {
		t.Fatalf("fourth reservation = %v, want a decodable denial", err)
	}
	if denied.Accepted || denied.Denial == nil {
		t.Fatalf("fourth reservation = %#v, want denial", denied)
	}
	if got, want := denied.Denial.ActiveUSD, pricing.MustUSD("120000.03"); got.Cmp(want) != 0 {
		t.Fatalf("denial active = %s, want %s", got, want)
	}
}

// TestLiveRedisBudgetMaterializerHandlesSingleReservationAboveHundredThousandUSD
// drives one reservation of at least 1e14 nano-USD through reserve, claim and
// exact finalization, covering the HINCRBY arguments and expiry-index members
// that are formatted from Lua numbers.
func TestLiveRedisBudgetMaterializerHandlesSingleReservationAboveHundredThousandUSD(t *testing.T) {
	ctx, materializer, now := openLiveMaterializer(t, "durable-large-operation", "generation-large", "incarnation-large")
	reservation := liveHourReservation("large-operation-policy", now, "120000.01", "200000")
	request := durable.ReserveRequest{OperationID: "large-single-operation", GenerationID: "generation-large", ExpiresAt: now.Add(time.Hour), Reservations: []admission.WindowReservation{reservation}}
	accepted, err := materializer.Accept(ctx, request)
	if err != nil || !accepted.Accepted || len(accepted.Events) != 1 {
		t.Fatalf("large reservation = %#v, %v; want accepted", accepted, err)
	}
	if _, err := materializer.Claim(ctx, durable.ClaimRequest{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: accepted.IncarnationID}); err != nil {
		t.Fatalf("claim = %v", err)
	}
	base := accepted.Events[0]
	completion := budget.CompletionEvent{
		EventID: "large-completion", GenerationID: string(request.GenerationID), OperationID: string(request.OperationID), WindowID: base.WindowID,
		BucketStart: base.BucketStart, ReservationRevision: base.ReservationRevision + 1, Kind: budget.JournalFinalizeExact,
		ReservedDecreaseUSD: base.AmountUSD, AccountedIncreaseUSD: base.AmountUSD, ActualCostUSD: ptrUSD(base.AmountUSD), CostStatus: budget.CostExact, OccurredAt: now,
	}
	if err := materializer.Reconcile(ctx, durable.ReconcileRequest{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: "incarnation-large", Events: []budget.CompletionEvent{completion}}); err != nil {
		t.Fatalf("exact finalization = %v", err)
	}
	// The finalized USD 120,000.01 stays accounted, so a further USD 80,000
	// would exceed the limit and must come back as a decodable denial.
	second := reservation
	second.AmountUSD = pricing.MustUSD("80000")
	denied, err := materializer.Accept(ctx, durable.ReserveRequest{OperationID: "large-single-denied", GenerationID: "generation-large", ExpiresAt: now.Add(time.Hour), Reservations: []admission.WindowReservation{second}})
	if err != nil {
		t.Fatalf("second reservation = %v, want a decodable denial", err)
	}
	if denied.Accepted || denied.Denial == nil || denied.Denial.ActiveUSD.Cmp(pricing.MustUSD("120000.01")) != 0 {
		t.Fatalf("second reservation = %#v, want denial at the accounted total", denied)
	}
}

// TestLiveRedisBudgetMaterializerWaitedSpendCountsFromAcceptance reproduces
// issue #963 against the provisioned Function: a quote booked into a bucket
// twelve minutes old (still inside its start lease) is accepted, claimed and
// finalized exactly under a ten-minute window, and a second reservation
// seconds later must be denied because the spend counts from acceptance.
func TestLiveRedisBudgetMaterializerWaitedSpendCountsFromAcceptance(t *testing.T) {
	ctx, materializer, now := openLiveMaterializer(t, "durable-waited", "generation-waited", "incarnation-waited")
	quoted := now.Add(-12 * time.Minute)
	reservation := admission.WindowReservation{
		PolicyID: "waited-policy", WindowID: "ten-minutes", Bucket: quoted.UnixNano() / int64(time.Minute),
		AmountUSD: pricing.MustUSD("0.90"), LimitUSD: pricing.MustUSD("1"),
		BucketNanos: int64(time.Minute), DurationNanos: int64(10 * time.Minute),
	}
	request := durable.ReserveRequest{OperationID: "waited-operation", GenerationID: "generation-waited", ExpiresAt: now.Add(3 * time.Minute), Reservations: []admission.WindowReservation{reservation}}
	accepted, err := materializer.Accept(ctx, request)
	if err != nil || !accepted.Accepted || len(accepted.Events) != 1 {
		t.Fatalf("waited reservation = %#v, %v; want accepted", accepted, err)
	}
	if _, err := materializer.Claim(ctx, durable.ClaimRequest{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: accepted.IncarnationID}); err != nil {
		t.Fatalf("claim = %v", err)
	}
	base := accepted.Events[0]
	completion := budget.CompletionEvent{
		EventID: "waited-completion", GenerationID: string(request.GenerationID), OperationID: string(request.OperationID), WindowID: base.WindowID,
		BucketStart: base.BucketStart, ReservationRevision: base.ReservationRevision + 1, Kind: budget.JournalFinalizeExact,
		ReservedDecreaseUSD: base.AmountUSD, AccountedIncreaseUSD: base.AmountUSD, ActualCostUSD: ptrUSD(base.AmountUSD), CostStatus: budget.CostExact, OccurredAt: now,
	}
	if err := materializer.Reconcile(ctx, durable.ReconcileRequest{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: "incarnation-waited", Events: []budget.CompletionEvent{completion}}); err != nil {
		t.Fatalf("exact finalization = %v", err)
	}
	second := reservation
	second.Bucket = now.UnixNano() / int64(time.Minute)
	denied, err := materializer.Accept(ctx, durable.ReserveRequest{OperationID: "waited-second", GenerationID: "generation-waited", ExpiresAt: now.Add(3 * time.Minute), Reservations: []admission.WindowReservation{second}})
	if err != nil {
		t.Fatalf("second reservation = %v, want a denial", err)
	}
	if denied.Accepted || denied.Denial == nil || denied.Denial.ActiveUSD.Cmp(pricing.MustUSD("0.90")) != 0 {
		t.Fatalf("second reservation = %#v, want denial at the waited spend", denied)
	}
}
