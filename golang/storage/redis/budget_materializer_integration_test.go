//go:build integration

package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/conformance"
	durable "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
	redisclient "github.com/redis/go-redis/v9"
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
	testassert.NoError(t, err)
	return ctx, materializer, now
}

// openLiveRedisWithAdmissionLua opens the live Redis with the admission Lua
// script loaded and a context that ends with the test or after timeout.
func openLiveRedisWithAdmissionLua(t *testing.T, timeout time.Duration) (*redisclient.Client, context.Context) {
	t.Helper()
	client := openLiveRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	testassert.NoError(t, client.ScriptLoad(ctx, AdmissionLuaSource()).Err())
	return client, ctx
}

// acquireLive requires m to accept req.
func acquireLive(t *testing.T, m *RedisBudgetMaterializer, req durable.ReserveRequest) durable.ReserveResult {
	t.Helper()
	result, err := m.Accept(context.Background(), req)
	if err != nil || !result.Accepted {
		t.Fatalf("acquire = %#v, %v", result, err)
	}
	return result
}

// claimOf is the claim request for an accepted reservation.
func claimOf(r durable.ReserveResult) durable.ClaimRequest {
	return durable.ClaimRequest{OperationID: r.OperationID, GenerationID: r.GenerationID, IncarnationID: r.IncarnationID}
}

// claimAccepted claims request's accepted reservation.
func claimAccepted(t *testing.T, ctx context.Context, m *RedisBudgetMaterializer, request durable.ReserveRequest, accepted durable.ReserveResult) {
	t.Helper()
	testassert.NoError(t, testassert.Err(m.Claim(ctx, durable.ClaimRequest{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: accepted.IncarnationID})), "claim")
}

// exactLiveCompletion finalizes accepted's only reservation event at exactly
// its reserved amount.
func exactLiveCompletion(id string, request durable.ReserveRequest, accepted durable.ReserveResult, now time.Time) budget.CompletionEvent {
	base := accepted.Events[0]
	return budget.CompletionEvent{
		EventID: id, GenerationID: string(request.GenerationID), OperationID: string(request.OperationID), WindowID: base.WindowID,
		BucketStart: base.BucketStart, ReservationRevision: base.ReservationRevision + 1, Kind: budget.JournalFinalizeExact,
		ReservedDecreaseUSD: base.AmountUSD, AccountedIncreaseUSD: base.AmountUSD, ActualCostUSD: ptrUSD(base.AmountUSD), CostStatus: budget.CostExact, OccurredAt: now,
	}
}

// liveMinuteReservation reserves amount against limit in the current minute
// of a one-hour rolling window.
func liveMinuteReservation(policy string, now time.Time, amount, limit string) admission.WindowReservation {
	return admission.WindowReservation{PolicyID: policy, WindowID: "hour", Bucket: now.Unix() / 60, BucketNanos: int64(time.Minute), DurationNanos: int64(time.Hour), AmountUSD: pricing.MustUSD(amount), LimitUSD: pricing.MustUSD(limit)}
}

// liveMinuteRequest reserves amount of a 1 USD "tenant" budget for operation id.
func liveMinuteRequest(generation durable.GenerationID, id, amount string) durable.ReserveRequest {
	return durable.ReserveRequest{OperationID: durable.OperationID(id), GenerationID: generation, Reservations: []admission.WindowReservation{liveMinuteReservation("tenant", time.Now().UTC(), amount, "1")}}
}

// exactSettledEvent finalizes r's reservation event e at exactly cost.
func exactSettledEvent(r durable.ReserveResult, e budget.ReservationEvent, id string, cost pricing.USD, at time.Time) budget.CompletionEvent {
	return budget.CompletionEvent{EventID: id, GenerationID: string(r.GenerationID), OperationID: string(r.OperationID), WindowID: e.WindowID, BucketStart: e.BucketStart, ReservationRevision: 2, Kind: budget.JournalFinalizeExact, ReservedDecreaseUSD: e.AmountUSD, AccountedIncreaseUSD: cost, ActualCostUSD: &cost, CostStatus: budget.CostExact, OccurredAt: at}
}

// liveSettlement settles every reservation event of r at amount.
func liveSettlement(r durable.ReserveResult, amount string) durable.ReconcileRequest {
	cost := pricing.MustUSD(amount)
	result := durable.ReconcileRequest{OperationID: r.OperationID, GenerationID: r.GenerationID, IncarnationID: r.IncarnationID}
	for i, e := range r.Events {
		result.Events = append(result.Events, exactSettledEvent(r, e, fmt.Sprintf("%s-complete-%d", r.OperationID, i), cost, time.Now().UTC()))
	}
	return result
}

// liveReconcile settles request's claim by incarnation with events.
func liveReconcile(request durable.ReserveRequest, incarnation durable.IncarnationID, events ...budget.CompletionEvent) durable.ReconcileRequest {
	return durable.ReconcileRequest{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: incarnation, Events: events}
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
	testassert.NoError(t, err, "accepted reservation")
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
	testassert.NoError(t, err, "denied reservation")
	if denied.Accepted || denied.Denial == nil {
		t.Fatalf("denied reservation = %#v", denied)
	}
	deniedReplay, err := materializer.Accept(ctx, deniedRequest)
	testassert.NoError(t, err, "denied replay")
	if deniedReplay.Accepted || deniedReplay.Denial == nil || deniedReplay.Denial.PolicyID != denied.Denial.PolicyID {
		t.Fatalf("denied replay = %#v", deniedReplay)
	}

	claimAccepted(t, ctx, materializer, request, accepted)
	completion := exactLiveCompletion("durable-completion-1", request, accepted, now)
	reconcile := liveReconcile(request, "incarnation-live", completion)
	testassert.NoError(t, materializer.Reconcile(ctx, reconcile), "completion reconciliation")
	testassert.NoError(t, materializer.Reconcile(ctx, reconcile), "duplicate completion reconciliation")
}

func TestLiveRedisBudgetMaterializerRejectsMixedBatchAtomically(t *testing.T) {
	ctx, materializer, now := openLiveMaterializer(t, "durable-materializer-batch", "generation-batch", "incarnation-batch")
	reservation := liveHourReservation("batch-policy", now, "0.01", "0.02")
	request := durable.ReserveRequest{OperationID: "batch-operation", GenerationID: "generation-batch", ExpiresAt: now.Add(time.Hour), Reservations: []admission.WindowReservation{reservation}}
	accepted, err := materializer.Accept(ctx, request)
	testassert.NoError(t, err, "accepted reservation")
	claimAccepted(t, ctx, materializer, request, accepted)
	first, second := exactLiveCompletion("batch-first", request, accepted, now), exactLiveCompletion("batch-second", request, accepted, now)
	if err := materializer.Reconcile(ctx, liveReconcile(request, "incarnation-batch", first, second)); err == nil {
		t.Fatalf("mixed completion batch = %v, want conflict", err)
	}
	testassert.NoError(t, materializer.Reconcile(ctx, liveReconcile(request, "incarnation-batch", first)), "first completion after rejected batch")
}

func TestLiveRedisRollingBudget(t *testing.T) {
	client := openLiveRedis(t)
	testassert.NoError(t, client.ScriptLoad(context.Background(), AdmissionLuaSource()).Err())
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
				m := mustCall(t, NewRedisBudgetMaterializer, RedisBudgetMaterializerOptions{Client: client, Mode: mode, Keys: keys, GenerationID: "rolling-gen", IncarnationID: "rolling-inc", Clock: time.Now})
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
	claimAccepted(t, ctx, materializer, request, accepted)
	completion := exactLiveCompletion("large-completion", request, accepted, now)
	testassert.NoError(t, materializer.Reconcile(ctx, liveReconcile(request, "incarnation-large", completion)), "exact finalization")
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
	claimAccepted(t, ctx, materializer, request, accepted)
	completion := exactLiveCompletion("waited-completion", request, accepted, now)
	testassert.NoError(t, materializer.Reconcile(ctx, liveReconcile(request, "incarnation-waited", completion)), "exact finalization")
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
