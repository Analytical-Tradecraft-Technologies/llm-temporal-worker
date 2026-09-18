//go:build integration

package redis

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	durable "github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// TestLiveRedisBudgetMaterializerContract exercises the active durable budget
// port against the same provisioned Redis Function used by production. It
// covers acceptance, idempotent denial, and duplicate completion reconciliation
// without depending on process-local state.
func TestLiveRedisBudgetMaterializerContract(t *testing.T) {
	client := openLiveRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	keys := liveKeyOptions("durable-materializer")
	cleanupLivePrefix(t, client, keys.Prefix)

	now := time.Now().UTC().Truncate(time.Second)
	materializer, err := NewRedisBudgetMaterializer(RedisBudgetMaterializerOptions{
		Client: client, Mode: AdmissionModeFunction, Keys: keys,
		GenerationID:  durable.GenerationID("generation-live"),
		IncarnationID: durable.IncarnationID("incarnation-live"),
		Clock:         func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	reservation := admission.WindowReservation{
		PolicyID: "durable-policy", WindowID: "hour", Bucket: now.Unix() / 3600,
		AmountUSD: pricing.MustUSD("0.01"), LimitUSD: pricing.MustUSD("0.02"),
		BucketNanos: int64(time.Hour), DurationNanos: int64(24 * time.Hour),
	}
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
	client := openLiveRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	keys := liveKeyOptions("durable-materializer-batch")
	cleanupLivePrefix(t, client, keys.Prefix)
	now := time.Now().UTC().Truncate(time.Second)
	materializer, err := NewRedisBudgetMaterializer(RedisBudgetMaterializerOptions{
		Client: client, Mode: AdmissionModeFunction, Keys: keys,
		GenerationID: durable.GenerationID("generation-batch"), IncarnationID: durable.IncarnationID("incarnation-batch"),
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	reservation := admission.WindowReservation{
		PolicyID: "batch-policy", WindowID: "hour", Bucket: now.Unix() / 3600,
		AmountUSD: pricing.MustUSD("0.01"), LimitUSD: pricing.MustUSD("0.02"),
		BucketNanos: int64(time.Hour), DurationNanos: int64(24 * time.Hour),
	}
	request := durable.ReserveRequest{OperationID: "batch-operation", GenerationID: "generation-batch", ExpiresAt: now.Add(time.Hour), Reservations: []admission.WindowReservation{reservation}}
	accepted, err := materializer.Accept(ctx, request)
	if err != nil {
		t.Fatalf("accepted reservation = %v", err)
	}
	base := accepted.Events[0]
	completion := func(id string) budget.CompletionEvent {
		return budget.CompletionEvent{EventID: id, GenerationID: string(request.GenerationID), OperationID: string(request.OperationID), WindowID: base.WindowID,
			BucketStart: base.BucketStart, ReservationRevision: base.ReservationRevision + 1, Kind: budget.JournalFinalizeExact,
			ReservedDecreaseUSD: base.AmountUSD, AccountedIncreaseUSD: base.AmountUSD, ActualCostUSD: ptrUSD(base.AmountUSD), CostStatus: budget.CostExact, OccurredAt: now}
	}
	first, second := completion("batch-first"), completion("batch-second")
	if err := materializer.Reconcile(ctx, durable.ReconcileRequest{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: "incarnation-batch", Events: []budget.CompletionEvent{first, second}}); !errors.Is(err, ErrRedisBudgetConflict) {
		t.Fatalf("mixed completion batch = %v, want conflict", err)
	}
	if err := materializer.Reconcile(ctx, durable.ReconcileRequest{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: "incarnation-batch", Events: []budget.CompletionEvent{first}}); err != nil {
		t.Fatalf("first completion after rejected batch = %v", err)
	}
}

func TestLiveRedisBatchEscrowSequenceReplayAndClose(t *testing.T) {
	client := openLiveRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	keys := liveKeyOptions("durable-batch-escrow")
	cleanupLivePrefix(t, client, keys.Prefix)
	now := time.Now().UTC().Truncate(time.Second)
	materializer, err := NewRedisBudgetMaterializer(RedisBudgetMaterializerOptions{Client: client, Mode: AdmissionModeFunction, Keys: keys, GenerationID: "generation-escrow", IncarnationID: "incarnation-escrow", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	reservation := durableTestReservation(now, "0.10", "0.10")
	secondWindow := reservation
	secondWindow.WindowID = "second-window"
	escrow := durableTestRequest(now)
	escrow.OperationID = "escrow-operation"
	escrow.GenerationID = "generation-escrow"
	escrow.IncarnationID = "incarnation-escrow"
	escrow.ExpiresAt = now.Add(time.Hour)
	escrow.LogicalCostUSD = pricing.MustUSD("0.10")
	escrow.Reservations = []admission.WindowReservation{reservation, secondWindow}
	escrowDigest := sha256.Sum256([]byte("escrow"))
	if result, acceptErr := materializer.AcceptBatchEscrow(ctx, escrowDigest, escrow); acceptErr != nil || !result.Accepted {
		t.Fatalf("accept escrow = %#v, %v", result, acceptErr)
	}
	child := func(id, amount string) durable.ReserveRequest {
		request := escrow
		request.OperationID = durable.OperationID(id)
		request.LogicalCostUSD = pricing.MustUSD(amount)
		request.ExpiresAt = now.Add(30 * time.Minute)
		value := reservation
		value.AmountUSD = pricing.MustUSD(amount)
		other := secondWindow
		other.AmountUSD = request.LogicalCostUSD
		request.Reservations = []admission.WindowReservation{value, other}
		return request
	}
	first := child("grant-first", "0.03")
	firstDigest := sha256.Sum256([]byte("allocation-one"))
	firstResult, err := materializer.AllocateBatchGrants(ctx, firstDigest, escrow, 1, []durable.ReserveRequest{first})
	if err != nil || len(firstResult) != 1 || !firstResult[0].Accepted {
		t.Fatalf("first allocation = %#v, %v", firstResult, err)
	}
	replay, err := materializer.AllocateBatchGrants(ctx, firstDigest, escrow, 1, []durable.ReserveRequest{first})
	if err != nil || len(replay) != 1 || replay[0].OperationID != firstResult[0].OperationID {
		t.Fatalf("same-sequence replay = %#v, %v", replay, err)
	}
	escrowTTL, ttlErr := client.PTTL(ctx, materializer.space.durableBudgetOperationKey(string(escrow.GenerationID), string(escrow.OperationID))).Result()
	if ttlErr != nil || escrowTTL <= 45*time.Minute {
		t.Fatalf("escrow TTL after shorter first wave = %v, %v; want phase retention", escrowTTL, ttlErr)
	}
	changedDigest := sha256.Sum256([]byte("changed-sequence-one"))
	if _, err = materializer.AllocateBatchGrants(ctx, changedDigest, escrow, 1, []durable.ReserveRequest{child("grant-changed", "0.01")}); !errors.Is(err, ErrRedisBudgetConflict) {
		t.Fatalf("changed sequence replay = %v, want conflict", err)
	}
	skippedDigest := sha256.Sum256([]byte("skipped-sequence"))
	if _, err = materializer.AllocateBatchGrants(ctx, skippedDigest, escrow, 3, []durable.ReserveRequest{child("grant-skipped", "0.01")}); !errors.Is(err, ErrRedisBudgetConflict) {
		t.Fatalf("skipped sequence = %v, want conflict", err)
	}
	second := child("grant-second", "0.04")
	secondDigest := sha256.Sum256([]byte("allocation-two"))
	if _, err = materializer.AllocateBatchGrants(ctx, secondDigest, escrow, 2, []durable.ReserveRequest{second}); err != nil {
		t.Fatalf("second allocation = %v", err)
	}
	remainingRequest, _, remainingErr := materializer.LoadBatchEscrow(ctx, escrowDigest, escrow.OperationID)
	if remainingErr != nil || len(remainingRequest.Reservations) != 2 || remainingRequest.LogicalCostUSD.Cmp(pricing.MustUSD("0.10")) != 0 || remainingRequest.RemainingEscrowCostUSD.Cmp(pricing.MustUSD("0.03")) != 0 || remainingRequest.Reservations[0].AmountUSD.Cmp(pricing.MustUSD("0.10")) != 0 {
		t.Fatalf("remaining escrow = %#v, %v", remainingRequest, remainingErr)
	}
	oversubDigest := sha256.Sum256([]byte("oversubscribe"))
	if _, err = materializer.AllocateBatchGrants(ctx, oversubDigest, escrow, 3, []durable.ReserveRequest{child("grant-oversubscribe", "0.04")}); !errors.Is(err, ErrRedisBudgetConflict) {
		t.Fatalf("oversubscribe = %v, want conflict", err)
	}
	expired := child("grant-expired", "0.01")
	expired.ExpiresAt = now
	if _, err = materializer.AllocateBatchGrants(ctx, sha256.Sum256([]byte("expired")), escrow, 3, []durable.ReserveRequest{expired}); err == nil {
		t.Fatal("expired allocation unexpectedly succeeded")
	}
	closeDigest := sha256.Sum256([]byte("close"))
	refund, existing, err := materializer.CloseBatchEscrow(ctx, escrowDigest, escrow, closeDigest)
	if err != nil || existing || refund.String() != "0.030000000000000000" {
		t.Fatalf("close = %s, existing=%v, err=%v", refund.String(), existing, err)
	}
	replayRefund, existing, err := materializer.CloseBatchEscrow(ctx, escrowDigest, escrow, closeDigest)
	if err != nil || !existing || replayRefund.Cmp(refund) != 0 {
		t.Fatalf("close replay = %s, existing=%v, err=%v", replayRefund.String(), existing, err)
	}
	afterCloseDigest := sha256.Sum256([]byte("after-close"))
	if _, err = materializer.AllocateBatchGrants(ctx, afterCloseDigest, escrow, 3, []durable.ReserveRequest{child("grant-after-close", "0.01")}); !errors.Is(err, ErrRedisBudgetConflict) {
		t.Fatalf("allocation after close = %v, want conflict", err)
	}
	now = now.Add(2 * time.Hour)
	snapshot, found, err := materializer.LoadBatchMaterialization(ctx, firstDigest)
	if err != nil || !found || len(snapshot.Requests) != 1 || snapshot.Requests[0].OccurredAt != first.OccurredAt || snapshot.Requests[0].ExpiresAt != first.ExpiresAt || snapshot.Requests[0].LogicalCostUSD.Cmp(first.LogicalCostUSD) != 0 || snapshot.RemainingEscrowCostUSD.Cmp(pricing.MustUSD("0.07")) != 0 {
		t.Fatalf("historical allocation after close = %#v, found=%v, err=%v", snapshot, found, err)
	}
}

func TestLiveRedisDispatchFenceRetainsTerminalReconciliationPastLease(t *testing.T) {
	client := openLiveRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	keys := liveKeyOptions("durable-dispatch-fence")
	cleanupLivePrefix(t, client, keys.Prefix)
	now := time.Now().UTC()
	materializer, err := NewRedisBudgetMaterializer(RedisBudgetMaterializerOptions{
		Client: client, Mode: AdmissionModeFunction, Keys: keys,
		GenerationID: "generation-fence", IncarnationID: "incarnation-fence",
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseUntil := now.Add(5 * time.Second)
	route := durable.DispatchRouteFacts{
		RouteID: "route-fence", EndpointID: "endpoint-fence", Provider: "provider-fence",
		ResolvedModel: "model-fence", ServiceClass: "standard", PriceVersion: "prices-fence",
	}
	reservation := admission.WindowReservation{
		PolicyID: "fence-policy", WindowID: "minute", Bucket: now.Unix() / 60,
		AmountUSD: pricing.MustUSD("0.01"), LimitUSD: pricing.MustUSD("1"),
		BucketNanos: int64(time.Minute), DurationNanos: int64(time.Hour),
	}
	request := durable.ReserveRequest{
		OperationID: "operation-fenced", GenerationID: "generation-fence", IncarnationID: "incarnation-fence",
		ExpiresAt: leaseUntil, OccurredAt: now, Route: route,
		Reservations: []admission.WindowReservation{reservation},
	}
	accepted, err := materializer.Accept(ctx, request)
	if err != nil || !accepted.Accepted {
		t.Fatalf("acceptance = %#v, %v", accepted, err)
	}
	expiredRequest := request
	expiredRequest.OperationID = "operation-unfenced"
	if result, err := materializer.Accept(ctx, expiredRequest); err != nil || !result.Accepted {
		t.Fatalf("unfenced acceptance = %#v, %v", result, err)
	}
	time.Sleep(time.Until(leaseUntil.Add(-300 * time.Millisecond)))
	fence := durable.DispatchFenceRequest{Reservation: request, RetainUntil: now.Add(30 * time.Second)}
	if err := materializer.FenceDispatch(ctx, fence); err != nil {
		t.Fatalf("near-expiry fence = %v", err)
	}
	stale := fence
	stale.Reservation.Route.RouteID = "route-stale"
	if err := materializer.FenceDispatch(ctx, stale); !errors.Is(err, ErrRedisBudgetConflict) {
		t.Fatalf("stale fence = %v, want conflict", err)
	}
	time.Sleep(time.Until(leaseUntil.Add(200 * time.Millisecond)))
	if err := materializer.FenceDispatch(ctx, fence); err != nil {
		t.Fatalf("idempotent fence after original lease = %v", err)
	}
	if err := materializer.FenceDispatch(ctx, durable.DispatchFenceRequest{
		Reservation: expiredRequest, RetainUntil: now.Add(30 * time.Second),
	}); !errors.Is(err, ErrRedisBudgetReservationNotFound) {
		t.Fatalf("expired unfenced reservation = %v, want not found", err)
	}
	event := accepted.Events[0]
	cost := pricing.MustUSD("0.005")
	completion := budget.CompletionEvent{
		EventID: "completion-fenced", GenerationID: string(request.GenerationID), OperationID: string(request.OperationID),
		WindowID: event.WindowID, BucketStart: event.BucketStart, ReservationRevision: event.ReservationRevision + 1,
		Kind: budget.JournalFinalizeExact, ReservedDecreaseUSD: event.AmountUSD, AccountedIncreaseUSD: cost,
		ActualCostUSD: &cost, CostStatus: budget.CostExact, OccurredAt: time.Now().UTC(),
	}
	if err := materializer.Reconcile(ctx, durable.ReconcileRequest{
		OperationID: request.OperationID, GenerationID: request.GenerationID,
		IncarnationID: request.IncarnationID, Events: []budget.CompletionEvent{completion},
	}); err != nil {
		t.Fatalf("terminal reconciliation after original lease = %v", err)
	}
}
