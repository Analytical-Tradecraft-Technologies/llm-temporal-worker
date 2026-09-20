//go:build integration

package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/storage/conformance"
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

// Concurrent terminal polls and retries after lost acknowledgements all reuse
// one event. Prove the resulting budget by admitting the exact remaining amount.
func TestLiveRedisPollSettlementOnce(t *testing.T) {
	client := openLiveRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	keys := liveKeyOptions("poll-settlement")
	cleanupLivePrefix(t, client, keys.Prefix)
	now := time.Now().UTC().Truncate(time.Second)
	materializer, err := NewRedisBudgetMaterializer(RedisBudgetMaterializerOptions{Client: client, Mode: AdmissionModeFunction, Keys: keys, GenerationID: "generation-poll", IncarnationID: "incarnation-poll", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	window := admission.WindowReservation{PolicyID: "poll-policy", WindowID: "hour", Bucket: now.Unix() / 3600, AmountUSD: pricing.MustUSD("0.01"), LimitUSD: pricing.MustUSD("0.02"), BucketNanos: int64(time.Hour), DurationNanos: int64(24 * time.Hour)}
	request := durable.ReserveRequest{OperationID: "poll-operation", GenerationID: "generation-poll", ExpiresAt: now.Add(time.Hour), Reservations: []admission.WindowReservation{window}}
	reservation, err := materializer.Accept(ctx, request)
	if err != nil || !reservation.Accepted {
		t.Fatalf("reserve: %v %v", reservation, err)
	}
	actual := pricing.MustUSD("0.003")
	results := make(chan error, 16)
	for i := 0; i < 16; i++ {
		go func() { results <- durable.ReconcilePollBudget(ctx, materializer, reservation, &actual, now) }()
	}
	for i := 0; i < 16; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	// Different content under the same settlement ID must never silently win.
	conflicting := pricing.MustUSD("0.002")
	if err := durable.ReconcilePollBudget(ctx, materializer, reservation, &conflicting, now); err == nil {
		t.Fatal("conflicting settlement accepted")
	}
	request.OperationID = "remaining-budget"
	window.AmountUSD = pricing.MustUSD("0.017")
	request.Reservations = []admission.WindowReservation{window}
	remaining, err := materializer.Accept(ctx, request)
	if err != nil || !remaining.Accepted {
		t.Fatalf("exact remaining budget unavailable: %v %v", remaining, err)
	}
	request.OperationID = "over-budget"
	window.AmountUSD = pricing.MustUSD("0.000001")
	request.Reservations = []admission.WindowReservation{window}
	excess, err := materializer.Accept(ctx, request)
	if err != nil || excess.Accepted {
		t.Fatalf("budget over-released: %v %v", excess, err)
	}
}
