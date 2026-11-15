package durable

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

func testCompactRequest() llm.CompactRequestV1 {
	return llm.CompactRequestV1{
		APIVersion:   llm.CompactAPIVersion,
		OperationKey: "compact-operation-1",
		Context:      llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
		Parent:       "checkpoint-parent",
	}
}

func testCompactFinalization(request llm.CompactRequestV1) CompactFinalization {
	parent := request.Parent
	return CompactFinalization{Response: llm.CompactResponseV1{
		APIVersion: llm.CompactAPIVersion, OperationKey: request.OperationKey, OperationID: "operation-id",
		Checkpoint: llm.CheckpointMetadata{Handle: "checkpoint-compacted", Parent: &parent, Kind: "compaction", Depth: 1},
		Cache:      llm.CacheDispositionV1{Disposition: "miss_populated", Variant: 0},
		Provenance: []byte(`{"source":"provider"}`),
		Cost:       llm.CostV1{Status: "exact", ActualCostUSD: stringPtr("0.01"), Method: "provider_reported"},
	}}
}

// testCompactCacheOrigin is the cached response from another operation that
// a cache hit replays.
func testCompactCacheOrigin(operationKey string) llm.CompactResponseV1 {
	origin := testCompactFinalization(testCompactRequest()).Response
	origin.OperationKey = operationKey
	origin.OperationID = "origin-operation-id"
	origin.Checkpoint.Handle = "origin-checkpoint"
	return origin
}

// testCompactCacheReplay is a zero-cost worker-cache finalization for request.
func testCompactCacheReplay(request llm.CompactRequestV1) CompactFinalization {
	result := testCompactFinalization(request)
	result.Response.Cache = llm.CacheDispositionV1{Disposition: "hit", Variant: 0}
	result.Response.Cost = llm.CostV1{Status: "exact", ActualCostUSD: stringPtr("0"), Method: "provider_reported"}
	result.Response.Provenance = []byte(`{"source":"worker_cache","origin_operation_id":"origin-operation"}`)
	return result
}

func testCompactRoutePlan() RoutePlan {
	return RoutePlan{OperationID: "operation-id", GenerationID: "compact-generation-id", RouteID: "compact-route", EndpointID: "compact-endpoint", Provider: "provider", Model: "model"}
}

func testCompactReservation(route RoutePlan) ReserveResult {
	now := time.Now().UTC()
	return ReserveResult{
		OperationID: route.OperationID, Accepted: true, GenerationID: route.GenerationID,
		IncarnationID: "compact-incarnation-id", Events: []budget.ReservationEvent{{
			EventID: "compact-event-1", GenerationID: string(route.GenerationID), OperationID: string(route.OperationID), WindowID: "window-1",
			BucketStart: now, ReservationRevision: 1, AmountUSD: pricing.MustUSD("0.01"), OccurredAt: now,
		}},
	}
}

func testCompactPorts(events *[]string, failStage string) CompactPorts {
	request := testCompactRequest()
	route := testCompactRoutePlan()
	reservation := testCompactReservation(route)
	fail := func(stage string) error {
		if stage == failStage {
			return errors.New("injected failure")
		}
		return nil
	}
	return CompactPorts{
		Replay: func(context.Context, llm.CompactRequestV1) (CompactReplay, error) {
			*events = append(*events, "replay")
			return CompactReplay{State: state.MaterializedState{Handle: state.Handle(request.Parent), Tenant: request.Context.Tenant, Project: request.Context.Project}}, fail("replay")
		},
		CacheLookup: func(context.Context, llm.CompactRequestV1, CompactReplay) (CompactCacheDecision, error) {
			*events = append(*events, "cache")
			return CompactCacheDecision{Disposition: CacheMiss}, fail("cache")
		},
		Route: func(context.Context, llm.CompactRequestV1, CompactReplay) (RoutePlan, error) {
			*events = append(*events, "route")
			return route, fail("route")
		},
		Reserve: func(context.Context, llm.CompactRequestV1, RoutePlan) (ReserveResult, error) {
			*events = append(*events, "reserve")
			return reservation, fail("reserve")
		},
		Claim: func(context.Context, llm.CompactRequestV1, RoutePlan, ReserveResult) (ClaimReceipt, error) {
			*events = append(*events, "claim")
			return ClaimReceipt{OperationID: route.OperationID, GenerationID: route.GenerationID, IncarnationID: "compact-incarnation-id"}, fail("claim")
		},
		Dispatch: func(context.Context, llm.CompactRequestV1, CompactReplay, RoutePlan, ClaimReceipt) (CompactDispatchResult, error) {
			*events = append(*events, "dispatch")
			return CompactDispatchResult{}, fail("dispatch")
		},
		Finalize: func(context.Context, llm.CompactRequestV1, CompactReplay, RoutePlan, ReserveResult, CompactDispatchResult) (CompactFinalization, error) {
			*events = append(*events, "finalize")
			return testCompactFinalization(request), fail("finalize")
		},
		FinalizeCache: func(context.Context, llm.CompactRequestV1, CompactReplay, CompactCacheDecision) (CompactFinalization, error) {
			*events = append(*events, "cache-finalize")
			return testCompactFinalization(request), fail("cache-finalize")
		},
		Reconcile: func(context.Context, llm.CompactRequestV1, RoutePlan, ReserveResult, CompactFinalization) error {
			*events = append(*events, "reconcile")
			return fail("reconcile")
		},
	}
}

func TestCompactV1RunsDistinctDurablePhasesInOrder(t *testing.T) {
	events := []string{}
	response, err := CompactV1(context.Background(), testCompactRequest(), testCompactPorts(&events, ""))
	if err != nil {
		t.Fatalf("CompactV1 error = %v", err)
	}
	if response.Checkpoint.Kind != "compaction" || response.OperationKey != "compact-operation-1" {
		t.Fatalf("response = %#v", response)
	}
	assertPhaseOrder(t, "phase order", events, "replay", "cache", "route", "reserve", "claim", "dispatch", "finalize", "reconcile")
}

func TestCompactV1RejectsReplayWithMismatchedToolFrontier(t *testing.T) {
	events := []string{}
	ports := testCompactPorts(&events, "")
	request := testCompactRequest()
	ports.Replay = func(context.Context, llm.CompactRequestV1) (CompactReplay, error) {
		events = append(events, "replay")
		return CompactReplay{State: state.MaterializedState{
			Handle: state.Handle(request.Parent), Tenant: request.Context.Tenant, Project: request.Context.Project,
			Items: []llm.Item{llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{}`)}},
		}}, nil
	}
	_, err := CompactV1(context.Background(), request, ports)
	assertStageFailure(t, err, "replay frontier stage failure")
	assertPhaseOrder(t, "phases after invalid frontier", events, "replay")
}

func TestCompactV1CompletedReplayReturnsWithoutCacheOrProviderWork(t *testing.T) {
	events := []string{}
	ports := testCompactPorts(&events, "")
	completed := testCompactFinalization(testCompactRequest()).Response
	ports.Replay = func(context.Context, llm.CompactRequestV1) (CompactReplay, error) {
		events = append(events, "replay")
		return CompactReplay{Completed: &completed}, nil
	}
	response, err := CompactV1(context.Background(), testCompactRequest(), ports)
	if err != nil {
		t.Fatalf("CompactV1 error = %v", err)
	}
	if response.OperationID != "operation-id" {
		t.Fatalf("response operation id = %q", response.OperationID)
	}
	assertPhaseOrder(t, "completed replay phases", events, "replay")
}

func TestCompactV1CacheHitCreatesZeroCostWorkerCacheChild(t *testing.T) {
	events := []string{}
	ports := testCompactPorts(&events, "")
	template := testCompactCacheOrigin("origin-operation")
	ports.CacheLookup = func(context.Context, llm.CompactRequestV1, CompactReplay) (CompactCacheDecision, error) {
		events = append(events, "cache")
		return CompactCacheDecision{Disposition: CacheHit, Response: &template}, nil
	}
	ports.FinalizeCache = func(_ context.Context, request llm.CompactRequestV1, _ CompactReplay, _ CompactCacheDecision) (CompactFinalization, error) {
		events = append(events, "cache-finalize")
		return testCompactCacheReplay(request), nil
	}
	response, err := CompactV1(context.Background(), testCompactRequest(), ports)
	if err != nil {
		t.Fatalf("CompactV1 cache hit error = %v", err)
	}
	if response.OperationKey != "compact-operation-1" || response.Cache.Disposition != "hit" {
		t.Fatalf("response = %#v", response)
	}
	assertPhaseOrder(t, "cache-hit phases", events, "replay", "cache", "cache-finalize")
}

func TestCompactV1CancellationAfterCacheFinalizationDoesNotReturnSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := []string{}
	ports := testCompactPorts(&events, "")
	template := testCompactCacheOrigin("origin-operation-key")
	ports.CacheLookup = func(context.Context, llm.CompactRequestV1, CompactReplay) (CompactCacheDecision, error) {
		events = append(events, "cache")
		return CompactCacheDecision{Disposition: CacheHit, Response: &template}, nil
	}
	ports.FinalizeCache = func(_ context.Context, request llm.CompactRequestV1, _ CompactReplay, _ CompactCacheDecision) (CompactFinalization, error) {
		events = append(events, "cache-finalize")
		result := testCompactCacheReplay(request)
		result.Response.Checkpoint.Handle = "cache-replay-checkpoint"
		cancel()
		return result, nil
	}
	_, err := CompactV1(ctx, testCompactRequest(), ports)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CompactV1 error = %v, want context cancellation", err)
	}
	if contains(events, "dispatch") || contains(events, "reconcile") {
		t.Fatalf("cache finalization cancellation reached later side effect: %v", events)
	}
}

func TestCompactV1FailsClosedBeforeDispatch(t *testing.T) {
	testFailsClosedBeforeDispatch(t, []string{"replay", "cache", "route", "reserve", "claim"}, func(failStage string) ([]string, error) {
		events := []string{}
		_, err := CompactV1(context.Background(), testCompactRequest(), testCompactPorts(&events, failStage))
		return events, err
	})
}

func TestCompactV1CancellationStopsBeforeTheNextDurablePhase(t *testing.T) {
	testCancellationStopsBeforeNextPhase(t, []cancellationPhase{
		{phase: "replay", forbid: "cache"},
		{phase: "cache", forbid: "route"},
		{phase: "route", forbid: "reserve"},
		{phase: "finalize", forbid: "reconcile"},
	}, func(ctx context.Context, cancel context.CancelFunc, phase string) ([]string, error) {
		events := []string{}
		ports := testCompactPorts(&events, "")
		cancelAfterCompactPhase(&ports, phase, cancel)
		_, err := CompactV1(ctx, testCompactRequest(), ports)
		return events, err
	})
}

func cancelAfterCompactPhase(ports *CompactPorts, phase string, cancel context.CancelFunc) {
	switch phase {
	case "replay":
		original := ports.Replay
		ports.Replay = func(ctx context.Context, request llm.CompactRequestV1) (CompactReplay, error) {
			result, err := original(ctx, request)
			cancel()
			return result, err
		}
	case "cache":
		original := ports.CacheLookup
		ports.CacheLookup = func(ctx context.Context, request llm.CompactRequestV1, replay CompactReplay) (CompactCacheDecision, error) {
			result, err := original(ctx, request, replay)
			cancel()
			return result, err
		}
	case "route":
		original := ports.Route
		ports.Route = func(ctx context.Context, request llm.CompactRequestV1, replay CompactReplay) (RoutePlan, error) {
			result, err := original(ctx, request, replay)
			cancel()
			return result, err
		}
	case "finalize":
		original := ports.Finalize
		ports.Finalize = func(ctx context.Context, request llm.CompactRequestV1, replay CompactReplay, route RoutePlan, reservation ReserveResult, dispatch CompactDispatchResult) (CompactFinalization, error) {
			result, err := original(ctx, request, replay, route, reservation, dispatch)
			cancel()
			return result, err
		}
	}
}

func TestCompactV1ReservationDenialPreservesRetryableProviderError(t *testing.T) {
	events := []string{}
	ports := testCompactPorts(&events, "")
	ports.Reserve = func(context.Context, llm.CompactRequestV1, RoutePlan) (ReserveResult, error) {
		events = append(events, "reserve")
		return ReserveResult{OperationID: "operation-id", GenerationID: "compact-generation-id", Accepted: false, RetryAfter: time.Second}, nil
	}
	_, err := CompactV1(context.Background(), testCompactRequest(), ports)
	assertReservationDeniedAfterOneSecond(t, err)
	assertPhaseOrder(t, "phase order", events, "replay", "cache", "route", "reserve")
}

func TestCompactV1ReconciliationFailureIsRetryableAfterFinalization(t *testing.T) {
	events := []string{}
	_, err := CompactV1(context.Background(), testCompactRequest(), testCompactPorts(&events, "reconcile"))
	assertReconcilePending(t, err)
	assertPhaseOrder(t, "phase order", events, "replay", "cache", "route", "reserve", "claim", "dispatch", "finalize", "reconcile")
}

func TestCompactV1RejectsParentAndOperationIdentityMismatch(t *testing.T) {
	events := []string{}
	ports := testCompactPorts(&events, "")
	ports.Finalize = func(context.Context, llm.CompactRequestV1, CompactReplay, RoutePlan, ReserveResult, CompactDispatchResult) (CompactFinalization, error) {
		result := testCompactFinalization(testCompactRequest())
		other := llm.CheckpointHandle("other-parent")
		result.Response.Checkpoint.Parent = &other
		result.Response.OperationID = "other-operation"
		return result, nil
	}
	_, err := CompactV1(context.Background(), testCompactRequest(), ports)
	assertStageFailure(t, err, "finalization identity failure")
}

func TestCompactV1RejectsMaterializedParentFromAnotherScope(t *testing.T) {
	events := []string{}
	ports := testCompactPorts(&events, "")
	ports.Replay = func(context.Context, llm.CompactRequestV1) (CompactReplay, error) {
		return CompactReplay{State: state.MaterializedState{Handle: "other-parent", Tenant: "other-tenant", Project: "project"}}, nil
	}
	_, err := CompactV1(context.Background(), testCompactRequest(), ports)
	assertStageFailure(t, err, "materialized parent validation failure")
	if contains(events, "dispatch") {
		t.Fatalf("wrong-scope replay reached dispatch: %v", events)
	}
}

func TestCompactV1RetriesPendingReconciliationWithoutDispatch(t *testing.T) {
	events := []string{}
	ports := testCompactPorts(&events, "")
	request := testCompactRequest()
	route := testCompactRoutePlan()
	reservation := testCompactReservation(route)
	finalization := testCompactFinalization(request)
	finalization.Response.OperationID = string(route.OperationID)
	ports.Replay = func(context.Context, llm.CompactRequestV1) (CompactReplay, error) {
		events = append(events, "replay")
		return CompactReplay{ReconciliationPending: &CompactReconciliation{Route: route, Reservation: reservation, Finalization: finalization}}, nil
	}
	ports.Reconcile = func(_ context.Context, _ llm.CompactRequestV1, gotRoute RoutePlan, gotReservation ReserveResult, gotFinalization CompactFinalization) error {
		events = append(events, "reconcile")
		if gotRoute.OperationID != route.OperationID || gotReservation.OperationID != route.OperationID || gotFinalization.Response.OperationID != string(route.OperationID) {
			t.Fatalf("pending reconciliation identities = %#v %#v %#v", gotRoute, gotReservation, gotFinalization)
		}
		return nil
	}
	response, err := CompactV1(context.Background(), request, ports)
	if err != nil {
		t.Fatalf("CompactV1 pending reconciliation error = %v", err)
	}
	if response.OperationID != string(route.OperationID) {
		t.Fatalf("response operation id = %q", response.OperationID)
	}
	assertPhaseOrder(t, "pending reconciliation phases", events, "replay", "reconcile")
}

func TestCompactV1RejectsNonZeroCacheVariant(t *testing.T) {
	events := []string{}
	ports := testCompactPorts(&events, "")
	ports.CacheLookup = func(context.Context, llm.CompactRequestV1, CompactReplay) (CompactCacheDecision, error) {
		return CompactCacheDecision{Disposition: CacheMiss, Response: nil}, nil
	}
	ports.Finalize = func(context.Context, llm.CompactRequestV1, CompactReplay, RoutePlan, ReserveResult, CompactDispatchResult) (CompactFinalization, error) {
		result := testCompactFinalization(testCompactRequest())
		result.Response.Cache.Variant = 1
		return result, nil
	}
	_, err := CompactV1(context.Background(), testCompactRequest(), ports)
	assertStageFailure(t, err, "variant validation failure")
}

func TestCompactV1RejectsMismatchedClaimIdentity(t *testing.T) {
	testClaimIdentityRejected(t, func(mutate func(*ClaimReceipt)) ([]string, error) {
		events := []string{}
		ports := testCompactPorts(&events, "")
		ports.Claim = func(_ context.Context, _ llm.CompactRequestV1, _ RoutePlan, reservation ReserveResult) (ClaimReceipt, error) {
			return mutatedClaim(reservation, mutate), nil
		}
		_, err := CompactV1(context.Background(), testCompactRequest(), ports)
		return events, err
	})
}
