package durable

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

func testGenerateRequest() llm.GenerateRequestV1 {
	return llm.GenerateRequestV1{
		APIVersion:   llm.APIVersion,
		OperationKey: "operation-1",
		Context:      llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
	}
}

func testRoutePlan() RoutePlan {
	return RoutePlan{OperationID: "operation-id", GenerationID: "generation-id", RouteID: "route-1", EndpointID: "endpoint-1", Provider: "provider", Model: "model"}
}

func testReservation(route RoutePlan) ReserveResult {
	now := time.Now().UTC()
	return ReserveResult{
		OperationID: route.OperationID, Accepted: true, GenerationID: route.GenerationID,
		IncarnationID: "incarnation-id", Events: []budget.ReservationEvent{{
			EventID: "event-1", GenerationID: string(route.GenerationID), OperationID: string(route.OperationID), WindowID: "window-1",
			BucketStart: now, ReservationRevision: 1, AmountUSD: pricing.MustUSD("0.01"), OccurredAt: now,
		}},
	}
}

func testFinalization(request llm.GenerateRequestV1) GenerateFinalization {
	return GenerateFinalization{Response: llm.GenerateResponseV1{
		APIVersion: llm.APIVersion, OperationKey: request.OperationKey, OperationID: "operation-id",
		Status:     llm.ResponseStatusCompleted,
		Checkpoint: llm.CheckpointMetadata{Handle: "checkpoint-1", Parent: request.Parent, Kind: "generation", Depth: 0},
		Cache:      llm.CacheDispositionV1{Disposition: "disabled"},
		Cost:       llm.CostV1{Status: "exact", ActualCostUSD: stringPtr("0"), Method: "provider_reported"},
	}}
}

func stringPtr(value string) *string { return &value }

// testGenerateCacheOrigin is the cached response from another operation that
// a cache hit replays.
func testGenerateCacheOrigin(operationKey string) llm.GenerateResponseV1 {
	origin := testFinalization(testGenerateRequest()).Response
	origin.OperationKey = operationKey
	origin.OperationID = "origin-operation-id"
	origin.Checkpoint.Handle = "origin-checkpoint"
	return origin
}

// testGenerateCacheReplay is a valid cache-hit finalization for request.
func testGenerateCacheReplay(request llm.GenerateRequestV1) llm.GenerateResponseV1 {
	response := testFinalization(request).Response
	response.Checkpoint.Kind = "cache_replay"
	response.Checkpoint.Handle = "cache-replay-checkpoint"
	response.Cache = llm.CacheDispositionV1{Disposition: "hit"}
	return response
}

func testGeneratePorts(events *[]string, failStage string) GeneratePorts {
	route := testRoutePlan()
	reservation := testReservation(route)
	fail := func(stage string) error {
		if stage == failStage {
			return errors.New("injected failure")
		}
		return nil
	}
	return GeneratePorts{
		Replay: func(context.Context, llm.GenerateRequestV1) (GenerateReplay, error) {
			*events = append(*events, "replay")
			return GenerateReplay{}, fail("replay")
		},
		CacheLookup: func(context.Context, llm.GenerateRequestV1, GenerateReplay) (CacheDecision, error) {
			*events = append(*events, "cache")
			return CacheDecision{Disposition: CacheMiss}, fail("cache")
		},
		CompactionDecision: func(context.Context, llm.GenerateRequestV1, GenerateReplay, CacheDecision) (CompactionDecision, error) {
			*events = append(*events, "compaction")
			return CompactionDecision{}, fail("compaction")
		},
		Route: func(context.Context, llm.GenerateRequestV1, GenerateReplay, CompactionDecision) (RoutePlan, error) {
			*events = append(*events, "route")
			return route, fail("route")
		},
		Reserve: func(context.Context, llm.GenerateRequestV1, RoutePlan) (ReserveResult, error) {
			*events = append(*events, "reserve")
			return reservation, fail("reserve")
		},
		Claim: func(context.Context, llm.GenerateRequestV1, RoutePlan, ReserveResult) (ClaimReceipt, error) {
			*events = append(*events, "claim")
			return ClaimReceipt{OperationID: route.OperationID, GenerationID: route.GenerationID, IncarnationID: "incarnation-id"}, fail("claim")
		},
		Dispatch: func(context.Context, llm.GenerateRequestV1, GenerateReplay, RoutePlan, ClaimReceipt) (DispatchResult, error) {
			*events = append(*events, "dispatch")
			return DispatchResult{}, fail("dispatch")
		},
		Finalize: func(_ context.Context, request llm.GenerateRequestV1, replay GenerateReplay, _ RoutePlan, _ ReserveResult, _ DispatchResult) (GenerateFinalization, error) {
			*events = append(*events, "finalize")
			finalization := testFinalization(request)
			if replay.State.Handle != "" {
				parent := llm.CheckpointHandle(replay.State.Handle)
				finalization.Response.Checkpoint.Parent = &parent
			}
			return finalization, fail("finalize")
		},
		FinalizeCache: func(_ context.Context, request llm.GenerateRequestV1, _ GenerateReplay, _ CacheDecision) (GenerateFinalization, error) {
			*events = append(*events, "cache-finalize")
			return testFinalization(request), fail("cache-finalize")
		},
		Reconcile: func(context.Context, llm.GenerateRequestV1, RoutePlan, ReserveResult, GenerateFinalization) error {
			*events = append(*events, "reconcile")
			return fail("reconcile")
		},
	}
}

func TestGenerateV1RunsDurablePhasesInOrder(t *testing.T) {
	events := []string{}
	response := mustGenerateV1(t, testGenerateRequest(), testGeneratePorts(&events, ""))
	assertPhaseOrder(t, "phase order", events, "replay", "cache", "compaction", "route", "reserve", "claim", "dispatch", "finalize", "reconcile")
	if response.OperationKey != "operation-1" || response.Checkpoint.Kind != "generation" {
		t.Fatalf("response = %#v", response)
	}
}

func TestGenerateV1CompletedReplayReturnsWithoutCacheOrProviderWork(t *testing.T) {
	events := []string{}
	ports := testGeneratePorts(&events, "")
	completed := testFinalization(testGenerateRequest()).Response
	ports.Replay = func(context.Context, llm.GenerateRequestV1) (GenerateReplay, error) {
		events = append(events, "replay")
		return GenerateReplay{Completed: &completed}, nil
	}
	response := mustGenerateV1(t, testGenerateRequest(), ports)
	if response.OperationKey != "operation-1" {
		t.Fatalf("response operation key = %q", response.OperationKey)
	}
	assertPhaseOrder(t, "completed replay phases", events, "replay")
}

func TestGenerateV1CompactsBeforeRoutingWhenRequired(t *testing.T) {
	events := []string{}
	ports := testGeneratePorts(&events, "")
	requireCompaction(&ports, &events)
	ports.Compact = func(context.Context, llm.GenerateRequestV1, GenerateReplay) (GenerateReplay, error) {
		events = append(events, "compact")
		return GenerateReplay{State: state.MaterializedState{Handle: "compacted", Tenant: "tenant", Project: "project"}}, nil
	}
	var routeHandle, dispatchHandle state.Handle
	ports.Route = func(_ context.Context, _ llm.GenerateRequestV1, replay GenerateReplay, _ CompactionDecision) (RoutePlan, error) {
		events = append(events, "route")
		routeHandle = replay.State.Handle
		return testRoutePlan(), nil
	}
	ports.Dispatch = func(_ context.Context, _ llm.GenerateRequestV1, replay GenerateReplay, _ RoutePlan, _ ClaimReceipt) (DispatchResult, error) {
		events = append(events, "dispatch")
		dispatchHandle = replay.State.Handle
		return DispatchResult{}, nil
	}
	response := mustGenerateV1(t, testGenerateRequest(), ports)
	if routeHandle != "compacted" || dispatchHandle != "compacted" {
		t.Fatalf("compacted state handles = route %q, dispatch %q", routeHandle, dispatchHandle)
	}
	if response.Checkpoint.Parent == nil || *response.Checkpoint.Parent != "compacted" {
		t.Fatalf("response checkpoint parent = %v, want compacted", response.Checkpoint.Parent)
	}
	assertPhaseOrder(t, "phase order", events, "replay", "cache", "compaction", "compact", "route", "reserve", "claim", "dispatch", "finalize", "reconcile")
}

func TestGenerateV1RejectsReplayStateOutsideRequestBinding(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*llm.GenerateRequestV1, *state.MaterializedState)
	}{
		{
			name: "root state",
			mutate: func(_ *llm.GenerateRequestV1, replay *state.MaterializedState) {
				replay.Handle = "root-state"
				replay.Tenant = "tenant"
				replay.Project = "project"
			},
		},
		{
			name: "wrong parent",
			mutate: func(request *llm.GenerateRequestV1, replay *state.MaterializedState) {
				parent := llm.CheckpointHandle("request-parent")
				request.Parent = &parent
				replay.Handle = "other-parent"
				replay.Tenant = request.Context.Tenant
				replay.Project = request.Context.Project
			},
		},
		{
			name: "wrong tenant",
			mutate: func(request *llm.GenerateRequestV1, replay *state.MaterializedState) {
				parent := llm.CheckpointHandle("request-parent")
				request.Parent = &parent
				replay.Handle = state.Handle(parent)
				replay.Tenant = "other-tenant"
				replay.Project = request.Context.Project
			},
		},
		{
			name: "wrong project",
			mutate: func(request *llm.GenerateRequestV1, replay *state.MaterializedState) {
				parent := llm.CheckpointHandle("request-parent")
				request.Parent = &parent
				replay.Handle = state.Handle(parent)
				replay.Tenant = request.Context.Tenant
				replay.Project = "other-project"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := testGenerateRequest()
			replayState := state.MaterializedState{}
			test.mutate(&request, &replayState)
			events := []string{}
			ports := testGeneratePorts(&events, "")
			ports.Replay = func(context.Context, llm.GenerateRequestV1) (GenerateReplay, error) {
				events = append(events, "replay")
				return GenerateReplay{State: replayState}, nil
			}

			_, err := GenerateV1(context.Background(), request, ports)
			assertStageFailure(t, err, "replay binding stage failure")
			assertPhaseOrder(t, "invalid replay phases", events, "replay")
		})
	}
}

func TestGenerateV1AllowsScopedPostCompactionParentReplacement(t *testing.T) {
	request := testGenerateRequest()
	parent := llm.CheckpointHandle("request-parent")
	request.Parent = &parent
	compacted := state.Handle("compacted-child")

	events := []string{}
	ports := testGeneratePorts(&events, "")
	ports.Replay = func(context.Context, llm.GenerateRequestV1) (GenerateReplay, error) {
		events = append(events, "replay")
		return GenerateReplay{State: state.MaterializedState{
			Handle:  state.Handle(parent),
			Tenant:  request.Context.Tenant,
			Project: request.Context.Project,
		}}, nil
	}
	requireCompaction(&ports, &events)
	ports.Compact = func(context.Context, llm.GenerateRequestV1, GenerateReplay) (GenerateReplay, error) {
		events = append(events, "compact")
		return GenerateReplay{State: state.MaterializedState{
			Handle:  compacted,
			Tenant:  request.Context.Tenant,
			Project: request.Context.Project,
		}}, nil
	}

	response := mustGenerateV1(t, request, ports)
	if response.Checkpoint.Parent == nil || *response.Checkpoint.Parent != llm.CheckpointHandle(compacted) {
		t.Fatalf("response checkpoint parent = %v, want %q", response.Checkpoint.Parent, compacted)
	}
	assertPhaseOrder(t, "phase order", events, "replay", "cache", "compaction", "compact", "route", "reserve", "claim", "dispatch", "finalize", "reconcile")
}

func TestGenerateV1AcceptsToolResultDeltaAgainstReplayedFrontier(t *testing.T) {
	request := testGenerateRequest()
	parent := llm.CheckpointHandle("parent")
	request.Parent = &parent
	request.Append = []llm.Item{llm.ToolResult{CallID: "call-1", Name: "lookup"}}
	events := []string{}
	ports := testGeneratePorts(&events, "")
	ports.Replay = func(context.Context, llm.GenerateRequestV1) (GenerateReplay, error) {
		events = append(events, "replay")
		return GenerateReplay{State: state.MaterializedState{
			Handle:           state.Handle(parent),
			Tenant:           request.Context.Tenant,
			Project:          request.Context.Project,
			Items:            []llm.Item{llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{}`)}},
			PendingToolCalls: []string{"call-1"},
		}}, nil
	}
	if _, err := GenerateV1(context.Background(), request, ports); err != nil {
		t.Fatalf("GenerateV1 error = %v, want tool-result delta to resolve replay frontier", err)
	}
	if got, want := events[0], "replay"; got != want {
		t.Fatalf("first phase = %q, want %q", got, want)
	}
}

func TestGenerateV1RejectsToolResultDeltaOutsideReplayedFrontier(t *testing.T) {
	request := testGenerateRequest()
	parent := llm.CheckpointHandle("parent")
	request.Parent = &parent
	request.Append = []llm.Item{llm.ToolResult{CallID: "unknown", Name: "lookup"}}
	events := []string{}
	ports := testGeneratePorts(&events, "")
	ports.Replay = func(context.Context, llm.GenerateRequestV1) (GenerateReplay, error) {
		events = append(events, "replay")
		return GenerateReplay{State: state.MaterializedState{
			Handle:           state.Handle(parent),
			Tenant:           request.Context.Tenant,
			Project:          request.Context.Project,
			Items:            []llm.Item{llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{}`)}},
			PendingToolCalls: []string{"call-1"},
		}}, nil
	}
	_, err := GenerateV1(context.Background(), request, ports)
	assertStageFailure(t, err, "replay frontier stage failure")
	assertPhaseOrder(t, "phases after invalid delta", events, "replay")
}

func TestGenerateV1FailsClosedBeforeDispatchWhenPreDispatchPhaseFails(t *testing.T) {
	testFailsClosedBeforeDispatch(t, []string{"replay", "cache", "compaction", "route", "reserve", "claim"}, func(failStage string) ([]string, error) {
		events := []string{}
		_, err := GenerateV1(context.Background(), testGenerateRequest(), testGeneratePorts(&events, failStage))
		return events, err
	})
}

func TestGenerateV1CancellationStopsBeforeTheNextDurablePhase(t *testing.T) {
	testCancellationStopsBeforeNextPhase(t, []cancellationPhase{
		{phase: "replay", forbid: "cache"},
		{phase: "cache", forbid: "compaction"},
		{phase: "compaction", forbid: "route"},
		{phase: "route", forbid: "reserve"},
		{phase: "finalize", forbid: "reconcile"},
	}, func(ctx context.Context, cancel context.CancelFunc, phase string) ([]string, error) {
		events := []string{}
		ports := testGeneratePorts(&events, "")
		cancelAfterGeneratePhase(&ports, phase, cancel)
		_, err := GenerateV1(ctx, testGenerateRequest(), ports)
		return events, err
	})
}

func TestGenerateV1CancellationAfterCompactStopsBeforeGenerateAdmission(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := []string{}
	ports := testGeneratePorts(&events, "")
	requireCompaction(&ports, &events)
	ports.Compact = func(context.Context, llm.GenerateRequestV1, GenerateReplay) (GenerateReplay, error) {
		events = append(events, "compact")
		cancel()
		return GenerateReplay{}, nil
	}
	_, err := GenerateV1(ctx, testGenerateRequest(), ports)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GenerateV1 error = %v, want context cancellation", err)
	}
	if slices.Contains(events, "route") || slices.Contains(events, "reserve") {
		t.Fatalf("canceled Compact entered Generate admission: %v", events)
	}
}

func TestGenerateV1CancellationAfterCacheFinalizationDoesNotReturnSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := []string{}
	ports := testGeneratePorts(&events, "")
	ports.CacheLookup = func(context.Context, llm.GenerateRequestV1, GenerateReplay) (CacheDecision, error) {
		events = append(events, "cache")
		origin := testGenerateCacheOrigin("origin-operation-key")
		return CacheDecision{Disposition: CacheHit, Response: &origin}, nil
	}
	ports.FinalizeCache = func(_ context.Context, request llm.GenerateRequestV1, _ GenerateReplay, _ CacheDecision) (GenerateFinalization, error) {
		events = append(events, "cache-finalize")
		cancel()
		return GenerateFinalization{Response: testGenerateCacheReplay(request)}, nil
	}
	_, err := GenerateV1(ctx, testGenerateRequest(), ports)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GenerateV1 error = %v, want context cancellation", err)
	}
	if slices.Contains(events, "dispatch") || slices.Contains(events, "reconcile") {
		t.Fatalf("cache finalization cancellation reached later side effect: %v", events)
	}
}

func cancelAfterGeneratePhase(ports *GeneratePorts, phase string, cancel context.CancelFunc) {
	switch phase {
	case "replay":
		original := ports.Replay
		ports.Replay = func(ctx context.Context, request llm.GenerateRequestV1) (GenerateReplay, error) {
			result, err := original(ctx, request)
			cancel()
			return result, err
		}
	case "cache":
		original := ports.CacheLookup
		ports.CacheLookup = func(ctx context.Context, request llm.GenerateRequestV1, replay GenerateReplay) (CacheDecision, error) {
			result, err := original(ctx, request, replay)
			cancel()
			return result, err
		}
	case "compaction":
		original := ports.CompactionDecision
		ports.CompactionDecision = func(ctx context.Context, request llm.GenerateRequestV1, replay GenerateReplay, cache CacheDecision) (CompactionDecision, error) {
			result, err := original(ctx, request, replay, cache)
			cancel()
			return result, err
		}
	case "route":
		original := ports.Route
		ports.Route = func(ctx context.Context, request llm.GenerateRequestV1, replay GenerateReplay, decision CompactionDecision) (RoutePlan, error) {
			result, err := original(ctx, request, replay, decision)
			cancel()
			return result, err
		}
	case "finalize":
		original := ports.Finalize
		ports.Finalize = func(ctx context.Context, request llm.GenerateRequestV1, replay GenerateReplay, route RoutePlan, reservation ReserveResult, dispatch DispatchResult) (GenerateFinalization, error) {
			result, err := original(ctx, request, replay, route, reservation, dispatch)
			cancel()
			return result, err
		}
	}
}

func TestGenerateV1StopsWhenRedisReservationIsDenied(t *testing.T) {
	events := []string{}
	ports := testGeneratePorts(&events, "")
	ports.Reserve = func(context.Context, llm.GenerateRequestV1, RoutePlan) (ReserveResult, error) {
		events = append(events, "reserve")
		return ReserveResult{
			OperationID:  "operation-id",
			GenerationID: "generation-id",
			Accepted:     false,
			RetryAfter:   time.Second,
		}, nil
	}
	_, err := GenerateV1(context.Background(), testGenerateRequest(), ports)
	assertReservationDeniedAfterOneSecond(t, err)
	assertPhaseOrder(t, "phase order", events, "replay", "cache", "compaction", "route", "reserve")
}

func TestGenerateV1ReconciliationFailureIsRetryableAfterFinalization(t *testing.T) {
	events := []string{}
	_, err := GenerateV1(context.Background(), testGenerateRequest(), testGeneratePorts(&events, "reconcile"))
	assertReconcilePending(t, err)
	assertPhaseOrder(t, "phase order", events, "replay", "cache", "compaction", "route", "reserve", "claim", "dispatch", "finalize", "reconcile")
}

func TestGenerateV1RetriesPendingReconciliationWithoutDispatch(t *testing.T) {
	events := []string{}
	ports := testGeneratePorts(&events, "")
	request := testGenerateRequest()
	route := testRoutePlan()
	reservation := testReservation(route)
	finalization := testFinalization(request)
	finalization.Response.OperationID = string(route.OperationID)
	ports.Replay = func(context.Context, llm.GenerateRequestV1) (GenerateReplay, error) {
		events = append(events, "replay")
		return GenerateReplay{ReconciliationPending: &GenerateReconciliation{
			Route: route, Reservation: reservation, Finalization: finalization,
		}}, nil
	}
	ports.Reconcile = func(_ context.Context, _ llm.GenerateRequestV1, gotRoute RoutePlan, gotReservation ReserveResult, gotFinalization GenerateFinalization) error {
		events = append(events, "reconcile")
		if gotRoute.OperationID != route.OperationID || gotReservation.OperationID != route.OperationID || gotFinalization.Response.OperationID != string(route.OperationID) {
			t.Fatalf("pending reconciliation identities = %#v %#v %#v", gotRoute, gotReservation, gotFinalization)
		}
		return nil
	}
	response, err := GenerateV1(context.Background(), request, ports)
	testassert.NoError(t, err, "GenerateV1 pending reconciliation error")
	if response.OperationID != string(route.OperationID) {
		t.Fatalf("response operation id = %q", response.OperationID)
	}
	assertPhaseOrder(t, "pending reconciliation phases", events, "replay", "reconcile")
}

func TestGenerateV1RejectsMismatchedCompletedAndPendingReplay(t *testing.T) {
	events := []string{}
	ports := testGeneratePorts(&events, "")
	request := testGenerateRequest()
	completed := testFinalization(request).Response
	completed.OperationID = "completed-operation"
	route := testRoutePlan()
	reservation := testReservation(route)
	finalization := testFinalization(request)
	finalization.Response.OperationID = string(route.OperationID)
	ports.Replay = func(context.Context, llm.GenerateRequestV1) (GenerateReplay, error) {
		return GenerateReplay{
			Completed: &completed,
			ReconciliationPending: &GenerateReconciliation{
				Route: route, Reservation: reservation, Finalization: finalization,
			},
		}, nil
	}
	_, err := GenerateV1(context.Background(), request, ports)
	assertStageFailure(t, err, "replay validation failure")
	if slices.Contains(events, "reconcile") || slices.Contains(events, "dispatch") {
		t.Fatalf("ambiguous replay reached side effects: %v", events)
	}
}

func TestGenerateV1CacheHitNeverDispatchesProvider(t *testing.T) {
	events := []string{}
	ports := testGeneratePorts(&events, "")
	ports.CacheLookup = func(context.Context, llm.GenerateRequestV1, GenerateReplay) (CacheDecision, error) {
		events = append(events, "cache")
		response := testGenerateCacheOrigin("origin-operation")
		return CacheDecision{Disposition: CacheHit, Response: &response}, nil
	}
	ports.FinalizeCache = func(_ context.Context, request llm.GenerateRequestV1, _ GenerateReplay, _ CacheDecision) (GenerateFinalization, error) {
		events = append(events, "cache-finalize")
		return GenerateFinalization{Response: testGenerateCacheReplay(request)}, nil
	}
	response, err := GenerateV1(context.Background(), testGenerateRequest(), ports)
	testassert.NoError(t, err, "GenerateV1 cache hit error")
	if response.OperationKey != "operation-1" || response.Checkpoint.Kind != "cache_replay" || response.Cache.Disposition != "hit" {
		t.Fatalf("response = %#v", response)
	}
	assertPhaseOrder(t, "cache-hit phases", events, "replay", "cache", "cache-finalize")
}

func TestGenerateV1RejectsInvalidCacheHitFinalization(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*llm.GenerateResponseV1)
	}{
		{name: "origin operation id", mutate: func(response *llm.GenerateResponseV1) {
			response.OperationID = "origin-operation-id"
		}},
		{name: "origin operation key", mutate: func(response *llm.GenerateResponseV1) {
			response.OperationKey = "origin-operation"
		}},
		{name: "origin checkpoint", mutate: func(response *llm.GenerateResponseV1) {
			response.Checkpoint.Handle = "origin-checkpoint"
		}},
		{name: "wrong checkpoint kind", mutate: func(response *llm.GenerateResponseV1) {
			response.Checkpoint.Kind = "generation"
		}},
		{name: "wrong cache disposition", mutate: func(response *llm.GenerateResponseV1) {
			response.Cache.Disposition = "miss_populated"
		}},
		{name: "nonzero cost", mutate: func(response *llm.GenerateResponseV1) {
			response.Cost.ActualCostUSD = stringPtr("0.01")
		}},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			events := []string{}
			ports := testGeneratePorts(&events, "")
			ports.CacheLookup = func(context.Context, llm.GenerateRequestV1, GenerateReplay) (CacheDecision, error) {
				origin := testGenerateCacheOrigin("origin-operation")
				return CacheDecision{Disposition: CacheHit, Response: &origin}, nil
			}
			ports.FinalizeCache = func(_ context.Context, request llm.GenerateRequestV1, _ GenerateReplay, _ CacheDecision) (GenerateFinalization, error) {
				response := testGenerateCacheReplay(request)
				test.mutate(&response)
				return GenerateFinalization{Response: response}, nil
			}

			_, err := GenerateV1(context.Background(), testGenerateRequest(), ports)
			assertStageFailure(t, err, "cache finalization stage failure")
			if slices.Contains(events, "dispatch") {
				t.Fatalf("invalid cache hit reached provider dispatch: %v", events)
			}
		})
	}
}

func claimIdentityMutations() map[string]func(*ClaimReceipt) {
	return map[string]func(*ClaimReceipt){
		"wrong operation":     func(r *ClaimReceipt) { r.OperationID = "other-operation" },
		"wrong generation":    func(r *ClaimReceipt) { r.GenerationID = "other-generation" },
		"wrong incarnation":   func(r *ClaimReceipt) { r.IncarnationID = "other-incarnation" },
		"missing operation":   func(r *ClaimReceipt) { r.OperationID = "" },
		"missing generation":  func(r *ClaimReceipt) { r.GenerationID = "" },
		"missing incarnation": func(r *ClaimReceipt) { r.IncarnationID = "" },
	}
}

func TestGenerateV1RejectsMismatchedClaimIdentity(t *testing.T) {
	testClaimIdentityRejected(t, func(mutate func(*ClaimReceipt)) ([]string, error) {
		events := []string{}
		ports := testGeneratePorts(&events, "")
		ports.Claim = func(_ context.Context, _ llm.GenerateRequestV1, _ RoutePlan, reservation ReserveResult) (ClaimReceipt, error) {
			return mutatedClaim(reservation, mutate), nil
		}
		_, err := GenerateV1(context.Background(), testGenerateRequest(), ports)
		return events, err
	})
}

func TestGenerateV1PreservesTypedStageErrors(t *testing.T) {
	raw := provider.NewError(provider.CodeStateUnavailable, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetrySameOperation, "state unavailable")
	events := []string{}
	ports := testGeneratePorts(&events, "")
	ports.Replay = func(context.Context, llm.GenerateRequestV1) (GenerateReplay, error) {
		return GenerateReplay{}, raw
	}
	_, err := GenerateV1(context.Background(), testGenerateRequest(), ports)
	var got *provider.Error
	if err == nil || !errors.Is(err, ErrV1Stage) || !errors.As(err, &got) {
		t.Fatalf("error = %v, want typed stage error", err)
	}
	testkit.ProviderError(t, got, testkit.ProviderClass{Code: provider.CodeStateUnavailable}, "provider code")
}

func TestGenerateV1RejectsFinalizationFromAnotherOperation(t *testing.T) {
	events := []string{}
	ports := testGeneratePorts(&events, "")
	ports.Finalize = func(context.Context, llm.GenerateRequestV1, GenerateReplay, RoutePlan, ReserveResult, DispatchResult) (GenerateFinalization, error) {
		response := testFinalization(testGenerateRequest())
		response.Response.OperationID = "other-operation"
		return response, nil
	}
	_, err := GenerateV1(context.Background(), testGenerateRequest(), ports)
	assertStageFailure(t, err, "finalization identity failure")
}

func TestValidateGenerateResponseParentRequiresExactEffectiveParent(t *testing.T) {
	parent := llm.CheckpointHandle("checkpoint-parent")
	otherParent := llm.CheckpointHandle("checkpoint-other")

	tests := []struct {
		name           string
		requestParent  *llm.CheckpointHandle
		responseParent *llm.CheckpointHandle
		wantError      bool
	}{
		{name: "root omits parent"},
		{name: "root rejects parent", responseParent: &parent, wantError: true},
		{name: "child retains exact parent", requestParent: &parent, responseParent: &parent},
		{name: "child rejects missing parent", requestParent: &parent, wantError: true},
		{name: "child rejects different parent", requestParent: &parent, responseParent: &otherParent, wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := testGenerateRequest()
			request.Parent = test.requestParent
			response := testFinalization(request).Response
			response.Checkpoint.Parent = test.responseParent

			err := validateGenerateResponseParent(test.requestParent, response)
			if test.wantError && err == nil {
				t.Fatal("validateGenerateResponseParent error = nil, want checkpoint parent mismatch")
			}
			if !test.wantError && err != nil {
				t.Fatalf("validateGenerateResponseParent error = %v", err)
			}
		})
	}
}

func TestGenerateV1CompletedReplayAcceptsCommittedPostCompactionParent(t *testing.T) {
	request := testGenerateRequest()
	parent := llm.CheckpointHandle("checkpoint-parent")
	request.Parent = &parent

	events := []string{}
	ports := testGeneratePorts(&events, "")
	completed := testFinalization(request).Response
	compactedParent := llm.CheckpointHandle("checkpoint-compacted")
	completed.Checkpoint.Parent = &compactedParent
	ports.Replay = func(context.Context, llm.GenerateRequestV1) (GenerateReplay, error) {
		events = append(events, "replay")
		return GenerateReplay{Completed: &completed}, nil
	}

	response := mustGenerateV1(t, request, ports)
	if response.Checkpoint.Parent == nil || *response.Checkpoint.Parent != compactedParent {
		t.Fatalf("response checkpoint parent = %v, want %q", response.Checkpoint.Parent, compactedParent)
	}
	assertPhaseOrder(t, "completed replay phases", events, "replay")
}

func TestGenerateV1RejectsFinalizationFromWrongEffectiveParent(t *testing.T) {
	request := testGenerateRequest()
	parent := llm.CheckpointHandle("checkpoint-parent")
	request.Parent = &parent

	events := []string{}
	ports := testGeneratePorts(&events, "")
	ports.Replay = func(context.Context, llm.GenerateRequestV1) (GenerateReplay, error) {
		events = append(events, "replay")
		return GenerateReplay{State: state.MaterializedState{Handle: state.Handle(parent), Tenant: request.Context.Tenant, Project: request.Context.Project}}, nil
	}
	ports.Finalize = func(context.Context, llm.GenerateRequestV1, GenerateReplay, RoutePlan, ReserveResult, DispatchResult) (GenerateFinalization, error) {
		events = append(events, "finalize")
		response := testFinalization(request)
		otherParent := llm.CheckpointHandle("checkpoint-other")
		response.Response.Checkpoint.Parent = &otherParent
		return response, nil
	}

	_, err := GenerateV1(context.Background(), request, ports)
	assertStageFailure(t, err, "finalization checkpoint parent failure")
	if slices.Contains(events, "reconcile") {
		t.Fatalf("wrong-parent finalization reached reconciliation: %v", events)
	}
}

// testClaimIdentityRejected runs a claim whose receipt carries each identity
// mutation and requires a stage failure before provider dispatch.
func testClaimIdentityRejected(t *testing.T, run func(mutate func(*ClaimReceipt)) ([]string, error)) {
	t.Helper()
	for name, mutate := range claimIdentityMutations() {
		t.Run(name, func(t *testing.T) {
			events, err := run(mutate)
			assertStageFailure(t, err, "claim identity failure")
			if slices.Contains(events, "dispatch") {
				t.Fatalf("mismatched claim reached dispatch: %v", events)
			}
		})
	}
}

// mutatedClaim echoes reservation's identity into a receipt, then applies mutate.
func mutatedClaim(reservation ReserveResult, mutate func(*ClaimReceipt)) ClaimReceipt {
	receipt := ClaimReceipt{OperationID: reservation.OperationID, GenerationID: reservation.GenerationID, IncarnationID: reservation.IncarnationID}
	mutate(&receipt)
	return receipt
}

// testFailsClosedBeforeDispatch fails each pre-dispatch stage in turn and
// requires a stage failure that never reaches provider dispatch.
func testFailsClosedBeforeDispatch(t *testing.T, stages []string, run func(failStage string) ([]string, error)) {
	t.Helper()
	for _, failStage := range stages {
		t.Run(failStage, func(t *testing.T) {
			events, err := run(failStage)
			assertStageFailure(t, err, "stage failure")
			if slices.Contains(events, "dispatch") {
				t.Fatalf("phase failure %q reached provider dispatch: %v", failStage, events)
			}
		})
	}
}

type cancellationPhase struct {
	phase  string
	forbid string
}

// testCancellationStopsBeforeNextPhase cancels the context once each phase
// returns and requires the runner to stop before the forbidden next phase.
func testCancellationStopsBeforeNextPhase(t *testing.T, phases []cancellationPhase, run func(ctx context.Context, cancel context.CancelFunc, phase string) ([]string, error)) {
	t.Helper()
	for _, test := range phases {
		t.Run(test.phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			events, err := run(ctx, cancel, test.phase)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("runner error = %v, want context cancellation", err)
			}
			if slices.Contains(events, test.forbid) {
				t.Fatalf("cancellation after %s reached %s: %v", test.phase, test.forbid, events)
			}
		})
	}
}

// requireCompaction makes ports demand compaction, recording a "compaction" event.
func requireCompaction(ports *GeneratePorts, events *[]string) {
	ports.CompactionDecision = func(context.Context, llm.GenerateRequestV1, GenerateReplay, CacheDecision) (CompactionDecision, error) {
		*events = append(*events, "compaction")
		return CompactionDecision{Required: true}, nil
	}
}

func mustGenerateV1(t *testing.T, request llm.GenerateRequestV1, ports GeneratePorts) llm.GenerateResponseV1 {
	t.Helper()
	response, err := GenerateV1(context.Background(), request, ports)
	testassert.NoError(t, err, "GenerateV1 error")
	return response
}

func assertPhaseOrder(t *testing.T, label string, got []string, want ...string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
}

func assertStageFailure(t *testing.T, err error, want string) {
	t.Helper()
	if !errors.Is(err, ErrV1Stage) {
		t.Fatalf("error = %v, want %s", err, want)
	}
}

func requireProviderError(t *testing.T, err, sentinel error, want string) *provider.Error {
	t.Helper()
	var providerErr *provider.Error
	if !errors.Is(err, sentinel) || !errors.As(err, &providerErr) {
		t.Fatalf("error = %v, want %s provider error", err, want)
	}
	return providerErr
}

func assertReservationDeniedAfterOneSecond(t *testing.T, err error) {
	t.Helper()
	providerErr := requireProviderError(t, err, ErrReservationDenied, "reservation denial")
	if providerErr.Code != provider.CodeBudgetDenied || providerErr.RetryAfter != time.Second {
		t.Fatalf("provider error = %#v", providerErr)
	}
}

func assertReconcilePending(t *testing.T, err error) {
	t.Helper()
	providerErr := requireProviderError(t, err, ErrReconcilePending, "reconciliation-pending")
	testkit.ProviderError(t, providerErr, testkit.ProviderClass{Code: provider.CodeStateUnavailable, Retry: provider.RetrySameOperation})
}
