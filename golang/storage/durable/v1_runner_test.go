package durable

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
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
		Journal: func(context.Context, llm.GenerateRequestV1, RoutePlan, ReserveResult) (JournalReceipt, error) {
			*events = append(*events, "journal")
			return JournalReceipt{OperationID: route.OperationID, GenerationID: route.GenerationID}, fail("journal")
		},
		Dispatch: func(context.Context, llm.GenerateRequestV1, GenerateReplay, RoutePlan, JournalReceipt) (DispatchResult, error) {
			*events = append(*events, "dispatch")
			return DispatchResult{}, fail("dispatch")
		},
		Finalize: func(_ context.Context, request llm.GenerateRequestV1, _ GenerateReplay, _ RoutePlan, _ ReserveResult, _ DispatchResult) (GenerateFinalization, error) {
			*events = append(*events, "finalize")
			return testFinalization(request), fail("finalize")
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
	response, err := GenerateV1(context.Background(), testGenerateRequest(), testGeneratePorts(&events, ""))
	if err != nil {
		t.Fatalf("GenerateV1 error = %v", err)
	}
	if got, want := events, []string{"replay", "cache", "compaction", "route", "reserve", "journal", "dispatch", "finalize", "reconcile"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("phase order = %v, want %v", got, want)
	}
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
	response, err := GenerateV1(context.Background(), testGenerateRequest(), ports)
	if err != nil {
		t.Fatalf("GenerateV1 error = %v", err)
	}
	if response.OperationKey != "operation-1" {
		t.Fatalf("response operation key = %q", response.OperationKey)
	}
	if got, want := events, []string{"replay"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("completed replay phases = %v, want %v", got, want)
	}
}

func TestGenerateV1CompactsBeforeRoutingWhenRequired(t *testing.T) {
	events := []string{}
	ports := testGeneratePorts(&events, "")
	ports.CompactionDecision = func(context.Context, llm.GenerateRequestV1, GenerateReplay, CacheDecision) (CompactionDecision, error) {
		events = append(events, "compaction")
		return CompactionDecision{Required: true}, nil
	}
	ports.Compact = func(context.Context, llm.GenerateRequestV1, GenerateReplay) (GenerateReplay, error) {
		events = append(events, "compact")
		return GenerateReplay{State: state.MaterializedState{Handle: "compacted"}}, nil
	}
	var routeHandle, dispatchHandle state.Handle
	ports.Route = func(_ context.Context, _ llm.GenerateRequestV1, replay GenerateReplay, _ CompactionDecision) (RoutePlan, error) {
		events = append(events, "route")
		routeHandle = replay.State.Handle
		return testRoutePlan(), nil
	}
	ports.Dispatch = func(_ context.Context, _ llm.GenerateRequestV1, replay GenerateReplay, _ RoutePlan, _ JournalReceipt) (DispatchResult, error) {
		events = append(events, "dispatch")
		dispatchHandle = replay.State.Handle
		return DispatchResult{}, nil
	}
	if _, err := GenerateV1(context.Background(), testGenerateRequest(), ports); err != nil {
		t.Fatalf("GenerateV1 error = %v", err)
	}
	if routeHandle != "compacted" || dispatchHandle != "compacted" {
		t.Fatalf("compacted state handles = route %q, dispatch %q", routeHandle, dispatchHandle)
	}
	if got, want := events, []string{"replay", "cache", "compaction", "compact", "route", "reserve", "journal", "dispatch", "finalize", "reconcile"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("phase order = %v, want %v", got, want)
	}
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
			Items:            []llm.Item{llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{}`)}},
			PendingToolCalls: []string{"call-1"},
		}}, nil
	}
	if _, err := GenerateV1(context.Background(), request, ports); err == nil || !errors.Is(err, ErrV1Stage) {
		t.Fatalf("GenerateV1 error = %v, want replay frontier stage failure", err)
	}
	if got, want := events, []string{"replay"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("phases after invalid delta = %v, want %v", got, want)
	}
}

func TestGenerateV1FailsClosedBeforeDispatchWhenPreDispatchPhaseFails(t *testing.T) {
	for _, failStage := range []string{"replay", "cache", "compaction", "route", "reserve", "journal"} {
		t.Run(failStage, func(t *testing.T) {
			events := []string{}
			_, err := GenerateV1(context.Background(), testGenerateRequest(), testGeneratePorts(&events, failStage))
			if err == nil || !errors.Is(err, ErrV1Stage) {
				t.Fatalf("error = %v, want stage failure", err)
			}
			if contains(events, "dispatch") {
				t.Fatalf("phase failure %q reached provider dispatch: %v", failStage, events)
			}
		})
	}
}

func TestGenerateV1CancellationStopsBeforeTheNextDurablePhase(t *testing.T) {
	for _, test := range []struct {
		phase  string
		forbid string
	}{
		{phase: "replay", forbid: "cache"},
		{phase: "cache", forbid: "compaction"},
		{phase: "compaction", forbid: "route"},
		{phase: "route", forbid: "reserve"},
		{phase: "finalize", forbid: "reconcile"},
	} {
		t.Run(test.phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			events := []string{}
			ports := testGeneratePorts(&events, "")
			cancelAfterGeneratePhase(&ports, test.phase, cancel)
			_, err := GenerateV1(ctx, testGenerateRequest(), ports)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("GenerateV1 error = %v, want context cancellation", err)
			}
			if contains(events, test.forbid) {
				t.Fatalf("cancellation after %s reached %s: %v", test.phase, test.forbid, events)
			}
		})
	}
}

func TestGenerateV1CancellationAfterCompactStopsBeforeGenerateAdmission(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := []string{}
	ports := testGeneratePorts(&events, "")
	ports.CompactionDecision = func(context.Context, llm.GenerateRequestV1, GenerateReplay, CacheDecision) (CompactionDecision, error) {
		events = append(events, "compaction")
		return CompactionDecision{Required: true}, nil
	}
	ports.Compact = func(context.Context, llm.GenerateRequestV1, GenerateReplay) (GenerateReplay, error) {
		events = append(events, "compact")
		cancel()
		return GenerateReplay{}, nil
	}
	_, err := GenerateV1(ctx, testGenerateRequest(), ports)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GenerateV1 error = %v, want context cancellation", err)
	}
	if contains(events, "route") || contains(events, "reserve") {
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
		origin := testFinalization(testGenerateRequest()).Response
		origin.OperationKey = "origin-operation-key"
		origin.OperationID = "origin-operation-id"
		origin.Checkpoint.Handle = "origin-checkpoint"
		return CacheDecision{Disposition: CacheHit, Response: &origin}, nil
	}
	ports.FinalizeCache = func(_ context.Context, request llm.GenerateRequestV1, _ GenerateReplay, _ CacheDecision) (GenerateFinalization, error) {
		events = append(events, "cache-finalize")
		result := testFinalization(request)
		result.Response.Checkpoint.Kind = "cache_replay"
		result.Response.Checkpoint.Handle = "cache-replay-checkpoint"
		result.Response.Cache = llm.CacheDispositionV1{Disposition: "hit"}
		cancel()
		return result, nil
	}
	_, err := GenerateV1(ctx, testGenerateRequest(), ports)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GenerateV1 error = %v, want context cancellation", err)
	}
	if contains(events, "dispatch") || contains(events, "reconcile") {
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
	var providerErr *provider.Error
	if err == nil || !errors.Is(err, ErrReservationDenied) || !errors.As(err, &providerErr) {
		t.Fatalf("error = %v, want reservation denial provider error", err)
	}
	if providerErr.Code != provider.CodeBudgetDenied || providerErr.RetryAfter != time.Second {
		t.Fatalf("provider error = %#v", providerErr)
	}
	if got, want := events, []string{"replay", "cache", "compaction", "route", "reserve"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("phase order = %v, want %v", got, want)
	}
}

func TestGenerateV1ReconciliationFailureIsRetryableAfterFinalization(t *testing.T) {
	events := []string{}
	_, err := GenerateV1(context.Background(), testGenerateRequest(), testGeneratePorts(&events, "reconcile"))
	var providerErr *provider.Error
	if err == nil || !errors.Is(err, ErrReconcilePending) || !errors.As(err, &providerErr) {
		t.Fatalf("error = %v, want reconciliation-pending provider error", err)
	}
	if providerErr.Code != provider.CodeStateUnavailable || providerErr.Retry != provider.RetrySameOperation {
		t.Fatalf("provider error = %#v", providerErr)
	}
	if got, want := events, []string{"replay", "cache", "compaction", "route", "reserve", "journal", "dispatch", "finalize", "reconcile"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("phase order = %v, want %v", got, want)
	}
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
	if err != nil {
		t.Fatalf("GenerateV1 pending reconciliation error = %v", err)
	}
	if response.OperationID != string(route.OperationID) {
		t.Fatalf("response operation id = %q", response.OperationID)
	}
	if got, want := events, []string{"replay", "reconcile"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pending reconciliation phases = %v, want %v", got, want)
	}
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
	if err == nil || !errors.Is(err, ErrV1Stage) {
		t.Fatalf("error = %v, want replay validation failure", err)
	}
	if contains(events, "reconcile") || contains(events, "dispatch") {
		t.Fatalf("ambiguous replay reached side effects: %v", events)
	}
}

func TestGenerateV1CacheHitNeverDispatchesProvider(t *testing.T) {
	events := []string{}
	ports := testGeneratePorts(&events, "")
	ports.CacheLookup = func(context.Context, llm.GenerateRequestV1, GenerateReplay) (CacheDecision, error) {
		events = append(events, "cache")
		response := testFinalization(testGenerateRequest()).Response
		response.OperationKey = "origin-operation"
		response.OperationID = "origin-operation-id"
		response.Checkpoint.Handle = "origin-checkpoint"
		return CacheDecision{Disposition: CacheHit, Response: &response}, nil
	}
	ports.FinalizeCache = func(_ context.Context, request llm.GenerateRequestV1, _ GenerateReplay, _ CacheDecision) (GenerateFinalization, error) {
		events = append(events, "cache-finalize")
		response := testFinalization(request).Response
		response.Checkpoint.Kind = "cache_replay"
		response.Checkpoint.Handle = "cache-replay-checkpoint"
		response.Cache = llm.CacheDispositionV1{Disposition: "hit"}
		return GenerateFinalization{Response: response}, nil
	}
	response, err := GenerateV1(context.Background(), testGenerateRequest(), ports)
	if err != nil {
		t.Fatalf("GenerateV1 cache hit error = %v", err)
	}
	if response.OperationKey != "operation-1" || response.Checkpoint.Kind != "cache_replay" || response.Cache.Disposition != "hit" {
		t.Fatalf("response = %#v", response)
	}
	if got, want := events, []string{"replay", "cache", "cache-finalize"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("cache-hit phases = %v, want %v", got, want)
	}
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
				origin := testFinalization(testGenerateRequest()).Response
				origin.OperationKey = "origin-operation"
				origin.OperationID = "origin-operation-id"
				origin.Checkpoint.Handle = "origin-checkpoint"
				return CacheDecision{Disposition: CacheHit, Response: &origin}, nil
			}
			ports.FinalizeCache = func(_ context.Context, request llm.GenerateRequestV1, _ GenerateReplay, _ CacheDecision) (GenerateFinalization, error) {
				response := testFinalization(request).Response
				response.Checkpoint.Kind = "cache_replay"
				response.Checkpoint.Handle = "cache-replay-checkpoint"
				response.Cache = llm.CacheDispositionV1{Disposition: "hit"}
				test.mutate(&response)
				return GenerateFinalization{Response: response}, nil
			}

			_, err := GenerateV1(context.Background(), testGenerateRequest(), ports)
			if err == nil || !errors.Is(err, ErrV1Stage) {
				t.Fatalf("error = %v, want cache finalization stage failure", err)
			}
			if contains(events, "dispatch") {
				t.Fatalf("invalid cache hit reached provider dispatch: %v", events)
			}
		})
	}
}

func TestGenerateV1RejectsMismatchedJournalIdentity(t *testing.T) {
	events := []string{}
	ports := testGeneratePorts(&events, "")
	ports.Journal = func(context.Context, llm.GenerateRequestV1, RoutePlan, ReserveResult) (JournalReceipt, error) {
		return JournalReceipt{OperationID: "other-operation", GenerationID: "generation-id"}, nil
	}
	_, err := GenerateV1(context.Background(), testGenerateRequest(), ports)
	if err == nil || !errors.Is(err, ErrV1Stage) {
		t.Fatalf("error = %v, want journal identity failure", err)
	}
	if contains(events, "dispatch") {
		t.Fatalf("mismatched journal reached dispatch: %v", events)
	}
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
	if got.Code != provider.CodeStateUnavailable {
		t.Fatalf("provider code = %q", got.Code)
	}
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
	if err == nil || !errors.Is(err, ErrV1Stage) {
		t.Fatalf("error = %v, want finalization identity failure", err)
	}
}

func TestValidateGenerateResponseRequiresExactCheckpointParent(t *testing.T) {
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

			err := validateGenerateResponse(request, "", response)
			if test.wantError && err == nil {
				t.Fatal("validateGenerateResponse error = nil, want checkpoint parent mismatch")
			}
			if !test.wantError && err != nil {
				t.Fatalf("validateGenerateResponse error = %v", err)
			}
		})
	}
}

func TestGenerateV1CompletedReplayRejectsCheckpointFromAnotherBranch(t *testing.T) {
	request := testGenerateRequest()
	parent := llm.CheckpointHandle("checkpoint-parent")
	request.Parent = &parent

	events := []string{}
	ports := testGeneratePorts(&events, "")
	completed := testFinalization(request).Response
	otherParent := llm.CheckpointHandle("checkpoint-other")
	completed.Checkpoint.Parent = &otherParent
	ports.Replay = func(context.Context, llm.GenerateRequestV1) (GenerateReplay, error) {
		events = append(events, "replay")
		return GenerateReplay{Completed: &completed}, nil
	}

	_, err := GenerateV1(context.Background(), request, ports)
	if err == nil || !errors.Is(err, ErrV1Stage) {
		t.Fatalf("error = %v, want replay checkpoint parent failure", err)
	}
	if got, want := events, []string{"replay"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("completed replay phases = %v, want %v", got, want)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
