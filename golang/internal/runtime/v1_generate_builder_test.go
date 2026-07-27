package runtime

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	durable "github.com/mfow/llm-temporal-worker/golang/storage/durable"
	postgresstore "github.com/mfow/llm-temporal-worker/golang/storage/postgres"
)

type generateBuilderClientSet struct{ capabilities V1RuntimeCapabilities }

func (set *generateBuilderClientSet) Close(context.Context) error { return nil }

type generateBuilderPlainClientSet struct{}

func (generateBuilderPlainClientSet) Close(context.Context) error { return nil }

func (set *generateBuilderClientSet) V1RuntimeCapabilities() V1RuntimeCapabilities {
	if set == nil {
		return V1RuntimeCapabilities{}
	}
	return set.capabilities
}

func completeGenerateCapabilities(factory GeneratePortsFactory) V1RuntimeCapabilities {
	return V1RuntimeCapabilities{
		Snapshot: engine.StaticSnapshot{Value: engine.Snapshot{Version: "snapshot-1"}},
		Planner:  routing.DeterministicPlanner{},
		Adapters: engine.AdapterMap{},
		Checkpoints: CheckpointCapabilities{
			Repository:   builderCheckpointRepository{},
			Blobs:        builderCheckpointBlobReader{},
			Materializer: builderCheckpointMaterializer{},
		},
		Journal:              builderJournal{},
		Clock:                time.Now,
		GeneratePortsFactory: factory,
	}
}

func TestGenerateV1RuntimeBuilderFailsClosedWhenCapabilitiesAreMissing(t *testing.T) {
	builder := NewGenerateV1RuntimeBuilder()
	_, err := builder(context.Background(), &config.Snapshot{}, nil, &generateBuilderClientSet{})
	if err == nil || !errors.Is(err, ErrGenerateV1Composition) {
		t.Fatalf("builder error = %v, want fail-closed composition error", err)
	}
	if !strings.Contains(err.Error(), "snapshot source") {
		t.Fatalf("builder error = %q, want missing snapshot diagnostic", err)
	}
}

func TestGenerateV1RuntimeBuilderRequiresCapabilitySource(t *testing.T) {
	_, err := NewGenerateV1RuntimeBuilder()(context.Background(), &config.Snapshot{}, nil, generateBuilderPlainClientSet{})
	if err == nil || !errors.Is(err, ErrGenerateV1Composition) || !strings.Contains(err.Error(), "V1RuntimeCapabilitiesSource") {
		t.Fatalf("builder error = %v, want missing capability-source failure", err)
	}
}

func TestV1RuntimeCapabilitiesValidateGenerateRequiresEveryCapability(t *testing.T) {
	validFactory := func(context.Context, V1RuntimeCapabilities) (durable.GeneratePorts, error) {
		return validBuilderGeneratePorts(nil), nil
	}
	base := completeGenerateCapabilities(validFactory)
	tests := []struct {
		name   string
		mutate func(*V1RuntimeCapabilities)
	}{
		{name: "snapshot", mutate: func(capabilities *V1RuntimeCapabilities) { capabilities.Snapshot = nil }},
		{name: "planner", mutate: func(capabilities *V1RuntimeCapabilities) { capabilities.Planner = nil }},
		{name: "adapters", mutate: func(capabilities *V1RuntimeCapabilities) { capabilities.Adapters = nil }},
		{name: "checkpoints", mutate: func(capabilities *V1RuntimeCapabilities) { capabilities.Checkpoints = CheckpointCapabilities{} }},
		{name: "journal", mutate: func(capabilities *V1RuntimeCapabilities) { capabilities.Journal = nil }},
		{name: "clock", mutate: func(capabilities *V1RuntimeCapabilities) { capabilities.Clock = nil }},
		{name: "ports factory", mutate: func(capabilities *V1RuntimeCapabilities) { capabilities.GeneratePortsFactory = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			capabilities := base
			test.mutate(&capabilities)
			if err := capabilities.ValidateGenerate(); err == nil {
				t.Fatal("ValidateGenerate unexpectedly accepted incomplete capabilities")
			}
		})
	}
}

func TestGenerateV1RuntimeBuilderRejectsIncompletePorts(t *testing.T) {
	builder := NewGenerateV1RuntimeBuilder()
	clients := &generateBuilderClientSet{capabilities: completeGenerateCapabilities(func(context.Context, V1RuntimeCapabilities) (durable.GeneratePorts, error) {
		return durable.GeneratePorts{}, nil
	})}
	_, err := builder(context.Background(), &config.Snapshot{}, nil, clients)
	if err == nil || !errors.Is(err, ErrGenerateV1Composition) || !strings.Contains(err.Error(), "validate Generate ports") {
		t.Fatalf("builder error = %v, want invalid-port failure", err)
	}
}

func TestGenerateV1RuntimeBuilderCapturesOneSnapshotCapabilityBundle(t *testing.T) {
	var seen []string
	clients := &generateBuilderClientSet{}
	clients.capabilities = completeGenerateCapabilities(func(ctx context.Context, capabilities V1RuntimeCapabilities) (durable.GeneratePorts, error) {
		got, err := capabilities.Snapshot.Current(ctx)
		if err != nil {
			return durable.GeneratePorts{}, err
		}
		seen = append(seen, got.Version)
		return validBuilderGeneratePorts(nil), nil
	})
	builder := NewGenerateV1RuntimeBuilder()
	runtimeValue, err := builder(context.Background(), &config.Snapshot{}, nil, clients)
	if err != nil {
		t.Fatalf("builder error = %v", err)
	}
	clients.capabilities.Snapshot = engine.StaticSnapshot{Value: engine.Snapshot{Version: "snapshot-2"}}
	if got, want := strings.Join(seen, ","), "snapshot-1"; got != want {
		t.Fatalf("factory observed snapshots = %q, want %q", got, want)
	}
	if _, ok := runtimeValue.(*activity.GenerateOnlyV1Runtime); !ok {
		t.Fatalf("runtime type = %T, want GenerateOnlyV1Runtime", runtimeValue)
	}
}

func TestGenerateV1RuntimeBuilderLeavesCompactAndQueryFailClosed(t *testing.T) {
	clients := &generateBuilderClientSet{capabilities: completeGenerateCapabilities(func(context.Context, V1RuntimeCapabilities) (durable.GeneratePorts, error) {
		return validBuilderGeneratePorts(nil), nil
	})}
	runtimeValue, err := NewGenerateV1RuntimeBuilder()(context.Background(), &config.Snapshot{}, nil, clients)
	if err != nil {
		t.Fatalf("builder error = %v", err)
	}
	if _, err := runtimeValue.CompactV1(context.Background(), llm.CompactRequestV1{}); err == nil {
		t.Fatal("Generate-only runtime unexpectedly served Compact")
	}
	if _, err := runtimeValue.QueryV1(context.Background(), llm.QueryRequestV1{}); err == nil {
		t.Fatal("Generate-only runtime unexpectedly served Query")
	}
}

func TestGenerateV1RuntimeBuilderPreservesReconciliationNoDuplicateDispatch(t *testing.T) {
	request := llm.GenerateRequestV1{APIVersion: llm.APIVersion, OperationKey: "builder-retry", Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "workflow"}}
	stateful := &builderGenerateRetryState{}
	clients := &generateBuilderClientSet{capabilities: completeGenerateCapabilities(func(context.Context, V1RuntimeCapabilities) (durable.GeneratePorts, error) {
		return stateful.ports(request), nil
	})}
	runtimeValue, err := NewGenerateV1RuntimeBuilder()(context.Background(), &config.Snapshot{}, nil, clients)
	if err != nil {
		t.Fatalf("builder error = %v", err)
	}
	if _, err := runtimeValue.GenerateV1(context.Background(), request); !errors.Is(err, durable.ErrReconcilePending) {
		t.Fatalf("first Generate error = %v, want reconciliation pending", err)
	}
	if _, err := runtimeValue.GenerateV1(context.Background(), request); err != nil {
		t.Fatalf("reconciliation retry error = %v", err)
	}
	if _, err := runtimeValue.GenerateV1(context.Background(), request); err != nil {
		t.Fatalf("completed replay error = %v", err)
	}
	if stateful.dispatchCalls != 1 || stateful.reconcileCalls != 2 {
		t.Fatalf("dispatch/reconcile calls = %d/%d, want 1/2", stateful.dispatchCalls, stateful.reconcileCalls)
	}
	wantEvents := []string{
		"replay", "cache", "compaction", "route", "reserve", "journal", "dispatch", "finalize", "reconcile",
		"replay", "reconcile",
		"replay",
	}
	if !reflect.DeepEqual(stateful.events, wantEvents) {
		t.Fatalf("Generate phase events = %v, want %v", stateful.events, wantEvents)
	}
}

func validBuilderGeneratePorts(events *[]string) durable.GeneratePorts {
	record := func(event string) {
		if events != nil {
			*events = append(*events, event)
		}
	}
	route := durable.RoutePlan{OperationID: "operation-1", GenerationID: "generation-1", RouteID: "route-1", EndpointID: "endpoint-1", Provider: "provider", Model: "model"}
	reservation := durable.ReserveResult{OperationID: route.OperationID, Accepted: true, GenerationID: route.GenerationID, IncarnationID: "incarnation-1", Events: []budget.ReservationEvent{{EventID: "event-1", GenerationID: string(route.GenerationID), OperationID: string(route.OperationID), WindowID: "window-1", BucketStart: time.Unix(10, 0).UTC(), ReservationRevision: 1, AmountUSD: pricing.MustUSD("0.01"), OccurredAt: time.Unix(11, 0).UTC()}}}
	return durable.GeneratePorts{
		Replay: func(context.Context, llm.GenerateRequestV1) (durable.GenerateReplay, error) {
			record("replay")
			return durable.GenerateReplay{}, nil
		},
		CacheLookup: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay) (durable.CacheDecision, error) {
			record("cache")
			return durable.CacheDecision{Disposition: durable.CacheMiss}, nil
		},
		CompactionDecision: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.CacheDecision) (durable.CompactionDecision, error) {
			record("compaction")
			return durable.CompactionDecision{}, nil
		},
		Compact: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay) (durable.GenerateReplay, error) {
			record("compact")
			return durable.GenerateReplay{}, nil
		},
		Route: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.CompactionDecision) (durable.RoutePlan, error) {
			record("route")
			return route, nil
		},
		Reserve: func(context.Context, llm.GenerateRequestV1, durable.RoutePlan) (durable.ReserveResult, error) {
			record("reserve")
			return reservation, nil
		},
		Journal: func(context.Context, llm.GenerateRequestV1, durable.RoutePlan, durable.ReserveResult) (durable.JournalReceipt, error) {
			record("journal")
			return durable.JournalReceipt{OperationID: route.OperationID, GenerationID: route.GenerationID}, nil
		},
		Dispatch: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.RoutePlan, durable.JournalReceipt) (durable.DispatchResult, error) {
			record("dispatch")
			return durable.DispatchResult{}, nil
		},
		Finalize: func(_ context.Context, request llm.GenerateRequestV1, _ durable.GenerateReplay, _ durable.RoutePlan, _ durable.ReserveResult, _ durable.DispatchResult) (durable.GenerateFinalization, error) {
			record("finalize")
			return builderFinalization(request, route.OperationID), nil
		},
		FinalizeCache: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.CacheDecision) (durable.GenerateFinalization, error) {
			record("finalize-cache")
			return durable.GenerateFinalization{}, nil
		},
		Reconcile: func(context.Context, llm.GenerateRequestV1, durable.RoutePlan, durable.ReserveResult, durable.GenerateFinalization) error {
			record("reconcile")
			return nil
		},
	}
}

func builderFinalization(request llm.GenerateRequestV1, operationID durable.OperationID) durable.GenerateFinalization {
	cost := "0"
	return durable.GenerateFinalization{Response: llm.GenerateResponseV1{APIVersion: llm.APIVersion, OperationKey: request.OperationKey, OperationID: string(operationID), Status: llm.ResponseStatusCompleted, Checkpoint: llm.CheckpointMetadata{Handle: "checkpoint-1", Kind: "generation"}, Cache: llm.CacheDispositionV1{Disposition: "disabled"}, Cost: llm.CostV1{Status: "exact", ActualCostUSD: &cost, Method: "provider_reported"}}}
}

type builderGenerateRetryState struct {
	finalized      bool
	reconciled     bool
	dispatchCalls  int
	reconcileCalls int
	events         []string
}

func (state *builderGenerateRetryState) ports(request llm.GenerateRequestV1) durable.GeneratePorts {
	ports := validBuilderGeneratePorts(&state.events)
	route := durable.RoutePlan{OperationID: "operation-1", GenerationID: "generation-1", RouteID: "route-1", EndpointID: "endpoint-1", Provider: "provider", Model: "model"}
	reservation := durable.ReserveResult{OperationID: route.OperationID, Accepted: true, GenerationID: route.GenerationID, IncarnationID: "incarnation-1", Events: []budget.ReservationEvent{{EventID: "event-1", GenerationID: string(route.GenerationID), OperationID: string(route.OperationID), WindowID: "window-1", BucketStart: time.Unix(10, 0).UTC(), ReservationRevision: 1, AmountUSD: pricing.MustUSD("0.01"), OccurredAt: time.Unix(11, 0).UTC()}}}
	finalization := builderFinalization(request, route.OperationID)
	ports.Replay = func(context.Context, llm.GenerateRequestV1) (durable.GenerateReplay, error) {
		state.events = append(state.events, "replay")
		switch {
		case state.reconciled:
			response := finalization.Response
			return durable.GenerateReplay{Completed: &response}, nil
		case state.finalized:
			return durable.GenerateReplay{ReconciliationPending: &durable.GenerateReconciliation{Route: route, Reservation: reservation, Finalization: finalization}}, nil
		default:
			return durable.GenerateReplay{}, nil
		}
	}
	ports.Dispatch = func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.RoutePlan, durable.JournalReceipt) (durable.DispatchResult, error) {
		state.events = append(state.events, "dispatch")
		state.dispatchCalls++
		return durable.DispatchResult{}, nil
	}
	ports.Finalize = func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.RoutePlan, durable.ReserveResult, durable.DispatchResult) (durable.GenerateFinalization, error) {
		state.events = append(state.events, "finalize")
		state.finalized = true
		return finalization, nil
	}
	ports.Reconcile = func(context.Context, llm.GenerateRequestV1, durable.RoutePlan, durable.ReserveResult, durable.GenerateFinalization) error {
		state.events = append(state.events, "reconcile")
		state.reconcileCalls++
		if state.reconcileCalls == 1 {
			return errors.New("simulated worker crash")
		}
		state.reconciled = true
		return nil
	}
	return ports
}

type builderCheckpointRepository struct{}

func (builderCheckpointRepository) Get(context.Context, string, state.CheckpointID) (state.DurableCheckpoint, error) {
	return state.DurableCheckpoint{}, errors.New("not implemented")
}
func (builderCheckpointRepository) BeginCheckpoint(context.Context) (state.CheckpointUnitOfWork, error) {
	return builderCheckpointUnit{}, nil
}

type builderCheckpointUnit struct{}

func (builderCheckpointUnit) PutCheckpoint(context.Context, state.CheckpointWrite) error { return nil }
func (builderCheckpointUnit) Commit(context.Context) error                               { return nil }
func (builderCheckpointUnit) Rollback(context.Context) error                             { return nil }

type builderCheckpointBlobReader struct{}

func (builderCheckpointBlobReader) Read(context.Context, string, state.CheckpointBlobReference) ([]byte, error) {
	return nil, nil
}

type builderCheckpointMaterializer struct{}

func (builderCheckpointMaterializer) Materialize(context.Context, string, state.CheckpointID, state.MaterializeLimits) (state.MaterializedState, error) {
	return state.MaterializedState{}, nil
}
func (builderCheckpointMaterializer) MaterializeHandle(context.Context, string, string, state.MaterializeLimits) (state.MaterializedState, error) {
	return state.MaterializedState{}, nil
}

type builderJournal struct{}

func (builderJournal) AppendReservation(context.Context, budget.ReservationEvent) (postgresstore.JournalRecord, error) {
	return postgresstore.JournalRecord{}, nil
}
func (builderJournal) AppendCompletion(context.Context, budget.CompletionEvent) (postgresstore.JournalRecord, error) {
	return postgresstore.JournalRecord{}, nil
}
