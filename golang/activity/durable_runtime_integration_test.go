package activity

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// These tests compose the Activity-facing DurableV1Runtime with the
// storage-neutral phase runners. They intentionally use in-process ports:
// provider credentials and live Redis/PostgreSQL are release evidence, not
// pull-request prerequisites. The composition is nevertheless the exact
// one-shot path an immutable runtime snapshot supplies to the Activity.

func TestDurableV1RuntimeGenerateActivityRunsEveryPhaseInOrder(t *testing.T) {
	events := []string{}
	request := validGenerateV1Request()
	route := durable.RoutePlan{
		OperationID: "operation-id", GenerationID: "generation-id",
		RouteID: "route", EndpointID: "endpoint", Provider: "provider", Model: "model",
	}
	reservation := durableTestReservation(route)
	ports := durable.GeneratePorts{
		Replay: func(context.Context, llm.GenerateRequestV1) (durable.GenerateReplay, error) {
			events = append(events, "replay")
			return durable.GenerateReplay{}, nil
		},
		CacheLookup: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay) (durable.CacheDecision, error) {
			events = append(events, "cache")
			return durable.CacheDecision{Disposition: durable.CacheMiss}, nil
		},
		CompactionDecision: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.CacheDecision) (durable.CompactionDecision, error) {
			events = append(events, "compaction")
			return durable.CompactionDecision{}, nil
		},
		Route: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.CompactionDecision) (durable.RoutePlan, error) {
			events = append(events, "route")
			return route, nil
		},
		Reserve: func(context.Context, llm.GenerateRequestV1, durable.RoutePlan) (durable.ReserveResult, error) {
			events = append(events, "reserve")
			return reservation, nil
		},
		Journal: func(context.Context, llm.GenerateRequestV1, durable.RoutePlan, durable.ReserveResult) (durable.JournalReceipt, error) {
			events = append(events, "journal")
			return durable.JournalReceipt{OperationID: route.OperationID, GenerationID: route.GenerationID}, nil
		},
		Dispatch: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.RoutePlan, durable.JournalReceipt) (durable.DispatchResult, error) {
			events = append(events, "dispatch")
			return durable.DispatchResult{}, nil
		},
		Finalize: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.RoutePlan, durable.ReserveResult, durable.DispatchResult) (durable.GenerateFinalization, error) {
			events = append(events, "finalize")
			return durable.GenerateFinalization{Response: durableGenerateResponse(request)}, nil
		},
		FinalizeCache: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.CacheDecision) (durable.GenerateFinalization, error) {
			events = append(events, "cache-finalize")
			return durable.GenerateFinalization{}, nil
		},
		Reconcile: func(context.Context, llm.GenerateRequestV1, durable.RoutePlan, durable.ReserveResult, durable.GenerateFinalization) error {
			events = append(events, "reconcile")
			return nil
		},
	}
	runtime := &DurableV1Runtime{Generate: ports}
	response, err := (&Activities{V1Runtime: runtime}).GenerateV1(context.Background(), request)
	if err != nil {
		t.Fatalf("GenerateV1 error = %v", err)
	}
	if response == nil || response.OperationID != string(route.OperationID) {
		t.Fatalf("GenerateV1 response = %#v", response)
	}
	want := []string{"replay", "cache", "compaction", "route", "reserve", "journal", "dispatch", "finalize", "reconcile"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("GenerateV1 phase order = %v, want %v", events, want)
	}
}

func TestDurableV1RuntimeCompactActivityUsesDistinctCompactionPath(t *testing.T) {
	events := []string{}
	request := validCompactV1Request()
	route := durable.RoutePlan{
		OperationID: "compact-operation", GenerationID: "compact-generation",
		RouteID: "compact-route", EndpointID: "compact-endpoint", Provider: "provider", Model: "model",
	}
	reservation := durableTestReservation(route)
	ports := durable.CompactPorts{
		Replay: func(context.Context, llm.CompactRequestV1) (durable.CompactReplay, error) {
			events = append(events, "replay")
			return durable.CompactReplay{State: durableTestMaterializedState(request)}, nil
		},
		CacheLookup: func(context.Context, llm.CompactRequestV1, durable.CompactReplay) (durable.CompactCacheDecision, error) {
			events = append(events, "cache")
			return durable.CompactCacheDecision{Disposition: durable.CacheMiss}, nil
		},
		Route: func(context.Context, llm.CompactRequestV1, durable.CompactReplay) (durable.RoutePlan, error) {
			events = append(events, "route")
			return route, nil
		},
		Reserve: func(context.Context, llm.CompactRequestV1, durable.RoutePlan) (durable.ReserveResult, error) {
			events = append(events, "reserve")
			return reservation, nil
		},
		Journal: func(context.Context, llm.CompactRequestV1, durable.RoutePlan, durable.ReserveResult) (durable.JournalReceipt, error) {
			events = append(events, "journal")
			return durable.JournalReceipt{OperationID: route.OperationID, GenerationID: route.GenerationID}, nil
		},
		Dispatch: func(context.Context, llm.CompactRequestV1, durable.CompactReplay, durable.RoutePlan, durable.JournalReceipt) (durable.CompactDispatchResult, error) {
			events = append(events, "dispatch")
			return durable.CompactDispatchResult{}, nil
		},
		Finalize: func(context.Context, llm.CompactRequestV1, durable.CompactReplay, durable.RoutePlan, durable.ReserveResult, durable.CompactDispatchResult) (durable.CompactFinalization, error) {
			events = append(events, "finalize")
			return durable.CompactFinalization{Response: durableCompactResponse(request)}, nil
		},
		FinalizeCache: func(context.Context, llm.CompactRequestV1, durable.CompactReplay, durable.CompactCacheDecision) (durable.CompactFinalization, error) {
			events = append(events, "cache-finalize")
			return durable.CompactFinalization{}, nil
		},
		Reconcile: func(context.Context, llm.CompactRequestV1, durable.RoutePlan, durable.ReserveResult, durable.CompactFinalization) error {
			events = append(events, "reconcile")
			return nil
		},
	}
	runtime := &DurableV1Runtime{Compact: ports}
	response, err := (&Activities{V1Runtime: runtime}).CompactV1(context.Background(), request)
	if err != nil {
		t.Fatalf("CompactV1 error = %v", err)
	}
	if response == nil || response.Checkpoint.Kind != "compaction" || response.OperationID != string(route.OperationID) {
		t.Fatalf("CompactV1 response = %#v", response)
	}
	want := []string{"replay", "cache", "route", "reserve", "journal", "dispatch", "finalize", "reconcile"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("CompactV1 phase order = %v, want %v", events, want)
	}
}

func durableTestReservation(route durable.RoutePlan) durable.ReserveResult {
	now := time.Unix(1_750_000_000, 0).UTC()
	return durable.ReserveResult{
		OperationID: route.OperationID, Accepted: true, GenerationID: route.GenerationID, IncarnationID: "incarnation",
		Events: []budget.ReservationEvent{{
			EventID: "event", GenerationID: string(route.GenerationID), OperationID: string(route.OperationID), WindowID: "window",
			BucketStart: now, ReservationRevision: 1, AmountUSD: pricing.MustUSD("0.01"), OccurredAt: now,
		}},
	}
}

func durableGenerateResponse(request llm.GenerateRequestV1) llm.GenerateResponseV1 {
	return llm.GenerateResponseV1{
		APIVersion: llm.APIVersion, OperationKey: request.OperationKey, OperationID: "operation-id", Status: llm.ResponseStatusCompleted,
		Output:     []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "ok"}}}},
		Checkpoint: llm.CheckpointMetadata{Handle: "checkpoint", Kind: "generation"},
		Cache:      llm.CacheDispositionV1{Disposition: "disabled"}, Cost: llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "provider_reported"},
	}
}

func durableCompactResponse(request llm.CompactRequestV1) llm.CompactResponseV1 {
	parent := request.Parent
	return llm.CompactResponseV1{
		APIVersion: llm.CompactAPIVersion, OperationKey: request.OperationKey, OperationID: "compact-operation",
		Checkpoint: llm.CheckpointMetadata{Handle: "compacted", Parent: &parent, Kind: "compaction"},
		Cache:      llm.CacheDispositionV1{Disposition: "miss_populated"}, Cost: llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "provider_reported"},
	}
}

func durableTestMaterializedState(request llm.CompactRequestV1) state.MaterializedState {
	return state.MaterializedState{Handle: state.Handle(request.Parent), Tenant: request.Context.Tenant, Project: request.Context.Project, Settings: state.RootModelState("model"), Lineage: []state.Handle{state.Handle(request.Parent)}}
}
