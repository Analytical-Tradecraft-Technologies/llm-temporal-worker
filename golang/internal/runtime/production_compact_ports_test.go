package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	durablestore "github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

func TestCompactProviderValidationRejectsInvalidMessageAndSummaryShapes(t *testing.T) {
	tests := []struct {
		name     string
		response llm.Response
	}{
		{
			name: "invalid message",
			response: llm.Response{
				Status: llm.ResponseStatusCompleted,
				Output: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "summary"}}}},
			},
		},
		{
			name: "empty compacted summary",
			response: llm.Response{
				Status: llm.ResponseStatusCompleted,
				Output: []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: ""}}}},
			},
		},
		{
			name: "incomplete compacted summary",
			response: llm.Response{
				Status: llm.ResponseStatusLength,
				Output: []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "truncated"}}}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateCompactProviderResponse(test.response, 1024); !errors.Is(err, errCompactInvalidProviderOutput) {
				t.Fatalf("validation error = %v, want %v", err, errCompactInvalidProviderOutput)
			}
		})
	}
}

func TestCompactReplayRejectsCheckpointBoundsBeforeRouting(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	request := llm.CompactRequestV1{
		APIVersion: llm.CompactAPIVersion, OperationKey: "compact-bounds",
		Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
		Parent:  "checkpoint-parent",
	}
	tests := []struct {
		name  string
		state state.MaterializedState
	}{
		{name: "depth", state: state.MaterializedState{Depth: 2}},
		{name: "rows", state: state.MaterializedState{Lineage: []state.Handle{"one", "two"}}},
		{name: "items", state: state.MaterializedState{Items: []llm.Item{llm.Message{}, llm.Message{}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			operationID := strictOperationIdentity(rawScope(request.Context), request.Context.Actor, "compact", llm.CompactAPIVersion, request.OperationKey)
			store := &compactFailureReplayStore{transitionAdmissionStore: transitionAdmissionStore{operation: admission.Operation{
				ID: string(operationID), State: admission.StateReserved, ExpiresAt: now.Add(time.Hour),
			}}}
			binding := &productionPhaseBinding{
				cap: V1RuntimeCapabilities{
					Clock: func() time.Time { return now },
					ResolveScope: func(context.Context, llm.RequestContext) (string, error) {
						return "scope-1", nil
					},
					CheckpointLimits: state.MaterializeLimits{MaxDepth: 2, MaxRows: 2, MaxItems: 1, MaxBytes: 1024},
					Checkpoints:      CheckpointCapabilities{Materializer: compactFailureCheckpointMaterializer{materialized: test.state}},
				},
				composition: durablestore.Composition{Operations: store},
			}
			if _, err := binding.replayCompact(context.Background(), request); err == nil {
				t.Fatalf("compact replay accepted out-of-bounds %s checkpoint", test.name)
			}
		})
	}
}

func TestCompactCheckpointFailureTerminalizesAndReconcilesAcceptedProviderOutcome(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	route := durableRouteFixture(t, now, now.Add(time.Hour))
	route.Execution.Candidate.AttemptedClass = llm.ServiceClassStandard
	store := &postResponseOperationStore{transitionAdmissionStore: transitionAdmissionStore{operation: admission.Operation{
		ID: string(route.OperationID), State: admission.StateDispatching, DispatchToken: "dispatch-token",
	}}}
	materializer := &transitionMaterializer{}
	binding := &productionPhaseBinding{
		cap: V1RuntimeCapabilities{
			Clock: func() time.Time { return now },
			ResolveScope: func(context.Context, llm.RequestContext) (string, error) {
				return "invalid-scope", nil
			},
			CheckpointLimits: state.MaterializeLimits{MaxDepth: 8, MaxRows: 8, MaxItems: 8, MaxBytes: 1024},
		},
		composition: durablestore.Composition{Operations: store, Journal: builderJournal{}, Materializer: materializer},
	}
	actual := pricing.MustUSD("0.125")
	request := llm.CompactRequestV1{
		APIVersion: llm.CompactAPIVersion, OperationKey: "compact-checkpoint-failure",
		Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
		Parent:  "checkpoint-parent",
	}
	_, err := binding.finalizeCompact(context.Background(), request, durablestore.CompactReplay{}, route, *route.Reservation, durablestore.CompactDispatchResult{
		Response: llm.Response{Cost: llm.Cost{Status: llm.CostStatusKnown, ActualCostUSD: &actual, Method: "provider_reported"}},
	})
	if err == nil {
		t.Fatal("compact checkpoint publication failure returned success")
	}
	if store.operation.State != admission.StateDefiniteFailed || store.failCalls != 1 ||
		!store.failure.PostResponse || store.failure.Certainty != admission.Accepted ||
		store.failure.Reason != "checkpoint_publication_failed" || materializer.reconciliations != 1 {
		t.Fatalf("compact terminal failure outcome: operation=%#v failure=%#v reconciliations=%d", store.operation, store.failure, materializer.reconciliations)
	}
}

type compactFailureReplayStore struct {
	transitionAdmissionStore
	completedAt time.Time
	failRequest admission.FailRequest
}

func (store *compactFailureReplayStore) Begin(context.Context, admission.BeginRequest) (admission.BeginResult, error) {
	return admission.BeginResult{Operation: store.operation.Clone(), Existing: true}, nil
}

func (store *compactFailureReplayStore) Fail(_ context.Context, request admission.FailRequest) error {
	if store.operation.State != admission.StateDispatching || request.DispatchToken != store.operation.DispatchToken {
		return admission.ErrInvalidTransition
	}
	store.failRequest = request
	store.operation.State = admission.StateDefiniteFailed
	store.operation.CompletedAt = store.completedAt
	store.operation.CostStatus = request.CostStatus
	store.operation.CostMethod = request.CostMethod
	store.operation.CostUnknownReason = request.UnknownReason
	store.operation.FailureReason = request.Reason
	store.operation.IncurredMicroUSD = request.Incurred
	if request.CostStatus == "exact" {
		actual := request.IncurredCostUSD
		store.operation.IncurredCostUSD = &actual
		store.operation.ActualCostUSD = &actual
	}
	return nil
}

type compactFailureBudgetMaterializer struct {
	failures int
	requests []durablestore.ReconcileRequest
}

func (*compactFailureBudgetMaterializer) Accept(context.Context, durablestore.ReserveRequest) (durablestore.ReserveResult, error) {
	return durablestore.ReserveResult{}, errors.New("unexpected reservation acceptance")
}

func (*compactFailureBudgetMaterializer) Confirm(context.Context, durablestore.ReserveRequest) (durablestore.ReserveResult, error) {
	return durablestore.ReserveResult{}, errors.New("unexpected reservation confirmation")
}

func (*compactFailureBudgetMaterializer) FenceDispatch(context.Context, durablestore.DispatchFenceRequest) error {
	return errors.New("unexpected dispatch fence")
}

func (materializer *compactFailureBudgetMaterializer) Reconcile(_ context.Context, request durablestore.ReconcileRequest) error {
	materializer.requests = append(materializer.requests, request)
	if materializer.failures > 0 {
		materializer.failures--
		return errors.New("injected post-response reconciliation failure")
	}
	return nil
}

type compactFailureCheckpointMaterializer struct {
	materialized state.MaterializedState
}

func (materializer compactFailureCheckpointMaterializer) Materialize(context.Context, string, state.CheckpointID, state.MaterializeLimits) (state.MaterializedState, error) {
	return materializer.materialized, nil
}

func (materializer compactFailureCheckpointMaterializer) MaterializeHandle(context.Context, string, string, state.MaterializeLimits) (state.MaterializedState, error) {
	return materializer.materialized, nil
}

func TestCompactInvalidProviderOutputReplaysPendingExactCostReconciliation(t *testing.T) {
	occurredAt := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	completedAt := occurredAt.Add(time.Second)
	expiresAt := occurredAt.Add(time.Hour)
	request := llm.CompactRequestV1{
		APIVersion: llm.CompactAPIVersion, OperationKey: "compact-operation",
		Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
		Parent:  "checkpoint-parent",
	}
	operationID := strictOperationIdentity(rawScope(request.Context), request.Context.Actor, "compact", llm.CompactAPIVersion, request.OperationKey)
	reserved := pricing.MustUSD("0.2")
	reservationRequest := durablestore.ReserveRequest{
		OperationID: operationID, GenerationID: "generation-1", IncarnationID: "incarnation-1",
		Reservations: []admission.WindowReservation{{
			PolicyID: "policy-1", WindowID: "window-1", Bucket: occurredAt.Unix(),
			Amount: 200000, AmountUSD: reserved, Limit: 1000000, LimitUSD: pricing.MustUSD("1"),
			BucketNanos: int64(time.Second), DurationNanos: int64(time.Hour),
		}},
		ExpiresAt: expiresAt, OccurredAt: occurredAt,
	}
	reservation, err := durablestore.PlannedReserveResult(reservationRequest)
	if err != nil {
		t.Fatal(err)
	}
	candidate := routing.Candidate{RouteID: "route-1", EndpointID: "endpoint-1", Provider: "provider-1", Model: "model-1", AttemptedClass: llm.ServiceClassStandard}
	route := durablestore.RoutePlan{
		OperationID: operationID, GenerationID: "generation-1",
		RouteID: candidate.RouteID, EndpointID: candidate.EndpointID, Provider: candidate.Provider, Model: candidate.Model,
		PriceVersion: "price-1", ReservationExpiresAt: expiresAt, Reservation: &reservation,
		Execution: &durablestore.RouteExecution{
			Candidate: candidate, Price: pricing.Entry{Version: "price-1"},
			Reservations: reservationRequest.Reservations, EstimatedUSD: reserved,
		},
	}
	facts, err := persistedFactsForRoute(route, reservation)
	if err != nil {
		t.Fatal(err)
	}
	immutableFacts, err := json.Marshal(facts)
	if err != nil {
		t.Fatal(err)
	}
	store := &compactFailureReplayStore{
		transitionAdmissionStore: transitionAdmissionStore{operation: admission.Operation{
			ID: string(operationID), State: admission.StateDispatching, DispatchToken: "dispatch-token",
			ImmutableFacts: immutableFacts, ExpiresAt: expiresAt,
		}},
		completedAt: completedAt,
	}
	materializer := &compactFailureBudgetMaterializer{failures: 1}
	binding := &productionPhaseBinding{
		cap: V1RuntimeCapabilities{
			Clock:            func() time.Time { return completedAt.Add(time.Hour) },
			CheckpointLimits: state.MaterializeLimits{MaxDepth: 8, MaxRows: 8, MaxItems: 32, MaxBytes: 4096},
			ResolveScope:     func(context.Context, llm.RequestContext) (string, error) { return "scope-1", nil },
			Checkpoints: CheckpointCapabilities{Materializer: compactFailureCheckpointMaterializer{materialized: state.MaterializedState{
				Items:    []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "conversation"}}}},
				Settings: state.RootModelState("model-1"),
			}}},
		},
		composition: durablestore.Composition{Operations: store, Journal: builderJournal{}, Materializer: materializer},
	}
	actual := pricing.MustUSD("0.125")
	response := llm.Response{Cost: llm.Cost{Status: llm.CostStatusKnown, ActualCostUSD: &actual, Method: "provider_reported"}}
	injected := binding.failCompactProviderOutput(context.Background(), route, response, errCompactInvalidProviderOutput)
	if injected == nil || errors.Is(injected, errCompactInvalidProviderOutput) {
		t.Fatalf("first failure = %v, want injected reconciliation failure", injected)
	}
	if store.operation.State != admission.StateDefiniteFailed || store.operation.FailureReason != compactInvalidProviderOutputReason {
		t.Fatalf("terminal operation = %#v", store.operation)
	}
	if !store.failRequest.PostResponse || store.failRequest.Certainty != admission.Accepted || store.failRequest.CostStatus != "exact" || store.failRequest.IncurredCostUSD.Cmp(actual) != 0 {
		t.Fatalf("persisted provider outcome/cost = %#v", store.failRequest)
	}
	if _, err := binding.replayCompact(context.Background(), request); !errors.Is(err, errCompactInvalidProviderOutput) {
		t.Fatalf("replay error = %v, want %v", err, errCompactInvalidProviderOutput)
	}
	if _, err := binding.replayCompact(context.Background(), request); !errors.Is(err, errCompactInvalidProviderOutput) {
		t.Fatalf("settled replay error = %v, want stable %v", err, errCompactInvalidProviderOutput)
	}
	if len(materializer.requests) != 3 {
		t.Fatalf("reconciliation attempts = %d, want 3", len(materializer.requests))
	}
	firstEvents, secondEvents, thirdEvents := materializer.requests[0].Events, materializer.requests[1].Events, materializer.requests[2].Events
	if !reflect.DeepEqual(firstEvents, secondEvents) || !reflect.DeepEqual(firstEvents, thirdEvents) {
		t.Fatalf("replayed compensation changed: first=%#v second=%#v third=%#v", firstEvents, secondEvents, thirdEvents)
	}
	if len(secondEvents) != 1 || !secondEvents[0].OccurredAt.Equal(completedAt) || secondEvents[0].Kind != budget.JournalFinalizeExact || secondEvents[0].ActualCostUSD == nil || secondEvents[0].ActualCostUSD.Cmp(actual) != 0 || secondEvents[0].ReservedDecreaseUSD.Cmp(reserved) != 0 || secondEvents[0].AccountedIncreaseUSD.Cmp(actual) != 0 {
		t.Fatalf("replayed exact-cost compensation = %#v", secondEvents)
	}
}

func newCompactTerminalReplayFixture(
	t *testing.T,
	terminalState admission.OperationState,
	failureReason string,
	costStatus string,
	costMethod string,
	unknownReason string,
	actual *pricing.USD,
) (*productionPhaseBinding, llm.CompactRequestV1, *compactFailureBudgetMaterializer, pricing.USD, time.Time) {
	t.Helper()
	occurredAt := time.Date(2026, 8, 10, 13, 0, 0, 0, time.UTC)
	completedAt := occurredAt.Add(time.Second)
	expiresAt := occurredAt.Add(time.Hour)
	request := llm.CompactRequestV1{
		APIVersion: llm.CompactAPIVersion, OperationKey: "compact-terminal-replay",
		Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
		Parent:  "expired-parent-that-must-not-be-materialized",
	}
	operationID := strictOperationIdentity(rawScope(request.Context), request.Context.Actor, "compact", llm.CompactAPIVersion, request.OperationKey)
	reserved := pricing.MustUSD("0.2")
	reserveRequest := durablestore.ReserveRequest{
		OperationID: operationID, GenerationID: "generation-terminal", IncarnationID: "incarnation-terminal",
		Reservations: []admission.WindowReservation{{
			PolicyID: "policy-terminal", WindowID: "window-terminal", Bucket: occurredAt.Unix(),
			Amount: 200000, AmountUSD: reserved, Limit: 1000000, LimitUSD: pricing.MustUSD("1"),
			BucketNanos: int64(time.Second), DurationNanos: int64(time.Hour),
		}},
		ExpiresAt: expiresAt, OccurredAt: occurredAt,
	}
	reservation, err := durablestore.PlannedReserveResult(reserveRequest)
	if err != nil {
		t.Fatal(err)
	}
	candidate := routing.Candidate{
		RouteID: "route-terminal", EndpointID: "endpoint-terminal",
		Provider: "provider-terminal", Model: "model-terminal",
		AttemptedClass: llm.ServiceClassStandard,
	}
	route := durablestore.RoutePlan{
		OperationID: operationID, GenerationID: reserveRequest.GenerationID,
		RouteID: candidate.RouteID, EndpointID: candidate.EndpointID,
		Provider: candidate.Provider, Model: candidate.Model,
		PriceVersion: "price-terminal", ReservationExpiresAt: expiresAt, Reservation: &reservation,
		Execution: &durablestore.RouteExecution{
			Candidate: candidate, Price: pricing.Entry{Version: "price-terminal"},
			Reservations: reserveRequest.Reservations, EstimatedUSD: reserved,
		},
	}
	facts, err := persistedFactsForRoute(route, reservation)
	if err != nil {
		t.Fatal(err)
	}
	immutableFacts, err := json.Marshal(facts)
	if err != nil {
		t.Fatal(err)
	}
	operation := admission.Operation{
		ID: string(operationID), State: terminalState, ImmutableFacts: immutableFacts,
		CompletedAt: completedAt, ExpiresAt: expiresAt, FailureReason: failureReason,
		CostStatus: costStatus, CostMethod: costMethod, CostUnknownReason: unknownReason,
	}
	if actual != nil {
		value := *actual
		operation.IncurredCostUSD = &value
		operation.ActualCostUSD = &value
	}
	store := &compactFailureReplayStore{
		transitionAdmissionStore: transitionAdmissionStore{operation: operation},
		completedAt:              completedAt,
	}
	materializer := &compactFailureBudgetMaterializer{failures: 1}
	binding := &productionPhaseBinding{
		cap: V1RuntimeCapabilities{
			Clock: func() time.Time { return completedAt.Add(time.Hour) },
			ResolveScope: func(context.Context, llm.RequestContext) (string, error) {
				t.Fatal("terminal Compact replay rematerialized its expired parent")
				return "", errors.New("unexpected parent materialization")
			},
		},
		composition: durablestore.Composition{
			Operations: store, Journal: builderJournal{}, Materializer: materializer,
		},
	}
	return binding, request, materializer, reserved, completedAt
}

func TestCompactTerminalFailuresReplayPendingBudgetAccounting(t *testing.T) {
	exactZero := pricing.MustUSD("0")
	tests := []struct {
		name          string
		state         admission.OperationState
		reason        string
		costStatus    string
		costMethod    string
		unknownReason string
		actual        *pricing.USD
		wantError     error
		wantKind      budget.JournalEventKind
		wantUnknown   string
	}{
		{
			name:  "generic definite provider failure with exact zero cost",
			state: admission.StateDefiniteFailed, reason: "provider_dispatch_failed",
			costStatus: "exact", costMethod: "worker_cache_zero", actual: &exactZero,
			wantError: errCompactOperationPreviouslyFailed,
			wantKind:  budget.JournalRelease,
		},
		{
			name:  "generic definite provider failure with unknown cost",
			state: admission.StateDefiniteFailed, reason: "provider_dispatch_failed",
			costStatus: "unknown", unknownReason: "provider_did_not_report_cost",
			wantError: errCompactOperationPreviouslyFailed,
			wantKind:  budget.JournalFinalizeUnknown, wantUnknown: "provider_did_not_report_cost",
		},
		{
			name:  "ambiguous terminal provider failure",
			state: admission.StateAmbiguous, reason: "provider_dispatch_failed",
			costStatus: "unknown", unknownReason: "ambiguous_dispatch",
			wantError: errCompactProviderOutcomeNeedsRecovery,
			wantKind:  budget.JournalRetainAmbiguous, wantUnknown: "ambiguous_dispatch",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			binding, request, materializer, reserved, completedAt := newCompactTerminalReplayFixture(
				t, test.state, test.reason, test.costStatus, test.costMethod, test.unknownReason, test.actual,
			)
			if _, err := binding.replayCompact(context.Background(), request); err == nil || errors.Is(err, test.wantError) {
				t.Fatalf("first replay error = %v, want reconciliation failure before terminal sentinel", err)
			}
			if _, err := binding.replayCompact(context.Background(), request); !errors.Is(err, test.wantError) {
				t.Fatalf("second replay error = %v, want %v", err, test.wantError)
			}
			if _, err := binding.replayCompact(context.Background(), request); !errors.Is(err, test.wantError) {
				t.Fatalf("settled replay error = %v, want stable %v", err, test.wantError)
			}
			if len(materializer.requests) != 3 {
				t.Fatalf("reconciliation attempts = %d, want 3", len(materializer.requests))
			}
			firstEvents := materializer.requests[0].Events
			for index, request := range materializer.requests[1:] {
				if !reflect.DeepEqual(firstEvents, request.Events) {
					t.Fatalf("reconciliation %d changed: first=%#v replay=%#v", index+2, firstEvents, request.Events)
				}
			}
			if len(firstEvents) != 1 {
				t.Fatalf("terminal compensation events = %#v", firstEvents)
			}
			event := firstEvents[0]
			if event.Kind != test.wantKind || event.UnknownReasonCode != test.wantUnknown || !event.OccurredAt.Equal(completedAt) {
				t.Fatalf("terminal compensation = %#v", event)
			}
			switch test.wantKind {
			case budget.JournalRetainAmbiguous:
				if !event.ReservedDecreaseUSD.IsZero() || !event.AccountedIncreaseUSD.IsZero() {
					t.Fatalf("ambiguous retained bound changed accounting totals: %#v", event)
				}
			case budget.JournalRelease:
				if event.ReservedDecreaseUSD.Cmp(reserved) != 0 || !event.AccountedIncreaseUSD.IsZero() || event.ActualCostUSD == nil || event.ActualCostUSD.Cmp(exactZero) != 0 {
					t.Fatalf("exact-zero failure did not release reservation: %#v", event)
				}
			default:
				if event.ReservedDecreaseUSD.Cmp(reserved) != 0 || event.AccountedIncreaseUSD.Cmp(reserved) != 0 {
					t.Fatalf("unknown-cost finalization did not settle reservation: %#v", event)
				}
			}
		})
	}
}
