package durable

import (
	"context"
	"errors"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

var errSimulatedWorkerCrash = errors.New("simulated worker crash before Redis reconciliation")

// TestGenerateV1CrashAfterPostgresFinalizationDoesNotResubmit proves the
// Task 21 crash boundary without requiring a live provider or database. The
// fake store treats Finalize as the durable PostgreSQL commit, then injects a
// worker failure before Redis reconciliation. A retry replays the committed
// identities and only invokes Reconcile; provider dispatch, reservation,
// journaling, and finalization each remain exactly once.
func TestGenerateV1CrashAfterPostgresFinalizationDoesNotResubmit(t *testing.T) {
	request := testGenerateRequest()
	store := &generateCrashRecoveryStore{
		request:     request,
		route:       testRoutePlan(),
		reservation: testReservation(testRoutePlan()),
	}
	ports := store.ports()

	first, err := GenerateV1(context.Background(), request, ports)
	if err == nil || !errors.Is(err, ErrReconcilePending) {
		t.Fatalf("first attempt error = %v, want reconciliation-pending crash result", err)
	}
	if first.OperationID != "" {
		t.Fatalf("first attempt returned response after simulated crash: %#v", first)
	}
	if !store.finalized {
		t.Fatal("simulated PostgreSQL finalization did not commit before crash")
	}

	second, err := GenerateV1(context.Background(), request, ports)
	if err != nil {
		t.Fatalf("reconciliation retry error = %v", err)
	}
	if second.OperationID != string(store.route.OperationID) {
		t.Fatalf("reconciliation response operation id = %q, want %q", second.OperationID, store.route.OperationID)
	}
	if !store.reconciled {
		t.Fatal("reconciliation retry did not commit Redis completion")
	}

	third, err := GenerateV1(context.Background(), request, ports)
	if err != nil {
		t.Fatalf("completed replay error = %v", err)
	}
	if third.OperationID != second.OperationID {
		t.Fatalf("completed replay operation id = %q, want %q", third.OperationID, second.OperationID)
	}

	if store.dispatchCalls != 1 {
		t.Fatalf("provider dispatch count = %d, want exactly one", store.dispatchCalls)
	}
	if store.reserveCalls != 1 || store.journalCalls != 1 || store.finalizeCalls != 1 {
		t.Fatalf("pre-dispatch/finalization calls = reserve %d, journal %d, finalize %d; want one each", store.reserveCalls, store.journalCalls, store.finalizeCalls)
	}
	if store.reconcileCalls != 2 {
		t.Fatalf("reconciliation calls = %d, want crash retry plus successful retry", store.reconcileCalls)
	}
}

type generateCrashRecoveryStore struct {
	request      llm.GenerateRequestV1
	route        RoutePlan
	reservation  ReserveResult
	finalization GenerateFinalization

	finalized  bool
	reconciled bool

	reserveCalls   int
	journalCalls   int
	dispatchCalls  int
	finalizeCalls  int
	reconcileCalls int
}

func (store *generateCrashRecoveryStore) ports() GeneratePorts {
	return GeneratePorts{
		Replay: func(context.Context, llm.GenerateRequestV1) (GenerateReplay, error) {
			switch {
			case store.reconciled:
				response := store.finalization.Response
				return GenerateReplay{Completed: &response}, nil
			case store.finalized:
				return GenerateReplay{ReconciliationPending: &GenerateReconciliation{
					Route: store.route, Reservation: store.reservation, Finalization: store.finalization,
				}}, nil
			default:
				return GenerateReplay{}, nil
			}
		},
		CacheLookup: func(context.Context, llm.GenerateRequestV1, GenerateReplay) (CacheDecision, error) {
			return CacheDecision{Disposition: CacheMiss}, nil
		},
		CompactionDecision: func(context.Context, llm.GenerateRequestV1, GenerateReplay, CacheDecision) (CompactionDecision, error) {
			return CompactionDecision{}, nil
		},
		Route: func(context.Context, llm.GenerateRequestV1, GenerateReplay, CompactionDecision) (RoutePlan, error) {
			return store.route, nil
		},
		Reserve: func(context.Context, llm.GenerateRequestV1, RoutePlan) (ReserveResult, error) {
			store.reserveCalls++
			return store.reservation, nil
		},
		Journal: func(context.Context, llm.GenerateRequestV1, RoutePlan, ReserveResult) (JournalReceipt, error) {
			store.journalCalls++
			return JournalReceipt{OperationID: store.route.OperationID, GenerationID: store.route.GenerationID}, nil
		},
		Dispatch: func(context.Context, llm.GenerateRequestV1, GenerateReplay, RoutePlan, JournalReceipt) (DispatchResult, error) {
			store.dispatchCalls++
			return DispatchResult{}, nil
		},
		Finalize: func(context.Context, llm.GenerateRequestV1, GenerateReplay, RoutePlan, ReserveResult, DispatchResult) (GenerateFinalization, error) {
			store.finalizeCalls++
			store.finalization = testFinalization(store.request)
			store.finalization.Response.OperationID = string(store.route.OperationID)
			store.finalized = true
			return store.finalization, nil
		},
		FinalizeCache: func(context.Context, llm.GenerateRequestV1, GenerateReplay, CacheDecision) (GenerateFinalization, error) {
			return GenerateFinalization{}, errors.New("cache finalization was not expected")
		},
		Reconcile: func(context.Context, llm.GenerateRequestV1, RoutePlan, ReserveResult, GenerateFinalization) error {
			store.reconcileCalls++
			if store.reconcileCalls == 1 {
				return errSimulatedWorkerCrash
			}
			store.reconciled = true
			return nil
		},
	}
}
