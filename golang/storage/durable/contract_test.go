package durable

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	postgresstore "github.com/mfow/llm-temporal-worker/golang/storage/postgres"
)

func validIdentity() StateIdentity {
	return StateIdentity{
		Postgres:     PostgresIdentity{Database: "llmtw", Schema: "worker", TablePrefix: "prod_"},
		Redis:        RedisIdentity{KeyPrefix: "llmtw", HashTag: "admission"},
		ConfigDigest: sha256.Sum256([]byte("snapshot")),
	}
}

func TestStateIdentityRejectsUnboundSnapshot(t *testing.T) {
	identity := validIdentity()
	if err := identity.Validate(); err != nil {
		t.Fatalf("valid identity rejected: %v", err)
	}
	identity.ConfigDigest = [32]byte{}
	if err := identity.Validate(); err == nil {
		t.Fatal("zero configuration digest accepted")
	}
	identity = validIdentity()
	identity.Redis.KeyPrefix = "worker:{unsafe}"
	if err := identity.Validate(); err == nil {
		t.Fatal("unsafe Redis prefix accepted")
	}
}

func TestCompositionValidateRejectsTypedNilCapabilities(t *testing.T) {
	cases := []struct {
		name string
		set  func(*Composition)
	}{
		{
			name: "operations",
			set: func(composition *Composition) {
				var value *typedNilAdmissionStore
				composition.Operations = value
			},
		},
		{
			name: "continuations",
			set: func(composition *Composition) {
				var value *typedNilContinuationStore
				composition.Continuations = value
			},
		},
		{
			name: "results",
			set: func(composition *Composition) {
				var value *typedNilResultStore
				composition.Results = value
			},
		},
		{
			name: "journal",
			set: func(composition *Composition) {
				var value *typedNilJournal
				composition.Journal = value
			},
		},
		{
			name: "materializer",
			set: func(composition *Composition) {
				var value *typedNilMaterializer
				composition.Materializer = value
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			composition := validComposition()
			tc.set(&composition)
			if err := composition.Validate(); err == nil {
				t.Fatal("typed-nil capability passed composition validation")
			}
		})
	}
}

func TestCompositionValidateAcceptsCompleteCapabilities(t *testing.T) {
	if err := validComposition().Validate(); err != nil {
		t.Fatalf("complete composition rejected: %v", err)
	}
}

func validComposition() Composition {
	return Composition{
		Identity:      validIdentity(),
		Operations:    compositionAdmissionStub{},
		Continuations: compositionContinuationStub{},
		Results:       compositionResultStub{},
		Journal:       compositionJournalStub{},
		Materializer:  compositionMaterializerStub{},
	}
}

// Embedding the interface in these pointer types gives the test a typed nil
// implementation without needing to invoke any of its methods.
type typedNilAdmissionStore struct{ admission.AdmissionStore }
type typedNilContinuationStore struct{ state.ContinuationStore }
type typedNilResultStore struct{ ResultStore }
type typedNilJournal struct{ Journal }
type typedNilMaterializer struct{ BudgetMaterializer }

type compositionAdmissionStub struct{}

func (compositionAdmissionStub) Begin(context.Context, admission.BeginRequest) (admission.BeginResult, error) {
	return admission.BeginResult{}, nil
}
func (compositionAdmissionStub) MarkDispatching(context.Context, admission.DispatchRequest) error {
	return nil
}
func (compositionAdmissionStub) Continue(context.Context, admission.ContinueRequest) (admission.ContinueResult, error) {
	return admission.ContinueResult{}, nil
}
func (compositionAdmissionStub) Complete(context.Context, admission.CompleteRequest) error {
	return nil
}
func (compositionAdmissionStub) Fail(context.Context, admission.FailRequest) error { return nil }
func (compositionAdmissionStub) Get(context.Context, string) (admission.Operation, error) {
	return admission.Operation{}, nil
}

type compositionContinuationStub struct{}

func (compositionContinuationStub) Get(context.Context, state.Handle) (state.Continuation, error) {
	return state.Continuation{}, nil
}
func (compositionContinuationStub) GetForTenant(context.Context, string, state.Handle) (state.Continuation, error) {
	return state.Continuation{}, nil
}
func (compositionContinuationStub) PutChild(context.Context, state.PutChildRequest) (state.Handle, error) {
	return "", nil
}

type compositionResultStub struct{}

func (compositionResultStub) Get(context.Context, string) (llm.Response, error) {
	return llm.Response{}, nil
}
func (compositionResultStub) Put(context.Context, string, llm.Response) (state.BlobRef, error) {
	return state.BlobRef{}, nil
}

type compositionJournalStub struct{}

func (compositionJournalStub) AppendReservation(context.Context, budget.ReservationEvent) (postgresstore.JournalRecord, error) {
	return postgresstore.JournalRecord{}, nil
}
func (compositionJournalStub) AppendCompletion(context.Context, budget.CompletionEvent) (postgresstore.JournalRecord, error) {
	return postgresstore.JournalRecord{}, nil
}

type compositionMaterializerStub struct{}

func (compositionMaterializerStub) Accept(context.Context, ReserveRequest) (ReserveResult, error) {
	return ReserveResult{}, nil
}
func (compositionMaterializerStub) Reconcile(context.Context, ReconcileRequest) error { return nil }

func TestReserveResultRequiresJournalEventsAfterAcceptance(t *testing.T) {
	request := ReserveRequest{
		OperationID:  OperationID("op-1"),
		GenerationID: GenerationID("gen-1"),
		ExpiresAt:    time.Now().Add(time.Minute),
	}
	result := ReserveResult{OperationID: request.OperationID, Accepted: true, GenerationID: request.GenerationID, IncarnationID: IncarnationID("inc-1")}
	if err := result.Validate(request); err == nil {
		t.Fatal("accepted reservation without journal event passed validation")
	}

	result.Events = []budget.ReservationEvent{{
		EventID: "event-1", GenerationID: "gen-1", OperationID: "op-1", WindowID: "window-1",
		BucketStart: time.Now().UTC(), ReservationRevision: 1,
		AmountUSD: pricing.MustUSD("0.01"), OccurredAt: time.Now().UTC(),
	}}
	if err := result.Validate(request); err != nil {
		t.Fatalf("valid accepted reservation rejected: %v", err)
	}
	result.Accepted = false
	if err := result.Validate(request); err == nil {
		t.Fatal("denied reservation with journal event passed validation")
	}
}

func TestLifecycleRequiresJournalBeforeDispatchAndReconcileLast(t *testing.T) {
	var lifecycle Lifecycle
	if err := lifecycle.Advance(PhaseRedisAccepted); err == nil {
		t.Fatal("lifecycle allowed Redis acceptance before operation replay")
	}
	if err := lifecycle.Advance(PhaseOperationReplay); err != nil {
		t.Fatalf("advance operation replay: %v", err)
	}
	if err := lifecycle.Advance(PhaseRedisAccepted); err != nil {
		t.Fatalf("advance Redis acceptance: %v", err)
	}
	if err := lifecycle.Advance(PhaseDispatched); !errors.Is(err, ErrJournalRequired) {
		t.Fatalf("dispatch without PostgreSQL journal returned %v, want %v", err, ErrJournalRequired)
	}
	for _, phase := range []Phase{PhaseOperationReplay, PhaseRedisAccepted, PhasePostgresJournaled, PhaseDispatched, PhasePostgresFinalized, PhaseRedisReconciled} {
		// The first two phases were already recorded above.
		if phase <= PhaseRedisAccepted {
			continue
		}
		if err := lifecycle.Advance(phase); err != nil {
			t.Fatalf("advance %s: %v", phase, err)
		}
	}
	if got := lifecycle.Phases(); len(got) != 6 || got[len(got)-1] != PhaseRedisReconciled {
		t.Fatalf("unexpected lifecycle phases: %#v", got)
	}
	if err := lifecycle.Advance(PhasePostgresFinalized); err == nil {
		t.Fatal("lifecycle allowed phase after reconciliation")
	}
}

func TestReconcileFailureIsRetryableOnlyAfterPostgresFinalization(t *testing.T) {
	var lifecycle Lifecycle
	for _, phase := range []Phase{PhaseOperationReplay, PhaseRedisAccepted, PhasePostgresJournaled, PhaseDispatched, PhasePostgresFinalized} {
		if err := lifecycle.Advance(phase); err != nil {
			t.Fatalf("advance %s: %v", phase, err)
		}
	}
	if err := lifecycle.ReconcileFailure(context.DeadlineExceeded); err == nil || !errors.Is(err, ErrReconcilePending) {
		t.Fatalf("reconciliation failure was not marked retryable: %v", err)
	}
	var beforeDispatch Lifecycle
	if err := beforeDispatch.ReconcileFailure(context.DeadlineExceeded); err == nil || !errors.Is(err, ErrInvalidPhase) {
		t.Fatalf("pre-finalization reconciliation failure was accepted: %v", err)
	}
}

func TestReconcileRequestBindsGenerationAndOperation(t *testing.T) {
	request := ReconcileRequest{OperationID: "op-1", GenerationID: "gen-1", IncarnationID: "inc-1"}
	request.Events = []budget.CompletionEvent{{
		EventID: "event-2", GenerationID: "gen-1", OperationID: "other-op", WindowID: "window-1",
		BucketStart: time.Now().UTC(), ReservationRevision: 1, Kind: budget.JournalRelease,
		ActualCostUSD: ptr(pricing.MustUSD("0")), CostStatus: budget.CostExact,
		ReservedDecreaseUSD: pricing.MustUSD("0.01"), OccurredAt: time.Now().UTC(),
	}}
	if err := request.Validate(); err == nil {
		t.Fatal("reconciliation event from another operation accepted")
	}
}

func ptr[T any](value T) *T { return &value }

// Compile-time assertions document the ports this contract intentionally
// consumes. Implementations live in storage/runtime packages.
var (
	_ admission.AdmissionStore = nil
	_ state.ContinuationStore  = nil
	_ Journal                  = (postgresstore.BudgetJournal)(nil)
	_ ResultStore              = llmResultStoreStub{}
)

type llmResultStoreStub struct{}

func (llmResultStoreStub) Get(context.Context, string) (llm.Response, error) {
	return llm.Response{}, nil
}
func (llmResultStoreStub) Put(context.Context, string, llm.Response) (state.BlobRef, error) {
	return state.BlobRef{}, nil
}
