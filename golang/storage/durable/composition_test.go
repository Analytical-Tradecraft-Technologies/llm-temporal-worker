package durable

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/budget"
)

func completeCompositionPorts(materializer BudgetMaterializer, journal Journal) CompositionPorts {
	return CompositionPorts{
		Operations:    compositionAdmissionStub{},
		Continuations: compositionContinuationStub{},
		Results:       compositionResultStub{},
		Journal:       journal,
		Materializer:  materializer,
	}
}

func TestCompositionBuilderCopiesAndValidatesSnapshotPorts(t *testing.T) {
	materializer := compositionMaterializerStub{}
	journal := compositionJournalStub{}
	builder := CompositionBuilder{
		Identity: validIdentity(),
		Ports:    completeCompositionPorts(materializer, journal),
	}
	composition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	// A reload mutates the builder input only after the snapshot has been
	// published. The already-built value retains its complete port set and
	// identity instead of observing the newer, incomplete bundle.
	builder.Identity.ConfigDigest = [32]byte{}
	builder.Ports = CompositionPorts{}
	if err := composition.Validate(); err != nil {
		t.Fatalf("built composition changed after builder mutation: %v", err)
	}
	if err := composition.BudgetBoundary().Validate(); err != nil {
		t.Fatalf("built budget boundary invalid: %v", err)
	}
	if phases := composition.NewLifecycle().Phases(); len(phases) != 0 {
		t.Fatalf("new lifecycle phases = %v, want empty", phases)
	}
}

func TestCompositionBuilderFailsClosedBeforeCallingAnyPort(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*CompositionPorts)
	}{
		{name: "operations", mutate: func(ports *CompositionPorts) { ports.Operations = nil }},
		{name: "continuations", mutate: func(ports *CompositionPorts) { ports.Continuations = nil }},
		{name: "results", mutate: func(ports *CompositionPorts) { ports.Results = nil }},
		{name: "journal", mutate: func(ports *CompositionPorts) { ports.Journal = nil }},
		{name: "materializer", mutate: func(ports *CompositionPorts) { ports.Materializer = nil }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			ports := completeCompositionPorts(compositionMaterializerStub{}, compositionJournalStub{})
			test.mutate(&ports)
			if _, err := (CompositionBuilder{Identity: validIdentity(), Ports: ports}).Build(); err == nil {
				t.Fatal("incomplete ports were accepted")
			} else if !errors.Is(err, ErrCompositionBuilderInvalid) {
				t.Fatalf("error = %v, want ErrCompositionBuilderInvalid", err)
			}
		})
	}
}

func TestCompositionBuilderBindsBudgetOrderingAndRecovery(t *testing.T) {
	now := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	request := boundaryRequest(now)
	calls := []string{}
	materializer := &boundaryMaterializer{
		result: boundaryAcceptedResult(request, now),
		calls:  calls,
	}
	journal := &boundaryJournal{calls: &materializer.calls}
	composition, err := (CompositionBuilder{
		Identity: validIdentity(),
		Ports:    completeCompositionPorts(materializer, journal),
	}).Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	lifecycle := composition.NewLifecycle()
	if err := lifecycle.Advance(PhaseOperationReplay); err != nil {
		t.Fatal(err)
	}
	reservation, err := composition.BudgetBoundary().Reserve(context.Background(), &lifecycle, request)
	if err != nil || !reservation.DispatchReady() {
		t.Fatalf("reserve = %#v, %v", reservation, err)
	}
	if got, want := materializer.calls, []string{"accept", "reservation:reservation-event-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pre-dispatch ordering = %v, want %v", got, want)
	}
	if err := lifecycle.Advance(PhaseDispatched); err != nil {
		t.Fatal(err)
	}

	materializer.reconcileErr = errors.New("Redis unavailable")
	boundary := composition.BudgetBoundary()
	completion := boundaryCompletion(request, now)
	if err := boundary.Finalize(context.Background(), &lifecycle, reservation, []budget.CompletionEvent{completion}); !errors.Is(err, ErrReconcilePending) {
		t.Fatalf("first finalization = %v, want ErrReconcilePending", err)
	}
	if current, _ := lifecycle.Current(); current != PhasePostgresFinalized {
		t.Fatalf("recovery lifecycle phase = %s, want postgres_finalized", current)
	}
	materializer.reconcileErr = nil
	if err := boundary.Finalize(context.Background(), &lifecycle, reservation, []budget.CompletionEvent{completion}); err != nil {
		t.Fatalf("reconciliation retry = %v", err)
	}
	if current, _ := lifecycle.Current(); current != PhaseRedisReconciled {
		t.Fatalf("final lifecycle phase = %s, want redis_reconciled", current)
	}
	if got, want := len(journal.completions), 1; got != want {
		t.Fatalf("completion journal count = %d, want %d", got, want)
	}
}
