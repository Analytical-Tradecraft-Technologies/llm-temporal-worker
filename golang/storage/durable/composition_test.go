package durable

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
)

func completeCompositionPorts(materializer BudgetLeaser) CompositionPorts {
	return CompositionPorts{
		Operations:    compositionAdmissionStub{},
		Continuations: compositionContinuationStub{},
		Results:       compositionResultStub{},
		Materializer:  materializer,
	}
}

func TestCompositionBuilderCopiesAndValidatesSnapshotPorts(t *testing.T) {
	materializer := compositionMaterializerStub{}
	builder := CompositionBuilder{
		Identity: validIdentity(),
		Ports:    completeCompositionPorts(materializer),
	}
	composition, err := builder.Build()
	testassert.NoError(t, err, "Build() error")

	// A reload mutates the builder input only after the snapshot has been
	// published. The already-built value retains its complete port set and
	// identity instead of observing the newer, incomplete bundle.
	builder.Identity.ConfigDigest = [32]byte{}
	builder.Ports = CompositionPorts{}
	testassert.NoError(t, composition.Validate(), "built composition changed after builder mutation")
	testassert.NoError(t, composition.BudgetBoundary().Validate(), "built budget boundary invalid")
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
		{name: "materializer", mutate: func(ports *CompositionPorts) { ports.Materializer = nil }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			ports := completeCompositionPorts(compositionMaterializerStub{})
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
	composition, err := (CompositionBuilder{
		Identity: validIdentity(),
		Ports:    completeCompositionPorts(materializer),
	}).Build()
	testassert.NoError(t, err, "Build() error")

	lifecycle := composition.NewLifecycle()
	testassert.NoError(t, lifecycle.Advance(PhaseOperationReplay))
	reservation, err := composition.BudgetBoundary().Reserve(context.Background(), &lifecycle, request)
	if err != nil || reservation.DispatchReady() {
		t.Fatalf("reserve = %#v, %v", reservation, err)
	}
	testassert.NoError(t, testassert.Err(composition.BudgetBoundary().Claim(context.Background(), &lifecycle, &reservation)))
	if got, want := materializer.calls, []string{"accept", "claim"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pre-dispatch ordering = %v, want %v", got, want)
	}
	testassert.NoError(t, lifecycle.Advance(PhaseDispatched))

	materializer.reconcileErr = errors.New("Redis unavailable")
	boundary := composition.BudgetBoundary()
	completion := boundaryCompletion(request, now)
	testassert.ErrorIs(t, boundary.Finalize(context.Background(), &lifecycle, reservation, []budget.CompletionEvent{completion}), ErrReconcilePending, "first finalization")
	if current, _ := lifecycle.Current(); current != PhaseResultFinalized {
		t.Fatalf("recovery lifecycle phase = %s, want result_finalized", current)
	}
	materializer.reconcileErr = nil
	testassert.NoError(t, boundary.Finalize(context.Background(), &lifecycle, reservation, []budget.CompletionEvent{completion}), "reconciliation retry")
	if current, _ := lifecycle.Current(); current != PhaseRedisReconciled {
		t.Fatalf("final lifecycle phase = %s, want redis_reconciled", current)
	}
}
