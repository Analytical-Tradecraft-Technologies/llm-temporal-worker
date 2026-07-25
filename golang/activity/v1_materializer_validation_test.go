package activity

import (
	"context"
	"errors"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

func validMaterializedStateForValidation(handle state.Handle) state.MaterializedState {
	return state.MaterializedState{
		Handle:   handle,
		Tenant:   "scope-id",
		Depth:    1,
		Items:    []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "replayed"}}}},
		Settings: state.RootModelState("logical-model"),
		Lineage:  []state.Handle{"root", handle},
	}
}

type validatingMaterializer struct {
	result state.MaterializedState
}

func (materializer validatingMaterializer) MaterializeHandle(context.Context, string, string, state.MaterializeLimits) (state.MaterializedState, error) {
	return materializer.result, nil
}

func TestMaterializingV1RuntimeRejectsInvalidMaterializedStateBeforeDispatch(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*state.MaterializedState)
	}{
		{
			name: "handle mismatch",
			mutate: func(value *state.MaterializedState) {
				value.Handle = "other"
			},
		},
		{
			name: "scope mismatch",
			mutate: func(value *state.MaterializedState) {
				value.Tenant = "other-scope"
			},
		},
		{
			name: "lineage mismatch",
			mutate: func(value *state.MaterializedState) {
				value.Lineage[len(value.Lineage)-1] = "other"
			},
		},
		{
			name: "missing model",
			mutate: func(value *state.MaterializedState) {
				value.Settings.Model = ""
			},
		},
		{
			name: "negative depth",
			mutate: func(value *state.MaterializedState) {
				value.Depth = -1
			},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			value := validMaterializedStateForValidation("parent")
			test.mutate(&value)
			wrapped := &MaterializingV1Runtime{
				Runtime:      &v1RuntimeStub{},
				Materializer: validatingMaterializer{result: value},
				Scope:        func(llm.RequestContext) (string, error) { return "scope-id", nil },
			}
			request := validGenerateV1Request()
			parent := llm.CheckpointHandle("parent")
			request.Parent = &parent
			_, err := wrapped.GenerateV1(context.Background(), request)
			if err == nil {
				t.Fatal("GenerateV1 unexpectedly accepted invalid materialized state")
			}
			var providerErr *provider.Error
			if !errors.As(err, &providerErr) {
				t.Fatalf("error = %T %v, want provider error", err, err)
			}
			if providerErr.Code != provider.CodeStateCorrupt || providerErr.Phase != provider.PhaseStateLoad || providerErr.Dispatch != provider.DispatchNotDispatched || providerErr.Retry != provider.RetryNever {
				t.Fatalf("provider error = %#v, want non-retryable state-corrupt state-load error", providerErr)
			}
		})
	}
}

func TestMaterializingV1RuntimeRejectsToolFrontierMismatch(t *testing.T) {
	value := validMaterializedStateForValidation("parent")
	value.PendingToolCalls = []string{"unrecorded"}
	wrapped := &MaterializingV1Runtime{
		Runtime:      &materializingRuntimeProbe{events: new([]string)},
		Materializer: validatingMaterializer{result: value},
		Scope:        func(llm.RequestContext) (string, error) { return "scope-id", nil },
	}
	request := validGenerateV1Request()
	parent := llm.CheckpointHandle("parent")
	request.Parent = &parent
	_, err := wrapped.GenerateV1(context.Background(), request)
	var providerErr *provider.Error
	if err == nil || !errors.As(err, &providerErr) {
		t.Fatalf("GenerateV1 error = %v, want provider error", err)
	}
	if providerErr.Code != provider.CodeStateCorrupt || providerErr.Retry != provider.RetryNever {
		t.Fatalf("provider error = %#v, want non-retryable state-corrupt error", providerErr)
	}
}

func TestMaterializingV1RuntimeAcceptsEmptyMaterializedTranscript(t *testing.T) {
	value := validMaterializedStateForValidation("parent")
	value.Items = nil
	value.PendingToolCalls = nil
	wrapped := &MaterializingV1Runtime{
		Runtime:      &materializingRuntimeProbe{events: new([]string)},
		Materializer: validatingMaterializer{result: value},
		Scope:        func(llm.RequestContext) (string, error) { return "scope-id", nil },
	}
	request := validGenerateV1Request()
	parent := llm.CheckpointHandle("parent")
	request.Parent = &parent
	if _, err := wrapped.GenerateV1(context.Background(), request); err != nil {
		t.Fatalf("GenerateV1 rejected an empty materialized transcript: %v", err)
	}
}
