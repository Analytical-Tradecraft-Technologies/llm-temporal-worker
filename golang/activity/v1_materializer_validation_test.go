package activity

import (
	"context"
	"errors"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

func validMaterializedStateForValidation(handle state.Handle) state.MaterializedState {
	value := validMaterializedState(handle)
	value.Depth = 1
	return value
}

type validatingMaterializer struct {
	result state.MaterializedState
}

func (materializer validatingMaterializer) MaterializeHandle(context.Context, string, string, state.MaterializeLimits) (state.MaterializedState, error) {
	return materializer.result, nil
}

// generateWithMaterializedState runs a child Generate through runtime wrapped
// to materialize value.
func generateWithMaterializedState(runtime V1Runtime, value state.MaterializedState) error {
	wrapped := &MaterializingV1Runtime{Runtime: runtime, Materializer: validatingMaterializer{result: value}, Scope: fixedScopeID}
	_, err := wrapped.GenerateV1(context.Background(), childGenerateV1Request("parent"))
	return err
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
			err := generateWithMaterializedState(&v1RuntimeStub{}, value)
			if err == nil {
				t.Fatal("GenerateV1 unexpectedly accepted invalid materialized state")
			}
			assertProviderError(t, err, provider.CodeStateCorrupt, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever)
		})
	}
}

func TestMaterializingV1RuntimeRejectsToolFrontierMismatch(t *testing.T) {
	value := validMaterializedStateForValidation("parent")
	value.PendingToolCalls = []string{"unrecorded"}
	err := generateWithMaterializedState(&materializingRuntimeProbe{events: new([]string)}, value)
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
	if err := generateWithMaterializedState(&materializingRuntimeProbe{events: new([]string)}, value); err != nil {
		t.Fatalf("GenerateV1 rejected an empty materialized transcript: %v", err)
	}
}
