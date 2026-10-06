package provider_test

import (
	"context"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

type compileOnlyAdapter struct{}

func (compileOnlyAdapter) Name() string { return "compile-only" }
func (compileOnlyAdapter) Capabilities(context.Context, provider.CapabilityQuery) (provider.CapabilitySet, error) {
	return provider.CapabilitySet{}, nil
}
func (compileOnlyAdapter) Compile(context.Context, provider.CompileInput) (provider.Call, error) {
	return provider.Call{SDKParams: struct{ Model string }{}}, nil
}
func (compileOnlyAdapter) Invoke(context.Context, provider.Call, provider.Observer) (provider.Result, error) {
	return provider.Result{Response: llm.Response{Status: llm.ResponseStatusCompleted}}, nil
}

var _ provider.Adapter = compileOnlyAdapter{}

func TestAdapterPortKeepsSDKParamsOpaque(t *testing.T) {
	var adapter provider.Adapter = compileOnlyAdapter{}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{})
	if err != nil {
		t.Fatal(err)
	}
	if call.SDKParams == nil {
		t.Fatal("adapter call lost SDK params")
	}
}
