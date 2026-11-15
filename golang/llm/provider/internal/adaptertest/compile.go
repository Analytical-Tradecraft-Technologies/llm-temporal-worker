package adaptertest

import (
	"context"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// Compiler is the Compile method every provider adapter implements.
type Compiler interface {
	Compile(context.Context, provider.CompileInput) (provider.Call, error)
}

// Compile compiles request on endpointID for family, querying the request's
// own model; strict selects strict portability.
func Compile(adapter Compiler, endpointID string, family provider.Family, request llm.Request, strict bool) (provider.Call, error) {
	return adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: endpointID, Family: family, Model: request.Model},
		Strict:  strict,
	})
}

// CompileStrict strictly compiles request on endpointID for family, querying
// the request's own model, and fails t if Compile returns an error.
func CompileStrict(t testing.TB, adapter Compiler, endpointID string, family provider.Family, request llm.Request) provider.Call {
	t.Helper()
	call, err := Compile(adapter, endpointID, family, request, true)
	if err != nil {
		t.Fatalf("Compile(%s) error = %v", request.OperationKey, err)
	}
	return call
}
