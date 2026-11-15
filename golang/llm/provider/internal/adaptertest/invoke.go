package adaptertest

import (
	"context"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// Invoker is the Invoke method every provider adapter implements.
type Invoker interface {
	Invoke(context.Context, provider.Call, provider.Observer) (provider.Result, error)
}

// Invoke invokes call without an observer and fails t if Invoke returns an
// error.
func Invoke(t testing.TB, adapter Invoker, call provider.Call) provider.Result {
	t.Helper()
	result, err := adapter.Invoke(context.Background(), call, provider.NopObserver{})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
