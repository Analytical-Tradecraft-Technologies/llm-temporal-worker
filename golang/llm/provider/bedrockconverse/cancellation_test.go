package bedrockconverse

import (
	"context"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestCanceledTransportRemainsAmbiguous(t *testing.T) {
	mapped := mapError(context.Canceled, "test")
	if mapped.Code != provider.CodeCanceled || mapped.Dispatch != provider.DispatchAmbiguous {
		t.Fatalf("cancellation classification = %#v", mapped)
	}
	before := dispatchContextError(context.Canceled)
	if before.Dispatch != provider.DispatchNotDispatched {
		t.Fatal("preflight cancellation became ambiguous")
	}
}
