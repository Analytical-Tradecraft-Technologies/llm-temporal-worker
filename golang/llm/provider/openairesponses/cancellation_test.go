package openairesponses

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/openai/openai-go/v3/responses"
)

func TestCanceledTransportRemainsAmbiguous(t *testing.T) {
	mapped := mapError(context.Canceled)
	if mapped.Code != provider.CodeCanceled || mapped.Dispatch != provider.DispatchAmbiguous {
		t.Fatalf("cancellation classification = %#v", mapped)
	}
	before := dispatchContextError(context.Canceled)
	if before.Dispatch != provider.DispatchNotDispatched {
		t.Fatal("preflight cancellation became ambiguous")
	}
}

func TestInvokeCancellationAfterTransportConsumesRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumed := false
	client, err := NewClient(ClientConfig{BaseURL: "https://127.0.0.1/fixture", APIKey: "test-key", HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if _, err := io.Copy(io.Discard, request.Body); err != nil {
			t.Fatal(err)
		}
		consumed = true
		cancel()
		return nil, request.Context().Err()
	})}})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Invoke(ctx, provider.Call{EndpointID: "test", Family: provider.FamilyOpenAIResponses, SDKParams: responses.ResponseNewParams{Model: "test-model"}}, nil)
	var classified *provider.Error
	if !consumed || !errors.As(err, &classified) || classified.Dispatch != provider.DispatchAmbiguous {
		t.Fatalf("consumed=%v error=%#v", consumed, err)
	}
}
