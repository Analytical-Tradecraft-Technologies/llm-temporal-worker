package openairesponses

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/openai/openai-go/v3/responses"
)

func TestInvokeCancellationAfterTransportConsumesRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumed := false
	client, err := NewClient(ClientConfig{BaseURL: "https://127.0.0.1/fixture", APIKey: "test-key", HTTPClient: &http.Client{Transport: testkit.RoundTripFunc(func(request *http.Request) (*http.Response, error) {
		testassert.NoError(t, testassert.Err(io.Copy(io.Discard, request.Body)))
		consumed = true
		cancel()
		return nil, request.Context().Err()
	})}})
	testassert.NoError(t, err)
	adapter, err := New(client, "test", "test")
	testassert.NoError(t, err)
	_, err = adapter.Invoke(ctx, provider.Call{EndpointID: "test", Family: provider.FamilyOpenAIResponses, SDKParams: responses.ResponseNewParams{Model: "test-model"}}, nil)
	var classified *provider.Error
	if !consumed || !errors.As(err, &classified) || classified.Dispatch != provider.DispatchAmbiguous {
		t.Fatalf("consumed=%v error=%#v", consumed, err)
	}
}
