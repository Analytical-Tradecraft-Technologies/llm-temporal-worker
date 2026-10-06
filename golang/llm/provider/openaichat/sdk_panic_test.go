package openaichat

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// panickingBody panics when the SDK decodes the response, as an SDK
// deserializer meeting an unknown union member does.
type panickingBody struct{}

func (panickingBody) Read([]byte) (int, error) { panic("slice bounds out of range") }
func (panickingBody) Close() error             { return nil }

func TestInvokeRecoversAnSDKPanicWhileDecodingTheResponse(t *testing.T) {
	client, err := NewClient(ClientConfig{
		BaseURL: "https://127.0.0.1/contract",
		APIKey:  "test-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: panickingBody{}, Request: request}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "chat-prod", testProfile())
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "op-panic", Model: "chat-model"},
		Query:   provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Invoke(context.Background(), call, nil)
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Dispatch != provider.DispatchAccepted || mapped.Retry != provider.RetryNever || mapped.Code != provider.CodeProviderInvalidResponse || mapped.OperationID != "op-panic" {
		t.Fatalf("Invoke() error = %#v, want an accepted, never-retried invalid response", err)
	}
}
