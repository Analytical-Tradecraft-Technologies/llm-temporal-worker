package anthropicmessages

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	anthropicaws "github.com/anthropics/anthropic-sdk-go/aws"
)

// panickingBody panics when the SDK decodes the response, as an SDK
// deserializer meeting an unknown union member does.
type panickingBody struct{}

func (panickingBody) Read([]byte) (int, error) { panic("slice bounds out of range") }
func (panickingBody) Close() error             { return nil }

func invokeWithPanickingResponse(t *testing.T, client *Client, baseURL, endpointID string) error {
	t.Helper()
	profile := testProfile()
	profile.ExpectedBaseURL = baseURL
	adapter, err := New(client, endpointID, profile)
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "op-panic", Model: "claude-contract", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}},
		Query:   provider.CapabilityQuery{EndpointID: endpointID, Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Invoke(context.Background(), call, nil)
	return err
}

func panickingTransport() *http.Client {
	return &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: panickingBody{}, Request: request}, nil
	})}
}

func TestInvokeRecoversAnSDKPanicWhileDecodingTheResponse(t *testing.T) {
	client, err := NewClient(ClientConfig{BaseURL: "http://127.0.0.1/contract", APIKey: "test-key", HTTPClient: panickingTransport()})
	if err != nil {
		t.Fatal(err)
	}
	err = invokeWithPanickingResponse(t, client, "http://127.0.0.1/contract", "anthropic-prod")
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Dispatch != provider.DispatchAccepted || mapped.Retry != provider.RetryNever || mapped.Code != provider.CodeProviderInvalidResponse || mapped.OperationID != "op-panic" {
		t.Fatalf("Invoke() error = %#v, want an accepted, never-retried invalid response", err)
	}
}

func TestInvokeRecoversAnSDKPanicInTheAWSGatewayClient(t *testing.T) {
	client, err := NewAWSClient(context.Background(), AWSClientConfig{
		BaseURL:    "http://127.0.0.1/aws-contract",
		HTTPClient: panickingTransport(),
		AWSConfig:  anthropicaws.ClientConfig{AWSRegion: "us-east-1", WorkspaceID: "ws-contract", SkipAuth: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = invokeWithPanickingResponse(t, client, "http://127.0.0.1/aws-contract", "anthropic-aws")
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Dispatch != provider.DispatchAccepted || mapped.Retry != provider.RetryNever || mapped.Code != provider.CodeProviderInvalidResponse || mapped.OperationID != "op-panic" {
		t.Fatalf("Invoke() error = %#v, want an accepted, never-retried invalid response", err)
	}
}
