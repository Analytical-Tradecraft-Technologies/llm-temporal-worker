package bedrockmessages

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// panickingBody panics when the SDK decodes the response, as an SDK
// deserializer meeting an unknown union member does.
type panickingBody struct{}

func (panickingBody) Read([]byte) (int, error) { panic("slice bounds out of range") }
func (panickingBody) Close() error             { return nil }

// The Bedrock client reads the response in AWS middleware before the SDK
// records it, so a panic there cannot be shown to follow the response; it is
// still recovered and never retried.
func TestInvokeRecoversAnSDKPanicWhileDecodingTheResponse(t *testing.T) {
	client, err := NewClient(context.Background(), ClientConfig{
		BaseURL: "http://127.0.0.1",
		HTTPClient: &http.Client{Transport: bedrockRoundTrip(func(request *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: panickingBody{}, Request: request}, nil
		})},
		AWSConfig: aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("contract-access", "contract-secret", "")},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "bedrock-prod", mustBedrockProfile(t, "http://127.0.0.1"))
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "op-panic", Model: "claude-contract"},
		Query:   provider.CapabilityQuery{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract"}, Strict: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Invoke(context.Background(), call, nil)
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever || mapped.OperationID != "op-panic" {
		t.Fatalf("Invoke() error = %#v, want an ambiguous, never-retried outcome", err)
	}
}
