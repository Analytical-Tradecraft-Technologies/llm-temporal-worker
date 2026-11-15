package anthropicmessages

import (
	"context"
	"net/http"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	anthropicaws "github.com/anthropics/anthropic-sdk-go/aws"
)

func invokeWithPanickingResponse(t *testing.T, client *Client, baseURL, endpointID string) error {
	t.Helper()
	profile := testProfile()
	profile.ExpectedBaseURL = baseURL
	adapter, err := New(client, endpointID, profile)
	if err != nil {
		t.Fatal(err)
	}
	call := adaptertest.CompileStrict(t, adapter, endpointID, provider.FamilyAnthropicMessages, llm.Request{OperationKey: "op-panic", Model: "claude-contract", Input: []llm.Item{llm.HumanText("hello")}})
	_, err = adapter.Invoke(context.Background(), call, nil)
	return err
}

func panickingTransport() *http.Client {
	return &http.Client{Transport: adaptertest.PanickingTransport(http.StatusOK)}
}

func TestInvokeRecoversAnSDKPanicWhileDecodingTheResponse(t *testing.T) {
	client, err := NewClient(ClientConfig{BaseURL: "http://127.0.0.1/contract", APIKey: "test-key", HTTPClient: panickingTransport()})
	if err != nil {
		t.Fatal(err)
	}
	err = invokeWithPanickingResponse(t, client, "http://127.0.0.1/contract", "anthropic-prod")
	adaptertest.RecoveredDecodePanic(t, err, "op-panic")
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
	adaptertest.RecoveredDecodePanic(t, err, "op-panic")
}
