package openaichat

import (
	"context"
	"net/http"
	"testing"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/shared"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestNewClientSendsLoopbackHTTPThroughTheGuardedClient(t *testing.T) {
	adaptertest.SendsLoopbackThroughGuardedClient(t, "/v1/chat/completions", `{"id":"chatcmpl-1","object":"chat.completion","created":1700000000,"model":"gpt-loopback","choices":[]}`, func(baseURL string, httpClient *http.Client) error {
		client, err := NewClient(ClientConfig{BaseURL: baseURL, APIKey: "loopback-key", HTTPClient: httpClient})
		if err != nil {
			return err
		}
		_, err = client.sdk.Chat.Completions.New(context.Background(), openai.ChatCompletionNewParams{Model: shared.ChatModel("gpt-loopback"), Messages: []openai.ChatCompletionMessageParamUnion{openai.UserMessage("hi")}})
		return err
	})
}

func TestNewAzureClientSendsCredentialsToLoopbackHTTPThroughSDKTransport(t *testing.T) {
	server, requests := adaptertest.LoopbackJSONServer(t, "/openai/deployments/deployment-a/chat/completions", "Api-Key", "loopback-azure-key", `{"id":"chatcmpl-1","object":"chat.completion","created":1700000000,"model":"deployment-a","choices":[]}`)
	client, err := NewAzureClient(AzureClientConfig{Endpoint: server.URL, APIVersion: "2025-01-01", APIKey: "loopback-azure-key", HTTPClient: adaptertest.RejectingHTTPClient()})
	testassert.NoError(t, err)
	testassert.NoError(t, testassert.Err(client.sdk.Chat.Completions.New(context.Background(), openai.ChatCompletionNewParams{Model: shared.ChatModel("deployment-a"), Messages: []openai.ChatCompletionMessageParamUnion{openai.UserMessage("hi")}})))
	if got := requests.Load(); got != 1 {
		t.Fatalf("loopback requests = %d, want 1", got)
	}
}
