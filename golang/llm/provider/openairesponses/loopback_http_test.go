package openairesponses

import (
	"context"
	"net/http"
	"testing"

	"github.com/openai/openai-go/v3/responses"
	"github.com/openai/openai-go/v3/shared"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

const loopbackResponse = `{"id":"resp_1","object":"response","created_at":1700000000,"model":"gpt-loopback","status":"completed","output":[]}`

func TestNewClientSendsLoopbackHTTPThroughTheGuardedClient(t *testing.T) {
	adaptertest.SendsLoopbackThroughGuardedClient(t, "/v1/responses", loopbackResponse, func(baseURL string, httpClient *http.Client) error {
		client, err := NewClient(ClientConfig{BaseURL: baseURL, APIKey: "loopback-key", HTTPClient: httpClient})
		if err != nil {
			return err
		}
		_, err = client.sdk.Responses.New(context.Background(), responses.ResponseNewParams{Model: shared.ResponsesModel("gpt-loopback")})
		return err
	})
}

func TestNewAzureClientsSendCredentialsToLoopbackHTTPThroughSDKTransport(t *testing.T) {
	for name, test := range map[string]struct {
		header, want string
		newClient    func(endpoint string) (*Client, error)
	}{
		"api key": {header: "Api-Key", want: "loopback-azure-key", newClient: func(endpoint string) (*Client, error) {
			return NewAzureClient(AzureClientConfig{Endpoint: endpoint, APIVersion: "v1", APIKey: "loopback-azure-key", HTTPClient: adaptertest.RejectingHTTPClient()})
		}},
		"token credential": {header: "Authorization", want: "Bearer azure-token", newClient: func(endpoint string) (*Client, error) {
			return NewAzureTokenClient(AzureTokenClientConfig{Endpoint: endpoint, APIVersion: "v1", TokenCredential: &fakeAzureTokenCredential{}, HTTPClient: adaptertest.RejectingHTTPClient()})
		}},
	} {
		t.Run(name, func(t *testing.T) {
			server, requests := adaptertest.LoopbackJSONServer(t, "/openai/v1/responses", test.header, test.want, loopbackResponse)
			client, err := test.newClient(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.sdk.Responses.New(context.Background(), responses.ResponseNewParams{Model: shared.ResponsesModel("gpt-loopback")}); err != nil {
				t.Fatal(err)
			}
			if got := requests.Load(); got != 1 {
				t.Fatalf("loopback requests = %d, want 1", got)
			}
		})
	}
}
