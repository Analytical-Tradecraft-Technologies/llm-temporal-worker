package anthropicmessages

import (
	"context"
	"net/http"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"
	anthropicaws "github.com/anthropics/anthropic-sdk-go/aws"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/messagestest"
)

func TestLargeNonStreamingRequestReachesTransport(t *testing.T) {
	for name, newClient := range map[string]func(httpClient *http.Client) (*Client, error){
		"direct": func(httpClient *http.Client) (*Client, error) {
			return NewClient(ClientConfig{BaseURL: "http://127.0.0.1", APIKey: "test-key", HTTPClient: httpClient})
		},
		"gateway": func(httpClient *http.Client) (*Client, error) {
			return NewAWSClient(context.Background(), AWSClientConfig{BaseURL: "http://127.0.0.1", HTTPClient: httpClient, AWSConfig: anthropicaws.ClientConfig{AWSRegion: "us-east-1", WorkspaceID: "ws-contract", SkipAuth: true}})
		},
	} {
		t.Run(name, func(t *testing.T) {
			messagestest.LargeNonStreamingRequestReachesTransport(t, func(t *testing.T, httpClient *http.Client, params anthropic.MessageNewParams) error {
				client, err := newClient(httpClient)
				if err != nil {
					t.Fatal(err)
				}
				_, err = client.messages.New(context.Background(), params)
				return err
			})
		})
	}
}
