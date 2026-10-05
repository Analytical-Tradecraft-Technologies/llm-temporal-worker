package anthropicmessages

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	anthropicaws "github.com/anthropics/anthropic-sdk-go/aws"
)

func TestLargeNonStreamingRequestReachesTransport(t *testing.T) {
	for _, gateway := range []bool{false, true} {
		for _, timeout := range []time.Duration{0, time.Second} {
			calls := 0
			httpClient := &http.Client{Timeout: timeout, Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				calls++
				deadline, ok := request.Context().Deadline()
				if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 10*time.Minute {
					t.Error("request has no active timeout")
				}
				if timeout > 0 && time.Until(deadline) > timeout {
					t.Error("request exceeds configured timeout")
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"id":"msg-local","type":"message","role":"assistant","content":[],"model":"claude-contract","stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`)), Request: request}, nil
			})}
			var client *Client
			var err error
			if gateway {
				client, err = NewAWSClient(context.Background(), AWSClientConfig{BaseURL: "http://127.0.0.1", HTTPClient: httpClient, AWSConfig: anthropicaws.ClientConfig{AWSRegion: "us-east-1", WorkspaceID: "ws-contract", SkipAuth: true}})
			} else {
				client, err = NewClient(ClientConfig{BaseURL: "http://127.0.0.1", APIKey: "test-key", HTTPClient: httpClient})
			}
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.messages.New(context.Background(), anthropic.MessageNewParams{MaxTokens: 32768, Model: "claude-contract", Messages: []anthropic.MessageParam{anthropic.NewUserMessage(anthropic.NewTextBlock("hello"))}})
			if err != nil || calls != 1 {
				t.Fatalf("gateway=%v timeout=%s calls=%d error=%v", gateway, timeout, calls, err)
			}
		}
	}
}
