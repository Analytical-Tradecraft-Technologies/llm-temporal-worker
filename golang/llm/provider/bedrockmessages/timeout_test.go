package bedrockmessages

import (
	"context"
	"net/http"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/messagestest"
)

func TestLargeNonStreamingRequestReachesTransport(t *testing.T) {
	messagestest.LargeNonStreamingRequestReachesTransport(t, func(t *testing.T, httpClient *http.Client, params anthropic.MessageNewParams) error {
		client, err := NewClient(context.Background(), ClientConfig{BaseURL: "http://127.0.0.1", HTTPClient: httpClient, AWSConfig: aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test-access", "test-secret", "")}})
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.messages.New(context.Background(), params)
		return err
	})
}
