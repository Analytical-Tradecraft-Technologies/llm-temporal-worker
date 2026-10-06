package anthropicmessages

import (
	"context"
	"net/http"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/contract"
)

func TestSDKRetriesDisabled(t *testing.T) {
	server := contract.NewRetryServer(t)
	client, err := NewClient(ClientConfig{BaseURL: server.URL, APIKey: "test-key", HTTPClient: server.HTTPClient})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.sdk.Messages.New(context.Background(), anthropic.MessageNewParams{
		MaxTokens: 8,
		Model:     anthropic.Model("claude-contract"),
		Messages:  []anthropic.MessageParam{anthropic.NewUserMessage(anthropic.NewTextBlock("hello"))},
	})
	if err == nil {
		t.Fatal("expected retryable response error")
	}
	if got := server.Count(); got != 1 {
		t.Fatalf("request count = %d, want 1", got)
	}
}

func TestClientValidatesResolvedConfig(t *testing.T) {
	for _, config := range []ClientConfig{
		{BaseURL: "https://api.anthropic.com", APIKey: "", HTTPClient: http.DefaultClient},
		{BaseURL: "https://api.anthropic.com", APIKey: "key", HTTPClient: nil},
		{BaseURL: "http://untrusted.example", APIKey: "key", HTTPClient: http.DefaultClient},
	} {
		if _, err := NewClient(config); err == nil {
			t.Errorf("NewClient(%#v) unexpectedly succeeded", config)
		}
	}
}
