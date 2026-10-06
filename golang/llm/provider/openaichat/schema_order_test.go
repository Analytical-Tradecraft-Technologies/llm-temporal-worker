package openaichat

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// The response-format schema and tool parameters reach the provider with
// properties in "required" order (#1096).
func TestInvokeSendsSchemaPropertiesInRequiredOrder(t *testing.T) {
	var sent []byte
	client, err := NewClient(ClientConfig{BaseURL: "https://127.0.0.1/v1", APIKey: "test-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			sent, _ = io.ReadAll(request.Body)
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
				Body: io.NopCloser(strings.NewReader(`{"id":"chatcmpl-1","object":"chat.completion","created":1700000000,"model":"chat-model","choices":[]}`)), Request: request}, nil
		})}})
	if err != nil {
		t.Fatal(err)
	}
	adapter := testAdapter(t)
	adapter.client = client
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "op-order", Model: "chat-model",
			Input:  []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hi"}}}},
			Tools:  []llm.Tool{{Name: "lookup", InputSchema: json.RawMessage(`{"type":"object","required":["zeta","alpha"],"properties":{"alpha":{"type":"string"},"zeta":{"type":"string"}}}`)}},
			Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Strict: true, Schema: json.RawMessage(`{"type":"object","required":["reasoning","answer"],"properties":{"answer":{"type":"string"},"reasoning":{"type":"string"}},"additionalProperties":false}`)}}},
		Query: provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// The canned response is empty; only the request matters.
	_, _ = adapter.Invoke(context.Background(), call, provider.NopObserver{})
	body := string(sent)
	for _, pair := range [][2]string{{`"reasoning":`, `"answer":`}, {`"zeta":`, `"alpha":`}} {
		first, second := strings.Index(body, pair[0]), strings.Index(body, pair[1])
		if first < 0 || second < 0 || first > second {
			t.Fatalf("request body does not list %s before %s:\n%s", pair[0], pair[1], body)
		}
	}
}
