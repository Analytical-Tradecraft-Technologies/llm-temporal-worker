package openaichat

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// The provider requires response_format.json_schema.name and restricts it to
// this pattern (openai-go shared.ResponseFormatJSONSchemaJSONSchemaParam.Name).
var providerSchemaNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func TestJSONSchemaOutputWithoutNameSendsValidDefaultName(t *testing.T) {
	responseBody := `{"id":"chatcmpl-name","object":"chat.completion","created":1700000000,"model":"chat-model","service_tier":"default","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"{\"answer\":\"ok\"}","refusal":""}}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`
	var wire []byte
	client, err := NewClient(ClientConfig{
		BaseURL: "https://127.0.0.1/contract",
		APIKey:  "test-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			wire, _ = io.ReadAll(request.Body)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}, "X-Request-Id": []string{"req-name"}},
				Body:       io.NopCloser(strings.NewReader(responseBody)),
				Request:    request,
			}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "chat-prod", testProfile())
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, 2)
	for range 2 {
		call, err := adapter.Compile(context.Background(), provider.CompileInput{
			Request: llm.Request{
				OperationKey: "op-name", Model: "chat-model",
				Input:  []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "extract"}}}},
				Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Strict: true, Schema: json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}`)}},
			},
			Query:  provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"},
			Strict: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := adapter.Invoke(context.Background(), call, nil); err != nil {
			t.Fatal(err)
		}
		var body struct {
			ResponseFormat struct {
				Type       string `json:"type"`
				JSONSchema struct {
					Name *string `json:"name"`
				} `json:"json_schema"`
			} `json:"response_format"`
		}
		if err := json.Unmarshal(wire, &body); err != nil {
			t.Fatalf("wire body %s: %v", wire, err)
		}
		if body.ResponseFormat.Type != "json_schema" || body.ResponseFormat.JSONSchema.Name == nil || !providerSchemaNamePattern.MatchString(*body.ResponseFormat.JSONSchema.Name) {
			t.Fatalf("wire response_format = %s", wire)
		}
		names = append(names, *body.ResponseFormat.JSONSchema.Name)
	}
	if names[0] != names[1] {
		t.Fatalf("default name is not stable: %q", names)
	}
}
