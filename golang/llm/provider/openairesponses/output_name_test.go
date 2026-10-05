package openairesponses

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

// The provider requires text.format.name and restricts it to this pattern
// (openai-go responses.ResponseFormatTextJSONSchemaConfigParam.Name).
var providerSchemaNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func TestJSONSchemaOutputWithoutNameSendsValidDefaultName(t *testing.T) {
	responseBody := `{"id":"resp-name","object":"response","created_at":1710000000,"model":"gpt-contract","status":"completed","service_tier":"default",` +
		`"output":[{"id":"msg-1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"{\"answer\":\"ok\"}","annotations":[]}]}],` +
		`"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}`
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
	adapter, err := New(client, "openai-prod", "cap-test")
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, 2)
	for range 2 {
		call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: llm.Request{
			OperationKey: "op-name", Model: "gpt-contract",
			Input:  []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "extract"}}}},
			Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Strict: true, Schema: json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}`)}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := adapter.Invoke(context.Background(), call, nil); err != nil {
			t.Fatal(err)
		}
		var body struct {
			Text struct {
				Format struct {
					Type string  `json:"type"`
					Name *string `json:"name"`
				} `json:"format"`
			} `json:"text"`
		}
		if err := json.Unmarshal(wire, &body); err != nil {
			t.Fatalf("wire body %s: %v", wire, err)
		}
		if body.Text.Format.Type != "json_schema" || body.Text.Format.Name == nil || !providerSchemaNamePattern.MatchString(*body.Text.Format.Name) {
			t.Fatalf("wire text.format = %s", wire)
		}
		names = append(names, *body.Text.Format.Name)
	}
	if names[0] != names[1] {
		t.Fatalf("default name is not stable: %q", names)
	}
}
