package anthropicmessages

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/messagestest"
)

// structuredOutputAdapter answers every request with one text block and
// records the request body the SDK put on the wire.
func structuredOutputAdapter(t *testing.T, text string, wire *[]byte) *Adapter {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"id": "msg_structured", "type": "message", "role": "assistant", "model": "claude-contract",
		"content": []any{map[string]any{"type": "text", "text": text}}, "stop_reason": "end_turn",
		"usage": map[string]any{"input_tokens": 2, "output_tokens": 1, "service_tier": "standard"},
	})
	testassert.NoError(t, err)
	client, err := NewClient(ClientConfig{
		BaseURL: "http://127.0.0.1/contract",
		APIKey:  "test-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			*wire, _ = io.ReadAll(request.Body)
			return adaptertest.JSONResponse(request, http.StatusOK, string(body), "Request-Id", "req-structured"), nil
		})},
	})
	testassert.NoError(t, err)
	profile := testProfile()
	profile.ExpectedBaseURL = "http://127.0.0.1/contract"
	adapter, err := New(client, "anthropic-prod", profile)
	testassert.NoError(t, err)
	return adapter
}

func structuredOutputInput(schema string, strict bool) provider.CompileInput {
	return provider.CompileInput{
		Request: llm.Request{
			OperationKey: "structured", Model: "claude-contract",
			Input:  []llm.Item{llm.HumanText("extract")},
			Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Strict: true, Schema: json.RawMessage(schema)}},
		},
		Query:  provider.CapabilityQuery{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
		Strict: strict,
	}
}

func TestStructuredOutputSendsProviderValidSchemaAndValidatesCallerSchema(t *testing.T) {
	messagestest.StructuredOutputSendsProviderValidSchemaAndValidatesCallerSchema(t, newStructuredOutputAdapter, structuredOutputInput)
}

func TestStructuredOutputRejectsUnrepresentableSchemasAtCompile(t *testing.T) {
	messagestest.StructuredOutputRejectsUnrepresentableSchemasAtCompile(t, newStructuredOutputAdapter, structuredOutputInput)
}

func TestStructuredOutputRejectsExponentialSchemaAtCompile(t *testing.T) {
	messagestest.StructuredOutputRejectsExponentialSchemaAtCompile(t, newStructuredOutputAdapter, structuredOutputInput)
}

func newStructuredOutputAdapter(t *testing.T, text string, wire *[]byte) messagestest.StructuredOutputAdapter {
	return structuredOutputAdapter(t, text, wire)
}
