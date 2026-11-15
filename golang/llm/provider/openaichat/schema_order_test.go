package openaichat

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

// sendSchemaRequest invokes a request with one "lookup" tool taking
// toolSchema and a strict outputSchema, and returns the request body sent.
func sendSchemaRequest(t *testing.T, toolSchema, outputSchema string) []byte {
	t.Helper()
	var sent []byte
	client, err := NewClient(ClientConfig{BaseURL: "https://127.0.0.1/v1", APIKey: "test-key",
		HTTPClient: &http.Client{Transport: testkit.RoundTripFunc(func(request *http.Request) (*http.Response, error) {
			sent, _ = io.ReadAll(request.Body)
			return adaptertest.JSONResponse(request, http.StatusOK, `{"id":"chatcmpl-1","object":"chat.completion","created":1700000000,"model":"chat-model","choices":[]}`), nil
		})}})
	testassert.NoError(t, err)
	adapter := testAdapter(t)
	adapter.client = client
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "op-order", Model: "chat-model",
			Input:  []llm.Item{llm.HumanText("hi")},
			Tools:  []llm.Tool{{Name: "lookup", InputSchema: json.RawMessage(toolSchema)}},
			Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Strict: true, Schema: json.RawMessage(outputSchema)}}},
		Query: provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"},
	})
	testassert.NoError(t, err)
	// The canned response is empty; only the request matters.
	_, _ = adapter.Invoke(context.Background(), call, provider.NopObserver{})
	return sent
}

// The response-format schema and tool parameters reach the provider with
// properties in "required" order (#1096).
func TestInvokeSendsSchemaPropertiesInRequiredOrder(t *testing.T) {
	adaptertest.PropertiesInRequiredOrder(t, sendSchemaRequest(t, adaptertest.OutOfOrderToolSchema, adaptertest.OutOfOrderOutputSchema))
}

func TestInvokePreservesExactSchemaNumbers(t *testing.T) {
	sent := sendSchemaRequest(t, adaptertest.ExactNumberToolSchema, adaptertest.ExactNumberOutputSchema)
	var requestBody struct {
		ResponseFormat struct {
			JSONSchema struct {
				Schema json.RawMessage `json:"schema"`
			} `json:"json_schema"`
		} `json:"response_format"`
		Tools []struct {
			Function struct {
				Parameters json.RawMessage `json:"parameters"`
			} `json:"function"`
		} `json:"tools"`
	}
	testassert.NoError(t, json.Unmarshal(sent, &requestBody))
	if len(requestBody.Tools) != 1 {
		t.Fatalf("tool schema missing: %s", sent)
	}
	adaptertest.ExactSchemaNumbers(t, requestBody.ResponseFormat.JSONSchema.Schema, requestBody.Tools[0].Function.Parameters)
	adaptertest.PropertiesInRequiredOrder(t, sent)
}
