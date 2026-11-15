package openairesponses

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

// sendSchemaRequest invokes a request with one "lookup" tool taking
// toolSchema, and toolOutputSchema when it is set, and a strict
// outputSchema, and returns the request body sent.
func sendSchemaRequest(t *testing.T, toolSchema, toolOutputSchema, outputSchema string) []byte {
	t.Helper()
	responseBody, err := os.ReadFile("testdata/contracts/openai-responses/response.completed.json")
	testassert.NoError(t, err)
	var sent []byte
	adapter := newOpenAIProdAdapter(t, "https://127.0.0.1/contract", &http.Client{Transport: testkit.RoundTripFunc(func(request *http.Request) (*http.Response, error) {
		sent, _ = io.ReadAll(request.Body)
		return adaptertest.JSONResponse(request, http.StatusOK, string(responseBody)), nil
	})})
	tool := llm.Tool{Name: "lookup", InputSchema: json.RawMessage(toolSchema)}
	if toolOutputSchema != "" {
		tool.OutputSchema = json.RawMessage(toolOutputSchema)
	}
	call, err := adaptertest.Compile(adapter, "openai-prod", provider.FamilyOpenAIResponses, llm.Request{OperationKey: "op-order", Model: "gpt-contract",
		Tools:  []llm.Tool{tool},
		Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Strict: true, Schema: json.RawMessage(outputSchema)}}}, false)
	testassert.NoError(t, err)
	// The canned response does not satisfy the schema; only the request matters.
	_, _ = adapter.Invoke(context.Background(), call, &adaptertest.Observer{})
	return sent
}

// The output schema and tool parameters reach the provider with properties in
// "required" order, not the alphabetical order the canonical request form and
// the SDK's map encoding produce (#1096).
func TestInvokeSendsSchemaPropertiesInRequiredOrder(t *testing.T) {
	adaptertest.PropertiesInRequiredOrder(t, sendSchemaRequest(t, adaptertest.OutOfOrderToolSchema, "", adaptertest.OutOfOrderOutputSchema))
}

func TestInvokePreservesExactSchemaNumbers(t *testing.T) {
	sent := sendSchemaRequest(t, adaptertest.ExactNumberToolSchema, adaptertest.ExactNumberOutputSchema, adaptertest.ExactNumberOutputSchema)
	var requestBody struct {
		Text struct {
			Format struct {
				Schema json.RawMessage `json:"schema"`
			} `json:"format"`
		} `json:"text"`
		Tools []struct {
			Parameters   json.RawMessage `json:"parameters"`
			OutputSchema json.RawMessage `json:"output_schema"`
		} `json:"tools"`
	}
	testassert.NoError(t, json.Unmarshal(sent, &requestBody))
	if len(requestBody.Tools) != 1 {
		t.Fatalf("tool schema missing: %s", sent)
	}
	adaptertest.ExactSchemaNumbers(t, requestBody.Text.Format.Schema, requestBody.Tools[0].Parameters, requestBody.Tools[0].OutputSchema)
	adaptertest.PropertiesInRequiredOrder(t, sent)
}
