package provider

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestNewCallCompletesMetadataAndCarriesOutputSchema(t *testing.T) {
	schema := []byte(`{"type":"object"}`)
	normalized := llm.Request{
		Model:        "model-a",
		OperationKey: "op-1",
		Output:       &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Schema: schema}},
	}
	params := map[string]any{"model": "model-a"}
	call, err := NewCall("endpoint-1", FamilyOpenAIChat, normalized, llm.ServiceClassStandard, params, CallMetadata{WebSearch: true}, "caps/v1", "default")
	testassert.NoError(t, err)
	if call.EndpointID != "endpoint-1" || call.Family != FamilyOpenAIChat || call.Model != "model-a" || call.OperationKey != "op-1" || call.ServiceClass != llm.ServiceClassStandard {
		t.Fatalf("call identity = %+v", call)
	}
	if got, ok := call.SDKParams.(map[string]any); !ok || got["model"] != "model-a" {
		t.Fatalf("SDKParams = %#v", call.SDKParams)
	}
	if string(call.OutputSchema) != string(schema) {
		t.Fatalf("OutputSchema = %s", call.OutputSchema)
	}
	want, err := CompileMetadata(CallMetadata{WebSearch: true}, normalized, "caps/v1", "default")
	testassert.NoError(t, err)
	if call.Metadata != want || !call.Metadata.WebSearch || call.Metadata.ProviderTier != "default" {
		t.Fatalf("Metadata = %+v, want %+v", call.Metadata, want)
	}
}
