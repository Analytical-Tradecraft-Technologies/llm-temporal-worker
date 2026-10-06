package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

// publishedExecutionResultSchema compiles execution-result.schema.json with
// the Generate and Compact response schemas it references.
func publishedExecutionResultSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	compiler := jsonschema.NewCompiler()
	var target string
	for _, name := range []string{"generate-response", "compact-response", "execution-result"} {
		data, err := os.ReadFile(filepath.Join("..", "..", "api", "schema", "v1", name+".schema.json"))
		if err != nil {
			t.Fatal(err)
		}
		document, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		target = document.(map[string]any)["$id"].(string)
		if err := compiler.AddResource(target, document); err != nil {
			t.Fatal(err)
		}
	}
	compiled, err := compiler.Compile(target)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func assertPublishedExecutionResult(t *testing.T, compiled *jsonschema.Schema, result llm.ExecutionResultV1) {
	t.Helper()
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	if err := compiled.Validate(instance); err != nil {
		t.Fatalf("published schema rejects the worker's result: %v\n%s", err, encoded)
	}
}

// The runtime publishes adapter output, usage, cost and diagnostics unfiltered,
// so the records adapters really produce must satisfy the published schemas.
func TestCloudExecutionRuntimePublishesSchemaValidAdapterRecords(t *testing.T) {
	compiled := publishedExecutionResultSchema(t)
	f := boundedCloud(t, false)
	policy := json.RawMessage(`{"recent_turns":0}`)
	f.request.SettingsPatch.CompactionPolicy.Set = &policy
	original := f.adapter.invoke
	generate := true
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		v, err := original(ctx, call, o)
		v.Response.Usage.ProviderRaw = map[string]json.RawMessage{"total_tokens": json.RawMessage(`15`)}
		v.Response.Diagnostics = []llm.Diagnostic{{Code: "transform_applied", Severity: llm.DiagnosticWarning, Path: "/instructions", Message: "flattened", Details: map[string]string{"levels": "2"}}}
		if generate {
			// Reasoning state, a typed refusal and a citation, as the OpenAI,
			// Anthropic, Bedrock and Exa lifts emit them.
			v.Response.Output = []llm.Item{
				llm.ProviderState{Provider: "openai", EndpointFamily: "openai_responses", MediaType: "application/json", Opaque: []byte(`{"type":"reasoning"}`)},
				llm.Message{Actor: llm.ActorModel, Content: []llm.Part{
					llm.ProviderStatePart{Provider: "anthropic", EndpointFamily: "anthropic_messages", MediaType: "application/json", Opaque: []byte(`{"type":"thinking"}`)},
					llm.TextPart{Text: "answer"},
					llm.RefusalPart{Text: "no", ProviderCode: "openai.refusal"},
				}},
				llm.Reference{URI: "https://example.com/source", Metadata: map[string]json.RawMessage{"title": json.RawMessage(`"Source"`)}},
			}
		}
		return v, err
	}
	parent := f.finish(t)
	if parent.Generate == nil || len(parent.Generate.Output) != 3 || parent.Generate.Usage == nil || len(parent.Generate.Usage.ProviderRaw) == 0 || len(parent.Generate.Diagnostics) != 1 {
		t.Fatalf("adapter record was not published unfiltered: %+v", parent.Generate)
	}
	assertPublishedExecutionResult(t, compiled, parent)

	generate = false
	f.now = f.now.Add(time.Minute)
	ctx := context.Background()
	request := llm.CompactRequestV1{OperationKey: "compact", Context: f.request.Context, Parent: parent.Generate.Checkpoint.Handle}
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Compact: &request})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: request.Context}
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionAcquired)
	v, err = f.runtime.CompactStepV1(ctx, request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	compacted := boundedState(t, v, err, llm.ExecutionCompleted)
	if compacted.Compact == nil || compacted.Compact.Cost.CatalogVersion == "" || compacted.Compact.Usage == nil || len(compacted.Compact.Usage.ProviderRaw) == 0 || len(compacted.Compact.Diagnostics) != 1 {
		t.Fatalf("priced compaction record was not published unfiltered: %+v", compacted.Compact)
	}
	assertPublishedExecutionResult(t, compiled, compacted)
}
