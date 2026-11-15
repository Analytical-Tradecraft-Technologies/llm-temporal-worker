package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
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
		data := testassert.ReadFile(t, filepath.Join("..", "..", "api", "schema", "v1", name+".schema.json"))
		document, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
		testassert.NoError(t, err)
		target = document.(map[string]any)["$id"].(string)
		testassert.NoError(t, compiler.AddResource(target, document))
	}
	compiled, err := compiler.Compile(target)
	testassert.NoError(t, err)
	return compiled
}

func assertPublishedExecutionResult(t *testing.T, compiled *jsonschema.Schema, result llm.ExecutionResultV1) {
	t.Helper()
	encoded := testassert.MarshalJSON(t, result)
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
	testassert.NoError(t, err)
	if err := compiled.Validate(instance); err != nil {
		t.Fatalf("published schema rejects the worker's result: %v\n%s", err, encoded)
	}
}

// The runtime publishes adapter output, usage, cost and diagnostics unfiltered,
// so the records adapters really produce must satisfy the published schemas.
func TestCloudExecutionRuntimePublishesSchemaValidAdapterRecords(t *testing.T) {
	compiled := publishedExecutionResultSchema(t)
	f := boundedCloud(t, false)
	keepNoRecentTurns(&f.request)
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
	request := f.compactRequest(parent, nil)
	compacted := f.runCompact(t, ctx, request, false)
	if compacted.Compact == nil || compacted.Compact.Cost.CatalogVersion == "" || compacted.Compact.Usage == nil || len(compacted.Compact.Usage.ProviderRaw) == 0 || len(compacted.Compact.Diagnostics) != 1 {
		t.Fatalf("priced compaction record was not published unfiltered: %+v", compacted.Compact)
	}
	assertPublishedExecutionResult(t, compiled, compacted)
}
