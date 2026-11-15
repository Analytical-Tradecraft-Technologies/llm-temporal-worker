package llm_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

func TestExecutionSchemaMatchesCodecs(t *testing.T) {
	compiler := jsonschema.NewCompiler()
	documents := map[string]string{}
	for _, name := range []string{"generate-request", "compact-request", "generate-response", "compact-response", "execution-reference", "prepare-execution", "execution-result"} {
		data, err := os.ReadFile(filepath.Join("..", "api", "schema", "v1", name+".schema.json"))
		testassert.NoError(t, err)
		document := testassert.UnmarshalJSON[map[string]any](t, data, "decode "+name+" schema")
		id := document["$id"].(string)
		documents[name] = id
		testassert.NoError(t, compiler.AddResource(id, document))
	}
	cases := []struct {
		name   string
		target func() any
		values []string
	}{
		{"execution-reference", func() any { return new(llm.ExecutionReferenceV1) }, []string{
			`{"request_id":"` + requestID + `","context":{"tenant":"t","project":"p","actor":"a"}}`,
			`{"request_id":"` + requestID + `","context":null}`, `{"request_id":"provider-job-id","context":{"tenant":"t","project":"p","actor":"a"}}`,
		}},
		{"prepare-execution", func() any { return new(llm.PrepareExecutionV1) }, []string{
			`{"generate":` + string(readV1Fixture(t, "generate-root.json")) + `}`, `{"compact":` + string(readV1Fixture(t, "compact-request.json")) + `}`,
			`{"generate":` + string(readV1Fixture(t, "generate-root.json")) + `,"compact":` + string(readV1Fixture(t, "compact-request.json")) + `}`, `{}`, `{"compact":null}`,
			`{"generate":` + string(readV1Fixture(t, "generate-root.json")) + `,"original_generate":{}}`,
			`{"generate":` + string(readV1Fixture(t, "generate-root.json")) + `,"original_generate":{"parent":"ckp_v1.original"}}`,
			`{"generate":` + string(readV1Fixture(t, "generate-root.json")) + `,"original_generate":{"parent":null}}`,
			`{"generate":` + string(readV1Fixture(t, "generate-root.json")) + `,"original_generate":{"parent":""}}`,
			`{"generate":` + string(readV1Fixture(t, "generate-root.json")) + `,"original_generate":{"append":[]}}`,
			`{"compact":` + string(readV1Fixture(t, "compact-request.json")) + `,"original_generate":{}}`,
		}},
		{"execution-result", func() any { return new(llm.ExecutionResultV1) }, []string{
			`{"request_id":"` + requestID + `","kind":"generate","state":"completed","generate":` + string(readV1Fixture(t, "generate-response.json")) + `}`,
			`{"request_id":"` + requestID + `","kind":"compact","state":"completed","compact":` + string(readV1Fixture(t, "compact-response.json")) + `}`,
			`{"request_id":"` + requestID + `","kind":"generate","state":"completed","compact":` + string(readV1Fixture(t, "compact-response.json")) + `}`,
		}},
	}
	for _, state := range []string{"budget_required", "acquired", "budget_wait", "cache_wait", "pending", "provider_completed", "completed", "failed", "outcome_unknown", "unknown"} {
		for _, fields := range []string{"", `,"retry_after_seconds":0`, `,"retry_after_seconds":5`, `,"retry_after_seconds":86401`, `,"retryable":false`, `,"failure_code":"provider_error","retryable":true`, `,"failure_code":"raw-secret"`, `,"provider_job_id":"secret"`, `,"generate":null`} {
			cases[2].values = append(cases[2].values, `{"request_id":"`+requestID+`","kind":"generate","state":"`+state+`"`+fields+`}`)
		}
	}
	for _, test := range cases {
		compiled, err := compiler.Compile(documents[test.name])
		testassert.NoError(t, err)
		for _, value := range test.values {
			t.Run(test.name+"/"+value, func(t *testing.T) {
				schemaErr := compiled.Validate(testassert.UnmarshalJSON[any](t, []byte(value), "decode instance"))
				codecErr := json.Unmarshal([]byte(value), test.target())
				if (schemaErr == nil) != (codecErr == nil) {
					t.Fatalf("schema=%v codec=%v", schemaErr, codecErr)
				}
			})
		}
	}
}
