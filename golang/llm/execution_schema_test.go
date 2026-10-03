package llm_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

func TestExecutionSchemaMatchesCodecs(t *testing.T) {
	compiler := jsonschema.NewCompiler()
	documents := map[string]string{}
	for _, name := range []string{"generate-request", "compact-request", "generate-response", "compact-response", "execution-reference", "prepare-execution", "execution-result"} {
		data, err := os.ReadFile(filepath.Join("..", "api", "schema", "v1", name+".schema.json"))
		if err != nil {
			t.Fatal(err)
		}
		var document map[string]any
		if err := json.Unmarshal(data, &document); err != nil {
			t.Fatal(err)
		}
		id := document["$id"].(string)
		documents[name] = id
		if err := compiler.AddResource(id, document); err != nil {
			t.Fatal(err)
		}
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
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range test.values {
			t.Run(test.name+"/"+value, func(t *testing.T) {
				var instance any
				if err := json.Unmarshal([]byte(value), &instance); err != nil {
					t.Fatal(err)
				}
				schemaErr := compiled.Validate(instance)
				codecErr := json.Unmarshal([]byte(value), test.target())
				if (schemaErr == nil) != (codecErr == nil) {
					t.Fatalf("schema=%v codec=%v", schemaErr, codecErr)
				}
			})
		}
	}
}
