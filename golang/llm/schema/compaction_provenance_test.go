package schema_test

import (
	"encoding/json"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"testing"
)

func TestCloudCompactionProvenanceSchema(t *testing.T) {
	compiled := readV1Schema(t, "compact-response.schema.json")
	var response map[string]json.RawMessage
	testassert.NoError(t, json.Unmarshal(readV1Fixture(t, "compact-response.json"), &response))
	for _, test := range []struct {
		provenance string
		valid      bool
	}{
		{`{"source":"provider","policy_version":"policy-v1","prompt_version":"prompt-v1"}`, true},
		{`{"source":"worker_cache","policy_version":"policy-v1","prompt_version":"prompt-v1"}`, true},
		{`{"source":"no_work","policy_version":"policy-v1","prompt_version":"prompt-v1"}`, true},
		{`{"source":"other","policy_version":"policy-v1","prompt_version":"prompt-v1"}`, false},
		{`{"source":"provider","policy_version":1}`, false},
		{`{"source":"provider","prompt_version":1}`, false},
		{`{"source":"provider","extra":true}`, false},
	} {
		t.Run(test.provenance, func(t *testing.T) {
			response["provenance"] = json.RawMessage(test.provenance)
			raw, err := json.Marshal(response)
			testassert.NoError(t, err)
			if err := compiled.Validate(raw); (err == nil) != test.valid {
				t.Fatalf("valid=%t: %v", test.valid, err)
			}
		})
	}
}
