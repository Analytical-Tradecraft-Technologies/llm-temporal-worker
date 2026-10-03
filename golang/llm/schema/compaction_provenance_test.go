package schema_test

import (
	"encoding/json"
	"testing"
)

func TestCloudCompactionProvenanceSchema(t *testing.T) {
	compiled := readV1Schema(t, "compact-response.schema.json")
	var response map[string]json.RawMessage
	if err := json.Unmarshal(readV1Fixture(t, "compact-response.json"), &response); err != nil {
		t.Fatal(err)
	}
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
			if err != nil {
				t.Fatal(err)
			}
			if err := compiled.Validate(raw); (err == nil) != test.valid {
				t.Fatalf("valid=%t: %v", test.valid, err)
			}
		})
	}
}
