package llm

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// The published schema requires mode and parallel on tool_policy. The v1
// settings patch must reject a partial object instead of completing it with
// defaults, while the provider-neutral request model keeps its defaults.
func TestV1ToolPolicyPatchRequiresModeAndParallel(t *testing.T) {
	base, err := os.ReadFile("testdata/v1/generate-fork-patch-set.json")
	if err != nil {
		t.Fatal(err)
	}
	const complete = `"tool_policy": {"set": {"mode": "named", "name": "lookup", "parallel": false}}`
	if !strings.Contains(string(base), complete) {
		t.Fatalf("fixture does not contain %q", complete)
	}
	for name, set := range map[string]string{
		"empty object":     `{}`,
		"missing parallel": `{"mode": "auto"}`,
		"missing mode":     `{"parallel": true}`,
	} {
		t.Run(name, func(t *testing.T) {
			mutated := strings.Replace(string(base), complete, `"tool_policy": {"set": `+set+`}`, 1)
			var request GenerateRequestV1
			err := request.UnmarshalJSON([]byte(mutated))
			if err == nil || !strings.Contains(err.Error(), "tool_policy.set") {
				t.Fatalf("decode of tool_policy.set %s error = %v", set, err)
			}
		})
	}
	for name, set := range map[string]string{
		"auto":  `{"mode": "auto", "parallel": true}`,
		"named": `{"mode": "named", "name": "lookup", "parallel": false}`,
	} {
		t.Run("complete "+name, func(t *testing.T) {
			mutated := strings.Replace(string(base), complete, `"tool_policy": {"set": `+set+`}`, 1)
			var request GenerateRequestV1
			if err := request.UnmarshalJSON([]byte(mutated)); err != nil {
				t.Fatalf("decode of complete tool_policy.set %s: %v", set, err)
			}
		})
	}

	// The encoder always writes both fields, so every patch this module
	// stored, including one with a zero-value policy, still decodes.
	encoded, err := json.Marshal(SettingsPatchV1{ToolPolicy: Patch[ToolPolicy]{Set: &ToolPolicy{}}})
	if err != nil {
		t.Fatal(err)
	}
	var stored SettingsPatchV1
	if err := json.Unmarshal(encoded, &stored); err != nil {
		t.Fatalf("stored patch %s: %v", encoded, err)
	}
	if stored.ToolPolicy.Set == nil || *stored.ToolPolicy.Set != (ToolPolicy{Mode: ToolChoiceAuto}) {
		t.Fatalf("stored patch = %#v", stored.ToolPolicy)
	}

	// The provider-neutral request keeps its defaults for omitted fields.
	for _, policy := range []string{`{}`, `{"mode": "required"}`, `{"parallel": true}`} {
		var request Request
		if err := json.Unmarshal([]byte(`{"api_version": "`+APIVersion+`", "operation_key": "op", "model": "m", "tool_policy": `+policy+`}`), &request); err != nil {
			t.Fatalf("request tool_policy %s: %v", policy, err)
		}
	}
}
