package llm

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
)

// The published schema requires mode and parallel on tool_policy. The v1
// settings patch must reject a partial object instead of completing it with
// defaults, while the provider-neutral request model keeps its defaults.
func TestV1ToolPolicyPatchRequiresModeAndParallel(t *testing.T) {
	const complete = `"tool_policy": {"set": {"mode": "named", "name": "lookup", "parallel": false}}`
	base := readFixtureContaining(t, "testdata/v1/generate-fork-patch-set.json", complete)
	for name, set := range map[string]string{
		"empty object":     `{}`,
		"missing parallel": `{"mode": "auto"}`,
		"missing mode":     `{"parallel": true}`,
	} {
		t.Run(name, func(t *testing.T) {
			mutated := strings.Replace(base, complete, `"tool_policy": {"set": `+set+`}`, 1)
			var request GenerateRequestV1
			err := request.UnmarshalJSON([]byte(mutated))
			testassert.ErrorContains(t, err, "tool_policy.set", fmt.Sprintf("decode of tool_policy.set %s", set))
		})
	}
	for name, set := range map[string]string{
		"auto":  `{"mode": "auto", "parallel": true}`,
		"named": `{"mode": "named", "name": "lookup", "parallel": false}`,
	} {
		t.Run("complete "+name, func(t *testing.T) {
			mutated := strings.Replace(base, complete, `"tool_policy": {"set": `+set+`}`, 1)
			var request GenerateRequestV1
			if err := request.UnmarshalJSON([]byte(mutated)); err != nil {
				t.Fatalf("decode of complete tool_policy.set %s: %v", set, err)
			}
		})
	}

	// The encoder always writes both fields, so every patch this module
	// stored, including one with a zero-value policy, still decodes.
	encoded := testassert.MarshalJSON(t, SettingsPatchV1{ToolPolicy: Patch[ToolPolicy]{Set: &ToolPolicy{}}})
	stored := testassert.UnmarshalJSON[SettingsPatchV1](t, encoded, "stored patch "+string(encoded))
	if stored.ToolPolicy.Set == nil || *stored.ToolPolicy.Set != (ToolPolicy{Mode: ToolChoiceAuto}) {
		t.Fatalf("stored patch = %#v", stored.ToolPolicy)
	}

	// The provider-neutral request keeps its defaults for omitted fields.
	for _, policy := range []string{`{}`, `{"mode": "required"}`, `{"parallel": true}`} {
		testassert.UnmarshalJSON[Request](t, []byte(`{"api_version": "`+APIVersion+`", "operation_key": "op", "model": "m", "tool_policy": `+policy+`}`), "request tool_policy "+policy)
	}
}
