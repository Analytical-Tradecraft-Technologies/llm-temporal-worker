package llm

import (
	"os"
	"strings"
	"testing"
)

func TestGenerateRequestV1RejectsLooseToolPolicyAndNullScalars(t *testing.T) {
	base, err := os.ReadFile("testdata/v1/generate-fork-patch-set.json")
	if err != nil {
		t.Fatal(err)
	}
	var control GenerateRequestV1
	if err := control.UnmarshalJSON(base); err != nil {
		t.Fatalf("control fixture rejected: %v", err)
	}
	for name, replace := range map[string][2]string{
		"tool policy unknown field": {`"tool_policy": {"set": {"mode": "named", "name": "lookup", "parallel": false}}`, `"tool_policy": {"set": {"mode": "auto", "parallel": true, "surprise": {}}}`},
		"tool policy case folding":  {`"tool_policy": {"set": {"mode": "named", "name": "lookup", "parallel": false}}`, `"tool_policy": {"set": {"MODE": "none", "Parallel": false}}`},
		"tool policy null":          {`"tool_policy": {"set": {"mode": "named", "name": "lookup", "parallel": false}}`, `"tool_policy": {"set": null}`},
		"max tokens null":           {`"max_tokens": 64`, `"max_tokens": null`},
		"instructions null":         {`"instructions": {"set": [{"kind": "text", "text": "Follow policy."}]}`, `"instructions": {"set": null}`},
		"tools null":                {`"tools": {"set": [{"name": "lookup", "description": "Lookup", "input_schema": {"type": "object"}}]}`, `"tools": {"set": null}`},
		"cache variant null":        {`"variant": 2147483647`, `"variant": null`},
		"text null":                 {`{"kind": "text", "text": "Follow policy."}`, `{"kind": "text", "text": null}`},
	} {
		t.Run(name, func(t *testing.T) {
			mutated := strings.Replace(string(base), replace[0], replace[1], 1)
			if mutated == string(base) {
				t.Fatalf("fixture does not contain %q", replace[0])
			}
			var request GenerateRequestV1
			if err := request.UnmarshalJSON([]byte(mutated)); err == nil {
				t.Fatalf("decoder accepted %s", replace[1])
			}
		})
	}
}
