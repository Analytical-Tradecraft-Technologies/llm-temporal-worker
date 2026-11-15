package anthropicwire

import (
	"encoding/json"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"strings"
	"testing"
)

func TestFamilyParamsKeepsTheWireBodyAndNamesTheFamily(t *testing.T) {
	body := map[string]any{"model": "claude-test", "max_tokens": 64, "messages": []any{}, "unlisted_field": "kept"}
	params, err := Bedrock.Params(body)
	testassert.NoError(t, err)
	if params.Model != "claude-test" || params.MaxTokens != 64 {
		t.Fatalf("typed params = %q, %d", params.Model, params.MaxTokens)
	}
	encoded, err := json.Marshal(params)
	testassert.NoError(t, err)
	if !strings.Contains(string(encoded), `"unlisted_field":"kept"`) {
		t.Fatalf("params override lost the wire body: %s", encoded)
	}

	body["thinking"] = map[string]any{"type": "enabled", "budget_tokens": 64}
	for _, family := range []Family{Anthropic, Bedrock} {
		if _, err := family.Params(body); err == nil || !strings.HasPrefix(err.Error(), family.Name+" thinking token_budget") {
			t.Fatalf("%s thinking error = %v", family.Name, err)
		}
	}
}
