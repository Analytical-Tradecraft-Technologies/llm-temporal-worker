package bedrockmessages

import (
	"encoding/json"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"testing"
)

func TestNoToolsPolicyAllowsCompactionRequest(t *testing.T) {
	request := llm.Request{Model: "contract-model", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "Summarize this conversation"}}}}, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNone}}
	params, err := lowerRequest(request, mustBedrockProfile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"tools", "tool_choice"} {
		if _, exists := wire[key]; exists {
			t.Fatalf("unexpected %s in %s", key, raw)
		}
	}
}
