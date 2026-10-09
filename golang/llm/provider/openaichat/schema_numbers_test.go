package openaichat

import (
	"encoding/json"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestLocalValidationUsesExactOriginalSchemaNumbers(t *testing.T) {
	for _, tc := range []struct{ name, constraint, original, rounded string }{
		{"integer enum", `"enum":[9007199254740993]`, "9007199254740993", "9007199254740992"},
		{"integer const", `"const":9007199254740993`, "9007199254740993", "9007199254740992"},
		{"integer bounds", `"minimum":9007199254740993,"maximum":9007199254740993`, "9007199254740993", "9007199254740992"},
		{"decimal const", `"const":0.100000000000000000001`, "0.100000000000000000001", "0.1"},
		{"decimal bounds", `"minimum":0.100000000000000000001,"maximum":0.100000000000000000001`, "0.100000000000000000001", "0.1"},
		{"exponent enum", `"enum":[1.234567890123456789e+20]`, "1.234567890123456789e+20", "1.2345678901234568e+20"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := json.RawMessage(`{"type":"object","properties":{"id":{"type":"number",` + tc.constraint + `}},"required":["id"],"additionalProperties":false}`)
			params, err := lowerRequest(llm.Request{Model: "chat-model", Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Strict: true, Schema: schema}}}, testProfile(), "default")
			if err != nil {
				t.Fatal(err)
			}
			call := provider.Call{SDKParams: params}
			for _, tc := range []struct {
				text  string
				valid bool
			}{{`{"id":` + tc.original + `}`, true}, {`{"id":` + tc.rounded + `}`, false}} {
				output := []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: tc.text}}}}
				err := validateFinalJSON(call, output, llm.ResponseStatusCompleted, false, false)
				if (err == nil) != tc.valid {
					t.Fatalf("content %s valid=%v: %v", tc.text, tc.valid, err)
				}
			}
		})
	}
}
