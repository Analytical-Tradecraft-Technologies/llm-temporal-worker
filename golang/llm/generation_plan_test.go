package llm

import (
	"encoding/json"
	"testing"
)

func TestGenerationPlanClosedWire(t *testing.T) {
	for _, value := range []bool{false, true} {
		plan := GenerationPlanV1{CompactBeforeGenerate: value}
		data, err := json.Marshal(plan)
		if err != nil {
			t.Fatal(err)
		}
		var decoded GenerationPlanV1
		if err := json.Unmarshal(data, &decoded); err != nil || decoded != plan {
			t.Fatalf("round trip: %v", err)
		}
	}
	for _, data := range []string{`null`, `[]`, `{}`, `{"compact_before_generate":null}`, `{"compact_before_generate":1}`, `{"compact_before_generate":false,"transcript":"private"}`, `{"compact_before_generate":false,"compact_before_generate":true}`} {
		plan := GenerationPlanV1{CompactBeforeGenerate: true}
		if err := json.Unmarshal([]byte(data), &plan); err == nil || !plan.CompactBeforeGenerate {
			t.Fatalf("accepted or mutated invalid plan %s", data)
		}
	}
}
