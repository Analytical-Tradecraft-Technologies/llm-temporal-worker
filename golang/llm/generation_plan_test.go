package llm

import (
	"encoding/json"
	"reflect"
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

func TestGenerationPlanEffectiveParentWire(t *testing.T) {
	parent := CheckpointHandle("ckp_v1.saved")
	want := GenerationPlanV1{CompactBeforeGenerate: true, EffectiveParent: &parent}
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got GenerationPlanV1
	if err := json.Unmarshal(data, &got); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("bound plan round trip: %+v %v", got, err)
	}
	for _, input := range []string{`{"compact_before_generate":true,"effective_parent":null}`, `{"compact_before_generate":true,"effective_parent":""}`, `{"compact_before_generate":true,"effective_parent":1}`} {
		got = want
		if err := json.Unmarshal([]byte(input), &got); err == nil || !reflect.DeepEqual(got, want) {
			t.Fatal("invalid effective parent accepted or mutated receiver")
		}
	}
}

func TestGenerationResolutionClosedWire(t *testing.T) {
	plan := GenerationResolutionV1{}
	data, err := json.Marshal(plan)
	if err != nil || string(data) != `{"plan":{"compact_before_generate":false}}` {
		t.Fatalf("plan-only resolution = %s, %v", data, err)
	}
	var decoded GenerationResolutionV1
	if err := json.Unmarshal(data, &decoded); err != nil || !reflect.DeepEqual(decoded, plan) {
		t.Fatalf("round trip: %v", err)
	}
	pending := ExecutionResultV1{RequestID: "llmtw_req_49c0cb63-15ed-4da7-aa74-17a7d711c9a5", Kind: "generate", State: ExecutionBudgetRequired}
	parent := CheckpointHandle("ckp_v1.saved")
	for name, invalid := range map[string]GenerationResolutionV1{
		"pending result":         {Result: &pending},
		"result with compaction": {Plan: GenerationPlanV1{CompactBeforeGenerate: true, EffectiveParent: &parent}, Result: &pending},
	} {
		if _, err := json.Marshal(invalid); err == nil {
			t.Errorf("%s marshalled", name)
		}
	}
	for _, input := range []string{`null`, `{}`, `{"plan":null}`, `{"plan":{"compact_before_generate":false},"result":null}`, `{"plan":{"compact_before_generate":false},"extra":1}`,
		`{"plan":{"compact_before_generate":false},"result":{"request_id":"llmtw_req_49c0cb63-15ed-4da7-aa74-17a7d711c9a5","kind":"generate","state":"budget_required"}}`} {
		decoded = plan
		if err := json.Unmarshal([]byte(input), &decoded); err == nil {
			t.Errorf("accepted %s", input)
		}
	}
}
