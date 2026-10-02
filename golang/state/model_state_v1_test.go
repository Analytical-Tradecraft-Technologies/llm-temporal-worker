package state

import (
	"encoding/json"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func TestApplySettingsPatchV1PreservesCanonicalDecimalAndDetachedCollections(t *testing.T) {
	base := RootModelState("model")
	base.Instructions = []llm.Instruction{{Text: "inherited"}}
	decimal := llm.DecimalV1("0.100000000000000010")
	tools := []llm.Tool{{Name: "lookup", InputSchema: json.RawMessage(`{"type":"object"}`)}}
	wire := llm.SettingsPatchV1{Temperature: llm.Patch[llm.DecimalV1]{Set: &decimal}, Tools: llm.Patch[[]llm.Tool]{Set: &tools}}
	got, err := ApplySettingsPatchV1(base, wire)
	if err != nil {
		t.Fatal(err)
	}
	if got.TemperatureDecimal.String() != "0.10000000000000001" || *got.Temperature != 0.1 || decimal.String() != "0.100000000000000010" {
		t.Fatal("lost exact/canonical decimal or changed the caller's patch")
	}
	got.Tools[0].InputSchema[0] = '['
	got.Instructions[0].Text = "changed"
	if tools[0].InputSchema[0] != '{' || base.Instructions[0].Text != "inherited" {
		t.Fatal("result aliases patch/base")
	}
	got, err = ApplySettingsPatchV1(got, llm.SettingsPatchV1{Temperature: llm.Patch[llm.DecimalV1]{Clear: true}, Tools: llm.Patch[[]llm.Tool]{Clear: true}, Instructions: llm.Patch[[]llm.Instruction]{Clear: true}})
	if err != nil || got.Temperature != nil || got.TemperatureDecimal != nil || len(got.Tools) != 0 || len(got.Instructions) != 0 || got.Model != "model" {
		t.Fatalf("clear = %+v, %v", got, err)
	}
}

func TestApplySettingsPatchV1RejectsInvalidWireAndEffectiveSettings(t *testing.T) {
	badClass := llm.ServiceClass("sensitive-class")
	model := "new-model"
	tests := []llm.SettingsPatchV1{
		{Model: llm.Patch[string]{Set: &model, Clear: true}},
		{ServiceClass: llm.Patch[llm.ServiceClass]{Set: &badClass}},
		{Model: llm.Patch[string]{Clear: true}},
	}
	for _, wire := range tests {
		if _, err := ApplySettingsPatchV1(RootModelState("model"), wire); err == nil {
			t.Fatal("invalid patch was accepted")
		}
	}
}
