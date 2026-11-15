package schemawire

import (
	"encoding/json"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestIsObjectSchema(t *testing.T) {
	for _, test := range []struct {
		schema map[string]any
		want   bool
	}{
		{schema: map[string]any{"properties": map[string]any{}}, want: true},
		{schema: map[string]any{"type": "object"}, want: true},
		{schema: map[string]any{"type": []any{"null", "object"}}, want: true},
		{schema: map[string]any{"type": "string"}},
		{schema: map[string]any{}},
	} {
		if got := IsObjectSchema(test.schema); got != test.want {
			t.Errorf("IsObjectSchema(%v) = %t, want %t", test.schema, got, test.want)
		}
	}
}

func TestDescribeExtrasIsSortedAndUnescaped(t *testing.T) {
	got, err := DescribeExtras(map[string]any{"pattern": "^<a>$", "maxLength": json.Number("5"), "enum": []any{"x"}})
	if want := `{enum: ["x"], maxLength: 5, pattern: ^<a>$}`; err != nil || got != want {
		t.Fatalf("DescribeExtras() = %q, %v; want %q", got, err, want)
	}
}

func TestPointerHelpers(t *testing.T) {
	if got := PointerJoin("/properties", "a/b~c"); got != "/properties/a~1b~0c" {
		t.Fatalf("PointerJoin() = %q", got)
	}
	if DisplayPath("") != "/" || DisplayPath("/items") != "/items" {
		t.Fatal("DisplayPath() did not render the root as /")
	}
}

func TestFirstModelText(t *testing.T) {
	output := []llm.Item{
		llm.HumanText("user"),
		llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.RefusalPart{}}},
		llm.ModelText("model"),
	}
	if got, ok := FirstModelText(output); !ok || got != "model" {
		t.Fatalf("FirstModelText() = %q, %t", got, ok)
	}
	if _, ok := FirstModelText(nil); ok {
		t.Fatal("FirstModelText(nil) reported text")
	}
}
