package contract

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestAssertCanonicalJSONIgnoresKeyOrderAndWhitespace(t *testing.T) {
	AssertCanonicalJSON(t, []byte(`{"b":1,"a":[true,null]}`), []byte("{\n  \"a\": [true, null],\n  \"b\": 1\n}"), "reordered")
}

func TestDecodeJSONRoundTripsMarshalJSON(t *testing.T) {
	want := map[string]int{"a": 1, "b": 2}
	got := DecodeJSON[map[string]int](t, MarshalJSON(t, want))
	if len(got) != 2 || got["a"] != 1 || got["b"] != 2 {
		t.Fatalf("decoded = %#v, want %#v", got, want)
	}
}

func TestNormalizeRequestDefaultsTheServiceClass(t *testing.T) {
	normalized, serviceClass := NormalizeRequest(t, llm.Request{OperationKey: "op", Model: "model"})
	want, err := llm.NormalizeServiceClass("")
	if err != nil {
		t.Fatal(err)
	}
	if normalized.Model != "model" || serviceClass != want {
		t.Fatalf("normalized = %#v, service class %q, want model and %q", normalized, serviceClass, want)
	}
}
