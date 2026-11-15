package contract

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestAssertCanonicalJSONIgnoresKeyOrderAndWhitespace(t *testing.T) {
	AssertCanonicalJSON(t, []byte(`{"b":1,"a":[true,null]}`), []byte("{\n  \"a\": [true, null],\n  \"b\": 1\n}"), "reordered")
}

func TestNormalizeRequestDefaultsTheServiceClass(t *testing.T) {
	normalized, serviceClass := NormalizeRequest(t, llm.Request{OperationKey: "op", Model: "model"})
	want, err := llm.NormalizeServiceClass("")
	testassert.NoError(t, err)
	if normalized.Model != "model" || serviceClass != want {
		t.Fatalf("normalized = %#v, service class %q, want model and %q", normalized, serviceClass, want)
	}
}

func TestClassFactsDecodesWireFactsAndIndexesSemanticActuals(t *testing.T) {
	facts, actualByClass := ClassFacts(t,
		[]byte(`[{"class":"standard","actual":"standard"},{"class":"priority","actual":"standard"}]`),
		[]byte(`[{"class":"priority","supported":true,"requested_tier":"priority","actual_tier":"default","priority_capacity":false},{"class":"economy","supported":false}]`))
	if len(facts) != 2 || facts[0].Class != llm.ServiceClassPriority || !facts[0].Supported || facts[0].RequestedTier != "priority" || facts[0].ActualTier != "default" || facts[0].PriorityCapacity == nil || *facts[0].PriorityCapacity {
		t.Fatalf("wire facts = %#v", facts)
	}
	if facts[1].Supported || facts[1].PriorityCapacity != nil {
		t.Fatalf("unsupported fact = %#v", facts[1])
	}
	if len(actualByClass) != 2 || actualByClass[llm.ServiceClassPriority] != llm.ServiceClassStandard || actualByClass[llm.ServiceClassStandard] != llm.ServiceClassStandard {
		t.Fatalf("actual by class = %#v", actualByClass)
	}
}
