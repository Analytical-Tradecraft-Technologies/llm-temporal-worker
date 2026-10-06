package llm_test

import (
	"encoding/json"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestServiceClassContractInvariants(t *testing.T) {
	for _, test := range []struct {
		input string
		want  llm.ServiceClass
	}{
		{input: "", want: llm.ServiceClassStandard},
		{input: "economy", want: llm.ServiceClassEconomy},
		{input: "standard", want: llm.ServiceClassStandard},
		{input: "priority", want: llm.ServiceClassPriority},
	} {
		got, err := llm.NormalizeServiceClass(llm.ServiceClass(test.input))
		if err != nil || got != test.want {
			t.Errorf("NormalizeServiceClass(%q) = %q, %v; want %q", test.input, got, err, test.want)
		}
	}
	for _, value := range []llm.ServiceClass{"provider_default", "default", "auto", "priority ", "standard\n"} {
		if _, err := llm.NormalizeServiceClass(value); err == nil {
			t.Errorf("NormalizeServiceClass(%q) accepted a provider-specific or malformed class", value)
		}
	}

	var request llm.Request
	if err := json.Unmarshal([]byte(`{"api_version":"llm.temporal/v1","operation_key":"operation","model":"logical"}`), &request); err != nil {
		t.Fatal(err)
	}
	if request.ServiceClass != llm.ServiceClassStandard {
		t.Fatalf("omitted request service class = %q, want standard", request.ServiceClass)
	}
}
