package llm

import (
	"encoding/json"
	"testing"
)

func TestHostedSearchChargesAndUnknownExecution(t *testing.T) {
	for _, test := range []struct {
		raw     string
		model   string
		want    string
		unknown bool
	}{
		{`2`, "gpt-test", "0.020000000000000000", false}, {`0`, "gpt-4o-mini", "0.000000000000000000", false},
		{`null`, "gpt-test", "", true}, {`-1`, "gpt-test", "", true}, {`1`, "gpt-4o-mini", "", true},
	} {
		cost, err := HostedToolCharge(Usage{ProviderRaw: map[string]json.RawMessage{"web_search_calls": json.RawMessage(test.raw)}}, test.model)
		if (err != nil) != test.unknown || (!test.unknown && cost.String() != test.want) {
			t.Fatalf("%+v: %v %v", test, cost, err)
		}
	}
	if _, err := HostedToolCharge(Usage{ProviderRaw: map[string]json.RawMessage{"hosted_execution_used": json.RawMessage(`true`)}}, "claude"); err == nil {
		t.Fatal("invented exact container cost")
	}
}
func TestHostedPatchBoolValidation(t *testing.T) {
	for _, flag := range []string{"web_search", "web_fetch", "code_execution"} {
		for _, value := range []string{`{"set":true}`, `{"set":false}`, `{"clear":true}`} {
			var patch SettingsPatchV1
			if err := json.Unmarshal([]byte(`{"`+flag+`":`+value+`}`), &patch); err != nil {
				t.Fatal(err)
			}
		}
		for _, value := range []string{`{"set":null}`, `{"set":"true"}`, `{"set":1}`} {
			var patch SettingsPatchV1
			if json.Unmarshal([]byte(`{"`+flag+`":`+value+`}`), &patch) == nil {
				t.Fatalf("accepted invalid %s=%s", flag, value)
			}
		}
	}
}
