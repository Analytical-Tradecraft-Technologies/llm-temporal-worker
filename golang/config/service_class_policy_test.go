package config_test

import (
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestServiceClassPoliciesLoadAndKeepStandard(t *testing.T) {
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	if policies := loaded.ServiceClassPolicies; len(policies) != 1 || policies[0].ID != "acme-no-priority" || policies[0].Match.Tenant != "acme" ||
		len(policies[0].Classes) != 2 || policies[0].Classes[1] != llm.ServiceClassStandard {
		t.Fatalf("service class policies = %+v", policies)
	}
	const example = "    classes: [economy, standard]\n"
	for _, tc := range []struct {
		name    string
		replace string
		want    string
	}{
		{name: "fast and economy without standard", replace: "    classes: [economy, priority]\n", want: "must include standard"},
		{name: "empty classes", replace: "    classes: []\n", want: "must include standard"},
		{name: "unknown class", replace: "    classes: [standard, turbo]\n", want: "must be economy, standard, or priority"},
		{name: "repeated class", replace: "    classes: [standard, standard]\n", want: "repeats"},
		{name: "duplicate ID", replace: example + "  - id: acme-no-priority\n    match: {}\n    classes: [standard]\n", want: "duplicate policy ID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := string(exampleYAML(t))
			if !strings.Contains(data, example) {
				t.Fatal("example policy missing")
			}
			_, err := config.Load([]byte(strings.Replace(data, example, tc.replace, 1)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load = %v, want %q", err, tc.want)
			}
		})
	}
}
