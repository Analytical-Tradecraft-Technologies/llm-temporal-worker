package llm_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// The Go codec rejects what the published v1 schemas reject, so a caller
// that validates against the schema and one that does not see the same
// outcome (#1662).
func TestV1DecodersApplyTheSchemaBounds(t *testing.T) {
	long := strings.Repeat("k", 257)
	compact := `{"api_version":"llm.temporal/compact/v1","operation_key":%s,"context":{"tenant":%s,"project":"claims","actor":"workflow:claim-1"},"parent":"ckp_v1.child"}`
	query := `{"api_version":"llm.temporal/query/v1","operation_key":%s,"context":{"tenant":"acme","project":"claims","actor":"workflow:claim-1"},"kind":"provider_status","query":{"provider":"openai","include_healthy":false,"page_size":100,"cursor":null}}`
	quote := func(value string) string { encoded, _ := json.Marshal(value); return string(encoded) }
	for name, test := range map[string]struct {
		data   string
		target any
		want   string
	}{
		"compact operation_key": {data: sprintf(compact, quote(long), quote("acme")), target: new(llm.CompactRequestV1), want: "operation_key"},
		"compact context":       {data: sprintf(compact, quote("compact-1"), quote(long)), target: new(llm.CompactRequestV1), want: "context.tenant"},
		"query operation_key":   {data: sprintf(query, quote(long)), target: new(llm.QueryRequestV1), want: "operation_key"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(test.data), test.target); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Unmarshal = %v, want an error naming %s", err, test.want)
			}
		})
	}
	// The bounds are code points, so 256 multi-byte characters still fit.
	var accepted llm.CompactRequestV1
	if err := json.Unmarshal([]byte(sprintf(compact, quote(strings.Repeat("é", 256)), quote("acme"))), &accepted); err != nil {
		t.Fatalf("256 code points rejected: %v", err)
	}
}

func TestV1SettingsPatchDecodesTheCompactionPolicyShape(t *testing.T) {
	for policy, want := range map[string]string{
		`{"foo":1}`:                              "foo",
		`{"target_tokens":"x"}`:                  "target_tokens",
		`{"trigger_tokens":-5}`:                  "trigger_tokens",
		`{"target_tokens":10000000}`:             "target_tokens",
		`{"recent_turns":1001}`:                  "recent_turns",
		`{"summary_style":"long"}`:               "summary_style",
		`{"version":"generic-2026-01-01"}`:       "version",
		`{"materialization_threshold_bytes":-1}`: "materialization_threshold_bytes",
	} {
		var patch llm.SettingsPatchV1
		err := json.Unmarshal([]byte(`{"compaction_policy":{"set":`+policy+`}}`), &patch)
		if err == nil || !strings.Contains(err.Error(), "compaction_policy.set") || !strings.Contains(err.Error(), want) {
			t.Fatalf("policy %s: Unmarshal = %v, want a field error naming %s", policy, err, want)
		}
	}
	var patch llm.SettingsPatchV1
	valid := `{"compaction_policy":{"set":{"version":"generic-2026-07-18","prompt_version":"v1","summary_style":"concise","trigger_tokens":20000,"target_tokens":9000,"recent_turns":4,"output_reserve_tokens":2048,"materialization_threshold_bytes":0}}}`
	if err := json.Unmarshal([]byte(valid), &patch); err != nil {
		t.Fatalf("a complete valid policy was rejected: %v", err)
	}
	if _, err := patch.MarshalJSON(); err != nil {
		t.Fatalf("a complete valid policy does not encode: %v", err)
	}
}

func TestV1GenerateResponseRejectsANullActualClass(t *testing.T) {
	data, err := os.ReadFile("testdata/v1/generate-response-disabled-cache.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture map[string]any
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	service := fixture["service"].(map[string]any)
	delete(service, "actual")
	omitted, _ := json.Marshal(fixture)
	var response llm.GenerateResponseV1
	if err := json.Unmarshal(omitted, &response); err != nil || response.Service == nil || response.Service.Actual != nil {
		t.Fatalf("an omitted actual class = %+v, %v", response.Service, err)
	}
	service["actual"] = nil
	null, _ := json.Marshal(fixture)
	if err := json.Unmarshal(null, &response); err == nil || !strings.Contains(err.Error(), "actual") {
		t.Fatalf("service.actual null: Unmarshal = %v, want an error naming actual", err)
	}
}

func sprintf(format string, values ...string) string {
	for _, value := range values {
		format = strings.Replace(format, "%s", value, 1)
	}
	return format
}
