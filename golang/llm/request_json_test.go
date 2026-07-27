package llm_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func TestNormalizeServiceClass(t *testing.T) {
	tests := []struct{ in, want llm.ServiceClass }{
		{"", llm.ServiceClassStandard},
		{llm.ServiceClassEconomy, llm.ServiceClassEconomy},
		{llm.ServiceClassStandard, llm.ServiceClassStandard},
		{llm.ServiceClassPriority, llm.ServiceClassPriority},
	}
	for _, tt := range tests {
		got, err := llm.NormalizeServiceClass(tt.in)
		if err != nil || got != tt.want {
			t.Fatalf("NormalizeServiceClass(%q) = %q, %v; want %q", tt.in, got, err, tt.want)
		}
	}
}

func TestNormalizeServiceClassRejectsProviderDefault(t *testing.T) {
	if _, err := llm.NormalizeServiceClass("provider_default"); err == nil {
		t.Fatal("NormalizeServiceClass accepted provider_default")
	}
}

func TestRequestJSONFixtures(t *testing.T) {
	for _, name := range []string{"minimal.json", "full.json"} {
		data := readFixture(t, filepath.Join("request", name))
		var request llm.Request
		if err := json.Unmarshal(data, &request); err != nil {
			t.Fatalf("unmarshal %s: %v", name, err)
		}
		encoded, err := json.Marshal(request)
		if err != nil {
			t.Fatalf("marshal %s: %v", name, err)
		}
		var roundTripped llm.Request
		if err := json.Unmarshal(encoded, &roundTripped); err != nil {
			t.Fatalf("round-trip %s: %v\n%s", name, err, encoded)
		}
		if request.APIVersion != llm.APIVersion || roundTripped.ServiceClass == "" {
			t.Fatalf("%s lost normalized contract: %#v", name, roundTripped)
		}
	}

	var minimal llm.Request
	if err := json.Unmarshal(readFixture(t, filepath.Join("request", "minimal.json")), &minimal); err != nil {
		t.Fatal(err)
	}
	if minimal.ServiceClass != llm.ServiceClassStandard {
		t.Fatalf("omitted service class = %q, want standard", minimal.ServiceClass)
	}
	if got := mustMarshal(t, minimal); !bytes.Contains(got, []byte(`"service_class":"standard"`)) {
		t.Fatalf("normalized request omitted explicit standard: %s", got)
	}
}

func TestRequestFullFixturePreservesTypedItems(t *testing.T) {
	var request llm.Request
	if err := json.Unmarshal(readFixture(t, filepath.Join("request", "full.json")), &request); err != nil {
		t.Fatal(err)
	}
	if len(request.Input) != 5 {
		t.Fatalf("input length = %d, want 5", len(request.Input))
	}
	wantKinds := []llm.ItemKind{llm.ItemKindMessage, llm.ItemKindToolCall, llm.ItemKindToolResult, llm.ItemKindProviderState, llm.ItemKindReference}
	for index, want := range wantKinds {
		if got := request.Input[index].ItemKind(); got != want {
			t.Fatalf("input[%d] kind = %q, want %q", index, got, want)
		}
	}
	message := request.Input[0].(llm.Message)
	image, ok := message.Content[1].(llm.ImagePart)
	if !ok || !reflect.DeepEqual(image.Bytes, []byte{1, 2}) {
		t.Fatalf("image bytes were not decoded/copied: %#v", message.Content[1])
	}
}

func TestRequestRejectsUnknownAndAmbiguousFields(t *testing.T) {
	cases := []string{
		`{"api_version":"llm.temporal/v1","operation_key":"x","model":"m","unknown":true}`,
		`{"api_version":"llm.temporal/v1","operation_key":"x","model":"m","service_class":"provider_default"}`,
		`{"api_version":"llm.temporal/v1","operation_key":"x","model":"m","input":[{"kind":"message","actor":"assistant","content":[]}]}`,
		`{"api_version":"llm.temporal/v1","operation_key":"x","model":"m","input":[{"kind":"not-an-item"}]}`,
		`{"api_version":"llm.temporal/v1","operation_key":"x","model":"m","input":[{"kind":"message","actor":"human","content":[{"kind":"image","url":"https://example.test/a","bytes":"AQ==","media_type":"image/png"}]}]}`,
		`{"api_version":"llm.temporal/v1","operation_key":"x","model":"m","service_class_fallbacks":["standard","standard"]}`,
	}
	for _, input := range cases {
		var request llm.Request
		if err := json.Unmarshal([]byte(input), &request); err == nil {
			t.Errorf("accepted invalid request %s", input)
		}
	}
	if _, err := llm.NormalizeServiceClass("not-a-class"); err == nil {
		t.Fatal("accepted unknown service class")
	}
}

func TestPublicJSONRejectsNestedDuplicateKeys(t *testing.T) {
	cases := []struct {
		name string
		json string
	}{
		{
			name: "context tags",
			json: `{"api_version":"llm.temporal/v1","operation_key":"x","model":"m","context":{"tags":{"team":"a","team":"b"}}}`,
		},
		{
			name: "extension value",
			json: `{"api_version":"llm.temporal/v1","operation_key":"x","model":"m","extensions":{"vendor":{"enabled":true,"enabled":false}}}`,
		},
		{
			name: "tool schema",
			json: `{"api_version":"llm.temporal/v1","operation_key":"x","model":"m","tools":[{"name":"lookup","input_schema":{"type":"object","properties":{"id":{"type":"string","type":"integer"}}}}]}`,
		},
		{
			name: "tool arguments",
			json: `{"api_version":"llm.temporal/v1","operation_key":"x","model":"m","input":[{"kind":"tool_call","id":"call-1","name":"lookup","arguments":{"id":1,"id":2}}]}`,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var request llm.Request
			if err := json.Unmarshal([]byte(test.json), &request); err == nil {
				t.Fatalf("accepted nested duplicate key: %s", test.json)
			}
		})
	}
}

func TestResponseJSONFixtures(t *testing.T) {
	for _, name := range []string{"tool-calls.json", "completed.json"} {
		data := readFixture(t, filepath.Join("response", name))
		var response llm.Response
		if err := json.Unmarshal(data, &response); err != nil {
			t.Fatalf("unmarshal %s: %v", name, err)
		}
		encoded := mustMarshal(t, response)
		var roundTripped llm.Response
		if err := json.Unmarshal(encoded, &roundTripped); err != nil {
			t.Fatalf("round-trip %s: %v\n%s", name, err, encoded)
		}
		if response.Status != roundTripped.Status || response.OperationKey != roundTripped.OperationKey {
			t.Fatalf("round-trip changed response identity: %#v %#v", response, roundTripped)
		}
	}
}

func TestResponseCostStatus(t *testing.T) {
	response := llm.Response{
		OperationKey: "unknown-cost",
		Status:       llm.ResponseStatusCompleted,
		Cost:         llm.Cost{Status: llm.CostStatusUnknown},
	}
	encoded := mustMarshal(t, response)
	if !bytes.Contains(encoded, []byte(`"cost_status":"unknown"`)) {
		t.Fatalf("unknown-cost response omitted cost_status: %s", encoded)
	}
	for _, field := range []string{`"reserved_cost_usd":null`, `"actual_cost_usd":null`} {
		if !bytes.Contains(encoded, []byte(field)) {
			t.Fatalf("unknown-cost response omitted exact nullable field %s: %s", field, encoded)
		}
	}
	var decoded llm.Response
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal unknown-cost response: %v", err)
	}
	if decoded.Cost.Status != llm.CostStatusUnknown {
		t.Fatalf("cost status = %q, want unknown", decoded.Cost.Status)
	}
	invalid := bytes.Replace(encoded, []byte(`"cost_status":"unknown"`), []byte(`"cost_status":"estimated"`), 1)
	if err := json.Unmarshal(invalid, &llm.Response{}); err == nil {
		t.Fatalf("accepted invalid cost status: %s", invalid)
	}
}

func TestSchemasAreValidJSONAndKeepClosedServiceEnum(t *testing.T) {
	for _, name := range []string{"generate-request.schema.json", "generate-response.schema.json"} {
		data, err := os.ReadFile(filepath.Join("..", "api", "schema", "v1", name))
		if err != nil {
			t.Fatal(err)
		}
		if !json.Valid(data) {
			t.Fatalf("schema %s is not valid JSON", name)
		}
		if name == "generate-request.schema.json" {
			var schema struct {
				AdditionalProperties bool `json:"additionalProperties"`
			}
			if err := json.Unmarshal(data, &schema); err != nil {
				t.Fatal(err)
			}
			if schema.AdditionalProperties {
				t.Fatal("request schema must close top-level properties")
			}
			// Service class is now a sparse settings patch. Keep its closed
			// economy/standard/priority enum in the generated contract.
			for _, value := range []string{`"economy"`, `"standard"`, `"priority"`} {
				if !bytes.Contains(data, []byte(value)) {
					t.Fatalf("request schema omitted service class value %s", value)
				}
			}
		}
	}
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
