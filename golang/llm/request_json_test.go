package llm_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
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
		request := testassert.UnmarshalJSON[llm.Request](t, readFixture(t, filepath.Join("request", name)), "unmarshal "+name)
		encoded, err := json.Marshal(request)
		if err != nil {
			t.Fatalf("marshal %s: %v", name, err)
		}
		roundTripped := testassert.UnmarshalJSON[llm.Request](t, encoded, fmt.Sprintf("round-trip %s (%s)", name, encoded))
		if request.APIVersion != llm.APIVersion || roundTripped.ServiceClass == "" {
			t.Fatalf("%s lost normalized contract: %#v", name, roundTripped)
		}
	}

	minimal := readRequestFixture(t, "minimal.json")
	if minimal.ServiceClass != llm.ServiceClassStandard {
		t.Fatalf("omitted service class = %q, want standard", minimal.ServiceClass)
	}
	requireEncodedContains(t, testassert.MarshalJSON(t, minimal), `"service_class":"standard"`, "normalized request omitted explicit standard")
}

func TestRequestFullFixturePreservesTypedItems(t *testing.T) {
	request := readRequestFixture(t, "full.json")
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
			requireUnmarshalRejected[llm.Request](t, []byte(test.json), "accepted nested duplicate key: %s", test.json)
		})
	}
}

func TestResponseJSONFixtures(t *testing.T) {
	for _, name := range []string{"tool-calls.json", "completed.json"} {
		response := testassert.UnmarshalJSON[llm.Response](t, readFixture(t, filepath.Join("response", name)), "unmarshal "+name)
		encoded := testassert.MarshalJSON(t, response)
		roundTripped := testassert.UnmarshalJSON[llm.Response](t, encoded, fmt.Sprintf("round-trip %s (%s)", name, encoded))
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
	encoded := testassert.MarshalJSON(t, response)
	requireEncodedContains(t, encoded, `"cost_status":"unknown"`, "unknown-cost response omitted cost_status")
	for _, field := range []string{`"reserved_cost_usd":null`, `"actual_cost_usd":null`} {
		requireEncodedContains(t, encoded, field, "unknown-cost response omitted exact nullable field "+field)
	}
	decoded := testassert.UnmarshalJSON[llm.Response](t, encoded, "unmarshal unknown-cost response")
	if decoded.Cost.Status != llm.CostStatusUnknown {
		t.Fatalf("cost status = %q, want unknown", decoded.Cost.Status)
	}
	invalid := bytes.Replace(encoded, []byte(`"cost_status":"unknown"`), []byte(`"cost_status":"estimated"`), 1)
	requireUnmarshalRejected[llm.Response](t, invalid, "accepted invalid cost status: %s", invalid)
}

func TestSchemasAreValidJSONAndKeepClosedServiceEnum(t *testing.T) {
	for _, name := range []string{"generate-request.schema.json", "generate-response.schema.json"} {
		data, err := os.ReadFile(filepath.Join("..", "api", "schema", "v1", name))
		testassert.NoError(t, err)
		if !json.Valid(data) {
			t.Fatalf("schema %s is not valid JSON", name)
		}
		if name == "generate-request.schema.json" {
			schema := testassert.UnmarshalJSON[struct {
				AdditionalProperties bool `json:"additionalProperties"`
			}](t, data, "decode "+name)
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
	return testassert.ReadFile(t, filepath.Join("testdata", name))
}

// requireUnmarshalRejected fails with the formatted message when data decodes
// into a T.
func requireUnmarshalRejected[T any](t testing.TB, data []byte, format string, args ...any) {
	t.Helper()
	var value T
	if err := json.Unmarshal(data, &value); err == nil {
		t.Fatalf(format, args...)
	}
}

// requireEncodedContains fails with message (followed by the encoding) when
// encoded does not contain want.
func requireEncodedContains(t testing.TB, encoded []byte, want, message string) {
	t.Helper()
	if !bytes.Contains(encoded, []byte(want)) {
		t.Fatalf("%s: %s", message, encoded)
	}
}
