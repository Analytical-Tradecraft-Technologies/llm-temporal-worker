package workflows

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

const inputMarker = "SECRET customer text!"

// callerEncodeConverter mirrors a caller that encodes without this worker's
// limit: arguments are encoded by the default converter, while the worker side
// decodes through the production BoundedDataConverter.
type callerEncodeConverter struct {
	converter.DataConverter
}

func (callerEncodeConverter) ToPayloads(values ...interface{}) (*commonpb.Payloads, error) {
	return converter.GetDefaultDataConverter().ToPayloads(values...)
}

// wireFixture returns a v1 fixture verbatim, as a caller would send it.
func wireFixture(t *testing.T, fixture string) json.RawMessage {
	t.Helper()
	data, err := os.ReadFile("../llm/testdata/v1/" + fixture + ".json")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// wireJSON returns a v1 fixture as the JSON object a caller would send, after
// applying mutate.
func wireJSON(t *testing.T, fixture string, mutate func(map[string]any)) json.RawMessage {
	t.Helper()
	var fields map[string]any
	if err := json.Unmarshal(wireFixture(t, fixture), &fields); err != nil {
		t.Fatal(err)
	}
	mutate(fields)
	encoded, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// invalidInputEnvironment registers the workflows exactly as the worker does
// and decodes through the worker's bounded data converter.
func invalidInputEnvironment(limits activity.PayloadLimits) *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetDataConverter(callerEncodeConverter{activity.BoundedDataConverter(limits)})
	Register(env, limits)
	return env
}

func assertTypedInvalidInput(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("workflow accepted invalid input")
	}
	var applicationErr *temporal.ApplicationError
	if !errors.As(err, &applicationErr) {
		t.Fatalf("error = %T %v, want ApplicationError", err, err)
	}
	if applicationErr.Type() != activity.ErrorTypeInvalidArgument || !applicationErr.NonRetryable() {
		t.Fatalf("error type=%q nonRetryable=%v (%v), want non-retryable %q", applicationErr.Type(), applicationErr.NonRetryable(), err, activity.ErrorTypeInvalidArgument)
	}
	var details activity.SafeErrorDetails
	if !applicationErr.HasDetails() || applicationErr.Details(&details) != nil {
		t.Fatalf("error %v has no SafeErrorDetails", err)
	}
	if details != (activity.SafeErrorDetails{Code: "invalid_argument", Phase: "decode", Dispatch: "not_dispatched"}) {
		t.Fatalf("details = %#v, want invalid_argument/decode/not_dispatched", details)
	}
	for _, text := range []string{err.Error(), applicationErr.Message()} {
		if strings.Contains(text, inputMarker) || strings.Contains(text, "SECRET") {
			t.Fatalf("error %q echoes caller content", text)
		}
	}
}

func TestPublicWorkflowsRejectInvalidInputAsTypedNonRetryable(t *testing.T) {
	limits := activity.PayloadLimits{MaxInlineBytes: 4096}
	oversize := strings.Repeat("x", 2*limits.MaxInlineBytes)
	cases := []struct {
		workflow, name string
		input          any
	}{
		{GenerateWorkflowName, "unknown field", wireJSON(t, "generate-root", func(fields map[string]any) { fields["transcript"] = inputMarker })},
		{GenerateWorkflowName, "tool name with a space", wireJSON(t, "generate-root", func(fields map[string]any) {
			fields["settings_patch"].(map[string]any)["tools"] = map[string]any{"set": []any{map[string]any{"name": inputMarker, "description": "Lookup", "input_schema": map[string]any{"type": "object"}}}}
		})},
		// Valid apart from its size, so only the inline limit can reject it.
		{GenerateWorkflowName, "oversize payload", wireJSON(t, "generate-root", func(fields map[string]any) {
			fields["append"].([]any)[0].(map[string]any)["content"].([]any)[0].(map[string]any)["text"] = inputMarker + oversize
		})},
		{GenerateWorkflowName, "malformed JSON", converter.NewRawValue(&commonpb.Payload{
			Metadata: map[string][]byte{converter.MetadataEncoding: []byte(converter.MetadataEncodingJSON)},
			Data:     []byte(`{"api_version":"` + inputMarker + `"`),
		})},
		{GenerateWorkflowName, "no payload", nil},
		{CompactWorkflowName, "unknown field", wireJSON(t, "compact-request", func(fields map[string]any) { fields["transcript"] = inputMarker })},
		{CompactWorkflowName, "tool name with a space", wireJSON(t, "compact-request", func(fields map[string]any) {
			fields["policy"].(map[string]any)["tools"] = []any{map[string]any{"name": inputMarker}}
		})},
		{CompactWorkflowName, "caller value in decoder message", wireJSON(t, "compact-request", func(fields map[string]any) { fields["api_version"] = inputMarker })},
		{CompactWorkflowName, "oversize payload", wireJSON(t, "compact-request", func(fields map[string]any) { fields["operation_key"] = inputMarker + oversize })},
		{CompactWorkflowName, "no payload", nil},
	}
	for _, tc := range cases {
		t.Run(tc.workflow+"/"+tc.name, func(t *testing.T) {
			env := invalidInputEnvironment(limits)
			if tc.input == nil {
				env.ExecuteWorkflow(tc.workflow)
			} else {
				env.ExecuteWorkflow(tc.workflow, tc.input)
			}
			if !env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			assertTypedInvalidInput(t, env.GetWorkflowError())
		})
	}
}

// The internal workflows are normally started by the public ones, but they are
// registered by name on the same task queue and get the same typed failure.
func TestInternalWorkflowsRejectInvalidInputAsTypedNonRetryable(t *testing.T) {
	limits := activity.PayloadLimits{MaxInlineBytes: 4096}
	oversize := strings.Repeat("x", 2*limits.MaxInlineBytes)
	generate := wireFixture(t, "generate-root")
	reference := `{"request_id":"` + testRequestID + `","context":{"tenant":"acme","project":"claims","actor":"workflow:claim-1"}}`
	cases := []struct {
		workflow, name string
		input          any
	}{
		{RequestWorkflowName, "unknown field", json.RawMessage(`{"generate":` + string(generate) + `,"transcript":"` + inputMarker + `"}`)},
		{RequestWorkflowName, "invalid nested request", json.RawMessage(`{"generate":` + string(wireJSON(t, "generate-root", func(fields map[string]any) { fields["api_version"] = inputMarker })) + `}`)},
		{RequestWorkflowName, "oversize payload", json.RawMessage(`{"generate":` + string(wireJSON(t, "generate-root", func(fields map[string]any) {
			fields["append"].([]any)[0].(map[string]any)["content"].([]any)[0].(map[string]any)["text"] = inputMarker + oversize
		})) + `}`)},
		{BudgetWorkflowName, "malformed reference", json.RawMessage(`{"reference":{"request_id":"` + inputMarker + `"},"kind":"generate"}`)},
		{BudgetWorkflowName, "JSON null", json.RawMessage(`null`)},
		{BudgetWorkflowName, "unknown field", json.RawMessage(`{"reference":` + reference + `,"kind":"generate","transcript":"` + inputMarker + `"}`)},
		{BudgetWorkflowName, "invalid kind", json.RawMessage(`{"reference":` + reference + `,"kind":"` + inputMarker + `"}`)},
		{BudgetWorkflowName, "negative waits", json.RawMessage(`{"reference":` + reference + `,"kind":"generate","waits":-1}`)},
		{BudgetWorkflowName, "missing reference", json.RawMessage(`{"kind":"generate"}`)},
		{BudgetWorkflowName, "no payload", nil},
		{RequestWorkflowName, "no payload", nil},
		{BudgetWorkflowName, "oversize payload", json.RawMessage(`{"reference":{},"kind":"` + inputMarker + oversize + `"}`)},
	}
	for _, tc := range cases {
		t.Run(tc.workflow+"/"+tc.name, func(t *testing.T) {
			env := invalidInputEnvironment(limits)
			// Any dispatch would fail the assertion below with a different error:
			// no Activity is registered in this environment.
			if tc.input == nil {
				env.ExecuteWorkflow(tc.workflow)
			} else {
				env.ExecuteWorkflow(tc.workflow, tc.input)
			}
			if !env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			assertTypedInvalidInput(t, env.GetWorkflowError())
		})
	}
}
