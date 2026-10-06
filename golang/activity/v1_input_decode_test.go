package activity

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	commonpb "go.temporal.io/api/common/v1"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

// unboundedEncodeConverter mirrors a caller that encodes with a larger (or no)
// limit than this worker: arguments are encoded without the bound, while the
// worker side decodes through the production BoundedDataConverter.
type unboundedEncodeConverter struct {
	converter.DataConverter
}

func (unboundedEncodeConverter) ToPayloads(values ...interface{}) (*commonpb.Payloads, error) {
	return converter.GetDefaultDataConverter().ToPayloads(values...)
}

func rawJSONArgument(t *testing.T, data []byte) converter.RawValue {
	t.Helper()
	return converter.NewRawValue(&commonpb.Payload{
		Metadata: map[string][]byte{converter.MetadataEncoding: []byte(converter.MetadataEncodingJSON)},
		Data:     data,
	})
}

func generateV1JSON(t *testing.T, mutate func(map[string]any)) []byte {
	t.Helper()
	encoded, err := json.Marshal(validGenerateV1Request())
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	mutate(fields)
	encoded, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func assertTypedInvalidV1Input(t *testing.T, err error, secrets ...string) {
	t.Helper()
	if err == nil {
		t.Fatal("Activity unexpectedly accepted invalid input")
	}
	var applicationErr *temporal.ApplicationError
	if !errors.As(err, &applicationErr) {
		t.Fatalf("error = %T %v, want ApplicationError", err, err)
	}
	if applicationErr.Type() != ErrorTypeInvalidArgument || !applicationErr.NonRetryable() {
		t.Fatalf("error type=%q nonRetryable=%v (%v), want non-retryable %q", applicationErr.Type(), applicationErr.NonRetryable(), err, ErrorTypeInvalidArgument)
	}
	if !applicationErr.HasDetails() {
		t.Fatalf("error %v has no SafeErrorDetails", err)
	}
	var details SafeErrorDetails
	if detailsErr := applicationErr.Details(&details); detailsErr != nil {
		t.Fatalf("decode SafeErrorDetails: %v", detailsErr)
	}
	if details.Code != "invalid_argument" || details.Phase != "decode" || details.Dispatch != "not_dispatched" {
		t.Fatalf("details = %#v, want invalid_argument/decode/not_dispatched", details)
	}
	encodedDetails, _ := json.Marshal(details)
	for _, secret := range secrets {
		if strings.Contains(err.Error(), secret) || strings.Contains(string(encodedDetails), secret) {
			t.Fatalf("error %q / details %s echo caller content %q", err.Error(), encodedDetails, secret)
		}
	}
}

func TestRegisteredV1GenerateRejectsMalformedInputAsTypedNonRetryable(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		secrets []string
	}{
		{
			name: "unknown field",
			payload: generateV1JSON(t, func(fields map[string]any) {
				fields["transcript"] = "secret-transcript-value"
			}),
			secrets: []string{"secret-transcript-value", "transcript"},
		},
		{
			name: "caller value in decoder message",
			payload: generateV1JSON(t, func(fields map[string]any) {
				fields["api_version"] = "secret-version-value"
			}),
			secrets: []string{"secret-version-value"},
		},
		{
			name:    "malformed JSON",
			payload: []byte(`{"secret-json-value"`),
			secrets: []string{"secret-json-value"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runtime := &executionRuntimeStub{result: pendingExecution()}
			activities := &Activities{V1Runtime: runtime}
			var suite testsuite.WorkflowTestSuite
			environment := suite.NewTestActivityEnvironment()
			environment.RegisterActivityWithOptions(activities.generateV1Temporal, sdkactivity.RegisterOptions{Name: GenerateActivityName})
			_, err := environment.ExecuteActivity(GenerateActivityName, rawJSONArgument(t, tc.payload))
			assertTypedInvalidV1Input(t, err, tc.secrets...)
			if runtime.calls != 0 {
				t.Fatalf("runtime calls = %d, want zero for undecodable input", runtime.calls)
			}
		})
	}
}

func TestRegisteredV1ActivitiesRejectUndecodableInputAsTypedNonRetryable(t *testing.T) {
	registry := &v1Registry{}
	activities := &Activities{V1Runtime: &executionRuntimeStub{result: pendingExecution()}}
	activities.Register(registry)
	for index, registered := range registry.funcs {
		name := registry.names[index]
		t.Run(name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			environment := suite.NewTestActivityEnvironment()
			environment.RegisterActivityWithOptions(registered, sdkactivity.RegisterOptions{Name: name})
			_, err := environment.ExecuteActivity(name, rawJSONArgument(t, []byte(`{"secret-json-value"`)))
			assertTypedInvalidV1Input(t, err, "secret-json-value")
		})
	}
}

func TestRegisteredV1GenerateRejectsOversizeInputThroughBoundedConverter(t *testing.T) {
	limits := PayloadLimits{MaxInlineBytes: 512}
	runtime := &executionRuntimeStub{result: pendingExecution()}
	activities := &Activities{V1Runtime: runtime, PayloadLimits: limits}
	request := validGenerateV1Request()
	request.Append = []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "secret-" + strings.Repeat("x", 1024)}}}}
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestActivityEnvironment()
	environment.SetDataConverter(unboundedEncodeConverter{DataConverter: BoundedDataConverter(limits)})
	environment.RegisterActivityWithOptions(activities.generateV1Temporal, sdkactivity.RegisterOptions{Name: GenerateActivityName})
	_, err := environment.ExecuteActivity(GenerateActivityName, request)
	assertTypedInvalidV1Input(t, err, "secret-")
	if runtime.calls != 0 {
		t.Fatalf("runtime calls = %d, want zero for oversize input", runtime.calls)
	}
}

func TestRegisteredV1GenerateAcceptsValidInputThroughBoundedConverter(t *testing.T) {
	limits := PayloadLimits{MaxInlineBytes: 4096}
	runtime := &executionRuntimeStub{result: pendingExecution()}
	activities := &Activities{V1Runtime: runtime, PayloadLimits: limits}
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestActivityEnvironment()
	environment.SetDataConverter(BoundedDataConverter(limits))
	environment.RegisterActivityWithOptions(activities.generateV1Temporal, sdkactivity.RegisterOptions{Name: GenerateActivityName})
	result, err := environment.ExecuteActivity(GenerateActivityName, validGenerateV1Request())
	if err != nil {
		t.Fatal(err)
	}
	var response llm.ExecutionResultV1
	if err := result.Get(&response); err != nil {
		t.Fatal(err)
	}
	if response.State != llm.ExecutionPending || runtime.calls != 1 {
		t.Fatalf("result=%#v calls=%d", response, runtime.calls)
	}
}
