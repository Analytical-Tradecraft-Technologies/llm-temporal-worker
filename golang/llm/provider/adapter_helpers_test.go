package provider

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestStartCompileChecksContextThenQuery(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	var mapped *Error
	if err := StartCompile(canceled, CapabilityQuery{}, "openai chat", FamilyOpenAIChat, "ep"); !errors.As(err, &mapped) || mapped.Code != CodeCanceled {
		t.Fatalf("StartCompile(canceled) = %v, want a canceled compile", err)
	}
	err := StartCompile(context.Background(), CapabilityQuery{EndpointID: "other"}, "openai chat", FamilyOpenAIChat, "ep")
	if !errors.As(err, &mapped) || mapped.Code != CodeInvalidArgument || !strings.HasPrefix(mapped.SafeMessage, "openai chat: capability endpoint") {
		t.Fatalf("StartCompile(other endpoint) = %v, want a labelled compile error", err)
	}
	if err := StartCompile(context.Background(), CapabilityQuery{Family: FamilyOpenAIChat, EndpointID: "ep"}, "openai chat", FamilyOpenAIChat, "ep"); err != nil {
		t.Fatalf("StartCompile(matching query) = %v", err)
	}
}

func TestRequirePinnedContinuation(t *testing.T) {
	for _, test := range []struct {
		continuation *llm.Continuation
		rejected     bool
	}{
		{continuation: nil},
		{continuation: &llm.Continuation{EndpointID: "other"}},
		{continuation: &llm.Continuation{Pinned: true}},
		{continuation: &llm.Continuation{Pinned: true, EndpointID: "ep"}},
		{continuation: &llm.Continuation{Pinned: true, EndpointID: "other"}, rejected: true},
	} {
		if err := RequirePinnedContinuation(llm.Request{Continuation: test.continuation}, "ep"); (err != nil) != test.rejected {
			t.Errorf("RequirePinnedContinuation(%#v) = %v, rejected want %t", test.continuation, err, test.rejected)
		}
	}
}

func TestHostedToolHelpers(t *testing.T) {
	request := llm.Request{WebFetch: true, CodeExecution: true}
	if got := HostedToolFlag(request); got != "web_fetch" {
		t.Fatalf("HostedToolFlag() = %q, want web_fetch", got)
	}
	if got := HostedToolFlag(llm.Request{}); got != "" {
		t.Fatalf("HostedToolFlag(none) = %q", got)
	}
	metadata := CallMetadata{WebSearch: true}
	metadata.RecordHostedTools(request)
	if metadata.WebSearch || !metadata.WebFetch || !metadata.CodeExecution {
		t.Fatalf("RecordHostedTools() = %#v", metadata)
	}
}

func TestMixedInstructionLevels(t *testing.T) {
	policy := llm.Instruction{Level: llm.InstructionLevelPolicy}
	application := llm.Instruction{}
	if MixedInstructionLevels([]llm.Instruction{policy, policy}) || MixedInstructionLevels([]llm.Instruction{application}) || !MixedInstructionLevels([]llm.Instruction{application, policy}) {
		t.Fatal("MixedInstructionLevels() did not report only a mix of levels")
	}
}

func TestRequestFeaturesListsFirstUseOrder(t *testing.T) {
	request := llm.Request{
		Instructions: []llm.Instruction{{Content: []llm.Part{llm.TextPart{Text: "x"}}}},
		Input: []llm.Item{
			llm.Message{Content: []llm.Part{llm.ImagePart{}}},
			llm.ProviderState{},
			llm.ToolResult{},
		},
		Output:    &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSON}},
		Reasoning: &llm.ReasoningSpec{},
	}
	partFeature := func(part llm.Part) Feature {
		if part.PartKind() == llm.PartKindImage {
			return FeatureImage
		}
		return FeatureText
	}
	got := RequestFeatures(request, partFeature, []Feature{FeatureContinuation}, StructuredOutputKind)
	want := []Feature{FeatureText, FeatureUsage, FeatureImage, FeatureContinuation, FeatureToolCall, FeatureStructuredOutput, FeatureReasoning}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RequestFeatures() = %v, want %v", got, want)
	}
	got = RequestFeatures(request, partFeature, nil, func(kind llm.OutputKind) bool { return kind == llm.OutputKindJSONSchema })
	want = []Feature{FeatureText, FeatureUsage, FeatureImage, FeatureToolCall, FeatureReasoning}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RequestFeatures(json_schema only, no state features) = %v, want %v", got, want)
	}
}

func TestRequireAdapterArgsAndExpectedBaseURL(t *testing.T) {
	for _, test := range []struct {
		hasClient  bool
		endpointID string
		want       string
	}{
		{hasClient: false, endpointID: "ep", want: "openai chat: client is required"},
		{hasClient: true, endpointID: "", want: "openai chat: endpoint ID is required"},
		{hasClient: true, endpointID: "ep"},
	} {
		err := RequireAdapterArgs("openai chat", test.hasClient, test.endpointID)
		if got := fmt.Sprint(err); (err == nil) != (test.want == "") || (err != nil && got != test.want) {
			t.Errorf("RequireAdapterArgs(%t, %q) = %v, want %q", test.hasClient, test.endpointID, err, test.want)
		}
	}
	if err := RequireExpectedBaseURL("openai chat", "p", "", "https://a"); err != nil {
		t.Fatalf("RequireExpectedBaseURL(unpinned) = %v", err)
	}
	if err := RequireExpectedBaseURL("openai chat", "p", "https://a", "https://b"); err == nil || err.Error() != `openai chat profile "p" requires base URL "https://a", got "https://b"` {
		t.Fatalf("RequireExpectedBaseURL(mismatch) = %v", err)
	}
}

func TestNewInvalidResponseErrorWithID(t *testing.T) {
	mapped := NewInvalidResponseErrorWithID(Call{OperationKey: "op"}, "req", "resp", "bad")
	if mapped.Code != CodeProviderInvalidResponse || mapped.Dispatch != DispatchAccepted || mapped.OperationID != "op" ||
		mapped.Provider.RequestID != "req" || mapped.Provider.ResponseID != "resp" || mapped.SafeMessage != "bad" {
		t.Fatalf("NewInvalidResponseErrorWithID() = %#v", mapped)
	}
}

type sdkTestAPIError struct{ status int }

func (err *sdkTestAPIError) Error() string { return fmt.Sprintf("status %d", err.status) }

func TestMapSDKErrorOrder(t *testing.T) {
	mapAPI := func(apiErr *sdkTestAPIError, profile string) *Error {
		return NewError(CodeInvalidArgument, PhaseDispatch, DispatchRejected, RetryNever, fmt.Sprintf("%s %d", profile, apiErr.status))
	}
	if got := MapSDKError(nil, "p", mapAPI); got != nil {
		t.Fatalf("MapSDKError(nil) = %#v", got)
	}
	if got := MapSDKError(fmt.Errorf("wrapped: %w", context.Canceled), "p", mapAPI); got.Code != CodeCanceled {
		t.Fatalf("MapSDKError(canceled) = %#v, want the transport classification", got)
	}
	if got := MapSDKError(fmt.Errorf("wrapped: %w", &sdkTestAPIError{status: 400}), "p", mapAPI); got.SafeMessage != "p 400" {
		t.Fatalf("MapSDKError(API error) = %#v, want mapAPI's classification", got)
	}
	if got := MapSDKError(errors.New("reset"), "p", mapAPI); got.Dispatch != DispatchAmbiguous || got.SafeDetails["provider"] != "p" {
		t.Fatalf("MapSDKError(other) = %#v, want an unclassified dispatch error", got)
	}
}

func TestLowerEachPrefixesTheFailingIndex(t *testing.T) {
	lower := func(value int) (string, error) {
		if value < 0 {
			return "", errors.New("negative")
		}
		return fmt.Sprint(value), nil
	}
	got, err := LowerEach([]int{1, 2}, "part", lower)
	if err != nil || !reflect.DeepEqual(got, []any{"1", "2"}) {
		t.Fatalf("LowerEach() = %v, %v", got, err)
	}
	if _, err := LowerEach([]int{1, -1}, "tool result part", lower); err == nil || err.Error() != "tool result part 1: negative" {
		t.Fatalf("LowerEach(failure) = %v", err)
	}
}

func TestDecodeWireParams(t *testing.T) {
	type params struct {
		Model string `json:"model"`
	}
	got, encoded, err := DecodeWireParams[params](map[string]any{"model": "m"}, "openai chat")
	if err != nil || got.Model != "m" || string(encoded) != `{"model":"m"}` {
		t.Fatalf("DecodeWireParams() = %#v, %s, %v", got, encoded, err)
	}
	if _, _, err := DecodeWireParams[params](map[string]any{"model": 1}, "openai chat"); err == nil || !strings.HasPrefix(err.Error(), "openai chat parameter union: ") {
		t.Fatalf("DecodeWireParams(bad union) = %v", err)
	}
	if _, _, err := DecodeWireParams[params](map[string]any{"x": func() {}}, "openai chat"); err == nil || strings.Contains(err.Error(), "parameter union") {
		t.Fatalf("DecodeWireParams(unencodable) = %v, want the bare encoding error", err)
	}
}
