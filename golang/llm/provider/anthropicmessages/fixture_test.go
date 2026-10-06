package anthropicmessages

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestWireFixtureMatchesTypedSDKParams(t *testing.T) {
	profile := mustProfile(t, testProfile())
	params, err := lowerRequest(llm.Request{OperationKey: "fixture-op", Model: "claude-contract", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}}, profile, "standard_only")
	if err != nil {
		t.Fatal(err)
	}
	assertFixture(t, mustReadFixture(t, "request.wire.json"), mustJSON(t, params))
}

func TestMultimodalToolFixtureMatchesTypedSDKParams(t *testing.T) {
	profile := mustProfile(t, testProfile())
	maxTokens := 321
	temperature := 0.25
	topK := 5
	request := llm.Request{
		OperationKey: "fixture-multimodal-tools",
		Model:        "claude-contract",
		Instructions: []llm.Instruction{
			{Level: llm.InstructionLevelPolicy, Kind: llm.InstructionKindText, Text: "policy"},
			{Level: llm.InstructionLevelApplication, Kind: llm.InstructionKindParts, Content: []llm.Part{
				llm.TextPart{Text: "application"}, llm.JSONPart{Value: json.RawMessage(`{"kind":"context"}`)},
			}},
		},
		Input: []llm.Item{
			llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
				llm.TextPart{Text: "hello"},
				llm.ImagePart{URL: "https://example.test/image.png", MediaType: "image/png"},
				llm.DocumentPart{Bytes: []byte("plain document"), MediaType: "text/plain", Title: "notes"},
			}},
			llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "prior"}}},
			llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"sydney"}`)},
			llm.ToolResult{CallID: "call-1", Content: []llm.Part{llm.JSONPart{Value: json.RawMessage(`{"ok":true}`)}}},
		},
		Tools:      []llm.Tool{{Name: "lookup", Description: "look up a place", InputSchema: json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}`)}},
		ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNamed, Name: "lookup", Parallel: true},
		Output:     &llm.OutputSpec{MaxTokens: &maxTokens, Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Strict: true, Schema: json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"]}`)}},
		Sampling:   &llm.SamplingSpec{Temperature: &temperature, TopK: &topK, StopSequences: []string{"END"}},
		Reasoning:  &llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, TokenBudget: intPtr(2048), Summary: llm.ReasoningSummaryNone},
		Extensions: map[string]json.RawMessage{"anthropic.contract": json.RawMessage(`{"container_id":"container-1"}`)},
	}
	params, err := lowerRequest(request, profile, "auto")
	if err != nil {
		t.Fatal(err)
	}
	assertFixture(t, mustReadFixture(t, "request.multimodal-tools.json"), mustJSON(t, params))
}

func TestMediaRejectionFixture(t *testing.T) {
	var fixture struct {
		Cases []struct {
			Kind                  string `json:"kind"`
			MediaType             string `json:"media_type"`
			Detail                string `json:"detail"`
			ExpectedErrorContains string `json:"expected_error_contains"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(mustReadFixture(t, "request.media-rejection.json"), &fixture); err != nil {
		t.Fatal(err)
	}
	for _, test := range fixture.Cases {
		t.Run(test.Kind+"/"+test.MediaType, func(t *testing.T) {
			var part llm.Part
			switch test.Kind {
			case "image":
				part = llm.ImagePart{Bytes: []byte("image"), MediaType: test.MediaType, Detail: test.Detail}
			case "image_blob":
				part = llm.ImagePart{Blob: &llm.BlobRef{Digest: "sha256:blob", ByteLength: 5, MediaType: test.MediaType, Locator: "blob://blob-1"}, MediaType: test.MediaType}
			case "document_url":
				part = llm.DocumentPart{URL: "https://example.test/document", MediaType: test.MediaType}
			default:
				t.Fatalf("unknown fixture case %q", test.Kind)
			}
			if _, err := lowerPart(part); err == nil || !strings.Contains(err.Error(), test.ExpectedErrorContains) {
				t.Fatalf("error = %v, want substring %q", err, test.ExpectedErrorContains)
			}
		})
	}
}

func TestResponseFixturesMatchLiftedSemanticContract(t *testing.T) {
	var response anthropic.Message
	if err := json.Unmarshal(mustReadFixture(t, "response.completed.json"), &response); err != nil {
		t.Fatal(err)
	}
	call := provider.Call{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract", OperationKey: "fixture-op", ServiceClass: llm.ServiceClassStandard}
	lifted, err := mustProfile(t, testProfile()).liftResponse(call, &response, "req-fixture")
	if err != nil {
		t.Fatal(err)
	}
	assertFixture(t, mustReadFixture(t, "response.semantic.json"), mustJSON(t, lifted))
}

func TestThinkingToolFixtureLiftsProviderStateAndUsage(t *testing.T) {
	var response anthropic.Message
	if err := json.Unmarshal(mustReadFixture(t, "response.thinking-tool.json"), &response); err != nil {
		t.Fatal(err)
	}
	call := provider.Call{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract", OperationKey: "fixture-thinking", ServiceClass: llm.ServiceClassPriority}
	lifted, err := mustProfile(t, testProfile()).liftResponse(call, &response, "req-thinking")
	if err != nil {
		t.Fatal(err)
	}
	if lifted.Status != llm.ResponseStatusToolCalls || lifted.Service.Actual == nil || *lifted.Service.Actual != llm.ServiceClassPriority {
		t.Fatalf("status/service = %#v %#v", lifted.Status, lifted.Service)
	}
	if len(lifted.Continuation.ProviderStates) != 2 || lifted.Usage.ReasoningTokens != 4 || lifted.Usage.CacheReadTokens != 3 || lifted.Usage.CacheWriteTokens != 2 {
		t.Fatalf("lifted continuation/usage = %#v %#v", lifted.Continuation, lifted.Usage)
	}
}

func TestFixtureCompilationUsesNormalizedProviderRequest(t *testing.T) {
	adapter := &Adapter{endpointID: "anthropic-prod", profile: mustProfile(t, testProfile())}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "fixture-op", Model: "claude-contract"},
		Query:   provider.CapabilityQuery{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if call.Metadata.ProviderTier != "standard_only" || call.Metadata.CapabilityVersion != "anthropic-contract/v1" {
		t.Fatalf("fixture call metadata = %#v", call.Metadata)
	}
}

func mustReadFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/contracts/anthropic-direct/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func assertFixture(t *testing.T, want, got []byte) {
	t.Helper()
	wantCanonical, err := llm.CanonicalJSON(want)
	if err != nil {
		t.Fatal(err)
	}
	gotCanonical, err := llm.CanonicalJSON(got)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wantCanonical, gotCanonical) {
		t.Fatalf("fixture mismatch\n got: %s\nwant: %s", gotCanonical, wantCanonical)
	}
}
