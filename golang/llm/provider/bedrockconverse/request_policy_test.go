package bedrockconverse

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestCompilePreservesStopSequences(t *testing.T) {
	adapter := &Adapter{endpointID: "test", profile: DefaultProfile("nova")}
	for _, strict := range []bool{false, true} {
		request := llm.Request{OperationKey: "test-operation", Model: "amazon.nova-pro-v1:0", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "Hello"}}}}, Sampling: &llm.SamplingSpec{StopSequences: []string{"END", "\nSTOP"}}}
		call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Strict: strict})
		if err != nil {
			t.Fatal(err)
		}
		params := call.SDKParams.(bedrockruntime.ConverseInput)
		if params.InferenceConfig == nil || !reflect.DeepEqual(params.InferenceConfig.StopSequences, request.Sampling.StopSequences) {
			t.Fatalf("stop sequences lost: %#v", params.InferenceConfig)
		}
	}
}
func TestExplicitNoToolsNeverCompilesToAuto(t *testing.T) {
	adapter := &Adapter{endpointID: "test", profile: DefaultProfile("nova")}
	request := llm.Request{OperationKey: "test-operation", Model: "amazon.nova-pro-v1:0", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "Answer without tools"}}}}, Tools: []llm.Tool{{Name: "lookup", InputSchema: []byte(`{"type":"object"}`)}}, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNone}}
	for _, strict := range []bool{false, true} {
		_, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Strict: strict})
		if err == nil || !strings.Contains(err.Error(), "none") {
			t.Fatalf("explicit none must be rejected, got %v", err)
		}
	}
	request.Tools = nil
	if _, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Strict: true}); err != nil {
		t.Fatalf("no-tools request: %v", err)
	}
}

func TestCompileRejectsUnimplementedStructuredOutput(t *testing.T) {
	for _, native := range []bool{false, true} {
		profile := DefaultProfile("nova")
		if native {
			profile.Capabilities.Features[provider.FeatureStructuredOutput] = provider.Capability{State: provider.CapabilityNative}
		}
		adapter := &Adapter{endpointID: "test", profile: profile}
		for _, kind := range []llm.OutputKind{llm.OutputKindJSON, llm.OutputKindJSONSchema} {
			format := llm.OutputFormat{Kind: kind}
			if kind == llm.OutputKindJSONSchema {
				format.Name = "answer"
				format.Schema = []byte(`{"type":"object"}`)
			}
			request := llm.Request{OperationKey: "structured", Model: "amazon.nova-pro-v1:0", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "Hello"}}}}, Output: &llm.OutputSpec{Format: format}}
			for _, strict := range []bool{false, true} {
				_, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Strict: strict})
				if err == nil || !strings.Contains(err.Error(), "structured") {
					t.Fatalf("%s contract not rejected, native=%v strict=%v: %v", kind, native, strict, err)
				}
			}
		}
	}
}

func TestCompileRejectsUnimplementedSamplingControls(t *testing.T) {
	adapter := &Adapter{endpointID: "test", profile: DefaultProfile("nova")}
	topK, seed, penalty := 10, int64(42), 0.5
	for name, sampling := range map[string]*llm.SamplingSpec{
		"top_k": {TopK: &topK}, "seed": {Seed: &seed},
		"presence_penalty": {PresencePenalty: &penalty}, "frequency_penalty": {FrequencyPenalty: &penalty},
	} {
		t.Run(name, func(t *testing.T) {
			request := llm.Request{OperationKey: "sampling", Model: "amazon.nova-pro-v1:0", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "Hello"}}}}, Sampling: sampling}
			for _, strict := range []bool{false, true} {
				_, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Strict: strict})
				if err == nil || !strings.Contains(err.Error(), "sampling") {
					t.Fatalf("sampling control silently dropped, strict=%v: %v", strict, err)
				}
			}
		})
	}
}
