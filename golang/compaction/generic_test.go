package compaction

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func TestPolicyDefaultsAndValidation(t *testing.T) {
	policy := DefaultPolicy()
	if err := policy.Validate(); err != nil {
		t.Fatal(err)
	}
	policy.TargetTokens = policy.TriggerTokens
	if err := policy.Validate(); err == nil {
		t.Fatal("target at trigger threshold unexpectedly accepted")
	}
	policy = DefaultPolicy()
	policy.Version = "future"
	if err := policy.Validate(); err == nil {
		t.Fatal("unknown policy version unexpectedly accepted")
	}
}

func TestPrepareRequestIsolatesApplicationSettings(t *testing.T) {
	policy := DefaultPolicy()
	max := 100
	request := llm.Request{
		OperationKey: "generate-1", Model: "model-1",
		Tools:      []llm.Tool{{Name: "lookup"}},
		ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceRequired, Name: "lookup"},
		Output:     &llm.OutputSpec{MaxTokens: &max, Format: llm.OutputFormat{Kind: llm.OutputKindJSON}},
		Reasoning:  &llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, TokenBudget: &max},
		Input:      []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}},
	}
	original := request
	compact, err := PrepareRequest(request, "generate-1/compact", request.Input, policy)
	if err != nil {
		t.Fatal(err)
	}
	if len(compact.Tools) != 0 || compact.ToolPolicy.Mode != llm.ToolChoiceNone || compact.Continuation != nil || compact.Reasoning != nil {
		t.Fatalf("isolated request retained application controls: %#v", compact)
	}
	if compact.Output == nil || compact.Output.Format.Kind != llm.OutputKindText || compact.Output.MaxTokens == nil || *compact.Output.MaxTokens != policy.OutputReserveTokens {
		t.Fatalf("isolated output = %#v", compact.Output)
	}
	if len(compact.Instructions) < 2 || !strings.Contains(compact.Instructions[0].Text, "Do not call tools") || !strings.Contains(compact.Instructions[1].Text, "balanced") {
		t.Fatalf("compaction prompt/style missing: %#v", compact.Instructions)
	}
	if len(original.Tools) != 1 || original.ToolPolicy.Mode != llm.ToolChoiceRequired || original.Output.Format.Kind != llm.OutputKindJSON {
		t.Fatalf("source request mutated: %#v", original)
	}
}

func TestPrepareRequestPreservesGenerateSettingsForFollowingGenerate(t *testing.T) {
	maxTokens := 4096
	temperature := 0.2
	topP := 0.9
	topK := 20
	seed := int64(42)
	stop := []string{"<stop>"}
	reasoningBudget := 512
	expiresAt := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	source := llm.Request{
		APIVersion:   "llm.temporal/v1",
		OperationKey: "generate-settings",
		Context: llm.RequestContext{
			Tenant: "tenant-a", Project: "project-a", Actor: "workflow:settings",
			Tags: map[string]string{"environment": "test"},
		},
		Model:                 "model-1",
		ServiceClass:          llm.ServiceClassPriority,
		ServiceClassFallbacks: []llm.ServiceClass{llm.ServiceClassStandard, llm.ServiceClassEconomy},
		Portability:           llm.PortabilityBestEffort,
		Instructions: []llm.Instruction{
			{Kind: llm.InstructionKindText, Level: llm.InstructionLevelApplication, Text: "preserve this"},
			{Kind: llm.InstructionKindText, Level: llm.InstructionLevelPolicy, Text: "policy"},
		},
		Input:      []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "prefix"}}}},
		Tools:      []llm.Tool{{Name: "lookup", InputSchema: json.RawMessage(`{"type":"object"}`)}},
		ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNamed, Name: "lookup", Parallel: true},
		Output: &llm.OutputSpec{MaxTokens: &maxTokens, Format: llm.OutputFormat{
			Kind: llm.OutputKindJSONSchema, Name: "answer", Description: "typed answer", Strict: true,
			Schema: json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"}}}`),
		}},
		Sampling: &llm.SamplingSpec{
			Temperature: &temperature, TopP: &topP, TopK: &topK, Seed: &seed,
			StopSequences: stop,
		},
		Reasoning:    &llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, TokenBudget: &reasoningBudget},
		Continuation: &llm.Continuation{Handle: "checkpoint-parent", EndpointID: "endpoint-1", Model: "model-1", ExpiresAt: &expiresAt},
		Extensions:   map[string]json.RawMessage{"client": json.RawMessage(`{"version":1}`)},
	}
	originalJSON, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	compact, err := PrepareRequest(source, "generate-settings/compact", source.Input, DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	unchangedJSON, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(originalJSON, unchangedJSON) {
		t.Fatal("PrepareRequest mutated the source Generate settings")
	}
	if compact.Model != source.Model || compact.ServiceClass != source.ServiceClass ||
		!reflect.DeepEqual(compact.ServiceClassFallbacks, source.ServiceClassFallbacks) ||
		compact.Portability != source.Portability || !reflect.DeepEqual(compact.Context, source.Context) ||
		!reflect.DeepEqual(compact.Instructions[2:], source.Instructions) ||
		!reflect.DeepEqual(compact.Sampling, source.Sampling) || !reflect.DeepEqual(compact.Extensions, source.Extensions) {
		t.Fatal("compaction request did not preserve Generate routing and sampling settings")
	}

	// The compaction child stores no application settings of its own. A later
	// Generate starts from the materialized parent settings, so every setting
	// below must remain available after replacing only the operation and input.
	following := source
	following.OperationKey = "generate-settings/after-compact"
	following.Input = []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "summary"}}}}
	if following.Model != source.Model || following.ServiceClass != source.ServiceClass ||
		!reflect.DeepEqual(following.ServiceClassFallbacks, source.ServiceClassFallbacks) ||
		following.Portability != source.Portability || !reflect.DeepEqual(following.Instructions, source.Instructions) ||
		!reflect.DeepEqual(following.Tools, source.Tools) || !reflect.DeepEqual(following.ToolPolicy, source.ToolPolicy) ||
		!reflect.DeepEqual(following.Output, source.Output) || !reflect.DeepEqual(following.Sampling, source.Sampling) ||
		!reflect.DeepEqual(following.Reasoning, source.Reasoning) || !reflect.DeepEqual(following.Continuation, source.Continuation) ||
		!reflect.DeepEqual(following.Extensions, source.Extensions) {
		t.Fatal("following Generate did not preserve parent settings")
	}
	if compact.ToolPolicy.Mode != llm.ToolChoiceNone || compact.Output == nil || compact.Output.Format.Kind != llm.OutputKindText || compact.Continuation != nil || compact.Reasoning != nil {
		t.Fatalf("compaction isolation changed the parent settings contract: %#v", compact)
	}
}

func TestPlainTextSummaryRejectsToolAndBoundsOutput(t *testing.T) {
	response := llm.Response{Status: llm.ResponseStatusCompleted, Output: []llm.Item{
		llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "summary"}}},
	}}
	if got, err := PlainTextSummary(response, 64); err != nil || got != "summary" {
		t.Fatalf("summary = %q, err=%v", got, err)
	}
	response.Output = append(response.Output, llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{}`)})
	if _, err := PlainTextSummary(response, 64); err == nil {
		t.Fatal("tool call accepted as compaction output")
	}
	response.Output = []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: strings.Repeat("x", 65)}}}}
	if _, err := PlainTextSummary(response, 64); err == nil {
		t.Fatal("oversized summary accepted")
	}
}

func TestPromptVersionIsPinned(t *testing.T) {
	value, err := Prompt(PromptVersion)
	if err != nil || !strings.Contains(value, "Do not call tools") {
		t.Fatalf("prompt = %q, err=%v", value, err)
	}
	if _, err := Prompt("future"); err == nil {
		t.Fatal("unknown prompt version accepted")
	}
}

func TestFlattenSummarizerInstructionsOnlyForApplicationCallers(t *testing.T) {
	input := []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}
	for _, test := range []struct {
		name    string
		levels  []llm.InstructionLevel
		flatten bool
	}{
		{name: "none"},
		{name: "application", levels: []llm.InstructionLevel{llm.InstructionLevelApplication}, flatten: true},
		{name: "unset", levels: []llm.InstructionLevel{""}, flatten: true},
		{name: "policy", levels: []llm.InstructionLevel{llm.InstructionLevelPolicy}},
		{name: "mixed", levels: []llm.InstructionLevel{llm.InstructionLevelPolicy, llm.InstructionLevelApplication}},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := llm.Request{OperationKey: "generate-1", Model: "model-1", Input: input}
			for _, level := range test.levels {
				request.Instructions = append(request.Instructions, llm.Instruction{Kind: llm.InstructionKindText, Level: level, Text: "be helpful"})
			}
			compact, err := PrepareRequest(request, "generate-1/compact", input, DefaultPolicy())
			if err != nil {
				t.Fatal(err)
			}
			for index := 0; index < 2; index++ {
				if got := compact.Instructions[index].Level; got != llm.InstructionLevelPolicy {
					t.Fatalf("PrepareRequest summarizer instruction %d level = %q, want policy", index, got)
				}
			}
			flattened := FlattenSummarizerInstructions(compact)
			want := llm.InstructionLevelPolicy
			if test.flatten {
				want = llm.InstructionLevelApplication
			}
			for index := 0; index < 2; index++ {
				if got := flattened.Instructions[index].Level; got != want {
					t.Fatalf("flattened summarizer instruction %d level = %q, want %q", index, got, want)
				}
				if compact.Instructions[index].Level != llm.InstructionLevelPolicy {
					t.Fatal("FlattenSummarizerInstructions mutated its input")
				}
			}
			if !reflect.DeepEqual(flattened.Instructions[2:], compact.Instructions[2:]) {
				t.Fatalf("caller instructions changed: %#v", flattened.Instructions[2:])
			}
		})
	}
	ordinary := llm.Request{Instructions: []llm.Instruction{
		{Kind: llm.InstructionKindText, Level: llm.InstructionLevelPolicy, Text: "policy"},
		{Kind: llm.InstructionKindText, Level: llm.InstructionLevelPolicy, Text: "Summary style: x"},
		{Kind: llm.InstructionKindText, Level: llm.InstructionLevelApplication, Text: "app"},
	}}
	if got := FlattenSummarizerInstructions(ordinary); !reflect.DeepEqual(got, ordinary) {
		t.Fatalf("non-summarizer request changed: %#v", got)
	}
}
