package openaichat

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func wireMessages(t *testing.T, params openai.ChatCompletionNewParams) []map[string]any {
	t.Helper()
	encoded, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		Messages []map[string]any `json:"messages"`
	}
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	return wire.Messages
}

func TestEmptyLiftedAssistantReplyIsNotReplayed(t *testing.T) {
	filtered := openai.ChatCompletion{
		ID: "filtered", Model: "chat-model", ServiceTier: openai.ChatCompletionServiceTierDefault,
		Choices: []openai.ChatCompletionChoice{{FinishReason: "content_filter", Message: openai.ChatCompletionMessage{}}},
	}
	lifted, err := testProfile().liftResponse(provider.Call{ServiceClass: llm.ServiceClassStandard}, &filtered, "req")
	if err != nil {
		t.Fatal(err)
	}
	input := append([]llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "question"}}}}, lifted.Output...)
	input = append(input, llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "try again"}}})
	params, err := lowerRequest(llm.Request{Model: "chat-model", Input: input}, testProfile(), "default")
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range wireMessages(t, params) {
		if message["role"] == "assistant" && message["content"] == nil && message["tool_calls"] == nil {
			t.Fatalf("replayed an empty assistant message: %#v", message)
		}
	}

	// An assistant turn that only carries tool calls is still replayed.
	params, err = lowerRequest(llm.Request{Model: "chat-model", Input: []llm.Item{
		llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "look up"}}},
		llm.Message{Actor: llm.ActorModel},
		llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{}`)},
		llm.ToolResult{CallID: "call-1", Name: "lookup", Content: []llm.Part{llm.TextPart{Text: "ok"}}},
	}}, testProfile(), "default")
	if err != nil {
		t.Fatal(err)
	}
	if messages := wireMessages(t, params); len(messages) != 3 || messages[1]["tool_calls"] == nil {
		t.Fatalf("tool-call assistant turn = %#v", messages)
	}
}

func TestApplicationInstructionRoleFollowsProfile(t *testing.T) {
	instructions := []llm.Instruction{{Kind: llm.InstructionKindText, Level: llm.InstructionLevelApplication, Text: "be brief"}}
	direct := testProfile()
	params, err := lowerRequest(llm.Request{Model: "chat-model", Instructions: instructions}, direct, "default")
	if err != nil {
		t.Fatal(err)
	}
	if role := wireMessages(t, params)[0]["role"]; role != "developer" {
		t.Fatalf("default application role = %v, want developer", role)
	}
	compatible := testProfile()
	compatible.ApplicationInstructionRole = "system"
	params, err = lowerRequest(llm.Request{Model: "chat-model", Instructions: instructions}, compatible, "default")
	if err != nil {
		t.Fatal(err)
	}
	if role := wireMessages(t, params)[0]["role"]; role != "system" {
		t.Fatalf("compatible application role = %v, want system", role)
	}
	invalid := testProfile()
	invalid.ApplicationInstructionRole = "assistant"
	if _, err := NewProfile(invalid); err == nil {
		t.Fatal("invalid application instruction role accepted")
	}
	for name, build := range map[string]func() (Profile, error){
		"azure": func() (Profile, error) {
			return NewAzureProfile(AzureProfileConfig{ID: "azure", CapabilityVersion: "v1", BaseURL: "https://example.openai.azure.com", Deployment: "d", Capabilities: profileTestCapabilities("v1"), ServiceTiers: map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "default", llm.ServiceClassPriority: ""}, ActualServiceClasses: map[string]llm.ServiceClass{"default": llm.ServiceClassStandard}})
		},
		"exa": func() (Profile, error) {
			return NewExaProfile(ExaProfileConfig{ID: "exa", CapabilityVersion: "v1", BaseURL: exaBaseURL, Capabilities: profileTestCapabilities("v1"), ServiceTiers: map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "standard", llm.ServiceClassPriority: ""}, ActualServiceClasses: map[string]llm.ServiceClass{"standard": llm.ServiceClassStandard}})
		},
	} {
		profile, err := build()
		if err != nil {
			t.Fatalf("%s profile: %v", name, err)
		}
		if profile.applicationInstructionRole() != "system" {
			t.Fatalf("%s application instruction role = %q, want system", name, profile.applicationInstructionRole())
		}
	}
}

func TestStrictCompileRejectsMixedLevelsWhenApplicationUsesSystemRole(t *testing.T) {
	adapter := testAdapter(t)
	adapter.profile.ApplicationInstructionRole = "system"
	request := llm.Request{OperationKey: "op", Model: "chat-model", ServiceClass: llm.ServiceClassStandard, Instructions: []llm.Instruction{
		{Kind: llm.InstructionKindText, Level: llm.InstructionLevelPolicy, Text: "policy"},
		{Kind: llm.InstructionKindText, Level: llm.InstructionLevelApplication, Text: "application"},
	}}
	query := provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"}
	if _, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Query: query, Strict: true}); err == nil || !strings.Contains(err.Error(), "instruction hierarchy") || !isUnsupportedCapability(err) {
		t.Fatalf("strict mixed-level compile error = %v", err)
	}
	if _, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Query: query, Strict: false}); err != nil {
		t.Fatalf("best-effort mixed-level compile error = %v", err)
	}
}
