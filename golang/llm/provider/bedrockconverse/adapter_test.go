package bedrockconverse

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

type fakeConverse struct {
	input  *bedrockruntime.ConverseInput
	output *bedrockruntime.ConverseOutput
	err    error
}

func TestStreamingCapabilityCannotOutrunAdapterPort(t *testing.T) {
	profile := DefaultProfile("bedrock-converse-no-stream")
	if capability := profile.Capabilities.Features[provider.FeatureStreaming]; capability.State != provider.CapabilityUnsupported || capability.Reason == "" {
		t.Fatalf("default streaming capability = %#v, want unsupported with a reason", capability)
	}
	if _, ok := any((*Adapter)(nil)).(provider.StreamingAdapter); ok {
		t.Fatal("adapter advertises streaming capability without an OpenStream implementation")
	}
}

func (fake *fakeConverse) Converse(_ context.Context, input *bedrockruntime.ConverseInput, _ ...func(*bedrockruntime.Options)) (*bedrockruntime.ConverseOutput, error) {
	fake.input = input
	return fake.output, fake.err
}

func TestCompileAndInvokeConverse(t *testing.T) {
	fake := &fakeConverse{output: &bedrockruntime.ConverseOutput{
		Output: &types.ConverseOutputMemberMessage{Value: types.Message{Role: types.ConversationRoleAssistant, Content: []types.ContentBlock{
			&types.ContentBlockMemberText{Value: "Hello from Bedrock"},
		}}},
		StopReason:  types.StopReasonEndTurn,
		Usage:       &types.TokenUsage{InputTokens: aws.Int32(4), OutputTokens: aws.Int32(3)},
		ServiceTier: &types.ServiceTier{Type: types.ServiceTierTypePriority},
	}}
	adapter, err := New(&Client{converse: fake}, "bedrock-prod", DefaultProfile("nova"))
	if err != nil {
		t.Fatal(err)
	}
	// Nova Lite is Standard-only in Bedrock.  Use Nova Pro for the priority
	// contract so this fixture exercises a tier the documented model supports.
	request := llm.Request{OperationKey: "op-1", Model: "amazon.nova-pro-v1:0", ServiceClass: llm.ServiceClassPriority,
		Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "Hello"}}}}}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Query: provider.CapabilityQuery{Family: provider.FamilyBedrockConverse, EndpointID: "bedrock-prod", Model: request.Model}, Strict: true})
	if err != nil {
		t.Fatal(err)
	}
	params, ok := call.SDKParams.(bedrockruntime.ConverseInput)
	if !ok {
		t.Fatalf("SDK params type = %T", call.SDKParams)
	}
	if params.ModelId == nil || *params.ModelId != request.Model {
		t.Fatalf("model ID = %v, want %q", params.ModelId, request.Model)
	}
	if params.ServiceTier == nil || params.ServiceTier.Type != types.ServiceTierTypePriority {
		t.Fatalf("service tier = %#v, want priority", params.ServiceTier)
	}
	result, err := adapter.Invoke(context.Background(), call, provider.NopObserver{})
	if err != nil {
		t.Fatal(err)
	}
	if fake.input == nil || fake.input.ServiceTier.Type != types.ServiceTierTypePriority {
		t.Fatalf("invoked service tier = %#v, want priority", fake.input.ServiceTier)
	}
	if result.Response.Status != llm.ResponseStatusCompleted || result.Response.Usage.InputTokens != 4 || result.Response.Usage.OutputTokens != 3 {
		t.Fatalf("response = %#v", result.Response)
	}
	if len(result.Response.Output) != 1 {
		t.Fatalf("output length = %d, want one message", len(result.Response.Output))
	}
	message, ok := result.Response.Output[0].(llm.Message)
	if !ok || len(message.Content) != 1 || message.Content[0].(llm.TextPart).Text != "Hello from Bedrock" {
		t.Fatalf("output = %#v", result.Response.Output)
	}
}

func TestLowerToolCallUsesSmithyJSONDocument(t *testing.T) {
	profile, err := NewDefaultProfile("nova")
	if err != nil {
		t.Fatal(err)
	}
	request := llm.Request{Model: "amazon.nova-pro-v1:0", Input: []llm.Item{llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"city":"Sydney"}`)}}}
	input, err := lowerRequest(request, profile, string(types.ServiceTierTypeDefault), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(input.Messages) != 1 || len(input.Messages[0].Content) != 1 {
		t.Fatalf("lowered messages = %#v", input.Messages)
	}
	tool, ok := input.Messages[0].Content[0].(*types.ContentBlockMemberToolUse)
	if !ok || tool.Value.Input == nil {
		t.Fatalf("lowered tool block = %#v", input.Messages[0].Content[0])
	}
	encoded, err := tool.Value.Input.MarshalSmithyDocument()
	if err != nil || string(encoded) != `{"city":"Sydney"}` {
		t.Fatalf("tool input = %s, err=%v", encoded, err)
	}
}

func novaReasoningProfile() Profile {
	profile := DefaultProfile("nova-2-lite")
	profile.Capabilities.Features[provider.FeatureStructuredOutput] = provider.Capability{State: provider.CapabilityEmulated, Transform: jsonSchemaPromptTransform}
	profile.Capabilities.Features[provider.FeatureReasoning] = provider.Capability{State: provider.CapabilityNative}
	return profile
}

func novaReasoningRequest() llm.Request {
	return llm.Request{
		OperationKey: "nova-forecast", Model: "us.amazon.nova-2-lite-v1:0",
		Input:      []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "Forecast the two claims in order."}}}},
		Reasoning:  &llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, Effort: llm.ReasoningEffortMedium},
		ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNone},
		Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "claims", Strict: true,
			Schema: json.RawMessage(`{"type":"object","additionalProperties":false,"required":["claims"],"properties":{"claims":{"type":"array","minItems":2,"maxItems":2,"items":false,"prefixItems":[{"type":"object","additionalProperties":false,"required":["id","probability"],"properties":{"id":{"const":"first"},"probability":{"type":"number","minimum":0,"maximum":1}}},{"type":"object","additionalProperties":false,"required":["id","probability"],"properties":{"id":{"const":"second"},"probability":{"type":"number","minimum":0,"maximum":1}}}]}}}`),
		}},
	}
}

type converseRoundTrip func(*http.Request) (*http.Response, error)

func (roundTrip converseRoundTrip) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func TestNovaEmulatedJSONPreservesBoundsOrderAndReasoning(t *testing.T) {
	for _, test := range []struct {
		name  string
		text  string
		valid bool
	}{
		{"valid", `{"claims":[{"id":"first","probability":0.7},{"id":"second","probability":0.3}]}`, true},
		{"out_of_bounds", `{"claims":[{"id":"first","probability":1.2},{"id":"second","probability":0.3}]}`, false},
		{"reordered", `{"claims":[{"id":"second","probability":0.7},{"id":"first","probability":0.3}]}`, false},
		{"missing_claim", `{"claims":[{"id":"first","probability":0.7}]}`, false},
		{"malformed", "```json\n{}\n```", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			client, err := NewClient(context.Background(), ClientConfig{
				BaseURL:   "http://127.0.0.1",
				AWSConfig: aws.Config{Region: "us-east-2", Credentials: credentials.NewStaticCredentialsProvider("contract-access", "contract-secret", "")},
				HTTPClient: &http.Client{Transport: converseRoundTrip(func(request *http.Request) (*http.Response, error) {
					calls++
					var wire map[string]json.RawMessage
					if err := json.NewDecoder(request.Body).Decode(&wire); err != nil {
						t.Fatal(err)
					}
					var fields struct {
						ReasoningConfig struct {
							Type               string `json:"type"`
							MaxReasoningEffort string `json:"maxReasoningEffort"`
						} `json:"reasoningConfig"`
					}
					if err := json.Unmarshal(wire["additionalModelRequestFields"], &fields); err != nil {
						t.Fatal(err)
					}
					if fields.ReasoningConfig.Type != "enabled" || fields.ReasoningConfig.MaxReasoningEffort != "medium" {
						t.Fatalf("Nova reasoning request = %s", wire["additionalModelRequestFields"])
					}
					if _, exists := wire["outputConfig"]; exists {
						t.Fatal("emulated output sent native JSON grammar")
					}
					if _, exists := wire["toolConfig"]; exists {
						t.Fatal("tool-free emulation sent a tool configuration")
					}
					var system []struct {
						Text string `json:"text"`
					}
					if err := json.Unmarshal(wire["system"], &system); err != nil {
						t.Fatal(err)
					}
					if len(system) != 1 || !strings.Contains(system[0].Text, `"prefixItems"`) || !strings.Contains(system[0].Text, `"maximum":1`) {
						t.Fatalf("complete schema missing from prompt: %s", wire["system"])
					}
					body, err := json.Marshal(map[string]any{
						"output": map[string]any{"message": map[string]any{"role": "assistant", "content": []any{
							map[string]any{"reasoningContent": map[string]any{"reasoningText": map[string]any{"text": "[REDACTED]"}}},
							map[string]any{"text": test.text},
						}}},
						"stopReason": "end_turn",
						"usage":      map[string]any{"inputTokens": 10, "outputTokens": 120, "totalTokens": 130},
					})
					if err != nil {
						t.Fatal(err)
					}
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}, "X-Amzn-Requestid": {"nova-contract"}}, Body: io.NopCloser(strings.NewReader(string(body))), Request: request}, nil
				})},
			})
			if err != nil {
				t.Fatal(err)
			}
			profile := novaReasoningProfile()
			profile.Capabilities.Features[provider.FeatureToolCall] = provider.Capability{State: provider.CapabilityUnknown}
			adapter, err := New(client, "bedrock-prod", profile)
			if err != nil {
				t.Fatal(err)
			}
			call, err := adapter.Compile(context.Background(), provider.CompileInput{Strict: true, Request: novaReasoningRequest()})
			if err != nil {
				t.Fatal(err)
			}
			result, err := adapter.Invoke(context.Background(), call, nil)
			if !test.valid {
				var providerErr *provider.Error
				if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeProviderInvalidResponse || providerErr.Retry != provider.RetryNever || providerErr.Dispatch != provider.DispatchAccepted {
					t.Fatalf("invalid forecast must fail without retry after dispatch: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if result.Response.Status != llm.ResponseStatusCompleted || result.Response.Usage.OutputTokens != 120 || result.Response.Usage.ReasoningTokens != 0 {
					t.Fatalf("status or total billed output tokens changed: %#v", result.Response)
				}
				if len(result.Response.Output) != 2 {
					t.Fatalf("reasoning and answer not separated: %#v", result.Response.Output)
				}
				state, ok := result.Response.Output[0].(llm.ProviderState)
				if !ok || !strings.Contains(string(state.Opaque), "[REDACTED]") {
					t.Fatalf("opaque reasoning not preserved: %#v", result.Response.Output[0])
				}
				message, ok := result.Response.Output[1].(llm.Message)
				if !ok || len(message.Content) != 1 || message.Content[0].(llm.TextPart).Text != test.text {
					t.Fatalf("consumer JSON contaminated by reasoning: %#v", result.Response.Output[1])
				}
			}
			if calls != 1 {
				t.Fatalf("dispatched %d calls; want exactly one", calls)
			}
		})
	}
}

func TestNovaReasoningEffortContract(t *testing.T) {
	for _, effort := range []llm.ReasoningEffort{llm.ReasoningEffortLow, llm.ReasoningEffortMedium, llm.ReasoningEffortHigh} {
		t.Run(string(effort), func(t *testing.T) {
			request := novaReasoningRequest()
			request.Reasoning.Mode = ""
			request.Reasoning.Effort = effort
			adapter := &Adapter{endpointID: "bedrock-prod", profile: novaReasoningProfile()}
			call, err := adapter.Compile(context.Background(), provider.CompileInput{Strict: true, Request: request})
			if err != nil {
				t.Fatal(err)
			}
			params, _, _ := compiledParameters(call.SDKParams)
			wire, err := params.AdditionalModelRequestFields.MarshalSmithyDocument()
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]map[string]string
			if err := json.Unmarshal(wire, &fields); err != nil {
				t.Fatal(err)
			}
			if fields["reasoningConfig"]["type"] != "enabled" || fields["reasoningConfig"]["maxReasoningEffort"] != string(effort) {
				t.Fatalf("Nova effort contract = %s", wire)
			}
		})
	}
}

func TestNovaUnsupportedContractsFailBeforeDispatch(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*llm.Request, *Profile)
	}{
		{"unknown_transform", func(_ *llm.Request, p *Profile) {
			p.Capabilities.Features[provider.FeatureStructuredOutput] = provider.Capability{State: provider.CapabilityEmulated, Transform: "unknown"}
		}},
		{"unsupported_model", func(r *llm.Request, _ *Profile) { r.Model = "amazon.nova-lite-v1:0" }},
		{"maximum_effort", func(r *llm.Request, _ *Profile) { r.Reasoning.Effort = llm.ReasoningEffortMaximum }},
		{"minimal_effort", func(r *llm.Request, _ *Profile) { r.Reasoning.Effort = llm.ReasoningEffortMinimal }},
		{"missing_effort", func(r *llm.Request, _ *Profile) { r.Reasoning.Effort = "" }},
		{"adaptive_mode", func(r *llm.Request, _ *Profile) { r.Reasoning.Mode = llm.ReasoningModeAdaptive }},
		{"disabled_with_effort", func(r *llm.Request, _ *Profile) { r.Reasoning.Mode = llm.ReasoningModeDisabled }},
		{"token_budget", func(r *llm.Request, _ *Profile) { budget := 1000; r.Reasoning.TokenBudget = &budget }},
		{"summary", func(r *llm.Request, _ *Profile) { r.Reasoning.Summary = llm.ReasoningSummaryDetailed }},
		{"high_temperature", func(r *llm.Request, _ *Profile) {
			value := 0.7
			r.Reasoning.Effort = llm.ReasoningEffortHigh
			r.Sampling = &llm.SamplingSpec{Temperature: &value}
		}},
		{"high_top_p", func(r *llm.Request, _ *Profile) {
			value := 0.9
			r.Reasoning.Effort = llm.ReasoningEffortHigh
			r.Sampling = &llm.SamplingSpec{TopP: &value}
		}},
		{"high_top_k", func(r *llm.Request, _ *Profile) {
			value := 10
			r.Reasoning.Effort = llm.ReasoningEffortHigh
			r.Sampling = &llm.SamplingSpec{TopK: &value}
		}},
		{"unsupported_reasoning", func(_ *llm.Request, p *Profile) {
			p.Capabilities.Features[provider.FeatureReasoning] = provider.Capability{State: provider.CapabilityUnsupported}
		}},
		{"native_schema", func(_ *llm.Request, p *Profile) {
			p.Capabilities.Features[provider.FeatureStructuredOutput] = provider.Capability{State: provider.CapabilityNative}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			request, profile := novaReasoningRequest(), novaReasoningProfile()
			test.change(&request, &profile)
			fake := &fakeConverse{}
			adapter, err := New(&Client{converse: fake}, "bedrock-prod", profile)
			if err != nil {
				t.Fatal(err)
			}
			_, err = adapter.Compile(context.Background(), provider.CompileInput{Strict: true, Request: request})
			var providerErr *provider.Error
			if !errors.As(err, &providerErr) || providerErr.Dispatch != provider.DispatchNotDispatched || providerErr.Retry != provider.RetryNever || fake.input != nil {
				t.Fatalf("unsupported contract did not fail before dispatch: %v", err)
			}
		})
	}
}

func TestReasoningOutputRejectsUnknownBlocks(t *testing.T) {
	for _, block := range []types.ContentBlock{
		&types.UnknownUnionMember{Tag: "futureContent"},
		&types.ContentBlockMemberReasoningContent{Value: &types.UnknownUnionMember{Tag: "futureReasoning"}},
		&types.ContentBlockMemberReasoningContent{Value: &types.ReasoningContentBlockMemberReasoningText{}},
	} {
		adapter := &Adapter{profile: novaReasoningProfile()}
		_, err := adapter.liftResponse(provider.Call{}, &bedrockruntime.ConverseOutput{
			Output:     &types.ConverseOutputMemberMessage{Value: types.Message{Content: []types.ContentBlock{block, &types.ContentBlockMemberText{Value: "{}"}}}},
			StopReason: types.StopReasonEndTurn,
		}, "unknown-block")
		var providerErr *provider.Error
		if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeProviderInvalidResponse || providerErr.Retry != provider.RetryNever {
			t.Fatalf("unknown or malformed block %T was accepted: %v", block, err)
		}
	}
}
