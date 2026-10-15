package bedrockconverse

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// Converse has no sequential tool-use control, so Parallel false is refused in
// strict mode and dropped with a diagnostic in best-effort mode, for every
// choice that keeps tool definitions.
func TestCompileSequentialToolUsePolicy(t *testing.T) {
	adapter := &Adapter{endpointID: "test", profile: DefaultProfile("nova")}
	choices := map[string]llm.ToolPolicy{
		"automatic": {Mode: llm.ToolChoiceAuto},
		"default":   {},
		"required":  {Mode: llm.ToolChoiceRequired},
		"named":     {Mode: llm.ToolChoiceNamed, Name: "lookup"},
	}
	for name, choice := range choices {
		for _, parallel := range []bool{false, true} {
			for _, strict := range []bool{false, true} {
				policy := choice
				policy.Parallel = parallel
				request := llm.Request{OperationKey: "parallel-policy", Model: "amazon.nova-pro-v1:0",
					Input:      []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "Look up Sydney"}}}},
					Tools:      []llm.Tool{{Name: "lookup", InputSchema: json.RawMessage(`{"type":"object","properties":{}}`)}},
					ToolPolicy: policy}
				call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Strict: strict})
				if strict && !parallel {
					var mapped *provider.Error
					if !errors.As(err, &mapped) || mapped.Code != provider.CodeUnsupportedCapability || mapped.Dispatch != provider.DispatchNotDispatched || !strings.Contains(err.Error(), "parallel false") {
						t.Fatalf("%s strict parallel=false: got %v", name, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("%s parallel=%v strict=%v: %v", name, parallel, strict, err)
				}
				if params := call.SDKParams.(bedrockruntime.ConverseInput); params.ToolConfig == nil || params.ToolConfig.ToolChoice == nil || params.AdditionalModelRequestFields != nil {
					t.Fatalf("%s parallel=%v strict=%v: tool configuration = %#v", name, parallel, strict, params)
				}
				if want := !strict && !parallel; call.Metadata.SequentialToolUseDropped != want {
					t.Fatalf("%s parallel=%v strict=%v: dropped = %v", name, parallel, strict, call.Metadata.SequentialToolUseDropped)
				}
				lifted, err := adapter.liftResponse(call, &bedrockruntime.ConverseOutput{
					Output:     &types.ConverseOutputMemberMessage{Value: types.Message{Content: []types.ContentBlock{&types.ContentBlockMemberText{Value: "answer"}}}},
					StopReason: types.StopReasonEndTurn, ServiceTier: &types.ServiceTier{Type: types.ServiceTierTypeDefault}}, "request-id", nil)
				if err != nil {
					t.Fatal(err)
				}
				if call.Metadata.SequentialToolUseDropped {
					if len(lifted.Diagnostics) != 1 || lifted.Diagnostics[0].Code != "tool_policy_parallel_dropped" || lifted.Diagnostics[0].Severity != llm.DiagnosticWarning {
						t.Fatalf("%s best effort: diagnostics = %#v", name, lifted.Diagnostics)
					}
				} else if len(lifted.Diagnostics) != 0 {
					t.Fatalf("%s parallel=%v strict=%v: unexpected diagnostics %#v", name, parallel, strict, lifted.Diagnostics)
				}
			}
		}
	}
}

// The none choice with definitions keeps its own rejection whatever Parallel
// says, and a request without tools carries no tool-use constraint at all.
func TestCompileSequentialToolUseLeavesNoneAndToollessRequests(t *testing.T) {
	adapter := &Adapter{endpointID: "test", profile: DefaultProfile("nova")}
	input := []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "Hello"}}}}
	tools := []llm.Tool{{Name: "lookup", InputSchema: json.RawMessage(`{"type":"object"}`)}}
	for _, parallel := range []bool{false, true} {
		for _, strict := range []bool{false, true} {
			request := llm.Request{OperationKey: "none-policy", Model: "amazon.nova-pro-v1:0", Input: input, Tools: tools, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNone, Parallel: parallel}}
			if _, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Strict: strict}); err == nil || !strings.Contains(err.Error(), "tool policy none") {
				t.Fatalf("none with tools parallel=%v strict=%v: got %v", parallel, strict, err)
			}
			request.Tools = nil
			call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Strict: strict})
			if err != nil || call.Metadata.SequentialToolUseDropped {
				t.Fatalf("toolless request parallel=%v strict=%v: dropped=%v err=%v", parallel, strict, call.Metadata.SequentialToolUseDropped, err)
			}
		}
	}
}
