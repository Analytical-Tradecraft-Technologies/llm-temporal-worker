package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

// compactionWireMessage is one provider message of a compiled call: its role,
// its text and whether it carries a tool-use or tool-result block.
type compactionWireMessage struct {
	role  string
	text  string
	tools bool
}

// compactionWireMessages reads the messages a real adapter compiled, from the
// Converse input or from the Messages JSON body.
func compactionWireMessages(t *testing.T, call provider.Call) []compactionWireMessage {
	t.Helper()
	var messages []compactionWireMessage
	converse := func(input bedrockruntime.ConverseInput) []compactionWireMessage {
		for _, message := range input.Messages {
			wire := compactionWireMessage{role: string(message.Role)}
			for _, block := range message.Content {
				switch value := block.(type) {
				case *types.ContentBlockMemberText:
					wire.text += value.Value
				case *types.ContentBlockMemberToolUse, *types.ContentBlockMemberToolResult:
					wire.tools = true
				}
			}
			messages = append(messages, wire)
		}
		return messages
	}
	switch params := call.SDKParams.(type) {
	case bedrockruntime.ConverseInput:
		return converse(params)
	case *bedrockruntime.ConverseInput:
		return converse(*params)
	}
	encoded, err := json.Marshal(call.SDKParams)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Messages []struct {
			Role    string `json:"role"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(encoded, &body); err != nil || len(body.Messages) == 0 {
		t.Fatalf("compiled %T has no readable messages: %v: %s", call.SDKParams, err, encoded)
	}
	for _, message := range body.Messages {
		wire := compactionWireMessage{role: message.Role}
		for _, block := range message.Content {
			wire.text += block.Text
			wire.tools = wire.tools || block.Type == "tool_use" || block.Type == "tool_result"
		}
		messages = append(messages, wire)
	}
	return messages
}

// TestCloudCompactionRequestAndCheckpointShapeOnUserFirstFamilies compacts a
// tool-using conversation that ends on a model message and then continues
// from the compaction checkpoint, compiling every call with the real
// Messages and Converse adapters. The summarizer must receive the prefix as
// one quoted user message (no assistant prefill, no tool blocks without tool
// definitions), and the turn after compaction must open with the user role.
func TestCloudCompactionRequestAndCheckpointShapeOnUserFirstFamilies(t *testing.T) {
	for _, route := range hierarchyRoutes() {
		if route.family == provider.FamilyOpenAIChat {
			continue
		}
		t.Run(route.name, func(t *testing.T) {
			f := boundedCloud(t, false, func(b *budgetPlanningFixture) {
				route.configure(b)
				b.prices(t, []pricing.Entry{b.entry})
			})
			var calls []provider.Call
			f.cap.Adapters = engine.AdapterMap{"endpoint": hierarchyInvokeAdapter{Adapter: route.adapter(t), invoke: func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
				calls = append(calls, call)
				return f.adapter.invoke(ctx, call, o)
			}}}
			f.restart(t)
			f.answerWith(
				[]llm.Item{outputShapeCall("call_1")},
				[]llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "model answer"}}}},
				[]llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "the summary"}}}},
				[]llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "after"}}}},
			)
			policy := json.RawMessage(`{"recent_turns":0}`)
			f.request.SettingsPatch.CompactionPolicy.Set = &policy
			first := f.finish(t)
			f.request.SettingsPatch = llm.SettingsPatchV1{}
			f.continueFrom(first, "second", outputShapeResult("call_1"))
			parent := f.finish(t)

			f.now = f.now.Add(time.Minute)
			request := llm.CompactRequestV1{OperationKey: "compact", Context: f.request.Context, Parent: parent.Generate.Checkpoint.Handle}
			ctx := context.Background()
			v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Compact: &request})
			boundedState(t, v, err, llm.ExecutionBudgetRequired)
			ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: request.Context}
			v, err = f.runtime.AcquireBudgetV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionAcquired)
			v, err = f.runtime.CompactStepV1(ctx, request)
			boundedState(t, v, err, llm.ExecutionProviderCompleted)
			v, err = f.runtime.CompleteExecutionV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionCompleted)
			if v.Compact == nil || len(calls) != 3 {
				t.Fatalf("compaction did not dispatch exactly once: calls=%d", len(calls))
			}
			summarizer := compactionWireMessages(t, calls[2])
			if len(summarizer) != 1 || summarizer[0].role != "user" || summarizer[0].tools {
				t.Fatalf("summarizer messages = %+v", summarizer)
			}
			for _, quoted := range []string{"-----BEGIN TRANSCRIPT ", `model tool call id="call_1" name="lookup"`, `tool result call_id="call_1"`, "model answer", "Write the summary of this transcript now"} {
				if !strings.Contains(summarizer[0].text, quoted) {
					t.Fatalf("summarizer text lacks %q: %s", quoted, summarizer[0].text)
				}
			}

			f.request.OperationKey, f.request.Parent = "after-compaction", &v.Compact.Checkpoint.Handle
			f.request.Append = []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "next question"}}}}
			f.now = f.now.Add(time.Minute)
			f.finish(t)
			if len(calls) != 4 {
				t.Fatalf("calls = %d", len(calls))
			}
			following := compactionWireMessages(t, calls[3])
			var text string
			for _, message := range following {
				if message.role != "user" {
					t.Fatalf("turn after compaction is not user-only: %+v", following)
				}
				text += message.text
			}
			if !strings.Contains(text, "the summary") || !strings.Contains(text, "next question") {
				t.Fatalf("turn after compaction lost the summary or the new input: %+v", following)
			}
		})
	}
}
