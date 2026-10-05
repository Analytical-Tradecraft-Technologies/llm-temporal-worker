package openairesponses

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/openai/openai-go/v3/responses"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

const encryptedReasoningInclude = "reasoning.encrypted_content"

// storagePolicyAdapter builds the direct OpenAI adapter the way the production
// factory does: one option carrying the endpoint's provider_storage policy.
func storagePolicyAdapter(t *testing.T, permitted bool, body []byte) *ModelListerAdapter {
	t.Helper()
	fixture := newFixtureAdapterWithTransport(t, body, nil)
	adapter, err := NewOpenAIAdapter(fixture.client, "openai-prod", "cap-test", WithProviderStoragePermitted(permitted))
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func compileWire(t *testing.T, adapter *ModelListerAdapter, request llm.Request, strict bool) (provider.Call, map[string]any) {
	t.Helper()
	request.Model = "gpt-contract"
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: request.Model},
		Strict:  strict,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return call, marshalParams(t, call.SDKParams.(responses.ResponseNewParams))
}

func wireIncludes(wire map[string]any) []string {
	raw, _ := wire["include"].([]any)
	values := make([]string, 0, len(raw))
	for _, value := range raw {
		values = append(values, value.(string))
	}
	return values
}

func wireReasoningItems(wire map[string]any) []map[string]any {
	var items []map[string]any
	for _, raw := range wire["input"].([]any) {
		if item, ok := raw.(map[string]any); ok && item["type"] == "reasoning" {
			items = append(items, item)
		}
	}
	return items
}

func reasoningState(t *testing.T, id string, encrypted any) llm.ProviderState {
	t.Helper()
	item := map[string]any{"id": id, "type": "reasoning", "summary": []any{}}
	if encrypted != "absent" {
		item["encrypted_content"] = encrypted
	}
	opaque, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	return llm.ProviderState{Provider: "openai", EndpointFamily: "responses", MediaType: reasoningStateMediaType, Opaque: opaque}
}

func humanText(text string) llm.Message {
	return llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}}
}

func modelText(text string) llm.Message {
	return llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: text}}}
}

func TestStorageDeniedRequestsEncryptedReasoning(t *testing.T) {
	reasoning := &llm.ReasoningSpec{Effort: llm.ReasoningEffortLow}
	extension := func(body string) map[string]json.RawMessage {
		return map[string]json.RawMessage{"openai.responses": json.RawMessage(body)}
	}
	for _, test := range []struct {
		name      string
		permitted bool
		request   llm.Request
		want      []string
	}{
		{name: "reasoning configured", request: llm.Request{Reasoning: reasoning}, want: []string{encryptedReasoningInclude}},
		{
			name:    "caller include is preserved",
			request: llm.Request{Reasoning: reasoning, Extensions: extension(`{"include":["message.output_text.logprobs"]}`)},
			want:    []string{"message.output_text.logprobs", encryptedReasoningInclude},
		},
		{
			name:    "caller include is not duplicated",
			request: llm.Request{Reasoning: reasoning, Extensions: extension(`{"include":["reasoning.encrypted_content","message.output_text.logprobs","reasoning.encrypted_content"]}`)},
			want:    []string{encryptedReasoningInclude, "message.output_text.logprobs"},
		},
		// A model that does not reason may reject the include, so a request
		// with no sign of reasoning is left alone.
		{name: "no reasoning", request: llm.Request{}, want: []string{}},
		{name: "reasoning disabled", request: llm.Request{Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeDisabled}}, want: []string{}},
		{name: "storage permitted", permitted: true, request: llm.Request{Reasoning: reasoning}, want: []string{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := test.request
			request.OperationKey = "first-turn"
			request.Input = []llm.Item{humanText("question")}
			_, wire := compileWire(t, storagePolicyAdapter(t, test.permitted, nil), request, true)
			if got := wireIncludes(wire); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("include = %v, want %v", got, test.want)
			}
			if !test.permitted && wire["store"] != false {
				t.Fatalf("store = %v, want false", wire["store"])
			}
		})
	}
}

func TestStorageDeniedReplaysLiftedEncryptedReasoning(t *testing.T) {
	var body map[string]any
	if err := json.Unmarshal(readContractFixture(t, "openai-responses", "response.completed.json"), &body); err != nil {
		t.Fatal(err)
	}
	body["output"] = []any{
		map[string]any{"id": "rs-1", "type": "reasoning", "status": "completed", "summary": []any{}, "encrypted_content": "sealed-reasoning"},
		map[string]any{"id": "msg-1", "type": "message", "role": "assistant", "status": "completed", "content": []any{
			map[string]any{"type": "output_text", "text": "hello", "annotations": []any{}, "logprobs": []any{}},
		}},
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	adapter := storagePolicyAdapter(t, false, encoded)
	first := llm.Request{OperationKey: "first-turn", Reasoning: &llm.ReasoningSpec{Effort: llm.ReasoningEffortLow}, Input: []llm.Item{humanText("question")}}
	call, _ := compileWire(t, adapter, first, true)
	result, err := adapter.Invoke(context.Background(), call, nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}

	input := append([]llm.Item{humanText("question")}, result.Response.Output...)
	input = append(input, humanText("follow-up"))
	// The second turn configures no reasoning: the replayed state alone keeps
	// the conversation stateless-replayable.
	_, wire := compileWire(t, adapter, llm.Request{OperationKey: "second-turn", Input: input}, true)
	items := wireReasoningItems(wire)
	if len(items) != 1 || items[0]["id"] != "rs-1" || items[0]["encrypted_content"] != "sealed-reasoning" {
		t.Fatalf("replayed reasoning items = %v, want rs-1 with its encrypted content", items)
	}
	if got := wireIncludes(wire); !reflect.DeepEqual(got, []string{encryptedReasoningInclude}) {
		t.Fatalf("second-turn include = %v", got)
	}
	if wire["store"] != false {
		t.Fatalf("store = %v, want false", wire["store"])
	}
}

func TestStorageDeniedDropsReasoningWithoutEncryptedContent(t *testing.T) {
	for _, encrypted := range []any{nil, "", "absent"} {
		for _, strict := range []bool{true, false} {
			input := []llm.Item{humanText("question"), reasoningState(t, "rs-1", encrypted), modelText("answer"), humanText("follow-up")}
			_, wire := compileWire(t, storagePolicyAdapter(t, false, nil), llm.Request{OperationKey: "second-turn", Input: input}, strict)
			if items := wireReasoningItems(wire); len(items) != 0 {
				t.Fatalf("encrypted_content=%v strict=%v: unresolvable reasoning item sent: %v", encrypted, strict, items)
			}
			if got := len(wire["input"].([]any)); got != 3 {
				t.Fatalf("encrypted_content=%v strict=%v: input items = %d, want the three messages", encrypted, strict, got)
			}
			// The transcript shows the model reasons, so the next response is
			// asked for content that can be replayed.
			if got := wireIncludes(wire); !reflect.DeepEqual(got, []string{encryptedReasoningInclude}) {
				t.Fatalf("include = %v", got)
			}

			// With provider storage permitted the ID reference still resolves.
			_, wire = compileWire(t, storagePolicyAdapter(t, true, nil), llm.Request{OperationKey: "second-turn", Input: input}, strict)
			if items := wireReasoningItems(wire); len(items) != 1 {
				t.Fatalf("storage permitted: reasoning items = %v, want the stored reference", items)
			}
		}
	}
}

// Omitting a bare item leaves the reasoning item before it still followed by
// the output they produced, so that one stays replayable.
func TestStorageDeniedKeepsSealedReasoningBeforeBareItem(t *testing.T) {
	for name, tail := range map[string]llm.Item{
		"message":   modelText("answer"),
		"tool call": llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{}`)},
	} {
		input := []llm.Item{humanText("question"), reasoningState(t, "rs-1", "sealed-rs-1"), reasoningState(t, "rs-2", nil), tail}
		_, wire := compileWire(t, storagePolicyAdapter(t, false, nil), llm.Request{OperationKey: "replay", Input: input}, true)
		items := wireReasoningItems(wire)
		if len(items) != 1 || items[0]["id"] != "rs-1" || items[0]["encrypted_content"] != "sealed-rs-1" {
			t.Fatalf("%s: replayed reasoning = %v, want only rs-1 with its encrypted content", name, items)
		}
	}
}

func TestDanglingReasoningIsNotReplayed(t *testing.T) {
	sealed := func(id string) llm.ProviderState { return reasoningState(t, id, "sealed-"+id) }
	call := llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{}`)}
	for _, test := range []struct {
		name  string
		input []llm.Item
		want  []string
	}{
		{name: "reasoning-only response then next turn", input: []llm.Item{humanText("question"), sealed("rs-1"), humanText("follow-up")}},
		{name: "reasoning is the last item", input: []llm.Item{humanText("question"), sealed("rs-1")}},
		{name: "followed only by a tool result", input: []llm.Item{humanText("question"), sealed("rs-1"), llm.ToolResult{CallID: "call-1", Content: []llm.Part{llm.TextPart{Text: "ok"}}}}},
		{name: "followed by its message", input: []llm.Item{humanText("question"), sealed("rs-1"), modelText("answer"), humanText("follow-up")}, want: []string{"rs-1"}},
		{name: "followed by its tool call", input: []llm.Item{humanText("question"), sealed("rs-1"), call}, want: []string{"rs-1"}},
		{name: "consecutive reasoning before a message", input: []llm.Item{humanText("question"), sealed("rs-1"), sealed("rs-2"), modelText("answer")}, want: []string{"rs-1", "rs-2"}},
		{name: "earlier turn kept, dangling tail dropped", input: []llm.Item{humanText("question"), sealed("rs-1"), modelText("answer"), humanText("again"), sealed("rs-2"), humanText("follow-up")}, want: []string{"rs-1"}},
		// References and empty model messages have no wire form, so they
		// cannot be the output a reasoning item is followed by.
		{name: "followed only by a skipped reference", input: []llm.Item{humanText("question"), sealed("rs-1"), llm.Reference{URI: "https://example.com/source"}, humanText("follow-up")}},
		{name: "followed only by a skipped empty model message", input: []llm.Item{humanText("question"), sealed("rs-1"), llm.Message{Actor: llm.ActorModel}, humanText("follow-up")}},
		{name: "skipped items before its message", input: []llm.Item{humanText("question"), sealed("rs-1"), llm.Reference{URI: "https://example.com/source"}, llm.Message{Actor: llm.ActorModel}, modelText("answer")}, want: []string{"rs-1"}},
	} {
		for _, permitted := range []bool{false, true} {
			_, wire := compileWire(t, storagePolicyAdapter(t, permitted, nil), llm.Request{OperationKey: "replay", Input: test.input}, true)
			var got []string
			for _, item := range wireReasoningItems(wire) {
				got = append(got, item["id"].(string))
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("%s (storage permitted=%v): replayed reasoning = %v, want %v", test.name, permitted, got, test.want)
			}
		}
	}
}
