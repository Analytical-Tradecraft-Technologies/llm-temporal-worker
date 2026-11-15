package messagestest

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

// CompileTests runs every shared compile test against compile, each as a
// subtest named after it.
func CompileTests(t *testing.T, compile Compile) {
	for _, test := range []struct {
		name string
		run  func(*testing.T, Compile)
	}{
		{"RejectsIncompatibleManualThinkingCombinations", ManualThinkingCombinations},
		{"RejectsSamplingTheModelRefuses", SamplingTheModelRefuses},
		{"KeepsSamplingTheModelAccepts", SamplingTheModelAccepts},
		{"SendsExtraHighEffortToModelsThatAcceptIt", ExtraHighEffortByModel},
		{"SkipsReplayedEmptyModelMessage", SkipsReplayedEmptyModelMessage},
		{"SkipsReplayedReferenceAnnotations", SkipsReplayedReferenceAnnotations},
		{"ModelessReasoningNeverEnablesThinking", ModelessReasoningNeverEnablesThinking},
		{"ReasoningSummaryMapsToThinkingDisplay", ReasoningSummaryMapsToThinkingDisplay},
		{"ExtraHighReturnsUnsupportedCapability", ExtraHighReturnsUnsupportedCapability},
		{"DisabledReasoningIgnoresEffort", DisabledReasoningIgnoresEffort},
		{"StrictRejectsMixedInstructionLevels", StrictRejectsMixedInstructionLevels},
	} {
		t.Run(test.name, func(t *testing.T) { test.run(t, compile) })
	}
}

// Lower lowers request with the family's lowering on its contract profile
// and returns the SDK parameters.
type Lower func(t *testing.T, request llm.Request) (any, error)

func lowerToJSON(t *testing.T, lower Lower, request llm.Request) []byte {
	t.Helper()
	params, err := lower(t, request)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// NoToolsPolicyOmitsTools checks that a request with the none tool policy
// and no tools, as compaction sends, lowers without tools or tool_choice.
func NoToolsPolicyOmitsTools(t *testing.T, lower Lower) {
	request := llm.Request{Model: "contract-model", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "Summarize this conversation"}}}}, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNone}}
	raw := lowerToJSON(t, lower, request)
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"tools", "tool_choice"} {
		if _, exists := wire[key]; exists {
			t.Fatalf("unexpected %s in %s", key, raw)
		}
	}
}

// ToolHistoryPreservesJSONNumbers checks that lowering a replayed tool call
// keeps the exact numbers of its arguments.
func ToolHistoryPreservesJSONNumbers(t *testing.T, lower Lower) {
	adaptertest.ToolHistoryPreservesJSONNumbers(t, func(t *testing.T, request llm.Request) []byte {
		var wire struct {
			Messages []struct {
				Content []struct {
					Input json.RawMessage `json:"input"`
				} `json:"content"`
			} `json:"messages"`
		}
		if err := json.Unmarshal(lowerToJSON(t, lower, request), &wire); err != nil {
			t.Fatal(err)
		}
		return wire.Messages[0].Content[0].Input
	})
}

// InvokePreservesExactSchemaNumbers checks that a tool input schema and an
// output schema reach the provider with numbers a float64 cannot hold and
// their required properties first. send compiles and invokes a strict
// structured-output request for schema with one "lookup" tool taking schema,
// and returns the request body the provider received.
func InvokePreservesExactSchemaNumbers(t *testing.T, send func(t *testing.T, schema string) []byte) {
	const schema = `{"type":"object","properties":{"alpha":{"type":"number","const":0.100000000000000000001,"maximum":1.234567890123456789e+20},"zeta":{"type":"integer","enum":[9007199254740993,-9007199254740993,18446744073709551615],"minimum":9007199254740993}},"required":["zeta","alpha"],"additionalProperties":false}`
	wire := send(t, schema)
	var body struct {
		Tools []struct {
			InputSchema json.RawMessage `json:"input_schema"`
		} `json:"tools"`
		OutputConfig struct {
			Format struct {
				Schema json.RawMessage `json:"schema"`
			} `json:"format"`
		} `json:"output_config"`
	}
	if err := json.Unmarshal(wire, &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Tools) != 1 {
		t.Fatalf("tool schemas missing: %s", wire)
	}
	if !adaptertest.SameJSON(t, body.Tools[0].InputSchema, []byte(schema)) {
		t.Fatalf("tool schema changed on transport: %s", body.Tools[0].InputSchema)
	}
	for _, raw := range []json.RawMessage{body.Tools[0].InputSchema, body.OutputConfig.Format.Schema} {
		for _, literal := range []string{"9007199254740993", "-9007199254740993", "18446744073709551615", "0.100000000000000000001", "1.234567890123456789e+20"} {
			if !bytes.Contains(raw, []byte(literal)) {
				t.Fatalf("schema lost exact number %s: %s", literal, raw)
			}
		}
		if strings.Index(string(raw), `"zeta":`) > strings.Index(string(raw), `"alpha":`) {
			t.Fatalf("required property order changed: %s", raw)
		}
	}
}

// RemapsNonPortableToolCallIDs checks that lowerItem remaps an ID from
// another family, such as "functions.lookup:0", which violates the
// tool_use.id pattern, identically on the call and its result so the pair
// still matches, and keeps a portable ID unchanged.
func RemapsNonPortableToolCallIDs(t *testing.T, lowerItem func(llm.Item) (map[string]any, error)) {
	const id = "functions.lookup:0"
	call, err := lowerItem(llm.ToolCall{ID: id, Name: "lookup", Arguments: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	result, err := lowerItem(llm.ToolResult{CallID: id, Content: []llm.Part{llm.TextPart{Text: "ok"}}})
	if err != nil {
		t.Fatal(err)
	}
	callID := call["content"].([]any)[0].(map[string]any)["id"].(string)
	resultID := result["content"].([]any)[0].(map[string]any)["tool_use_id"].(string)
	if !regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`).MatchString(callID) || callID != resultID {
		t.Fatalf("tool_use.id = %q, tool_use_id = %q", callID, resultID)
	}
	valid, err := lowerItem(llm.ToolCall{ID: "toolu_01", Name: "lookup", Arguments: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if got := valid["content"].([]any)[0].(map[string]any)["id"]; got != "toolu_01" {
		t.Fatalf("portable ID changed to %q", got)
	}
}
