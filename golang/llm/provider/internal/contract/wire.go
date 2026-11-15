package contract

import (
	"encoding/json"
	"reflect"
	"sort"
	"strconv"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// WireDifferences lists every JSON path at which the captured body lost,
// gained or altered a value relative to the intended body.
func WireDifferences(path string, intended, captured any) []string {
	switch want := intended.(type) {
	case map[string]any:
		got, ok := captured.(map[string]any)
		if !ok {
			return []string{path + ": intended object, captured " + DescribeWireValue(captured)}
		}
		keys := make(map[string]struct{}, len(want)+len(got))
		for key := range want {
			keys[key] = struct{}{}
		}
		for key := range got {
			keys[key] = struct{}{}
		}
		sorted := make([]string, 0, len(keys))
		for key := range keys {
			sorted = append(sorted, key)
		}
		sort.Strings(sorted)
		var differences []string
		for _, key := range sorted {
			wantValue, wanted := want[key]
			gotValue, present := got[key]
			child := path + "." + key
			switch {
			case !present:
				differences = append(differences, child+": dropped, intended "+DescribeWireValue(wantValue))
			case !wanted:
				differences = append(differences, child+": added "+DescribeWireValue(gotValue))
			default:
				differences = append(differences, WireDifferences(child, wantValue, gotValue)...)
			}
		}
		return differences
	case []any:
		got, ok := captured.([]any)
		if !ok || len(got) != len(want) {
			return []string{path + ": intended " + DescribeWireValue(intended) + ", captured " + DescribeWireValue(captured)}
		}
		var differences []string
		for index := range want {
			differences = append(differences, WireDifferences(path+"["+strconv.Itoa(index)+"]", want[index], got[index])...)
		}
		return differences
	default:
		if !reflect.DeepEqual(intended, captured) {
			return []string{path + ": intended " + DescribeWireValue(intended) + ", captured " + DescribeWireValue(captured)}
		}
		return nil
	}
}

// DescribeWireValue renders a JSON value for a wire-difference message.
func DescribeWireValue(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "<unencodable>"
	}
	return string(encoded)
}

// WireAuditTool is a function tool with a small strict schema, for wire
// audits.
func WireAuditTool(name string) llm.Tool {
	return llm.Tool{Name: name, Description: "look up " + name, InputSchema: json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"},"limit":{"type":"integer","minimum":1}},"required":["q"],"additionalProperties":false}`)}
}

// WireAuditUserText is a single human text message, for wire audits.
func WireAuditUserText(text string) []llm.Item {
	return []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}}}
}

// WireAuditCase is a named request for a wire audit.
type WireAuditCase struct {
	Name    string
	Request llm.Request
}

// WireAuditCommonCases are the wire-audit requests that the OpenAI Chat and
// Responses families both lower: tool policies, output limits and formats,
// reasoning modes without an effort, and images.
func WireAuditCommonCases() []WireAuditCase {
	maxTokens := 256
	tools := []llm.Tool{WireAuditTool("lookup"), WireAuditTool("search")}
	schema := json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"},"score":{"type":"number"}},"required":["answer"],"additionalProperties":false}`)
	hello := WireAuditUserText("hello")
	return []WireAuditCase{
		{Name: "minimal", Request: llm.Request{Input: hello}},
		{Name: "tool policy none without tools", Request: llm.Request{Input: hello, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNone}}},
		{Name: "tools default policy", Request: llm.Request{Input: hello, Tools: tools}},
		{Name: "tools auto parallel", Request: llm.Request{Input: hello, Tools: tools, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceAuto, Parallel: true}}},
		{Name: "tools none", Request: llm.Request{Input: hello, Tools: tools, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNone}}},
		{Name: "tools required", Request: llm.Request{Input: hello, Tools: tools, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceRequired}}},
		{Name: "tools named", Request: llm.Request{Input: hello, Tools: tools, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNamed, Name: "search"}}},
		{Name: "tools named parallel", Request: llm.Request{Input: hello, Tools: tools, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNamed, Name: "lookup", Parallel: true}}},
		{Name: "max tokens", Request: llm.Request{Input: hello, Output: &llm.OutputSpec{MaxTokens: &maxTokens}}},
		{Name: "json object output", Request: llm.Request{Input: WireAuditUserText("reply in JSON"), Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSON}}}},
		{Name: "json schema strict", Request: llm.Request{Input: hello, Output: &llm.OutputSpec{MaxTokens: &maxTokens, Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Description: "answer object", Strict: true, Schema: schema}}}},
		{Name: "json schema lenient", Request: llm.Request{Input: hello, Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Schema: schema}}}},
		{Name: "reasoning disabled", Request: llm.Request{Input: hello, Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeDisabled}}},
		{Name: "reasoning provider default", Request: llm.Request{Input: hello, Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeProviderDefault}}},
		{Name: "image url", Request: llm.Request{Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
			llm.TextPart{Text: "describe"},
			llm.ImagePart{URL: "https://example.test/image.png", MediaType: "image/png", Detail: "high"},
			llm.ImagePart{URL: "https://example.test/other.png", MediaType: "image/png"},
		}}}}},
		{Name: "image bytes", Request: llm.Request{Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
			llm.ImagePart{Bytes: []byte{0x89, 'P', 'N', 'G'}, MediaType: "image/png", Detail: "low"},
			llm.JSONPart{Value: json.RawMessage(`{"caption":true}`)},
		}}}}},
	}
}
