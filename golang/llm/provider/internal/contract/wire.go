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
