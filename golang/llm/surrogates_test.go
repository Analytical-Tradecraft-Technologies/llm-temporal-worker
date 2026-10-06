package llm

import (
	"encoding/json"
	"testing"
)

func TestSanitizeLoneSurrogates(t *testing.T) {
	for input, want := range map[string]string{
		`{"q":"x\ud83d"}`:     `{"q":"x\ufffd"}`,
		`{"q":"\ude00x"}`:     `{"q":"\ufffdx"}`,
		`{"q":"😀"}`:           `{"q":"😀"}`,
		`{"q":"\ud83dA"}`:     `{"q":"\ufffdA"}`,
		`{"q":"\\ud83d"}`:     `{"q":"\\ud83d"}`,
		`{"q":"a\"\ud800"}`:   `{"q":"a\"\ufffd"}`,
		`{"q":"plain","n":1}`: `{"q":"plain","n":1}`,
		`["𐀀","\uDBFF"]`:      `["𐀀","\ufffd"]`,
	} {
		got := string(SanitizeLoneSurrogates(json.RawMessage(input)))
		if got != want {
			t.Errorf("SanitizeLoneSurrogates(%s) = %s, want %s", input, got, want)
		}
		if !json.Valid([]byte(got)) {
			t.Errorf("sanitized %s is not valid JSON", got)
		}
	}
	unchanged := json.RawMessage(`{"q":"x"}`)
	if got := SanitizeLoneSurrogates(unchanged); &got[0] != &unchanged[0] {
		t.Error("input without lone surrogates was copied")
	}
}

func TestSanitizeOutputSurrogatesCoversOpenJSON(t *testing.T) {
	output := SanitizeOutputSurrogates([]Item{
		ToolCall{ID: "call", Name: "lookup", Arguments: json.RawMessage(`{"q":"x\ud83d"}`)},
		Message{Actor: ActorModel, Content: []Part{JSONPart{Value: json.RawMessage(`"\udc00"`)}, TextPart{Text: "kept"}}},
	})
	if string(output[0].(ToolCall).Arguments) != `{"q":"x\ufffd"}` {
		t.Fatalf("tool call arguments = %s", output[0].(ToolCall).Arguments)
	}
	message := output[1].(Message)
	if string(message.Content[0].(JSONPart).Value) != `"\ufffd"` || message.Content[1].(TextPart).Text != "kept" {
		t.Fatalf("message = %#v", message)
	}
}

func TestSanitizeOutputSurrogatesCoversPointerForms(t *testing.T) {
	call := &ToolCall{ID: "call", Name: "lookup", Arguments: json.RawMessage(`{"q":"x\ud83d"}`)}
	part := &JSONPart{Value: json.RawMessage(`"\udc00"`)}
	output := SanitizeOutputSurrogates([]Item{call, &Message{Actor: ActorModel, Content: []Part{part}}, &ToolResult{CallID: "call", Name: "lookup", Content: []Part{part}}})
	if string(output[0].(*ToolCall).Arguments) != `{"q":"x\ufffd"}` || string(call.Arguments) != `{"q":"x\ud83d"}` {
		t.Fatalf("pointer tool call = %s (original %s)", output[0].(*ToolCall).Arguments, call.Arguments)
	}
	if string(output[1].(*Message).Content[0].(*JSONPart).Value) != `"\ufffd"` || string(output[2].(*ToolResult).Content[0].(*JSONPart).Value) != `"\ufffd"` {
		t.Fatal("pointer JSON parts were not sanitized")
	}
	if string(part.Value) != `"\udc00"` {
		t.Fatal("the caller's part was mutated")
	}
}
