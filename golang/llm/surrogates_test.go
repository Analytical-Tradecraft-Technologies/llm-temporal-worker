package llm

import (
	"encoding/json"
	"testing"
)

func TestSanitizeLoneSurrogates(t *testing.T) {
	for input, want := range map[string]string{
		`{"q":"x\ud83d"}`:     `{"q":"x�"}`,
		`{"q":"\ude00x"}`:     `{"q":"�x"}`,
		`{"q":"😀"}`:           `{"q":"😀"}`,
		`{"q":"\ud83dA"}`:     `{"q":"�A"}`,
		`{"q":"\\ud83d"}`:     `{"q":"\\ud83d"}`,
		`{"q":"a\"\ud800"}`:   `{"q":"a\"�"}`,
		`{"q":"plain","n":1}`: `{"q":"plain","n":1}`,
		`["𐀀","\uDBFF"]`:      `["𐀀","�"]`,
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
	if string(output[0].(ToolCall).Arguments) != `{"q":"x�"}` {
		t.Fatalf("tool call arguments = %s", output[0].(ToolCall).Arguments)
	}
	message := output[1].(Message)
	if string(message.Content[0].(JSONPart).Value) != `"�"` || message.Content[1].(TextPart).Text != "kept" {
		t.Fatalf("message = %#v", message)
	}
}
