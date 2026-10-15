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

func TestSanitizeOutputSurrogatesCoversReferenceMetadata(t *testing.T) {
	metadata := map[string]json.RawMessage{"id": json.RawMessage(`"r1"`), "title": json.RawMessage(`"Emoji cut \ud83d"`), "score": json.RawMessage(`0.5`)}
	output := SanitizeOutputSurrogates([]Item{
		Reference{URI: "https://example.com/a", Metadata: metadata},
		&Reference{URI: "https://example.com/b", Metadata: map[string]json.RawMessage{"title": json.RawMessage(`"\udc00 tail"`)}},
	})
	sanitized := output[0].(Reference).Metadata
	if string(sanitized["title"]) != `"Emoji cut \ufffd"` || string(sanitized["id"]) != `"r1"` || string(sanitized["score"]) != `0.5` {
		t.Fatalf("reference metadata = %s", sanitized)
	}
	if string(metadata["title"]) != `"Emoji cut \ud83d"` {
		t.Fatal("the caller's metadata was mutated")
	}
	if got := string(output[1].(*Reference).Metadata["title"]); got != `"\ufffd tail"` {
		t.Fatalf("pointer reference title = %s", got)
	}
}

func TestSanitizeRawMapSurrogates(t *testing.T) {
	clean := map[string]json.RawMessage{"total_tokens": json.RawMessage(`3`), "empty": nil}
	if got := SanitizeRawMapSurrogates(clean); len(got) != len(clean) || string(got["total_tokens"]) != `3` {
		t.Fatalf("clean map = %s", got)
	}
	if SanitizeRawMapSurrogates(nil) != nil {
		t.Fatal("nil map was replaced")
	}
	raw := map[string]json.RawMessage{"note": json.RawMessage(`"x\ud800"`), "n": json.RawMessage(`1`)}
	got := SanitizeRawMapSurrogates(raw)
	if string(got["note"]) != `"x\ufffd"` || string(got["n"]) != `1` || string(raw["note"]) != `"x\ud800"` {
		t.Fatalf("sanitized map = %s (original %s)", got, raw)
	}
}
