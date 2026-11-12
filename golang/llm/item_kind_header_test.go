package llm

import "testing"

func TestKindedDecodersRejectTheWrongKindAndUnknownFields(t *testing.T) {
	for _, test := range []struct {
		name   string
		decode func([]byte) error
		data   string
		want   string
	}{
		{"message kind", func(data []byte) error { _, err := decodeMessage(data); return err }, `{"kind":"tool_call","actor":"human","content":[]}`, `item kind "tool_call" is not "message"`},
		{"tool result kind", func(data []byte) error { _, err := decodeToolResult(data); return err }, `{"kind":"message","call_id":"c","content":[]}`, `item kind "message" is not "tool_result"`},
		{"text part kind", func(data []byte) error { _, err := decodeTextPart(data); return err }, `{"kind":"json","text":"a"}`, `part kind "json" is not "text"`},
		{"image part kind", func(data []byte) error { _, err := decodeImagePart(data); return err }, `{"kind":"document","url":"https://example.test/a"}`, `part kind "document" is not "image"`},
		{"refusal part kind", func(data []byte) error { _, err := decodeRefusalPart(data); return err }, `{"kind":"text","text":"no"}`, `part kind "text" is not "refusal"`},
	} {
		err := test.decode([]byte(test.data))
		if err == nil || err.Error() != test.want {
			t.Fatalf("%s: error = %v, want %q", test.name, err, test.want)
		}
	}
	if _, err := decodeTextPart([]byte(`{"kind":"text","text":"a","extra":1}`)); err == nil {
		t.Fatal("decodeTextPart accepted an unknown field")
	}
	if _, err := decodeReference([]byte(`{"uri":"https://example.test"}`)); err == nil {
		t.Fatal("decodeReference accepted an object without kind")
	}
}
