package openaiwire

import (
	"encoding/json"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestMediaURLPrefersURLThenInlineBytes(t *testing.T) {
	if got, err := MediaURL("https://example.test/a.png", []byte("x"), "image/png"); err != nil || got != "https://example.test/a.png" {
		t.Fatalf("MediaURL with URL = %q, %v", got, err)
	}
	if got, err := MediaURL("", []byte("hi"), "text/plain"); err != nil || got != "data:text/plain;base64,aGk=" {
		t.Fatalf("MediaURL with bytes = %q, %v", got, err)
	}
	if got, err := MediaURL("", nil, "image/png"); err == nil || got != "" || err.Error() != "blob-backed media is not available to adapter" {
		t.Fatalf("MediaURL without source = %q, %v", got, err)
	}
}

func TestToolResultTextJoinsTextAndJSON(t *testing.T) {
	got, err := ToolResultText([]llm.Part{llm.TextPart{Text: "a"}, llm.JSONPart{Value: json.RawMessage(`{"b":1}`)}})
	if err != nil || got != `a{"b":1}` {
		t.Fatalf("ToolResultText = %q, %v", got, err)
	}
	if _, err := ToolResultText([]llm.Part{llm.JSONPart{Value: json.RawMessage(`{`)}}); err == nil || err.Error() != "tool result part 0 is invalid JSON" {
		t.Fatalf("invalid JSON error = %v", err)
	}
	if _, err := ToolResultText([]llm.Part{llm.TextPart{}, llm.ImagePart{URL: "https://example.test/a.png"}}); err == nil || err.Error() != `tool result part 1 kind "image" cannot be represented as text` {
		t.Fatalf("image error = %v", err)
	}
}
