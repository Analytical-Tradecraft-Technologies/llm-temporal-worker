package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func FuzzCanonicalTranscriptNeverPanics(f *testing.F) {
	f.Add("hello", `{"query":"x"}`, true)
	f.Add("", `null`, false)
	f.Fuzz(func(t *testing.T, text, arguments string, includeTool bool) {
		if len(text) > 1<<16 || len(arguments) > 1<<16 {
			t.Skip()
		}
		items := []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}}}
		if includeTool {
			items = append(items, llm.ToolCall{ID: "call-fuzz", Name: "lookup", Arguments: json.RawMessage(arguments)})
		}
		first, firstDigest, err := CanonicalTranscript(items)
		if err != nil {
			// Invalid provider/tool JSON is an expected rejection; the property
			// is that hostile input returns an error rather than panicking.
			return
		}
		second, secondDigest, err := CanonicalTranscript(items)
		if err != nil {
			t.Fatalf("second canonicalization rejected accepted input: %v", err)
		}
		if !bytes.Equal(first, second) || firstDigest != secondDigest {
			t.Fatalf("canonical transcript is not deterministic: %x/%x", firstDigest, secondDigest)
		}
		if got := sha256.Sum256(first); got != firstDigest {
			t.Fatalf("transcript digest = %x, want hash of canonical bytes %x", firstDigest, got)
		}
		canonical, err := llm.CanonicalJSON(first)
		if err != nil || !bytes.Equal(canonical, first) {
			t.Fatalf("canonical transcript is not idempotent: err=%v", err)
		}
	})
}
