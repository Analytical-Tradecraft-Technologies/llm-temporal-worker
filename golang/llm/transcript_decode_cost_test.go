package llm_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// transcriptDecodeAllocsPerItem bounds what decoding one small transcript
// item may allocate, including the single duplicate-key scan of the enclosing
// document. Rescanning every nested value cost about 600 allocations per item
// for these transcripts; the bound leaves room for the typed values themselves
// and nothing for a per-level scan.
const transcriptDecodeAllocsPerItem = 40

// tinyTranscript is the worst case for per-item decode overhead: many items,
// each as small as a turn of an agent loop can be. It alternates the item
// kinds a tool-using conversation is made of.
func tinyTranscript(tb testing.TB, items int) []llm.Item {
	tb.Helper()
	transcript := make([]llm.Item, 0, items)
	for index := 0; len(transcript) < items; index++ {
		callID := fmt.Sprintf("call-%d", index)
		for _, item := range []llm.Item{
			llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "next"}}},
			llm.ToolCall{ID: callID, Name: "lookup", Arguments: json.RawMessage(`{"q":"a"}`)},
			llm.ToolResult{CallID: callID, Name: "lookup", Content: []llm.Part{llm.JSONPart{Value: json.RawMessage(`{"hits":1}`)}}},
			llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "ok"}}},
		} {
			if len(transcript) < items {
				transcript = append(transcript, item)
			}
		}
	}
	return transcript
}

func tinyTranscriptJSON(tb testing.TB, items int) []byte {
	tb.Helper()
	data, err := json.Marshal(tinyTranscript(tb, items))
	if err != nil {
		tb.Fatal(err)
	}
	return data
}

func tinyTranscriptRequestJSON(tb testing.TB, items int) []byte {
	tb.Helper()
	data, err := json.Marshal(llm.Request{OperationKey: "transcript-decode-cost", Model: "logical", Input: tinyTranscript(tb, items)})
	if err != nil {
		tb.Fatal(err)
	}
	return data
}

func TestTranscriptDecodeAllocationsAreBoundedPerItem(t *testing.T) {
	const items = 4000
	transcript := tinyTranscriptJSON(t, items)
	request := tinyTranscriptRequestJSON(t, items)
	for name, decode := range map[string]func() error{
		"DecodeItems": func() error {
			decoded, err := llm.DecodeItems(transcript)
			if err == nil && len(decoded) != items {
				err = fmt.Errorf("decoded %d items", len(decoded))
			}
			return err
		},
		"Request": func() error {
			var decoded llm.Request
			err := json.Unmarshal(request, &decoded)
			if err == nil && len(decoded.Input) != items {
				err = fmt.Errorf("decoded %d items", len(decoded.Input))
			}
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			var failure error
			allocations := testing.AllocsPerRun(2, func() {
				if err := decode(); err != nil {
					failure = err
				}
			})
			if failure != nil {
				t.Fatal(failure)
			}
			if limit := float64(items * transcriptDecodeAllocsPerItem); allocations > limit {
				t.Fatalf("decoding %d items made %.0f allocations (%.0f per item), limit %d per item", items, allocations, allocations/items, transcriptDecodeAllocsPerItem)
			}
		})
	}
}

func BenchmarkDecodeItemsTinyTranscript(b *testing.B) {
	for _, items := range []int{100, 600, 4000} {
		data := tinyTranscriptJSON(b, items)
		b.Run(fmt.Sprintf("items=%d", items), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for b.Loop() {
				if _, err := llm.DecodeItems(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRequestUnmarshalTinyTranscript(b *testing.B) {
	for _, items := range []int{100, 600, 4000} {
		data := tinyTranscriptRequestJSON(b, items)
		b.Run(fmt.Sprintf("items=%d", items), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for b.Loop() {
				var request llm.Request
				if err := json.Unmarshal(data, &request); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
