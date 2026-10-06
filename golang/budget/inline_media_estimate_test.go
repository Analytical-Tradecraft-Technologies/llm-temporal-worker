package budget

import (
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
)

// Issue #1085: inline media bytes are base64 in the canonical request, so the
// byte-based text estimate must not count them against the context window.
func TestInlineMediaBytesAreNotCountedAsTextTokens(t *testing.T) {
	estimator := Estimator{MaxOutput: 32768}
	candidate := routing.Candidate{ID: "c", Model: "physical", AttemptedClass: llm.ServiceClassStandard, ContextTokens: 128_000}
	textOnly, err := estimator.CountInputTokens(mediaEstimateRequest(), candidate)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		part      llm.Part
		maxOutput *int
		allowance int64
	}{
		{"image 50 KiB", llm.ImagePart{Bytes: make([]byte, 50<<10), MediaType: "image/png"}, intPointer(1024), MediaImageInputTokenFloor},
		{"image 300 KiB", llm.ImagePart{Bytes: make([]byte, 300<<10), MediaType: "image/png"}, intPointer(1024), MediaImageInputTokenFloor},
		{"image 300 KiB default output", llm.ImagePart{Bytes: make([]byte, 300<<10), MediaType: "image/png"}, nil, MediaImageInputTokenFloor},
		{"image 600 KiB", llm.ImagePart{Bytes: make([]byte, 600<<10), MediaType: "image/png"}, intPointer(1024), MediaImageInputTokenFloor},
		{"image 600 KiB pointer", &llm.ImagePart{Bytes: make([]byte, 600<<10), MediaType: "image/png"}, intPointer(1024), MediaImageInputTokenFloor},
		{"pdf 600 KiB", llm.DocumentPart{Bytes: make([]byte, 600<<10), MediaType: "application/pdf"}, intPointer(1024), MediaDocumentInputTokenFloor},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := mediaEstimateRequest(test.part)
			request.Output.MaxTokens = test.maxOutput
			if err := estimator.ValidateContext(request, candidate); err != nil {
				t.Fatalf("context check rejected inline media: %v", err)
			}
			counted, err := estimator.CountInputTokens(request, candidate)
			if err != nil {
				t.Fatal(err)
			}
			// Only the part's structure (kind, media type) is text.
			if counted < textOnly || counted > textOnly+50 {
				t.Fatalf("counted input tokens = %d, want within 50 of text-only %d", counted, textOnly)
			}
			estimate, err := estimator.EstimateCandidate(request, candidate, mediaEstimateEntry())
			if err != nil {
				t.Fatalf("reservation rejected inline media: %v", err)
			}
			want := counted + test.allowance
			if room := candidate.ContextTokens - estimate.OutputTokens; want > room {
				want = room
			}
			if estimate.InputTokens != want {
				t.Fatalf("reserved input tokens = %d, want %d", estimate.InputTokens, want)
			}
		})
	}
}

// Issue #1089: an inline text document is bounded by its byte length instead
// of reserving the unknown-size document allowance.
func TestInlineTextDocumentReservesItsByteLength(t *testing.T) {
	estimator := Estimator{SafetyRatio: big.NewRat(135, 100)}
	text := []byte("twenty-three byte note.")
	if len(text) != 23 {
		t.Fatalf("fixture length = %d", len(text))
	}
	base, err := estimator.EstimateCandidate(mediaEstimateRequest(), routing.Candidate{ID: "c"}, mediaEstimateEntry())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		part      llm.Part
		length    int64
		unbounded bool
	}{
		{"text/plain", llm.DocumentPart{Bytes: text, MediaType: "text/plain"}, 23, false},
		{"text/plain pointer", &llm.DocumentPart{Bytes: text, MediaType: "text/plain"}, 23, false},
		{"text with charset", llm.DocumentPart{Bytes: text, MediaType: "Text/Markdown; charset=utf-8"}, 23, false},
		{"large text/plain", llm.DocumentPart{Bytes: make([]byte, 40_001), MediaType: "text/plain"}, 40_001, false},
		{"inline pdf", llm.DocumentPart{Bytes: text, MediaType: "application/pdf"}, 23, true},
		{"inline unknown type", llm.DocumentPart{Bytes: text, MediaType: "application/octet-stream"}, 23, true},
		{"text URL", llm.DocumentPart{URL: "https://example.com/note.txt", MediaType: "text/plain"}, 0, true},
		{"text blob", llm.DocumentPart{Blob: &llm.BlobRef{Digest: "digest", ByteLength: 23, MediaType: "text/plain", Locator: "blob"}, MediaType: "text/plain"}, 0, true},
	} {
		for _, contextTokens := range []int64{0, 200_000, 1_000_000} {
			t.Run(fmt.Sprintf("%s/context %d", test.name, contextTokens), func(t *testing.T) {
				candidate := routing.Candidate{ID: "c", ContextTokens: contextTokens}
				request := mediaEstimateRequest(test.part)
				got, err := estimator.EstimateCandidate(request, candidate, mediaEstimateEntry())
				if err != nil {
					t.Fatal(err)
				}
				if test.unbounded {
					want := base.InputTokens + MediaDocumentInputTokenFloor
					if contextTokens > 0 {
						want = contextTokens - got.OutputTokens
					}
					if got.InputTokens < want {
						t.Fatalf("context %d: input tokens = %d, want at least the unknown-size reservation %d", contextTokens, got.InputTokens, want)
					}
					return
				}
				// Upper bound: one token per decoded byte on top of the
				// text-only request. Tight: the part's own structure
				// (kind, media type) adds at most a few dozen tokens.
				if low, high := base.InputTokens+test.length, base.InputTokens+test.length+50; got.InputTokens < low || got.InputTokens > high {
					t.Fatalf("context %d: input tokens = %d, want between %d and %d", contextTokens, got.InputTokens, low, high)
				}
				counted, err := estimator.CountInputTokens(request, candidate)
				if err != nil {
					t.Fatal(err)
				}
				// The context check sees the text at the ordinary baseline.
				if low := base.InputTokens + (test.length+3)/4; counted < low || counted > got.InputTokens {
					t.Fatalf("context %d: counted input tokens = %d, want between %d and %d", contextTokens, counted, low, got.InputTokens)
				}
				if test.length == 23 && got.CostUSD.Cmp(pricing.MustUSD("0.03")) > 0 {
					t.Fatalf("context %d: tiny inline document reserved %s, want about the text-only %s", contextTokens, got.CostUSD.String(), base.CostUSD.String())
				}
			})
		}
	}
}

func TestInlineTextDocumentStillCountsAgainstContextWindow(t *testing.T) {
	estimator := Estimator{}
	candidate := routing.Candidate{ID: "c", Model: "physical", AttemptedClass: llm.ServiceClassStandard, ContextTokens: 128_000}
	request := mediaEstimateRequest(llm.DocumentPart{Bytes: make([]byte, 600<<10), MediaType: "text/plain"})
	if err := estimator.ValidateContext(request, candidate); !errors.Is(err, ErrContextLimit) {
		t.Fatalf("600 KiB of inline text fit a 128k window: %v", err)
	}
}

func TestEstimateDoesNotModifyInlineMediaRequest(t *testing.T) {
	request := mediaEstimateRequest(llm.ImagePart{Bytes: []byte{1, 2, 3, 4}, MediaType: "image/png"}, &llm.DocumentPart{Bytes: []byte("note"), MediaType: "text/plain"})
	if _, err := (Estimator{}).EstimateCandidate(request, routing.Candidate{ID: "c"}, mediaEstimateEntry()); err != nil {
		t.Fatal(err)
	}
	content := request.Input[0].(llm.Message).Content
	if got := content[1].(llm.ImagePart).Bytes; len(got) != 4 {
		t.Fatalf("image bytes were stripped from the caller's request: %v", got)
	}
	if got := content[2].(*llm.DocumentPart).Bytes; string(got) != "note" {
		t.Fatalf("document bytes were stripped from the caller's request: %q", got)
	}
}
