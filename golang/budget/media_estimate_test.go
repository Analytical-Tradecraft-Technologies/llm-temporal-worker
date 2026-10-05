package budget

import (
	"math/big"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

func mediaEstimateRequest(parts ...llm.Part) llm.Request {
	content := append([]llm.Part{llm.TextPart{Text: "hi"}}, parts...)
	return llm.Request{
		OperationKey: "media-estimate",
		Model:        "logical",
		Input:        []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: content}},
		Output:       &llm.OutputSpec{MaxTokens: intPointer(1000)},
	}
}

func mediaEstimateEntry() pricing.Entry {
	return pricing.Entry{Version: "v", Prices: pricing.UnitPrices{
		InputPerMillion:  pricing.MustDecimalUSD("3"),
		OutputPerMillion: pricing.MustDecimalUSD("15"),
	}}
}

func TestEstimateTextOnlyInputIsSerializedSizeEstimate(t *testing.T) {
	request := mediaEstimateRequest()
	estimator := Estimator{SafetyRatio: big.NewRat(135, 100)}
	got, err := estimator.EstimateCandidate(request, routing.Candidate{ID: "c"}, mediaEstimateEntry())
	if err != nil {
		t.Fatal(err)
	}
	want, err := estimator.CountInputTokens(request, routing.Candidate{ID: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if got.InputTokens != want || got.InputTokens >= 1000 {
		t.Fatalf("text-only input tokens = %d, want serialized estimate %d", got.InputTokens, want)
	}
}

func TestEstimateURLMediaReservesPerPartFloor(t *testing.T) {
	estimator := Estimator{SafetyRatio: big.NewRat(135, 100)}
	candidate := routing.Candidate{ID: "c"}
	base, err := estimator.EstimateCandidate(mediaEstimateRequest(), candidate, mediaEstimateEntry())
	if err != nil {
		t.Fatal(err)
	}
	image := llm.ImagePart{URL: "https://example.com/cat.png", MediaType: "image/png"}
	document := llm.DocumentPart{URL: "https://example.com/report.pdf", MediaType: "application/pdf"}
	for name, test := range map[string]struct {
		parts []llm.Part
		floor int64
	}{
		"image":    {[]llm.Part{image}, MediaImageInputTokenFloor},
		"document": {[]llm.Part{document}, MediaDocumentInputTokenFloor},
		"both":     {[]llm.Part{image, document}, MediaImageInputTokenFloor + MediaDocumentInputTokenFloor},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := estimator.EstimateCandidate(mediaEstimateRequest(test.parts...), candidate, mediaEstimateEntry())
			if err != nil {
				t.Fatal(err)
			}
			if got.InputTokens < base.InputTokens+test.floor {
				t.Fatalf("input tokens = %d, want at least %d", got.InputTokens, base.InputTokens+test.floor)
			}
			if got.CostUSD.Cmp(base.CostUSD) <= 0 {
				t.Fatalf("media reservation %s did not exceed text-only %s", got.CostUSD.String(), base.CostUSD.String())
			}
		})
	}
}

func TestEstimateMediaFloorCountsInstructionsAndToolResults(t *testing.T) {
	image := llm.ImagePart{URL: "https://example.com/cat.png", MediaType: "image/png"}
	request := llm.Request{
		OperationKey: "media-estimate",
		Model:        "logical",
		Instructions: []llm.Instruction{{Content: []llm.Part{image}}},
		Input: []llm.Item{
			llm.ToolCall{ID: "call", Name: "fetch", Arguments: []byte(`{}`)},
			llm.ToolResult{CallID: "call", Name: "fetch", Content: []llm.Part{image}},
		},
		Output: &llm.OutputSpec{MaxTokens: intPointer(10)},
	}
	if got := mediaInputAllowance(request); got != 2*MediaImageInputTokenFloor {
		t.Fatalf("media allowance = %d, want %d", got, 2*MediaImageInputTokenFloor)
	}
}

func TestEstimateMediaFloorIsCappedByContextWindow(t *testing.T) {
	estimator := Estimator{}
	request := mediaEstimateRequest(llm.DocumentPart{URL: "https://example.com/report.pdf", MediaType: "application/pdf"})
	// The document allowance exceeds every supported window, so a document
	// reserves the whole remaining input room on standard and 1M-token routes.
	for _, contextTokens := range []int64{200_000, 1_000_000} {
		candidate := routing.Candidate{ID: "c", ContextTokens: contextTokens}
		got, err := estimator.EstimateCandidate(request, candidate, mediaEstimateEntry())
		if err != nil {
			t.Fatalf("context %d: media floor must not cause a context-limit rejection: %v", contextTokens, err)
		}
		if want := candidate.ContextTokens - got.OutputTokens - got.ReasoningTokens; got.InputTokens != want {
			t.Fatalf("context %d: capped input tokens = %d, want remaining context %d", contextTokens, got.InputTokens, want)
		}
	}
}

func TestEstimateDocumentFloorCoversLargestSupportedPDF(t *testing.T) {
	// 600 pages x (3,000 text tokens + a rendered page image) must exceed a
	// 1M-token window so the context cap, not the constant, binds.
	if MediaDocumentPageAssumption < 600 {
		t.Fatalf("page assumption = %d, want at least the 600-page provider limit", MediaDocumentPageAssumption)
	}
	if MediaDocumentTokensPerPage < MediaDocumentTextTokensPerPage+MediaImageInputTokenFloor {
		t.Fatalf("tokens per page = %d must include text and a page image", MediaDocumentTokensPerPage)
	}
	if MediaDocumentInputTokenFloor <= 1_000_000 {
		t.Fatalf("document floor = %d, want above the largest 1M-token context window", MediaDocumentInputTokenFloor)
	}
	estimator := Estimator{}
	request := mediaEstimateRequest(llm.DocumentPart{URL: "https://example.com/report.pdf", MediaType: "application/pdf"})
	got, err := estimator.EstimateCandidate(request, routing.Candidate{ID: "c"}, mediaEstimateEntry())
	if err != nil {
		t.Fatal(err)
	}
	if got.InputTokens < MediaDocumentInputTokenFloor {
		t.Fatalf("undeclared-window input tokens = %d, want at least %d", got.InputTokens, MediaDocumentInputTokenFloor)
	}
}

// The fallback estimate counts the text prefix the OpenAI families add to a
// failed tool result, and nothing for families with a native error field.
func TestFallbackEstimateCountsToolResultErrorPrefix(t *testing.T) {
	for _, isError := range []bool{true, false} {
		request := mediaEstimateRequest()
		request.Input = append(request.Input, llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{}`)},
			llm.ToolResult{CallID: "call-1", Content: []llm.Part{llm.TextPart{Text: "timed out"}}, IsError: isError})
		native, err := Estimator{}.CountInputTokens(request, routing.Candidate{ID: "c", Family: "anthropic_messages"})
		if err != nil {
			t.Fatal(err)
		}
		for _, family := range []string{"openai_responses", "openai_chat"} {
			emulated, err := Estimator{}.CountInputTokens(request, routing.Candidate{ID: "c", Family: family})
			if err != nil {
				t.Fatal(err)
			}
			want := int64(0)
			if isError {
				want = int64(len(llm.ToolResultErrorTextPrefix)) / 4
			}
			if emulated-native < want || (!isError && emulated != native) {
				t.Fatalf("%s is_error=%t estimate %d, native %d, want at least %d more", family, isError, emulated, native, want)
			}
		}
	}
}
