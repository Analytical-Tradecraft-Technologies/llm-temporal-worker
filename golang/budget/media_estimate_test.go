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
	candidate := routing.Candidate{ID: "c", ContextTokens: 200_000}
	got, err := estimator.EstimateCandidate(request, candidate, mediaEstimateEntry())
	if err != nil {
		t.Fatalf("media floor must not cause a context-limit rejection: %v", err)
	}
	if want := candidate.ContextTokens - got.OutputTokens - got.ReasoningTokens; got.InputTokens != want {
		t.Fatalf("capped input tokens = %d, want remaining context %d", got.InputTokens, want)
	}
}
