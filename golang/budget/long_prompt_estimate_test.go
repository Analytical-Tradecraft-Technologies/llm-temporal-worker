package budget

import (
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
)

// gpt54LongPromptEntry is OpenAI's direct gpt-5.4 standard price, which
// bills 2x input and 1.5x output from 272,000 prompt tokens.
func gpt54LongPromptEntry() pricing.Entry {
	return pricing.Entry{Family: "openai_responses", Version: "v",
		Prices: pricing.UnitPrices{InputPerMillion: pricing.MustDecimalUSD("2.5"), OutputPerMillion: pricing.MustDecimalUSD("15"),
			CacheReadPerMillion: pricing.MustDecimalUSD("0.25"), CacheWritePerMillion: pricing.MustDecimalUSD("0"), ReasoningPerMillion: pricing.MustDecimalUSD("0"), PerRequest: pricing.MustDecimalUSD("0")},
		LongPrompt: &pricing.LongPromptPrices{MinPromptTokens: 272_000, Prices: pricing.UnitPrices{InputPerMillion: pricing.MustDecimalUSD("5"), OutputPerMillion: pricing.MustDecimalUSD("22.5"),
			CacheReadPerMillion: pricing.MustDecimalUSD("0.5"), CacheWritePerMillion: pricing.MustDecimalUSD("0"), ReasoningPerMillion: pricing.MustDecimalUSD("0"), PerRequest: pricing.MustDecimalUSD("0")}},
	}
}

func TestEstimateReservesLongPromptPriceWhenMediaCanReachThreshold(t *testing.T) {
	entry := gpt54LongPromptEntry()
	// The direct route declares OpenAI's real window, not the price threshold.
	candidate := routing.Candidate{ID: "c", Family: "openai_responses", ContextTokens: 1_050_000}
	text := llm.TextPart{Text: strings.Repeat("a", 720_000)} // about 180,000 tokens
	screenshots := make([]llm.Part, 0, 101)
	screenshots = append(screenshots, text)
	for range 100 {
		screenshots = append(screenshots, llm.ImagePart{URL: "https://example.com/screen.png", MediaType: "image/png"})
	}
	request := func(parts ...llm.Part) llm.Request {
		return llm.Request{OperationKey: "long-prompt", Model: "logical", Output: &llm.OutputSpec{MaxTokens: intPointer(16_000)},
			Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: parts}}}
	}
	for name, test := range map[string]struct {
		request llm.Request
		long    bool
	}{
		"text plus images": {request(screenshots...), true},
		"URL PDF":          {request(llm.TextPart{Text: "summarize"}, llm.DocumentPart{URL: "https://example.com/report.pdf", MediaType: "application/pdf"}), true},
		"short text":       {request(llm.TextPart{Text: "hi"}), false},
	} {
		t.Run(name, func(t *testing.T) {
			estimate, err := Estimator{}.EstimateCandidate(test.request, candidate, entry)
			if err != nil {
				t.Fatal(err)
			}
			prices := entry.Prices
			if test.long {
				prices = entry.LongPrompt.Prices
			}
			want, err := pricing.CostFromUsage(pricing.Entry{Prices: prices}, pricing.Usage{InputTokens: estimate.InputTokens, OutputTokens: estimate.OutputTokens, CacheWriteTokens: estimate.CacheWriteTokens})
			if err != nil {
				t.Fatal(err)
			}
			if estimate.CostUSD.Cmp(want.USD) != 0 {
				t.Fatalf("reservation = %s for %d input tokens, want %s (long-prompt price %v)", estimate.CostUSD.String(), estimate.InputTokens, want.USD.String(), test.long)
			}
			// A prompt that reaches the threshold settles at the long-prompt
			// price; the reservation must cover the smallest such prompt.
			if test.long {
				minimum, err := pricing.CostFromUsage(entry, pricing.Usage{InputTokens: entry.LongPrompt.MinPromptTokens, OutputTokens: estimate.OutputTokens})
				if err != nil {
					t.Fatal(err)
				}
				if estimate.CostUSD.Cmp(minimum.USD) < 0 {
					t.Fatalf("reservation %s is below the long-prompt cost %s", estimate.CostUSD.String(), minimum.USD.String())
				}
			}
		})
	}
}
