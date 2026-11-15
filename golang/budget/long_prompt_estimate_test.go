package budget

import (
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
)

// gpt54LongPromptEntry is OpenAI's direct gpt-5.4 standard price, which
// bills 2x input and 1.5x output over 272,000 prompt tokens.
func gpt54LongPromptEntry() pricing.Entry {
	return pricing.Entry{Family: "openai_responses", Version: "v",
		Prices:     tokenPrices("2.5", "15", "0.25", "0"),
		LongPrompt: &pricing.LongPromptPrices{MinPromptTokens: 272_000, Prices: tokenPrices("5", "22.5", "0.5", "0")},
	}
}

// tokenPrices returns per-million token prices with no reasoning or
// per-request charge.
func tokenPrices(input, output, cacheRead, cacheWrite string) pricing.UnitPrices {
	return pricing.UnitPrices{InputPerMillion: pricing.MustDecimalUSD(input), OutputPerMillion: pricing.MustDecimalUSD(output),
		CacheReadPerMillion: pricing.MustDecimalUSD(cacheRead), CacheWritePerMillion: pricing.MustDecimalUSD(cacheWrite), ReasoningPerMillion: pricing.MustDecimalUSD("0"), PerRequest: pricing.MustDecimalUSD("0")}
}

type longPromptCase struct {
	request llm.Request
	long    bool
}

// longPromptCases returns two requests whose uncapped estimate can cross a
// long-prompt threshold, text plus media and a URL PDF, and a short text
// request that cannot.
func longPromptCases(operationKey string, maxTokens int, media []llm.Part) map[string]longPromptCase {
	request := func(parts ...llm.Part) llm.Request {
		return llm.Request{OperationKey: operationKey, Model: "logical", Output: &llm.OutputSpec{MaxTokens: intPointer(maxTokens)},
			Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: parts}}}
	}
	return map[string]longPromptCase{
		"text plus images": {request(media...), true},
		"URL PDF":          {request(llm.TextPart{Text: "summarize"}, llm.DocumentPart{URL: "https://example.com/report.pdf", MediaType: "application/pdf"}), true},
		"short text":       {request(llm.TextPart{Text: "hi"}), false},
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
	for name, test := range longPromptCases("long-prompt", 16_000, screenshots) {
		t.Run(name, func(t *testing.T) {
			estimate := assertReservedAtPromptPrice(t, test.request, candidate, entry, test.long)
			// A prompt over the threshold settles at the long-prompt price;
			// the reservation must cover the smallest such prompt.
			if test.long {
				minimum, err := pricing.CostFromUsage(entry, pricing.Usage{InputTokens: entry.LongPrompt.MinPromptTokens + 1, OutputTokens: estimate.OutputTokens})
				testassert.NoError(t, err)
				if estimate.CostUSD.Cmp(minimum.USD) < 0 {
					t.Fatalf("reservation %s is below the long-prompt cost %s", estimate.CostUSD.String(), minimum.USD.String())
				}
			}
		})
	}
}

// Claude Haiku 5.5 serves a 1M window by default and bills prompts over
// 100,000 tokens at 5x, so text plus media whose uncapped estimate can cross
// the threshold must reserve at the long-prompt price.
func TestEstimateReservesHaiku55LongPromptPrice(t *testing.T) {
	entry := pricing.Entry{Family: "anthropic_messages", Version: "v",
		Prices:     tokenPrices("0.1", "0.5", "0.01", "0.125"),
		LongPrompt: &pricing.LongPromptPrices{MinPromptTokens: 100_000, Prices: tokenPrices("0.5", "2.5", "0.05", "0.625")},
	}
	candidate := routing.Candidate{ID: "c", Family: "anthropic_messages", ContextTokens: 1_000_000}
	parts := []llm.Part{llm.TextPart{Text: strings.Repeat("a", 320_000)}} // about 80,000 tokens
	for range 40 {
		parts = append(parts, llm.ImagePart{URL: "https://example.com/page.png", MediaType: "image/png"})
	}
	for name, test := range longPromptCases("haiku-long-prompt", 4_000, parts) {
		t.Run(name, func(t *testing.T) {
			assertReservedAtPromptPrice(t, test.request, candidate, entry, test.long)
		})
	}
}

// assertReservedAtPromptPrice estimates request on candidate and checks the
// reservation equals the estimated usage priced at the long-prompt tier when
// long is set, and at the standard prices otherwise.
func assertReservedAtPromptPrice(t *testing.T, request llm.Request, candidate routing.Candidate, entry pricing.Entry, long bool) Estimate {
	t.Helper()
	estimate := mustEstimateCandidate(t, Estimator{}, request, candidate, entry)
	prices := entry.Prices
	if long {
		prices = entry.LongPrompt.Prices
	}
	want, err := pricing.CostFromUsage(pricing.Entry{Prices: prices}, pricing.Usage{InputTokens: estimate.InputTokens, OutputTokens: estimate.OutputTokens, CacheWriteTokens: estimate.CacheWriteTokens})
	testassert.NoError(t, err)
	if estimate.CostUSD.Cmp(want.USD) != 0 {
		t.Fatalf("reservation = %s for %d input tokens, want %s (long-prompt price %v)", estimate.CostUSD.String(), estimate.InputTokens, want.USD.String(), long)
	}
	return estimate
}
