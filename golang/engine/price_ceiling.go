package engine

import (
	"math/big"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

// PriceCeiling is the per-unit ceiling of a call reserved at entry: the
// larger of its base and long-prompt prices. A nil entry, an unpriced call,
// has no ceiling.
func PriceCeiling(entry *pricing.Entry) *provider.PriceCeiling {
	if entry == nil {
		return nil
	}
	prompt, completion, request := entry.Prices.InputPerMillion, entry.Prices.OutputPerMillion, entry.Prices.PerRequest
	if long := entry.LongPrompt; long != nil {
		prompt = largerDecimal(prompt, long.Prices.InputPerMillion)
		completion = largerDecimal(completion, long.Prices.OutputPerMillion)
		request = largerDecimal(request, long.Prices.PerRequest)
	}
	return &provider.PriceCeiling{PromptPerMillion: prompt.CanonicalString(), CompletionPerMillion: completion.CanonicalString(), PerRequest: request.CanonicalString()}
}

func largerDecimal(left, right pricing.DecimalUSD) pricing.DecimalUSD {
	leftValue, leftOK := new(big.Rat).SetString(left.CanonicalString())
	rightValue, rightOK := new(big.Rat).SetString(right.CanonicalString())
	if leftOK && rightOK && rightValue.Cmp(leftValue) > 0 {
		return right
	}
	return left
}
