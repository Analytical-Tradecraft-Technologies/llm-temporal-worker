package budget

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

// ErrUnusablePrice marks an otherwise valid candidate whose active catalog
// entry cannot produce a safe reservation. Callers that are evaluating an
// ordered route plan may skip this candidate and continue to the next
// authorized fallback. Tokenizer, request, and estimator configuration
// errors deliberately do not use this sentinel and remain hard failures.
var ErrUnusablePrice = errors.New("candidate price is unusable")

type Estimator struct {
	SafetyRatio  *big.Rat
	MaxOutput    int64
	MaxReasoning int64
	// Tokenizer, when configured, is the provider-specific exact token
	// counter. It receives the candidate because tokenization can vary by
	// provider family/model. A nil tokenizer uses the conservative UTF-8
	// fallback below.
	Tokenizer Tokenizer
}

// Tokenizer returns the exact input token count for one authorized candidate.
// Implementations must be deterministic and must not perform provider I/O.
type Tokenizer func(llm.Request, routing.Candidate) (int64, error)

type Estimate struct {
	CandidateID      string
	InputTokens      int64
	OutputTokens     int64
	ReasoningTokens  int64
	CacheWriteTokens int64
	MicroUSD         pricing.MicroUSD
	// CostUSD is the exact fixed-scale reservation used by new callers.
	CostUSD        pricing.USD
	CatalogVersion string
}

func (estimator Estimator) EstimateCandidate(request llm.Request, candidate routing.Candidate, entry pricing.Entry) (Estimate, error) {
	inputTokens, err := estimator.estimateInput(request, candidate)
	if err != nil {
		return Estimate{}, err
	}
	outputTokens := estimator.MaxOutput
	if outputTokens <= 0 {
		outputTokens = 1_000
	}
	if request.Output != nil && request.Output.MaxTokens != nil {
		outputTokens = int64(*request.Output.MaxTokens)
	}
	if outputTokens < 0 {
		return Estimate{}, fmt.Errorf("output token limit is negative")
	}
	reasoningTokens := int64(0)
	if request.Reasoning != nil && request.Reasoning.TokenBudget != nil {
		reasoningTokens = int64(*request.Reasoning.TokenBudget)
	}
	if estimator.MaxReasoning > reasoningTokens {
		reasoningTokens = estimator.MaxReasoning
	}
	cacheWrite := inputTokens
	components := []struct {
		component     pricing.PriceComponent
		price         pricing.DecimalUSD
		units         int64
		unitsPerPrice int64
		name          string
	}{
		{pricing.PriceComponentInput, entry.Prices.InputPerMillion, inputTokens, 1_000_000, "input"},
		{pricing.PriceComponentOutput, entry.Prices.OutputPerMillion, outputTokens, 1_000_000, "output"},
		{pricing.PriceComponentReasoning, entry.Prices.ReasoningPerMillion, reasoningTokens, 1_000_000, "reasoning"},
		{pricing.PriceComponentCacheWrite, entry.Prices.CacheWritePerMillion, cacheWrite, 1_000_000, "cache_write"},
		// PerRequest is already an amount in USD for this invocation. It is
		// not quoted per million units like the token components.
		{pricing.PriceComponentPerRequest, entry.Prices.PerRequest, 1, 1, "per_request"},
	}
	totalUSD := pricing.MustUSD("0")
	legacyTotal := pricing.MicroUSD(0)
	for _, component := range components {
		if component.units > 0 && entry.ComponentUnknown(component.component) {
			return Estimate{}, fmt.Errorf("%w: estimate %s has no known USD catalog price", ErrUnusablePrice, component.name)
		}
		value, err := pricing.CeilUSD(component.price, component.units, component.unitsPerPrice)
		if err != nil {
			return Estimate{}, fmt.Errorf("estimate %s: %w", component.name, err)
		}
		totalUSD, err = totalUSD.Add(value)
		if err != nil {
			return Estimate{}, err
		}
		legacy, legacyErr := pricing.CeilMicroUSD(component.price, component.units, component.unitsPerPrice)
		if legacyErr != nil {
			// USD is authoritative, but the estimator is also responsible for
			// producing the bounded compatibility reservation consumed by Redis.
			// Silently dropping an overflowing component would under-reserve.
			return Estimate{}, fmt.Errorf("%w: estimate %s microUSD compatibility conversion: %w", ErrUnusablePrice, component.name, legacyErr)
		}
		legacyTotal, err = legacyTotal.Add(legacy)
		if err != nil {
			return Estimate{}, fmt.Errorf("%w: estimate %s microUSD compatibility total: %w", ErrUnusablePrice, component.name, err)
		}
	}
	if estimator.SafetyRatio != nil {
		if estimator.SafetyRatio.Sign() <= 0 {
			return Estimate{}, fmt.Errorf("safety ratio must be positive")
		}
		totalUSD, err = multiplyUSD(totalUSD, estimator.SafetyRatio)
		if err != nil {
			return Estimate{}, err
		}
		legacyTotal, err = multiplyCeil(legacyTotal, estimator.SafetyRatio)
		if err != nil {
			return Estimate{}, fmt.Errorf("%w: estimate microUSD compatibility multiplier: %w", ErrUnusablePrice, err)
		}
	}
	return Estimate{CandidateID: candidate.ID, InputTokens: inputTokens, OutputTokens: outputTokens, ReasoningTokens: reasoningTokens, CacheWriteTokens: cacheWrite, CostUSD: totalUSD, MicroUSD: legacyTotal, CatalogVersion: entry.Version}, nil
}

func (estimator Estimator) EstimatePlan(request llm.Request, plan routing.Plan, entries map[string]pricing.Entry) (Estimate, error) {
	if len(plan.Candidates) == 0 {
		return Estimate{}, fmt.Errorf("cannot estimate an empty route plan")
	}
	var maximum Estimate
	for index, candidate := range plan.Candidates {
		entry, ok := entries[candidate.ID]
		if !ok {
			return Estimate{}, fmt.Errorf("price missing for candidate %s", candidate.ID)
		}
		estimate, err := estimator.EstimateCandidate(request, candidate, entry)
		if err != nil {
			return Estimate{}, err
		}
		// Select the first candidate even when its estimate is exactly zero.
		// Otherwise an all-free plan would return an empty candidate identity,
		// losing the route that established the maximum.
		if index == 0 || estimate.CostUSD.Cmp(maximum.CostUSD) > 0 {
			maximum = estimate
		}
	}
	return maximum, nil
}

func (estimator Estimator) estimateInput(request llm.Request, candidate routing.Candidate) (int64, error) {
	if estimator.Tokenizer != nil {
		inputTokens, err := estimator.Tokenizer(request, candidate)
		if err != nil {
			return 0, fmt.Errorf("exact provider tokenization failed: %w", err)
		}
		if inputTokens < 0 {
			return 0, fmt.Errorf("exact provider tokenization returned a negative count")
		}
		return inputTokens, nil
	}
	data, err := llm.CanonicalJSON(mustRequestJSON(request))
	if err != nil {
		return 0, err
	}
	// UTF-8 bytes / 4 is a conservative provider-independent baseline for
	// ordinary text. Structural overhead is bounded by the serialized request.
	input := int64((len(data) + 3) / 4)
	if input < 1 {
		input = 1
	}
	if int64(len(data)) > int64(^uint64(0)>>1) {
		return 0, fmt.Errorf("request is too large to estimate")
	}
	return input, nil
}

func multiplyCeil(value pricing.MicroUSD, ratio *big.Rat) (pricing.MicroUSD, error) {
	if value < 0 || ratio == nil {
		return 0, fmt.Errorf("invalid estimate multiplier")
	}
	numerator := new(big.Int).Mul(big.NewInt(int64(value)), ratio.Num())
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(numerator, ratio.Denom(), remainder)
	if remainder.Sign() != 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if !quotient.IsInt64() {
		return 0, fmt.Errorf("estimate multiplier overflows int64")
	}
	result := pricing.MicroUSD(quotient.Int64())
	if !result.Valid() {
		return 0, fmt.Errorf("estimate exceeds safe range")
	}
	return result, nil
}

func multiplyUSD(value pricing.USD, ratio *big.Rat) (pricing.USD, error) {
	if ratio == nil {
		return pricing.USD{}, fmt.Errorf("invalid estimate multiplier")
	}
	return value.MulRatio(ratio.Num(), ratio.Denom())
}

func mustRequestJSON(request llm.Request) []byte {
	data, _ := json.Marshal(request)
	return data
}
