package budget

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

var ErrUnusablePrice = errors.New("candidate price is unusable")

type Estimator struct {
	SafetyRatio  *big.Rat
	MaxOutput    int64
	MaxReasoning int64
	Tokenizer    Tokenizer
}

type Tokenizer func(llm.Request, routing.Candidate) (int64, error)

type Estimate struct {
	CandidateID      string
	InputTokens      int64
	OutputTokens     int64
	ReasoningTokens  int64
	CacheWriteTokens int64
	MicroUSD         pricing.MicroUSD
	CostUSD          pricing.USD
	CatalogVersion   string
}

func (estimator Estimator) PrepareRequest(request llm.Request) (llm.Request, error) {
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		return llm.Request{}, err
	}
	limit, err := estimator.outputLimit(normalized)
	if err != nil {
		return llm.Request{}, err
	}
	if normalized.Output == nil {
		normalized.Output = &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindText}}
	}
	value := int(limit)
	normalized.Output.MaxTokens = &value
	return normalized, nil
}

func (estimator Estimator) outputLimit(request llm.Request) (int64, error) {
	limit := estimator.MaxOutput
	if limit <= 0 {
		limit = 1000
	}
	if request.Output != nil && request.Output.MaxTokens != nil {
		limit = int64(*request.Output.MaxTokens)
	}
	if limit <= 0 || limit > math.MaxInt32 {
		return 0, fmt.Errorf("output token limit must be between 1 and %d", int64(math.MaxInt32))
	}
	return limit, nil
}

func (estimator Estimator) EstimateCandidate(request llm.Request, candidate routing.Candidate, entry pricing.Entry) (Estimate, error) {
	var inputTokens int64
	var err error
	if estimator.Tokenizer != nil {
		inputTokens, err = estimator.Tokenizer(request, candidate)
		if err != nil {
			return Estimate{}, err
		}
	} else {
		encoded, err := json.Marshal(request)
		if err != nil {
			return Estimate{}, err
		}
		baseTokens := int64(len(encoded) / 4)
		mediaTokens := estimateMediaTokens(request)
		inputTokens = baseTokens + mediaTokens
	}

	safety := estimator.SafetyRatio
	if safety != nil {
		rat := new(big.Rat).SetInt64(inputTokens)
		rat.Mul(rat, safety)
		denom := rat.Denom()
		num := rat.Num()
		inputTokens = new(big.Int).Div(num, denom).Int64()
		if new(big.Int).Mod(num, denom).Sign() > 0 {
			inputTokens++
		}
	}

	outputTokens, err := estimator.outputLimit(request)
	if err != nil {
		return Estimate{}, err
	}

	reasoningTokens := estimator.MaxReasoning
	if reasoningTokens < 0 {
		reasoningTokens = 0
	}

	cacheWriteTokens := int64(0)

	micro, err := pricing.CalculateMicroUSD(entry.Prices, inputTokens, outputTokens, reasoningTokens, cacheWriteTokens)
	if err != nil {
		if errors.Is(err, pricing.ErrUnusablePrice) {
			return Estimate{}, ErrUnusablePrice
		}
		return Estimate{}, err
	}

	costUSD, err := pricing.CalculateUSD(entry.Prices, inputTokens, outputTokens, reasoningTokens, cacheWriteTokens)
	if err != nil {
		if errors.Is(err, pricing.ErrUnusablePrice) {
			return Estimate{}, ErrUnusablePrice
		}
		return Estimate{}, err
	}

	return Estimate{
		CandidateID:      candidate.ID,
		InputTokens:      inputTokens,
		OutputTokens:     outputTokens,
		ReasoningTokens:  reasoningTokens,
		CacheWriteTokens: cacheWriteTokens,
		MicroUSD:         micro,
		CostUSD:          costUSD,
		CatalogVersion:   entry.Version,
	}, nil
}

func estimateMediaTokens(request llm.Request) int64 {
	var tokens int64
	for _, item := range request.Input {
		switch msg := item.(type) {
		case llm.Message:
			for _, part := range msg.Content {
				switch p := part.(type) {
				case llm.ImagePart:
					if p.Detail == "high" {
						tokens += 10000
					} else {
						tokens += 1600
					}
				case llm.DocumentPart:
					pages := int64(p.PageLimit)
					if pages <= 0 {
						pages = 100
					}
					tokens += pages * 1500
				}
			}
		}
	}
	return tokens
}
