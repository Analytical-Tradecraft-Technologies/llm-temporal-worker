package budget

import (
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

func TestEstimatePlanUsesMaximumAuthorizedCandidate(t *testing.T) {
	request := llm.Request{OperationKey: "estimate", Model: "logical", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}, Output: &llm.OutputSpec{MaxTokens: intPointer(10)}}
	plan := routing.Plan{Candidates: []routing.Candidate{{ID: "economy", AttemptedClass: llm.ServiceClassEconomy}, {ID: "priority", AttemptedClass: llm.ServiceClassPriority}}}
	entries := map[string]pricing.Entry{
		"economy":  {Version: "e", Prices: pricing.UnitPrices{OutputPerMillion: pricing.MustDecimalUSD("1")}},
		"priority": {Version: "p", Prices: pricing.UnitPrices{OutputPerMillion: pricing.MustDecimalUSD("2")}},
	}
	estimator := Estimator{SafetyRatio: big.NewRat(1, 1)}
	got, err := estimator.EstimatePlan(request, plan, entries)
	if err != nil {
		t.Fatal(err)
	}
	if got.CandidateID != "priority" {
		t.Fatalf("maximum estimate candidate = %q", got.CandidateID)
	}
}

func TestEstimatePlanIdentifiesAllFreeMaximumCandidate(t *testing.T) {
	request := llm.Request{
		OperationKey: "free-estimate",
		Model:        "logical",
		Input:        []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}},
		Output:       &llm.OutputSpec{MaxTokens: intPointer(1)},
	}
	plan := routing.Plan{Candidates: []routing.Candidate{{ID: "free-first"}, {ID: "free-second"}}}
	entries := map[string]pricing.Entry{
		"free-first":  {Version: "free-v1", Prices: pricing.UnitPrices{}},
		"free-second": {Version: "free-v2", Prices: pricing.UnitPrices{}},
	}

	got, err := (Estimator{}).EstimatePlan(request, plan, entries)
	if err != nil {
		t.Fatal(err)
	}
	if got.CandidateID != "free-first" || got.CatalogVersion != "free-v1" {
		t.Fatalf("all-free maximum = %#v, want first candidate identity", got)
	}
	if !got.CostUSD.IsZero() {
		t.Fatalf("all-free maximum cost = %s, want zero", got.CostUSD.String())
	}
}

func TestEstimateCandidateChargesPerRequestInUSD(t *testing.T) {
	request := llm.Request{
		OperationKey: "estimate",
		Model:        "logical",
		Input:        []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}},
		Output:       &llm.OutputSpec{MaxTokens: intPointer(1)},
	}
	estimate, err := (Estimator{}).EstimateCandidate(request, routing.Candidate{ID: "candidate"}, pricing.Entry{
		Prices: pricing.UnitPrices{PerRequest: pricing.MustDecimalUSD("0.10")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if estimate.CostUSD.String() != "0.100000000000000000" || estimate.MicroUSD != 100000 {
		t.Fatalf("per-request estimate = %#v", estimate)
	}
}

func TestEstimateCandidateRejectsUnknownCatalogComponent(t *testing.T) {
	request := llm.Request{
		OperationKey: "estimate",
		Model:        "logical",
		Input:        []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}},
		Output:       &llm.OutputSpec{MaxTokens: intPointer(1)},
	}
	entry := pricing.Entry{
		Prices:            pricing.UnitPrices{InputPerMillion: pricing.MustDecimalUSD("1")},
		UnknownComponents: []pricing.PriceComponent{pricing.PriceComponentInput},
	}
	if _, err := (Estimator{}).EstimateCandidate(request, routing.Candidate{ID: "candidate"}, entry); err == nil {
		t.Fatal("EstimateCandidate accepted an omitted input price as known zero")
	} else if !errors.Is(err, ErrUnusablePrice) {
		t.Fatalf("unknown component error = %v, want ErrUnusablePrice", err)
	}
}

func TestEstimateCandidateUsesExactCandidateAwareTokenizer(t *testing.T) {
	request := llm.Request{
		OperationKey: "estimate-tokenizer",
		Model:        "logical",
		Input:        []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "tokenizer"}}}},
		Output:       &llm.OutputSpec{MaxTokens: intPointer(1)},
	}
	candidate := routing.Candidate{ID: "provider-b", Provider: "provider-b", Model: "model-b"}
	called := false
	estimate, err := (Estimator{Tokenizer: func(got llm.Request, gotCandidate routing.Candidate) (int64, error) {
		called = true
		if got.OperationKey != request.OperationKey || gotCandidate.ID != candidate.ID {
			t.Fatalf("tokenizer inputs = %q/%q, want request/candidate", got.OperationKey, gotCandidate.ID)
		}
		return 17, nil
	}}).EstimateCandidate(request, candidate, pricing.Entry{
		Version: "prices-tokenizer",
		Prices: pricing.UnitPrices{
			InputPerMillion:      pricing.MustDecimalUSD("1"),
			CacheWritePerMillion: pricing.MustDecimalUSD("1"),
			OutputPerMillion:     pricing.MustDecimalUSD("1"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !called || estimate.InputTokens != 17 || estimate.CacheReadTokens != 0 || estimate.CacheWriteTokens != 17 || estimate.CandidateID != candidate.ID {
		t.Fatalf("exact tokenizer estimate = %#v, called=%t", estimate, called)
	}
}

func TestEstimateCandidateRejectsInvalidExactTokenizerResult(t *testing.T) {
	request := llm.Request{OperationKey: "estimate-tokenizer-invalid", Model: "logical"}
	entry := pricing.Entry{Prices: pricing.UnitPrices{OutputPerMillion: pricing.MustDecimalUSD("1")}}
	for _, test := range []struct {
		name string
		fn   Tokenizer
	}{
		{name: "negative", fn: func(llm.Request, routing.Candidate) (int64, error) { return -1, nil }},
		{name: "error", fn: func(llm.Request, routing.Candidate) (int64, error) { return 0, fmt.Errorf("tokenizer unavailable") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := (Estimator{Tokenizer: test.fn}).EstimateCandidate(request, routing.Candidate{ID: "candidate"}, entry); err == nil {
				t.Fatal("invalid exact tokenizer result was accepted")
			}
		})
	}
}

func TestEstimateCandidateRejectsConfiguredTokenCeilings(t *testing.T) {
	output := 9
	reasoning := 7
	request := llm.Request{
		OperationKey: "estimate-token-limits",
		Model:        "logical",
		Output:       &llm.OutputSpec{MaxTokens: &output},
		Reasoning:    &llm.ReasoningSpec{TokenBudget: &reasoning},
	}
	entry := pricing.Entry{Prices: pricing.UnitPrices{}}
	tests := []struct {
		name      string
		estimator Estimator
	}{
		{name: "input", estimator: Estimator{MaxInput: 4, MaxOutput: 9, MaxReasoning: 7, Tokenizer: func(llm.Request, routing.Candidate) (int64, error) { return 5, nil }}},
		{name: "output", estimator: Estimator{MaxInput: 5, MaxOutput: 8, MaxReasoning: 7, Tokenizer: func(llm.Request, routing.Candidate) (int64, error) { return 5, nil }}},
		{name: "reasoning", estimator: Estimator{MaxInput: 5, MaxOutput: 9, MaxReasoning: 6, Tokenizer: func(llm.Request, routing.Candidate) (int64, error) { return 5, nil }}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.estimator.EstimateCandidate(request, routing.Candidate{ID: "candidate"}, entry); !errors.Is(err, ErrTokenLimit) {
				t.Fatalf("token ceiling error = %v, want %v", err, ErrTokenLimit)
			}
		})
	}
}

func TestEstimateCandidateRejectsMicroUSDCompatibilityOverflow(t *testing.T) {
	request := llm.Request{OperationKey: "estimate-overflow", Model: "logical"}
	candidate := routing.Candidate{ID: "overflow-candidate"}
	for _, test := range []struct {
		name      string
		tokens    int64
		cacheCost string
	}{
		{name: "component exceeds safe range", tokens: int64(^uint64(0) >> 1)},
		{name: "checked total exceeds safe range", tokens: 8_000_000_000_000_000, cacheCost: "1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			estimator := Estimator{Tokenizer: func(llm.Request, routing.Candidate) (int64, error) { return test.tokens, nil }}
			cachePrice := test.cacheCost
			if cachePrice == "" {
				cachePrice = "0"
			}
			entry := pricing.Entry{Prices: pricing.UnitPrices{
				InputPerMillion:      pricing.MustDecimalUSD("1"),
				CacheWritePerMillion: pricing.MustDecimalUSD(cachePrice),
			}}
			if _, err := estimator.EstimateCandidate(request, candidate, entry); err == nil {
				t.Fatal("estimate silently dropped an overflowing microUSD compatibility value")
			} else if !errors.Is(err, ErrUnusablePrice) {
				t.Fatalf("overflow error = %v, want ErrUnusablePrice", err)
			}
		})
	}
}

func TestEstimateCandidateChargesCacheOnlyUpToSignedCaps(t *testing.T) {
	request := llm.Request{OperationKey: "estimate-cache", Model: "logical", Output: &llm.OutputSpec{MaxTokens: intPointer(0)}}
	tokenizer := func(llm.Request, routing.Candidate) (int64, error) { return 1_000, nil }
	// Claude Opus 5.5 list prices: a cache write costs more than input, so the
	// uncapped assumption must still charge it.
	entry := pricing.Entry{Prices: pricing.UnitPrices{
		InputPerMillion:      pricing.MustDecimalUSD("4"),
		CacheReadPerMillion:  pricing.MustDecimalUSD("0.20"),
		CacheWritePerMillion: pricing.MustDecimalUSD("5"),
	}}
	unknownCache := pricing.Entry{Prices: entry.Prices, UnknownComponents: []pricing.PriceComponent{pricing.PriceComponentCacheRead, pricing.PriceComponentCacheWrite}}
	for _, test := range []struct {
		name              string
		bounds            *CacheTokenBounds
		entry             pricing.Entry
		read, write       int64
		cost              string
		unusablePriceWant bool
	}{
		{name: "uncapped", entry: entry, read: 1_000, write: 1_000, cost: "0.009200000000000000"},
		{name: "frozen zero caps", bounds: &CacheTokenBounds{}, entry: entry, cost: "0.004000000000000000"},
		{name: "read cap below input", bounds: &CacheTokenBounds{Read: 300}, entry: entry, read: 300, cost: "0.004060000000000000"},
		{name: "caps above input", bounds: &CacheTokenBounds{Read: 5_000, Write: 5_000}, entry: entry, read: 1_000, write: 1_000, cost: "0.009200000000000000"},
		{name: "zero caps ignore unknown cache prices", bounds: &CacheTokenBounds{}, entry: unknownCache, cost: "0.004000000000000000"},
		{name: "positive cap needs a known cache price", bounds: &CacheTokenBounds{Read: 1}, entry: unknownCache, unusablePriceWant: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			estimate, err := (Estimator{Tokenizer: tokenizer, CacheBounds: test.bounds}).EstimateCandidate(request, routing.Candidate{ID: "candidate"}, test.entry)
			if test.unusablePriceWant {
				if !errors.Is(err, ErrUnusablePrice) {
					t.Fatalf("error = %v, want %v", err, ErrUnusablePrice)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if estimate.CacheReadTokens != test.read || estimate.CacheWriteTokens != test.write || estimate.CostUSD.String() != test.cost {
				t.Fatalf("estimate = read %d write %d cost %s, want read %d write %d cost %s", estimate.CacheReadTokens, estimate.CacheWriteTokens, estimate.CostUSD.String(), test.read, test.write, test.cost)
			}
		})
	}
}

func TestMatcherContextIncludesCandidateClass(t *testing.T) {
	request := llm.Request{Model: "logical", ServiceClass: llm.ServiceClassStandard}
	context := ContextFor(request, routing.Candidate{EndpointID: "ep", AttemptedClass: llm.ServiceClassPriority}, "prod")
	if context.ServiceClass != llm.ServiceClassPriority || context.EndpointID != "ep" {
		t.Fatalf("unexpected context %#v", context)
	}
}

func intPointer(value int) *int { return &value }
