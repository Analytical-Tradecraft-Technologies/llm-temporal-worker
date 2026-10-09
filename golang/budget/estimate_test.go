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
	if !called || estimate.InputTokens != 17 || estimate.CacheWriteTokens != 17 || estimate.CandidateID != candidate.ID {
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

func TestMatcherContextIncludesCandidateClass(t *testing.T) {
	request := llm.Request{Model: "logical", ServiceClass: llm.ServiceClassStandard}
	context := ContextFor(request, routing.Candidate{EndpointID: "ep", AttemptedClass: llm.ServiceClassPriority}, "prod")
	if context.ServiceClass != llm.ServiceClassPriority || context.EndpointID != "ep" {
		t.Fatalf("unexpected context %#v", context)
	}
}

func intPointer(value int) *int { return &value }

func TestEstimateRejectsOverlappingReasoningPrices(t *testing.T) {
	for _, family := range []string{"openai_chat", "openai_responses"} {
		entry := pricing.Entry{Family: family, Prices: pricing.UnitPrices{ReasoningPerMillion: pricing.MustDecimalUSD("1")}}
		_, err := (Estimator{}).EstimateCandidate(llm.Request{}, routing.Candidate{Family: family}, entry)
		if !errors.Is(err, ErrUnusablePrice) {
			t.Fatalf("%s error = %v, want unusable price", family, err)
		}
	}
}

func TestHostedToolAllowanceAndKnownContext(t *testing.T) {
	request := llm.Request{OperationKey: "hosted", Model: "logical", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "calculate"}}}}, Output: &llm.OutputSpec{MaxTokens: intPointer(100)}}
	candidate := routing.Candidate{ID: "openai", Family: "openai_responses", ContextTokens: 1000}
	entry := pricing.Entry{Prices: pricing.UnitPrices{InputPerMillion: pricing.MustDecimalUSD("1"), OutputPerMillion: pricing.MustDecimalUSD("2")}}
	estimator := Estimator{SafetyRatio: big.NewRat(1, 1)}
	plain, err := estimator.EstimateCandidate(request, candidate, entry)
	if err != nil {
		t.Fatal(err)
	}
	request.WebSearch, request.CodeExecution = true, true
	hosted, err := estimator.EstimateCandidate(request, candidate, entry)
	if err != nil {
		t.Fatal(err)
	}
	if hosted.CostUSD.Cmp(plain.CostUSD) <= 0 || hosted.InputTokens != 4000 || hosted.OutputTokens != 400 {
		t.Fatalf("allowance=%+v plain=%+v", hosted, plain)
	}
	candidate.ContextTokens = 0
	if _, err := estimator.EstimateCandidate(request, candidate, entry); !errors.Is(err, ErrUnusablePrice) {
		t.Fatalf("unknown context accepted: %v", err)
	}
}

func TestResearchReservationUsesTaskSizeNotModelContext(t *testing.T) {
	request := llm.Request{OperationKey: "research", Model: "anthropic/claude-opus-5.5", WebSearch: true, Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "Find research questions about an election."}}}}, Output: &llm.OutputSpec{MaxTokens: intPointer(4096)}}
	candidate := routing.Candidate{ID: "anthropic", Family: "anthropic_messages", ContextTokens: 1_000_000}
	entry := pricing.Entry{Prices: pricing.UnitPrices{InputPerMillion: pricing.MustDecimalUSD("4"), OutputPerMillion: pricing.MustDecimalUSD("20"), CacheWritePerMillion: pricing.MustDecimalUSD("5")}}
	estimator := Estimator{SafetyRatio: big.NewRat(135, 100)}
	got, err := estimator.EstimateCandidate(request, candidate, entry)
	if err != nil {
		t.Fatal(err)
	}
	if got.OutputTokens != 4096 || got.InputTokens < 49152 || got.InputTokens > 100000 || got.CostUSD.Cmp(pricing.MustUSD("2")) >= 0 {
		t.Fatalf("task reservation = %+v", got)
	}
	candidate.ContextTokens = 200000
	smaller, err := estimator.EstimateCandidate(request, candidate, entry)
	if err != nil {
		t.Fatal(err)
	}
	if smaller.CostUSD.Cmp(got.CostUSD) != 0 {
		t.Fatalf("unused context changed reservation: %+v vs %+v", smaller, got)
	}
	request.WebFetch = true
	fetched, err := estimator.EstimateCandidate(request, candidate, entry)
	if err != nil {
		t.Fatal(err)
	}
	if fetched.InputTokens <= got.InputTokens || fetched.OutputTokens != got.OutputTokens {
		t.Fatalf("fetch allowance = %+v", fetched)
	}
}

func TestResearchReservationGrowsWithPromptAndFitsProductionWindow(t *testing.T) {
	candidate := routing.Candidate{Family: "anthropic_messages", ContextTokens: 1_000_000}
	entry := pricing.Entry{Prices: pricing.UnitPrices{InputPerMillion: pricing.MustDecimalUSD("4"), OutputPerMillion: pricing.MustDecimalUSD("20"), CacheWritePerMillion: pricing.MustDecimalUSD("5")}}
	input := int64(1000)
	estimator := Estimator{MaxOutput: 32768, SafetyRatio: big.NewRat(135, 100), Tokenizer: func(llm.Request, routing.Candidate) (int64, error) { return input, nil }}
	request := llm.Request{WebSearch: true}
	small, err := estimator.EstimateCandidate(request, candidate, entry)
	if err != nil {
		t.Fatal(err)
	}
	if small.CostUSD.Cmp(pricing.MustUSD("30")) >= 0 || small.OutputTokens != 32768 {
		t.Fatalf("production reservation=%+v", small)
	}
	input = 2000
	large, err := estimator.EstimateCandidate(request, candidate, entry)
	if err != nil {
		t.Fatal(err)
	}
	if large.InputTokens-small.InputTokens != 4000 || large.CostUSD.Cmp(small.CostUSD) <= 0 {
		t.Fatalf("prompt growth not counted for each pass: %+v vs %+v", large, small)
	}
	t.Logf("short research request reserves $%s under the $30 hourly window", small.CostUSD.String())
}

func TestResearchReservationBoundsEachContinuationByContext(t *testing.T) {
	request := llm.Request{WebSearch: true, Output: &llm.OutputSpec{MaxTokens: intPointer(100)}}
	estimator := Estimator{Tokenizer: func(llm.Request, routing.Candidate) (int64, error) { return 200, nil }}
	for _, family := range []string{"anthropic_messages", "openai_responses"} {
		got, err := estimator.EstimateCandidate(request, routing.Candidate{Family: family, ContextTokens: 1000}, pricing.Entry{})
		if err != nil {
			t.Fatal(err)
		}
		if got.InputTokens != 200+3*900 || got.OutputTokens != 100 {
			t.Fatalf("%s reservation=%+v", family, got)
		}
	}
}
