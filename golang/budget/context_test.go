package budget

import (
	"errors"
	"math"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

func TestContextTokenAccounting(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		limit, input, output, reasoning int64
		want                            bool
	}{
		{"unspecified", 0, 100, 100, 100, true}, {"boundary", 30, 10, 15, 5, true},
		{"input", 30, 11, 15, 5, false}, {"output", 30, 10, 16, 5, false}, {"reasoning", 30, 10, 15, 6, false},
		{"overflow", math.MaxInt64, math.MaxInt64, 1, 0, false}, {"max boundary", math.MaxInt64, math.MaxInt64 - 2, 1, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateContextCounts(tc.limit, tc.input, tc.output, tc.reasoning)
			if (err == nil) != tc.want || (!tc.want && !errors.Is(err, ErrContextLimit)) {
				t.Fatalf("got %v, fit=%t", err, tc.want)
			}
		})
	}
	for _, counts := range [][4]int64{{-1, 1, 1, 1}, {1, -1, 1, 1}, {1, 1, -1, 1}, {1, 1, 1, -1}} {
		if err := validateContextCounts(counts[0], counts[1], counts[2], counts[3]); err == nil || errors.Is(err, ErrContextLimit) {
			t.Fatalf("invalid counts treated as route overflow: %v", err)
		}
	}
}

func TestValidateContextUsesResolvedModelAndReservedTokens(t *testing.T) {
	request := llm.Request{OperationKey: "op", Model: "logical", ServiceClass: llm.ServiceClassPriority, ServiceClassFallbacks: []llm.ServiceClass{llm.ServiceClassStandard}}
	candidate := routing.Candidate{Model: "physical", AttemptedClass: llm.ServiceClassStandard, ContextTokens: 30}
	estimator := Estimator{MaxOutput: 15, MaxReasoning: 5, Tokenizer: func(r llm.Request, c routing.Candidate) (int64, error) {
		if r.Model != "physical" || r.ServiceClass != llm.ServiceClassStandard || len(r.ServiceClassFallbacks) != 0 {
			t.Fatalf("unresolved tokenizer request: %#v", r)
		}
		return 10, nil
	}}
	if err := estimator.ValidateContext(request, candidate); err != nil {
		t.Fatal(err)
	}
	candidate.ContextTokens--
	if err := estimator.ValidateContext(request, candidate); !errors.Is(err, ErrContextLimit) {
		t.Fatalf("got %v", err)
	}
	estimator.Tokenizer = func(llm.Request, routing.Candidate) (int64, error) { return 0, errors.New("counter unavailable") }
	if err := estimator.ValidateContext(request, candidate); err == nil || errors.Is(err, ErrContextLimit) {
		t.Fatalf("tokenizer failure hidden: %v", err)
	}
	candidate.ContextTokens = 0
	if err := estimator.ValidateContext(request, candidate); err != nil {
		t.Fatal(err)
	}
}

func TestEstimateCandidateCannotSkipContextValidation(t *testing.T) {
	estimator := Estimator{MaxOutput: 20, Tokenizer: func(llm.Request, routing.Candidate) (int64, error) { return 10, nil }}
	_, err := estimator.EstimateCandidate(llm.Request{}, routing.Candidate{ContextTokens: 29}, pricing.Entry{})
	if !errors.Is(err, ErrContextLimit) {
		t.Fatalf("got %v", err)
	}
}

func TestValidateContextReservesRequestedReasoning(t *testing.T) {
	estimator := Estimator{MaxOutput: 10, MaxReasoning: 5, Tokenizer: func(llm.Request, routing.Candidate) (int64, error) { return 10, nil }}
	request := llm.Request{OperationKey: "op", Model: "logical", Reasoning: &llm.ReasoningSpec{TokenBudget: intPointer(11)}}
	candidate := routing.Candidate{Model: "physical", AttemptedClass: llm.ServiceClassStandard, ContextTokens: 30}
	if err := estimator.ValidateContext(request, candidate); !errors.Is(err, ErrContextLimit) {
		t.Fatalf("requested reasoning not reserved: %v", err)
	}
}
