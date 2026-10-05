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
		name                 string
		limit, input, output int64
		want                 bool
	}{
		{"unspecified", 0, 100, 100, true}, {"boundary", 30, 10, 20, true},
		{"input", 30, 11, 20, false}, {"output", 30, 10, 21, false},
		{"overflow", math.MaxInt64, math.MaxInt64, 1, false}, {"max boundary", math.MaxInt64, math.MaxInt64 - 1, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateContextCounts(tc.limit, tc.input, tc.output)
			if (err == nil) != tc.want || (!tc.want && !errors.Is(err, ErrContextLimit)) {
				t.Fatalf("got %v, fit=%t", err, tc.want)
			}
		})
	}
	for _, counts := range [][3]int64{{-1, 1, 1}, {1, -1, 1}, {1, 1, -1}} {
		if err := validateContextCounts(counts[0], counts[1], counts[2]); err == nil || errors.Is(err, ErrContextLimit) {
			t.Fatalf("invalid counts treated as route overflow: %v", err)
		}
	}
}

func TestValidateContextUsesResolvedModelAndReservedTokens(t *testing.T) {
	request := llm.Request{OperationKey: "op", Model: "logical", ServiceClass: llm.ServiceClassPriority, ServiceClassFallbacks: []llm.ServiceClass{llm.ServiceClassStandard}}
	candidate := routing.Candidate{Model: "physical", AttemptedClass: llm.ServiceClassStandard, ContextTokens: 30}
	estimator := Estimator{MaxOutput: 20, Tokenizer: func(r llm.Request, c routing.Candidate) (int64, error) {
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

// Thinking is generated inside the output cap for every supported family
// (Anthropic budget_tokens must be less than max_tokens; OpenAI
// max_output_tokens and max_completion_tokens include reasoning tokens), so a
// reasoning budget must not be added on top of the cap.
func TestValidateContextCountsReasoningInsideOutputCap(t *testing.T) {
	tokenizer := func(llm.Request, routing.Candidate) (int64, error) { return 10, nil }
	for _, tc := range []struct {
		family    string
		estimator Estimator
		reasoning *llm.ReasoningSpec
	}{
		// Anthropic families carry the budget on the request.
		{"anthropic_messages", Estimator{Tokenizer: tokenizer}, &llm.ReasoningSpec{TokenBudget: intPointer(16)}},
		{"bedrock_messages", Estimator{Tokenizer: tokenizer}, &llm.ReasoningSpec{TokenBudget: intPointer(16)}},
		// OpenAI families reject token_budget and Bedrock Converse forwards no
		// reasoning controls; only the configured maximum applies.
		{"bedrock_converse", Estimator{MaxReasoning: 16, Tokenizer: tokenizer}, nil},
		{"openai_responses", Estimator{MaxReasoning: 16, Tokenizer: tokenizer}, &llm.ReasoningSpec{Effort: llm.ReasoningEffortHigh}},
		{"openai_chat", Estimator{MaxReasoning: 16, Tokenizer: tokenizer}, nil},
	} {
		t.Run(tc.family, func(t *testing.T) {
			request := llm.Request{OperationKey: "op", Model: "logical", Reasoning: tc.reasoning, Output: &llm.OutputSpec{MaxTokens: intPointer(20)}}
			candidate := routing.Candidate{ID: "c", Family: tc.family, Model: "physical", AttemptedClass: llm.ServiceClassStandard, ContextTokens: 30}
			if err := tc.estimator.ValidateContext(request, candidate); err != nil {
				t.Fatalf("reasoning inside the output cap rejected: %v", err)
			}
			estimate, err := tc.estimator.EstimateCandidate(request, candidate, pricing.Entry{Family: tc.family})
			if err != nil {
				t.Fatalf("reservation rejected a request that fits: %v", err)
			}
			// The reservation still carries the reasoning component.
			if estimate.InputTokens != 10 || estimate.OutputTokens != 20 || estimate.ReasoningTokens != 16 {
				t.Fatalf("estimate = %#v", estimate)
			}
			candidate.ContextTokens--
			if err := tc.estimator.ValidateContext(request, candidate); !errors.Is(err, ErrContextLimit) {
				t.Fatalf("input plus output cap over the window: %v", err)
			}
			if _, err := tc.estimator.EstimateCandidate(request, candidate, pricing.Entry{Family: tc.family}); !errors.Is(err, ErrContextLimit) {
				t.Fatalf("reservation over the window: %v", err)
			}
		})
	}
}
