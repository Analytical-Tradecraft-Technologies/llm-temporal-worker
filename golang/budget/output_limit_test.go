package budget

import (
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"math"
	"testing"
)

func TestPreparedOutputLimitMatchesEstimate(t *testing.T) {
	for _, configured := range []int64{0, 16} {
		for _, explicit := range []int{0, 7} {
			request := llm.Request{OperationKey: "limit", Model: "model", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}}
			if explicit != 0 {
				request.Output = &llm.OutputSpec{MaxTokens: &explicit}
			}
			estimator := Estimator{MaxOutput: configured}
			prepared, err := estimator.PrepareRequest(request)
			if err != nil {
				t.Fatal(err)
			}
			estimate, err := estimator.EstimateCandidate(request, routing.Candidate{}, pricing.Entry{})
			if err != nil {
				t.Fatal(err)
			}
			want := configured
			if want == 0 {
				want = 1000
			}
			if explicit != 0 {
				want = int64(explicit)
			}
			if prepared.Output == nil || prepared.Output.MaxTokens == nil || int64(*prepared.Output.MaxTokens) != want || estimate.OutputTokens != want {
				t.Fatalf("limit mismatch: prepared=%+v estimate=%+v want=%d", prepared.Output, estimate, want)
			}
			*prepared.Output.MaxTokens = 99
			if explicit == 0 && request.Output != nil {
				t.Fatal("mutated caller output")
			}
			if explicit != 0 && *request.Output.MaxTokens != explicit {
				t.Fatal("mutated caller limit")
			}
		}
	}
}

func TestOutputLimitRejectsZeroNegativeAndSDKOverflow(t *testing.T) {
	for _, limit := range []int64{0, -1, math.MaxInt32 + 1} {
		value := int(limit)
		request := llm.Request{OperationKey: "limit", Model: "model", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}, Output: &llm.OutputSpec{MaxTokens: &value}}
		estimator := Estimator{MaxOutput: 16}
		if _, err := estimator.PrepareRequest(request); err == nil {
			t.Fatalf("prepared invalid cap %d", limit)
		}
		if _, err := estimator.EstimateCandidate(request, routing.Candidate{}, pricing.Entry{}); err == nil {
			t.Fatalf("estimated invalid cap %d", limit)
		}
	}
}
