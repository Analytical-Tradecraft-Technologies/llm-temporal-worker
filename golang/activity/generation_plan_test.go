package activity

import (
	"context"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"testing"
)

type generationPlanningStub struct {
	v1RuntimeStub
	calls int
}

func (s *generationPlanningStub) PlanGenerationV1(context.Context, llm.GenerateRequestV1) (llm.GenerationPlanV1, error) {
	s.calls++
	return llm.GenerationPlanV1{CompactBeforeGenerate: true}, nil
}
func TestGenerationPlanActivityValidatesAndFailsClosed(t *testing.T) {
	request := validGenerateV1Request()
	for _, activities := range []*Activities{nil, {}, {V1Runtime: &v1RuntimeStub{}}} {
		if result, err := activities.PlanGenerationV1(context.Background(), request); err == nil || result != nil {
			t.Fatal("missing planner accepted")
		}
	}
	stub := &generationPlanningStub{}
	activities := &Activities{V1Runtime: stub}
	if result, err := activities.PlanGenerationV1(context.Background(), llm.GenerateRequestV1{}); err == nil || result != nil || stub.calls != 0 {
		t.Fatal("invalid input reached planner")
	}
	if result, err := activities.PlanGenerationV1(context.Background(), request); err != nil || result == nil || !result.CompactBeforeGenerate || stub.calls != 1 {
		t.Fatalf("valid plan failed: %v", err)
	}
}
