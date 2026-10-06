package budget

import (
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
)

type MatchedWindow struct {
	PolicyID string
	Window   Window
	Context  MatchContext
}

func MatchPolicies(policies []Policy, context MatchContext) []MatchedWindow {
	result := make([]MatchedWindow, 0)
	for _, policy := range policies {
		if !policy.Match.Matches(context) {
			continue
		}
		for _, window := range policy.Windows {
			result = append(result, MatchedWindow{PolicyID: policy.ID, Window: window, Context: context})
		}
	}
	return result
}

func MatchPlan(policies []Policy, request llm.Request, plan routing.Plan, environment string) map[string][]MatchedWindow {
	result := make(map[string][]MatchedWindow)
	for _, candidate := range plan.Candidates {
		context := ContextFor(request, candidate, environment)
		result[candidate.ID] = MatchPolicies(policies, context)
	}
	return result
}
