package openairesponses

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/openai/openai-go/v3/responses"
)

func TestCachedInputCostUsesDisjointComponents(t *testing.T) {
	adaptertest.CachedInputCostUsesDisjointComponents(t, "input_tokens", func(total, read, write int64) (llm.Response, error) {
		response := minimalResponse(responses.ResponseServiceTierDefault, responses.ResponseStatusCompleted)
		response.Usage.InputTokens = total
		response.Usage.InputTokensDetails.CachedTokens = read
		response.Usage.InputTokensDetails.CacheWriteTokens = write
		response.Usage.TotalTokens = total
		return liftResponse(provider.Call{ServiceClass: llm.ServiceClassStandard}, &response, "req")
	})
}
