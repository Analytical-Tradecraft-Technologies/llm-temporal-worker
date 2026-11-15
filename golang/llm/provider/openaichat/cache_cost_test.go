package openaichat

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/openai/openai-go/v3"
)

func TestCachedInputCostUsesDisjointComponents(t *testing.T) {
	adaptertest.CachedInputCostUsesDisjointComponents(t, "prompt_tokens", func(total, read, write int64) (llm.Response, error) {
		response := openai.ChatCompletion{ID: "resp", Model: "gpt", ServiceTier: openai.ChatCompletionServiceTierDefault, Choices: []openai.ChatCompletionChoice{{FinishReason: "stop", Message: openai.ChatCompletionMessage{Role: "assistant"}}}}
		response.Usage.PromptTokens = total
		response.Usage.PromptTokensDetails.CachedTokens = read
		response.Usage.PromptTokensDetails.CacheWriteTokens = write
		response.Usage.TotalTokens = total
		return liftStandard(&response)
	})
}
