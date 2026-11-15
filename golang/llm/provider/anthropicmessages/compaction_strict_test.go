package anthropicmessages

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestCompactionRequestCompilesInStrictModeWithApplicationInstructions(t *testing.T) {
	testassert.NoError(t, testassert.Err(lowerRequestWithStrict(adaptertest.StrictCompactionRequest(t), DefaultProfile("anthropic"), "", true)), "strict compaction request lowering error")
}
