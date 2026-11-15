package anthropicmessages

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestCompactionRequestCompilesInStrictModeWithApplicationInstructions(t *testing.T) {
	if _, err := lowerRequestWithStrict(adaptertest.StrictCompactionRequest(t), DefaultProfile("anthropic"), "", true); err != nil {
		t.Fatalf("strict compaction request lowering error = %v", err)
	}
}
