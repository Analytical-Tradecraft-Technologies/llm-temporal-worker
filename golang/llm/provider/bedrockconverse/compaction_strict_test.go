package bedrockconverse

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
)

func TestCompactionRequestCompilesInStrictModeWithApplicationInstructions(t *testing.T) {
	if _, err := lowerRequest(adaptertest.StrictCompactionRequest(t), DefaultProfile("nova"), string(types.ServiceTierTypeDefault), true); err != nil {
		t.Fatalf("strict compaction request lowering error = %v", err)
	}
}
