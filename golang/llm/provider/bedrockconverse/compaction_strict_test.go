package bedrockconverse

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
)

func TestCompactionRequestCompilesInStrictModeWithApplicationInstructions(t *testing.T) {
	testassert.NoError(t, testassert.Err(lowerRequest(adaptertest.StrictCompactionRequest(t), DefaultProfile("nova"), string(types.ServiceTierTypeDefault), true)), "strict compaction request lowering error")
}
