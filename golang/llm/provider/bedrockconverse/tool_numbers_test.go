package bedrockconverse

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
)

func TestToolHistoryPreservesJSONNumbers(t *testing.T) {
	adaptertest.ToolHistoryPreservesJSONNumbers(t, func(t *testing.T, request llm.Request) []byte {
		params, err := lowerRequest(request, Profile{}, "", false)
		if err != nil {
			t.Fatal(err)
		}
		block := params.Messages[0].Content[0].(*types.ContentBlockMemberToolUse)
		actual, err := block.Value.Input.MarshalSmithyDocument()
		if err != nil {
			t.Fatal(err)
		}
		return actual
	})
}
