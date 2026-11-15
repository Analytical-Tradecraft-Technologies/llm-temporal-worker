package anthropicmessages

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/messagestest"
)

func TestToolHistoryPreservesJSONNumbers(t *testing.T) {
	messagestest.ToolHistoryPreservesJSONNumbers(t, lowerContract)
}

// lowerContract lowers request on the validated test profile.
func lowerContract(t *testing.T, request llm.Request) (any, error) {
	return lowerRequest(request, mustProfile(t, testProfile()), "")
}
