package bedrockmessages

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/messagestest"
)

func TestLowerItemRemapsNonPortableToolCallIDs(t *testing.T) {
	messagestest.RemapsNonPortableToolCallIDs(t, lowerItem)
}
