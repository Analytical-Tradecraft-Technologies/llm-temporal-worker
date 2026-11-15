package bedrockmessages

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/messagestest"
)

func TestNoToolsPolicyAllowsCompactionRequest(t *testing.T) {
	messagestest.NoToolsPolicyOmitsTools(t, lowerContract)
}
