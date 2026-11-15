package anthropicmessages

import (
	"context"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/messagestest"
)

// compileShared compiles on an Anthropic Messages adapter whose contract
// profile expects request.Model, for the tests shared with Bedrock Messages.
func compileShared(t *testing.T, request llm.Request, strict bool) (provider.Call, error) {
	t.Helper()
	profile := testProfile()
	profile.ExpectedModel = request.Model
	adapter := &Adapter{endpointID: "anthropic-test", profile: mustProfile(t, profile)}
	return adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: "anthropic-test", Family: provider.FamilyAnthropicMessages, Model: request.Model},
		Strict:  strict,
	})
}

func TestCompileSharedWithTheOtherMessagesFamily(t *testing.T) {
	messagestest.CompileTests(t, compileShared)
}
