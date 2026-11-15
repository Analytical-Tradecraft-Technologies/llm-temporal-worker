package bedrockmessages

import (
	"context"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/messagestest"
)

// compileShared compiles on a Bedrock Messages adapter whose contract
// profile expects request.Model, for the tests shared with Anthropic Messages.
func compileShared(t *testing.T, request llm.Request, strict bool) (provider.Call, error) {
	t.Helper()
	profile := DefaultProfile("bedrock-contract")
	profile.CapabilityVersion = "bedrock-contract/v1"
	profile.Capabilities.Version = profile.CapabilityVersion
	profile.ExpectedModel = request.Model
	validated, err := NewProfile(profile)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &Adapter{endpointID: "bedrock-test", profile: validated}
	return adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: "bedrock-test", Family: provider.FamilyBedrockMessages, Model: request.Model},
		Strict:  strict,
	})
}

func TestCompileSharedWithTheOtherMessagesFamily(t *testing.T) {
	messagestest.CompileTests(t, compileShared)
}
func intPtr(value int) *int { return &value }
