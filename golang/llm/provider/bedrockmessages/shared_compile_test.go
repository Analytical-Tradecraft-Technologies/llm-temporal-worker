package bedrockmessages

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
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
	testassert.NoError(t, err)
	adapter := &Adapter{endpointID: "bedrock-test", profile: validated}
	return adaptertest.Compile(adapter, "bedrock-test", provider.FamilyBedrockMessages, request, strict)
}

func TestCompileSharedWithTheOtherMessagesFamily(t *testing.T) {
	messagestest.CompileTests(t, compileShared)
}
func intPtr(value int) *int { return &value }
