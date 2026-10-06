package anthropicmessages

import (
	"encoding/json"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func testProfile() Profile {
	profile := DefaultProfile("anthropic-contract")
	profile.CapabilityVersion = "anthropic-contract/v1"
	profile.Capabilities.Version = profile.CapabilityVersion
	profile.ExpectedBaseURL = "https://api.anthropic.com/v1"
	profile.ExpectedModel = "claude-contract"
	profile.AllowedExtensions = map[string]ExtensionSpec{
		"anthropic.contract": {Fields: map[string]string{"container_id": "container"}},
	}
	return profile
}

func testCapabilities(profile Profile) provider.CapabilitySet {
	copy, err := NewProfile(profile)
	if err != nil {
		panic(err)
	}
	return copy.Capabilities
}

func marshalWire(t *testing.T, value any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}
