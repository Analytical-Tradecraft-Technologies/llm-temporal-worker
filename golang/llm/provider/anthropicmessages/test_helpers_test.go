package anthropicmessages

import (
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func testProfile() Profile {
	profile := DefaultProfile("anthropic-contract")
	profile.CapabilityVersion = "anthropic-contract/v1"
	profile.Capabilities.Version = profile.CapabilityVersion
	profile.ExpectedBaseURL = "https://api.anthropic.com/v1"
	profile.ExpectedModel = "claude-contract"
	profile.AllowedExtensions = map[string]ExtensionSpec{
		"anthropic.contract": {Fields: map[string]string{"tag": "contract_tag"}},
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
