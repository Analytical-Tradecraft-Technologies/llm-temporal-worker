package anthropicmessages

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestNewProfileRejectsCollidingExtensionWireFields(t *testing.T) {
	adaptertest.NewProfileRejectsCollidingExtensions(t, "contract_tag", func(fields adaptertest.ExtensionFields) error {
		profile := testProfile()
		profile.AllowedExtensions = extensionSpecs(fields)
		_, err := NewProfile(profile)
		return err
	})
}

func TestCompileRejectsCollidingExtensionWireFields(t *testing.T) {
	adaptertest.CompileRejectsCollidingExtensions(t, "contract_tag", newExtensionCompiler)
}

func TestCompileNonOverlappingExtensionsSerializeConsistently(t *testing.T) {
	adaptertest.NonOverlappingExtensionsSerializeConsistently(t, "contract_tag", "contract_mode", newExtensionCompiler)
}

func extensionSpecs(fields adaptertest.ExtensionFields) map[string]ExtensionSpec {
	specs := make(map[string]ExtensionSpec, len(fields))
	for namespace, namespaceFields := range fields {
		specs[namespace] = ExtensionSpec{Fields: namespaceFields}
	}
	return specs
}

func newExtensionCompiler(t *testing.T, fields adaptertest.ExtensionFields, validate bool) adaptertest.ExtensionCompiler {
	profile := testProfile()
	profile.AllowedExtensions = extensionSpecs(fields)
	if validate {
		profile = mustProfile(t, profile)
	}
	adapter := &Adapter{endpointID: "anthropic-test", profile: profile}
	return func(extensions map[string]json.RawMessage) (provider.Call, error) {
		return adapter.Compile(context.Background(), provider.CompileInput{
			Request: llm.Request{
				OperationKey: "extension-collision",
				Model:        "claude-contract",
				Input:        []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}},
				Extensions:   extensions,
			},
			Query:  provider.CapabilityQuery{EndpointID: "anthropic-test", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
			Strict: true,
		})
	}
}
