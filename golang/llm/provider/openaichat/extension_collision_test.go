package openaichat

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestNewProfileRejectsCollidingExtensionWireFields(t *testing.T) {
	adaptertest.NewProfileRejectsCollidingExtensions(t, "user", func(fields adaptertest.ExtensionFields) error {
		profile := testProfile()
		profile.AllowedExtensions = extensionSpecs(fields)
		_, err := NewProfile(profile)
		return err
	})
}

// Lowering refuses every write to a reserved wire field, so aliases sharing a
// reserved field (as the OpenRouter provider controls do) cannot collide.
func TestNewProfileAllowsSharedReservedExtensionWireField(t *testing.T) {
	profile := testProfile()
	profile.AllowedExtensions = map[string]ExtensionSpec{
		"alpha": {Fields: map[string]string{"first": "provider", "second": "provider"}},
	}
	profile.ReservedWireFields = map[string]struct{}{"provider": {}}
	if _, err := NewProfile(profile); err != nil {
		t.Fatal(err)
	}
	// NewProfile reserves wire defaults too, so they need no explicit entry.
	profile.ReservedWireFields = nil
	profile.WireDefaults = map[string]json.RawMessage{"provider": json.RawMessage(`{"allow_fallbacks":false}`)}
	validated, err := NewProfile(profile)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &Adapter{endpointID: "chat-prod", profile: validated}
	request := collisionRequest(map[string]json.RawMessage{"alpha": json.RawMessage(`{"first":{}}`)})
	if _, err := adapter.Compile(context.Background(), collisionInput(request)); err == nil || !strings.Contains(err.Error(), "reserved wire field") {
		t.Fatalf("Compile() = %v, want reserved-field refusal", err)
	}
}

func TestCompileRejectsCollidingExtensionWireFields(t *testing.T) {
	adaptertest.CompileRejectsCollidingExtensions(t, "user", newExtensionCompiler)
}

func TestCompileNonOverlappingExtensionsSerializeConsistently(t *testing.T) {
	adaptertest.NonOverlappingExtensionsSerializeConsistently(t, "user", "safety_identifier", newExtensionCompiler)
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
		validated, err := NewProfile(profile)
		if err != nil {
			t.Fatal(err)
		}
		profile = validated
	}
	adapter := &Adapter{endpointID: "chat-prod", profile: profile}
	return func(extensions map[string]json.RawMessage) (provider.Call, error) {
		return adapter.Compile(context.Background(), collisionInput(collisionRequest(extensions)))
	}
}

func collisionRequest(extensions map[string]json.RawMessage) llm.Request {
	return llm.Request{
		OperationKey: "extension-collision",
		Model:        "chat-model",
		Input:        []llm.Item{llm.HumanText("hello")},
		Extensions:   extensions,
	}
}

func collisionInput(request llm.Request) provider.CompileInput {
	return provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"},
		Strict:  true,
	}
}
