package openaichat

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// Overlapping allowlist entries would let map iteration pick the provider
// value, so NewProfile rejects them across namespaces, within one namespace,
// and when an empty alias resolves to another field's wire name.
func TestNewProfileRejectsCollidingExtensionWireFields(t *testing.T) {
	cases := map[string]map[string]ExtensionSpec{
		"across namespaces": {
			"alpha": {Fields: map[string]string{"value": "user"}},
			"beta":  {Fields: map[string]string{"value": "user"}},
		},
		"within namespace": {
			"alpha": {Fields: map[string]string{"first": "user", "second": "user"}},
		},
		"empty alias": {
			"alpha": {Fields: map[string]string{"user": "", "value": "user"}},
		},
		"empty alias across namespaces": {
			"alpha": {Fields: map[string]string{"user": ""}},
			"beta":  {Fields: map[string]string{"value": "user"}},
		},
	}
	for name, extensions := range cases {
		t.Run(name, func(t *testing.T) {
			var first string
			for range 50 {
				profile := testProfile()
				profile.AllowedExtensions = extensions
				_, err := NewProfile(profile)
				if err == nil || !strings.Contains(err.Error(), `both map to wire field "user"`) {
					t.Fatalf("NewProfile() = %v, want wire collision", err)
				}
				if first == "" {
					first = err.Error()
				} else if err.Error() != first {
					t.Fatalf("collision error changed between runs: %q then %q", first, err.Error())
				}
			}
		})
	}
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

// A profile that bypasses NewProfile must still fail every compilation of a
// request that sets both colliding fields instead of choosing one at random.
func TestCompileRejectsCollidingExtensionWireFields(t *testing.T) {
	profile := testProfile()
	profile.AllowedExtensions = map[string]ExtensionSpec{
		"alpha": {Fields: map[string]string{"value": "user"}},
		"beta":  {Fields: map[string]string{"value": "user"}},
	}
	adapter := &Adapter{endpointID: "chat-prod", profile: profile}
	request := collisionRequest(map[string]json.RawMessage{
		"alpha": json.RawMessage(`{"value":"first"}`),
		"beta":  json.RawMessage(`{"value":"second"}`),
	})
	for range 200 {
		call, err := adapter.Compile(context.Background(), collisionInput(request))
		if err == nil || !strings.Contains(err.Error(), `wire field "user" already set`) {
			t.Fatalf("Compile() = %v, want wire collision", err)
		}
		if call.SDKParams != nil {
			t.Fatal("rejected extension produced dispatch parameters")
		}
	}
}

// Nonoverlapping mappings keep working and serialize identically every time.
func TestCompileNonOverlappingExtensionsSerializeConsistently(t *testing.T) {
	profile := testProfile()
	profile.AllowedExtensions = map[string]ExtensionSpec{
		"alpha": {Fields: map[string]string{"value": "user"}},
		"beta":  {Fields: map[string]string{"value": "safety_identifier"}},
	}
	validated, err := NewProfile(profile)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &Adapter{endpointID: "chat-prod", profile: validated}
	request := collisionRequest(map[string]json.RawMessage{
		"alpha": json.RawMessage(`{"value":"first"}`),
		"beta":  json.RawMessage(`{"value":"second"}`),
	})
	var first []byte
	for range 200 {
		call, err := adapter.Compile(context.Background(), collisionInput(request))
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(call.SDKParams)
		if err != nil {
			t.Fatal(err)
		}
		if first == nil {
			first = encoded
			wire := marshalWire(t, call.SDKParams)
			if wire["user"] != "first" || wire["safety_identifier"] != "second" {
				t.Fatalf("wire = %#v", wire)
			}
		} else if string(encoded) != string(first) {
			t.Fatalf("serialization changed between compilations:\n%s\n%s", first, encoded)
		}
	}
}

func collisionRequest(extensions map[string]json.RawMessage) llm.Request {
	return llm.Request{
		OperationKey: "extension-collision",
		Model:        "chat-model",
		Input:        []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}},
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
