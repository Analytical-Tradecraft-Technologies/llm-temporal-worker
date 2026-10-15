package anthropicmessages

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
			"alpha": {Fields: map[string]string{"value": "inference_geo"}},
			"beta":  {Fields: map[string]string{"value": "inference_geo"}},
		},
		"within namespace": {
			"alpha": {Fields: map[string]string{"first": "inference_geo", "second": "inference_geo"}},
		},
		"empty alias": {
			"alpha": {Fields: map[string]string{"inference_geo": "", "value": "inference_geo"}},
		},
		"empty alias across namespaces": {
			"alpha": {Fields: map[string]string{"inference_geo": ""}},
			"beta":  {Fields: map[string]string{"value": "inference_geo"}},
		},
	}
	for name, extensions := range cases {
		t.Run(name, func(t *testing.T) {
			var first string
			for range 50 {
				profile := testProfile()
				profile.AllowedExtensions = extensions
				_, err := NewProfile(profile)
				if err == nil || !strings.Contains(err.Error(), `both map to wire field "inference_geo"`) {
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

// A profile that bypasses NewProfile must still fail every compilation of a
// request that sets both colliding fields instead of choosing one at random.
func TestCompileRejectsCollidingExtensionWireFields(t *testing.T) {
	profile := testProfile()
	profile.AllowedExtensions = map[string]ExtensionSpec{
		"alpha": {Fields: map[string]string{"value": "inference_geo"}},
		"beta":  {Fields: map[string]string{"value": "inference_geo"}},
	}
	adapter := &Adapter{endpointID: "anthropic-test", profile: profile}
	request := collisionRequest(map[string]json.RawMessage{
		"alpha": json.RawMessage(`{"value":"us"}`),
		"beta":  json.RawMessage(`{"value":"eu"}`),
	})
	for range 200 {
		call, err := adapter.Compile(context.Background(), collisionInput(request))
		if err == nil || !strings.Contains(err.Error(), `wire field "inference_geo" already set`) {
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
		"alpha": {Fields: map[string]string{"value": "inference_geo"}},
		"beta":  {Fields: map[string]string{"value": "speed"}},
	}
	adapter := &Adapter{endpointID: "anthropic-test", profile: mustProfile(t, profile)}
	request := collisionRequest(map[string]json.RawMessage{
		"alpha": json.RawMessage(`{"value":"us"}`),
		"beta":  json.RawMessage(`{"value":"fast"}`),
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
			if wire["inference_geo"] != "us" || wire["speed"] != "fast" {
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
		Model:        "claude-contract",
		Input:        []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}},
		Extensions:   extensions,
	}
}

func collisionInput(request llm.Request) provider.CompileInput {
	return provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: "anthropic-test", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
		Strict:  true,
	}
}
