package anthropicmessages

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// US-only inference and fast mode multiply every token price. A workspace
// default_inference_geo applies with no request field, so the usage the
// provider reports decides whether the base catalog price can settle.
func TestLiftKeepsPriceModifiedUsageUnknown(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fields  string
		unknown bool
	}{
		{name: "no modifiers reported"},
		{name: "global inference", fields: `,"inference_geo":"global"`},
		{name: "standard speed", fields: `,"speed":"standard"`},
		{name: "null modifiers", fields: `,"inference_geo":null,"speed":null`},
		{name: "workspace default us inference", fields: `,"inference_geo":"us"`, unknown: true},
		{name: "fast mode", fields: `,"speed":"fast"`, unknown: true},
		{name: "us fast mode", fields: `,"inference_geo":"us","speed":"fast"`, unknown: true},
		{name: "unrecognized geo", fields: `,"inference_geo":"eu"`, unknown: true},
		{name: "non-string speed", fields: `,"speed":2`, unknown: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var message anthropic.Message
			if err := json.Unmarshal([]byte(`{"id":"msg-synthetic","type":"message","role":"assistant","model":"claude-contract","stop_reason":"end_turn","content":[{"type":"text","text":"answer"}],"usage":{"input_tokens":1000000,"output_tokens":1000000,"service_tier":"standard"`+tc.fields+`}}`), &message); err != nil {
				t.Fatal(err)
			}
			got, err := mustProfile(t, testProfile()).liftResponse(provider.Call{
				OperationKey: "price-modifier", Model: "claude-contract", ServiceClass: llm.ServiceClassStandard,
			}, &message, "req-synthetic")
			if err != nil {
				t.Fatal(err)
			}
			if got.Usage.InputTokens != 1000000 || got.Usage.OutputTokens != 1000000 {
				t.Fatalf("token usage lost: %#v", got.Usage)
			}
			if unknown := got.Cost.Status == llm.CostStatusUnknown; unknown != tc.unknown {
				t.Fatalf("cost = %#v, want unknown %v", got.Cost, tc.unknown)
			}
		})
	}
}

// An allowlist may not route a caller field to a price-multiplying wire field
// under any caller-side name.
func TestNewProfileRejectsPriceModifierExtensionTargets(t *testing.T) {
	for _, fields := range []map[string]string{
		{"geo": "inference_geo"},
		{"speed": "speed"},
		{"speed": ""},
		{"mode": "speed"},
	} {
		profile := testProfile()
		profile.AllowedExtensions["anthropic.price"] = ExtensionSpec{Fields: fields}
		if _, err := NewProfile(profile); err == nil || !strings.Contains(err.Error(), "cannot override") {
			t.Fatalf("NewProfile(%v) = %v, want request-owned rejection", fields, err)
		}
	}
}
