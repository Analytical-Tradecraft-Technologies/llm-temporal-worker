package openairesponses

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/contract"
)

func compileResponsesExtension(t *testing.T, extension string, strict bool, options ...AdapterOption) (provider.Call, error) {
	t.Helper()
	fixture := newFixtureAdapter(t, []byte(`{"id":"unused"}`))
	adapter, err := New(fixture.client, "openai-prod", "cap-test", options...)
	testassert.NoError(t, err)
	return adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{
			OperationKey: "extension-allowlist",
			Model:        "gpt-contract",
			Input:        []llm.Item{llm.HumanText("hello")},
			Extensions:   map[string]json.RawMessage{"openai.responses": json.RawMessage(extension)},
		},
		Query:  provider.CapabilityQuery{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract"},
		Strict: strict,
	})
}

// A background run returns queued, which the lift rejects after the provider
// has accepted and billed the work, so no endpoint can enable it.
func TestCompileAlwaysRejectsBackgroundExtension(t *testing.T) {
	for _, strict := range []bool{false, true} {
		for _, options := range [][]AdapterOption{nil, {WithAllowedExtensionFields("background", "store")}, {WithAllowedExtensionFields("background"), WithProviderStoragePermitted(true)}} {
			call, err := compileResponsesExtension(t, `{"background":true}`, strict, options...)
			testassert.ErrorContains(t, err, `"background" is not supported`, fmt.Sprintf("strict=%v Compile()", strict))
			if call.SDKParams != nil {
				t.Fatalf("strict=%v rejected extension produced dispatch parameters", strict)
			}
		}
	}
}

func TestCompileGatesExtensionFieldsByEndpointAllowlist(t *testing.T) {
	for _, strict := range []bool{false, true} {
		for _, test := range []struct {
			name      string
			extension string
			options   []AdapterOption
			wantError string
		}{
			{name: "no allowlist", extension: `{"truncation":"auto"}`, wantError: `"truncation" is not allowed by this endpoint`},
			{name: "other field allowed", extension: `{"store":true}`, options: []AdapterOption{WithAllowedExtensionFields("include")}, wantError: `"store" is not allowed by this endpoint`},
			{name: "unknown field", extension: `{"metadata":{}}`, options: []AdapterOption{WithAllowedExtensionFields("metadata")}, wantError: `"metadata" is not supported`},
			{name: "allowed field", extension: `{"truncation":"auto"}`, options: []AdapterOption{WithAllowedExtensionFields("truncation")}},
		} {
			call, err := compileResponsesExtension(t, test.extension, strict, test.options...)
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("%s strict=%v: %v", test.name, strict, err)
				}
				if wire := contract.JSONObject(t, call.SDKParams); wire["truncation"] != "auto" {
					t.Fatalf("%s strict=%v wire = %#v", test.name, strict, wire)
				}
				continue
			}
			testassert.ErrorContains(t, err, test.wantError, fmt.Sprintf("%s strict=%v Compile()", test.name, strict))
		}
	}
}
