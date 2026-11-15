package openairesponses

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/packages/param"
	"github.com/openai/openai-go/v3/responses"
)

func TestStoragePolicy(t *testing.T) {
	body, err := os.ReadFile("testdata/contracts/openai-responses/response.completed.json")
	testassert.NoError(t, err)
	for _, permitted := range []bool{false, true} {
		for _, extension := range []string{`{}`, `{"store":false}`, `{"store":true}`} {
			calls := 0
			fixture := newFixtureAdapterWithTransport(t, body, func() { calls++ })
			adapter, err := New(fixture.client, "openai-prod", "cap-test", WithProviderStoragePermitted(permitted), allowSupportedExtensions)
			testassert.NoError(t, err)
			call, err := adaptertest.Compile(adapter, "openai-prod", provider.FamilyOpenAIResponses, llm.Request{OperationKey: "policy", Model: "gpt-contract", Extensions: map[string]json.RawMessage{"openai.responses": json.RawMessage(extension)}}, true)
			if !permitted && extension == `{"store":true}` {
				if err == nil || calls != 0 {
					t.Fatalf("override accepted: %s", extension)
				}
				continue
			}
			testassert.NoError(t, err)
			params := call.SDKParams.(responses.ResponseNewParams)
			if !permitted && (!params.Store.Valid() || params.Store.Value) {
				t.Fatal("storage not explicitly disabled")
			}

			encoded := testassert.MarshalJSON(t, params)
			wire := testassert.UnmarshalJSON[map[string]any](t, encoded)
			if !permitted && wire["store"] != false {
				t.Fatalf("serialized store = %v", wire["store"])
			}
			result, err := adapter.Invoke(context.Background(), call, nil)
			testassert.NoError(t, err)
			if calls != 1 {
				t.Fatalf("HTTP calls=%d", calls)
			}
			if !permitted && result.Response.Continuation != nil {
				t.Fatal("storage denied but continuation returned")
			}
		}
	}
}

func TestStoragePolicyRejectsBypassingCompile(t *testing.T) {
	for name, params := range map[string]responses.ResponseNewParams{"store": {Store: openai.Bool(true)}, "background": {Background: openai.Bool(true)}, "continuation": {PreviousResponseID: openai.String("resp-private")}} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			fixture := newFixtureAdapterWithTransport(t, []byte(`{}`), func() { calls++ })
			adapter, err := New(fixture.client, "openai-prod", "cap-test", WithProviderStoragePermitted(false))
			testassert.NoError(t, err)
			_, err = adapter.Invoke(context.Background(), provider.Call{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, SDKParams: params}, nil)
			if err == nil || calls != 0 {
				t.Fatalf("error=%v calls=%d", err, calls)
			}
			caps, err := adapter.Capabilities(context.Background(), provider.CapabilityQuery{})
			testassert.NoError(t, err)
			if caps.Features[provider.FeatureContinuation].State != provider.CapabilityUnsupported {
				t.Fatal("continuation advertised")
			}
		})
	}
}

func TestStoragePolicyReconfigurationPreservesExactSchemas(t *testing.T) {
	const schema = `{"type":"object","properties":{"id":{"type":"integer","enum":[9007199254740993]}},"required":["id"],"additionalProperties":false}`
	for _, slot := range []string{"tool", "output", "both"} {
		t.Run(slot, func(t *testing.T) {
			var wire []byte
			client, err := NewClient(ClientConfig{BaseURL: "https://127.0.0.1/contract", APIKey: "test-key", HTTPClient: &http.Client{Transport: testkit.RoundTripFunc(func(request *http.Request) (*http.Response, error) {
				wire, _ = io.ReadAll(request.Body)
				return adaptertest.JSONResponse(request, http.StatusOK, `{}`), nil
			})}})
			testassert.NoError(t, err)
			permitted, err := New(client, "openai-prod", "cap-test", WithProviderStoragePermitted(true))
			testassert.NoError(t, err)
			denied, err := New(client, "openai-prod", "cap-test", WithProviderStoragePermitted(false))
			testassert.NoError(t, err)
			request := llm.Request{OperationKey: "policy-change", Model: "gpt-contract"}
			if slot != "output" {
				request.Tools = []llm.Tool{{Name: "lookup", InputSchema: json.RawMessage(schema)}}
			}
			if slot != "tool" {
				request.Output = &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Strict: true, Schema: json.RawMessage(schema)}}
			}
			call, err := adaptertest.Compile(permitted, "openai-prod", provider.FamilyOpenAIResponses, request, false)
			testassert.NoError(t, err)
			_, _ = denied.Invoke(context.Background(), call, nil)
			body := testassert.UnmarshalJSON[map[string]json.RawMessage](t, wire)
			if string(body["store"]) != "false" {
				t.Fatalf("storage denial missing from actual transport: %s", wire)
			}
			count := 1
			if slot == "both" {
				count = 2
			}
			if strings.Count(string(wire), `"enum":[9007199254740993]`) != count {
				t.Fatalf("schema changed under storage denial: %s", wire)
			}
		})
	}
}

func TestStoragePolicyRejectsForbiddenRawOverrides(t *testing.T) {
	for _, raw := range []string{`{"store":true}`, `{"background":true}`, `{"previous_response_id":"resp-stored"}`} {
		calls := 0
		fixture := newFixtureAdapterWithTransport(t, []byte(`{}`), func() { calls++ })
		adapter, err := New(fixture.client, "openai-prod", "cap-test", WithProviderStoragePermitted(false))
		testassert.NoError(t, err)
		params := responses.ResponseNewParams{}
		param.SetJSON([]byte(raw), &params)
		_, err = adapter.Invoke(context.Background(), provider.Call{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, SDKParams: params}, nil)
		if err == nil || calls != 0 {
			t.Fatalf("forbidden raw body dispatched: error=%v calls=%d", err, calls)
		}
	}
}
