package openairesponses

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/packages/param"
	"github.com/openai/openai-go/v3/responses"
)

func TestStoragePolicy(t *testing.T) {
	body, err := os.ReadFile("testdata/contracts/openai-responses/response.completed.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, permitted := range []bool{false, true} {
		for _, extension := range []string{`{}`, `{"store":false}`, `{"store":true}`, `{"background":true}`} {
			calls := 0
			fixture := newFixtureAdapterWithTransport(t, body, func() { calls++ })
			adapter, err := New(fixture.client, "openai-prod", "cap-test", WithProviderStoragePermitted(permitted))
			if err != nil {
				t.Fatal(err)
			}
			call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: llm.Request{OperationKey: "policy", Model: "gpt-contract", Extensions: map[string]json.RawMessage{"openai.responses": json.RawMessage(extension)}}, Query: provider.CapabilityQuery{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract"}, Strict: true})
			if !permitted && (extension == `{"store":true}` || extension == `{"background":true}`) {
				if err == nil || calls != 0 {
					t.Fatalf("override accepted: %s", extension)
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			params := call.SDKParams.(responses.ResponseNewParams)
			if !permitted && (!params.Store.Valid() || params.Store.Value) {
				t.Fatal("storage not explicitly disabled")
			}

			encoded, err := json.Marshal(params)
			if err != nil {
				t.Fatal(err)
			}
			var wire map[string]any
			if err := json.Unmarshal(encoded, &wire); err != nil {
				t.Fatal(err)
			}
			if !permitted && wire["store"] != false {
				t.Fatalf("serialized store = %v", wire["store"])
			}
			result, err := adapter.Invoke(context.Background(), call, nil)
			if err != nil {
				t.Fatal(err)
			}
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
			if err != nil {
				t.Fatal(err)
			}
			_, err = adapter.Invoke(context.Background(), provider.Call{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, SDKParams: params}, nil)
			if err == nil || calls != 0 {
				t.Fatalf("error=%v calls=%d", err, calls)
			}
			caps, err := adapter.Capabilities(context.Background(), provider.CapabilityQuery{})
			if err != nil {
				t.Fatal(err)
			}
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
			client, err := NewClient(ClientConfig{BaseURL: "https://127.0.0.1/contract", APIKey: "test-key", HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				wire, _ = io.ReadAll(request.Body)
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{}`)), Request: request}, nil
			})}})
			if err != nil {
				t.Fatal(err)
			}
			permitted, err := New(client, "openai-prod", "cap-test", WithProviderStoragePermitted(true))
			if err != nil {
				t.Fatal(err)
			}
			denied, err := New(client, "openai-prod", "cap-test", WithProviderStoragePermitted(false))
			if err != nil {
				t.Fatal(err)
			}
			request := llm.Request{OperationKey: "policy-change", Model: "gpt-contract"}
			if slot != "output" {
				request.Tools = []llm.Tool{{Name: "lookup", InputSchema: json.RawMessage(schema)}}
			}
			if slot != "tool" {
				request.Output = &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Strict: true, Schema: json.RawMessage(schema)}}
			}
			call, err := permitted.Compile(context.Background(), provider.CompileInput{Request: request, Query: provider.CapabilityQuery{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: request.Model}})
			if err != nil {
				t.Fatal(err)
			}
			_, _ = denied.Invoke(context.Background(), call, nil)
			var body map[string]json.RawMessage
			if err := json.Unmarshal(wire, &body); err != nil {
				t.Fatal(err)
			}
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
		if err != nil {
			t.Fatal(err)
		}
		params := responses.ResponseNewParams{}
		param.SetJSON([]byte(raw), &params)
		_, err = adapter.Invoke(context.Background(), provider.Call{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, SDKParams: params}, nil)
		if err == nil || calls != 0 {
			t.Fatalf("forbidden raw body dispatched: error=%v calls=%d", err, calls)
		}
	}
}
