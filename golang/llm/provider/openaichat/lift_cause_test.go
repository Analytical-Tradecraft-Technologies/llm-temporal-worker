package openaichat

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// A response rejected while lifting names a closed cause and the provider's
// finish reason in its safe details, so it can be diagnosed without logging
// any of its content.
func TestOpenRouterLiftFailuresNameTheirCause(t *testing.T) {
	maxTokens := 256
	output := &llm.OutputSpec{MaxTokens: &maxTokens, Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Strict: true,
		Schema: json.RawMessage(`{"type":"object","additionalProperties":false,"required":["url"],"properties":{"url":{"type":"string"}}}`)}}
	choice := func(finish, content string) string {
		encoded, _ := json.Marshal(content)
		return `{"id":"gen-1","model":"router-model","choices":[{"index":0,"finish_reason":"` + finish + `","message":{"role":"assistant","content":` + string(encoded) + `}}],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}`
	}
	for _, test := range []struct {
		name, body, cause, finish string
	}{
		{"trailing citations", choice("stop", "{\"url\":\"https://example.com\"}\n\n[1] https://example.com"), "json_trailing_text", "stop"},
		{"code fence", choice("stop", "```json\n{\"url\":\"https://example.com\"}\n```"), "json_fenced", "stop"},
		{"leading prose", choice("stop", "Here it is: {\"url\":\"https://example.com\"}"), "json_leading_text", "stop"},
		{"schema violation", choice("stop", `{"link":"https://example.com"}`), "json_schema_violation", "stop"},
		{"empty answer", choice("stop", ""), "json_text_missing", "stop"},
		{"tool finish without a call", choice("tool_calls", `{"url":"https://example.com"}`), "finish_reason_without_tool_call", "tool_calls"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := invokeOpenRouterRequest(t, llm.Request{OperationKey: "or-op", Model: "router-model", Output: output}, test.body)
			var mapped *provider.Error
			if !errors.As(err, &mapped) || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Phase != provider.PhaseLift {
				t.Fatalf("Invoke() error = %#v", err)
			}
			if mapped.SafeDetails["lift_cause"] != test.cause || mapped.SafeDetails["finish_reason"] != test.finish {
				t.Fatalf("safe details = %v, want cause %s finish %s", mapped.SafeDetails, test.cause, test.finish)
			}
			for _, value := range mapped.SafeDetails {
				if strings.Contains(value, "example.com") {
					t.Fatalf("response content in safe details: %v", mapped.SafeDetails)
				}
			}
		})
	}
	if err := invokeOpenRouterRequest(t, llm.Request{OperationKey: "or-op", Model: "router-model", Output: output}, choice("stop", "\n\n{\"url\":\"https://example.com\"}")); err != nil {
		t.Fatalf("valid JSON with leading whitespace rejected: %v", err)
	}
}

func invokeOpenRouterRequest(t *testing.T, request llm.Request, body string) error {
	t.Helper()
	transport := roundTripFunc(func(httpRequest *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: httpRequest}, nil
	})
	client, err := NewOpenRouterClient(OpenRouterClientConfig{BaseURL: openRouterBaseURL, APIKey: "or-key", HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := NewOpenRouterProfile(OpenRouterProfileConfig{
		ID: "openrouter-lift", CapabilityVersion: "openrouter/v1", BaseURL: openRouterBaseURL, Model: "router-model",
		Capabilities:              profileTestCapabilities("openrouter/v1"),
		ServiceTiers:              map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "standard", llm.ServiceClassPriority: ""},
		ActualServiceClasses:      map[string]llm.ServiceClass{"default": llm.ServiceClassStandard, "standard": llm.ServiceClassStandard},
		MissingActualServiceClass: llm.ServiceClassStandard,
		ProviderOrder:             []string{"ProviderA"},
		RequireParameters:         true,
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "openrouter-a", profile)
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Query: provider.CapabilityQuery{EndpointID: "openrouter-a", Family: provider.FamilyOpenAIChat, Model: "router-model"}, Strict: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Invoke(context.Background(), call, provider.NopObserver{})
	return err
}
