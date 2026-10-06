package openaichat

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestExaUsesAPIKeyExtraBodyCitationsAndExactCost(t *testing.T) {
	var got *http.Request
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		got = request
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "X-Request-Id": []string{"exa-req-1"}},
			Body:       io.NopCloser(strings.NewReader(`{"id":"exa-generation-1","model":"exa","service_tier":"standard","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"answer"}}],"usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8},"costDollars":{"total":"0.00000123"},"results":[{"url":"https://example.com/source","title":"Source"}]}`)),
			Request:    request,
		}, nil
	})
	client, err := NewExaClient(ExaClientConfig{BaseURL: exaBaseURL, APIKey: "exa-key", HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := NewExaProfile(ExaProfileConfig{
		ID:                        "exa-chat",
		CapabilityVersion:         "exa/v1",
		BaseURL:                   exaBaseURL,
		Capabilities:              profileTestCapabilities("exa/v1"),
		ServiceTiers:              map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "standard", llm.ServiceClassPriority: ""},
		ActualServiceClasses:      map[string]llm.ServiceClass{"standard": llm.ServiceClassStandard},
		MissingActualServiceClass: llm.ServiceClassStandard,
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "exa-a", profile)
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: llm.Request{OperationKey: "exa-op", Model: "exa"}, Query: provider.CapabilityQuery{EndpointID: "exa-a", Family: provider.FamilyOpenAIChat, Model: "exa"}, Strict: true})
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Invoke(context.Background(), call, provider.NopObserver{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Header.Get("x-api-key") != "exa-key" || got.Header.Get("Authorization") != "" {
		t.Fatalf("exa auth headers = %#v", got.Header)
	}
	body, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(body, &wire); err != nil {
		t.Fatal(err)
	}
	if wire["text"] != true {
		t.Fatalf("exa text = %#v", wire["text"])
	}
	for _, field := range []string{"extra_body", "service_tier"} {
		if _, present := wire[field]; present {
			t.Fatalf("exa request sent %q: %s", field, body)
		}
	}
	if result.Response.Cost.ActualCostUSD == nil || result.Response.Cost.ActualCostUSD.String() != "0.000001230000000000" || result.Response.Provider.RequestID != "exa-req-1" || result.Response.Provider.GenerationID != "exa-generation-1" {
		t.Fatalf("exa response metadata = %#v", result.Response)
	}
	if len(result.Response.Output) != 2 {
		t.Fatalf("exa output = %#v", result.Response.Output)
	}
	if _, ok := result.Response.Output[1].(llm.Reference); !ok {
		t.Fatalf("exa citation output = %#v", result.Response.Output[1])
	}
}

func TestExaRejectsNegativeReportedCost(t *testing.T) {
	profile := Profile{ResponseAugment: augmentExa}
	lifted := &llm.Response{Provider: llm.ProviderFacts{Raw: map[string]json.RawMessage{}}, Usage: llm.Usage{ProviderRaw: map[string]json.RawMessage{}}}
	response := &openai.ChatCompletion{}
	responseJSON := `{"id":"exa","choices":[] ,"costDollars":{"total":"-0.1"}}`
	if err := json.Unmarshal([]byte(responseJSON), response); err != nil {
		t.Fatal(err)
	}
	if err := profile.ResponseAugment(provider.Call{}, response, lifted); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("negative cost error = %v", err)
	}
}

func TestExaLiftsCitationsFromSelectedMessage(t *testing.T) {
	for name, body := range map[string]string{
		"message":                 `{"id":"exa-generation-2","model":"exa","service_tier":"standard","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"answer","citations":[{"url":"https://example.com/message-source","title":"Message source"}]}}],"usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8}}`,
		"null message falls back": `{"id":"exa-generation-3","model":"exa","service_tier":"standard","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"answer","citations":null}}],"citations":[{"url":"https://example.com/message-source","title":"Root source"}],"usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8}}`,
	} {
		t.Run(name, func(t *testing.T) { assertExaCitationLifted(t, body) })
	}
}

func assertExaCitationLifted(t *testing.T, body string) {
	t.Helper()
	client, err := NewExaClient(ExaClientConfig{BaseURL: exaBaseURL, APIKey: "exa-key", HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})}})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := NewExaProfile(ExaProfileConfig{
		ID: "exa-chat", CapabilityVersion: "exa/v1", BaseURL: exaBaseURL, Capabilities: profileTestCapabilities("exa/v1"),
		ServiceTiers:              map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "standard", llm.ServiceClassPriority: ""},
		ActualServiceClasses:      map[string]llm.ServiceClass{"standard": llm.ServiceClassStandard},
		MissingActualServiceClass: llm.ServiceClassStandard,
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "exa-a", profile)
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: llm.Request{OperationKey: "exa-op", Model: "exa"}, Query: provider.CapabilityQuery{EndpointID: "exa-a", Family: provider.FamilyOpenAIChat, Model: "exa"}, Strict: true})
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Invoke(context.Background(), call, provider.NopObserver{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Response.Output) != 2 {
		t.Fatalf("exa output = %#v", result.Response.Output)
	}
	reference, ok := result.Response.Output[1].(llm.Reference)
	if !ok || !strings.Contains(mustMarshal(t, reference), "message-source") {
		t.Fatalf("exa message citation = %#v", result.Response.Output[1])
	}
	if _, ok := result.Response.Provider.Raw["exa_citations"]; !ok {
		t.Fatalf("exa raw citations missing: %#v", result.Response.Provider.Raw)
	}
}

func mustMarshal(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// A checkpoint stores the lifted output, citations included, and the next
// turn replays it as input. The citations are annotations with no Chat
// Completions wire form, so the follow-up must compile without them.
func TestExaCompilesFollowUpAfterCitationResponse(t *testing.T) {
	body := `{"id":"exa-generation-4","model":"exa","service_tier":"standard","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"answer","citations":[{"url":"https://example.com/message-source","title":"Message source"},{"url":"https://example.com/second-source"}]}}],"usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8}}`
	client, err := NewExaClient(ExaClientConfig{BaseURL: exaBaseURL, APIKey: "exa-key", HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})}})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewExaAdapter(client, "exa-a", ExaProfileConfig{
		ID: "exa-chat", CapabilityVersion: "exa/v1", BaseURL: exaBaseURL, Capabilities: profileTestCapabilities("exa/v1"),
		ServiceTiers:              map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "standard", llm.ServiceClassPriority: ""},
		ActualServiceClasses:      map[string]llm.ServiceClass{"standard": llm.ServiceClassStandard},
		MissingActualServiceClass: llm.ServiceClassStandard,
	})
	if err != nil {
		t.Fatal(err)
	}
	query := provider.CapabilityQuery{EndpointID: "exa-a", Family: provider.FamilyOpenAIChat, Model: "exa"}
	human := func(text string) llm.Message {
		return llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}}
	}
	first := llm.Request{OperationKey: "exa-turn-1", Model: "exa", Input: []llm.Item{human("question")}}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: first, Query: query, Strict: true})
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Invoke(context.Background(), call, provider.NopObserver{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Response.Output) != 3 {
		t.Fatalf("exa output = %#v", result.Response.Output)
	}
	for _, item := range result.Response.Output[1:] {
		if _, ok := item.(llm.Reference); !ok {
			t.Fatalf("exa citation output = %#v", item)
		}
	}
	second := llm.Request{OperationKey: "exa-turn-2", Model: "exa"}
	second.Input = append(append(append(second.Input, first.Input...), result.Response.Output...), human("follow-up"))
	for name, strict := range map[string]bool{"strict": true, "best effort": false} {
		t.Run(name, func(t *testing.T) {
			call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: second, Query: query, Strict: strict})
			if err != nil {
				t.Fatalf("compile follow-up turn: %v", err)
			}
			wire := marshalWire(t, call.SDKParams)
			messages := wire["messages"].([]any)
			roles := make([]string, 0, len(messages))
			for _, message := range messages {
				roles = append(roles, message.(map[string]any)["role"].(string))
			}
			if got, want := strings.Join(roles, ","), "user,assistant,user"; got != want {
				t.Fatalf("message roles = %s, want %s", got, want)
			}
			if encoded := mustMarshal(t, wire); strings.Contains(encoded, "example.com") {
				t.Fatalf("citation reached the wire: %s", encoded)
			}
		})
	}
}

// A citation between a tool call and its result must not detach the call from
// the assistant message it belongs to or from its result.
func TestLowerSkipsReferenceInsideToolExchange(t *testing.T) {
	request := llm.Request{OperationKey: "reference-tools", Model: "chat-contract", Input: []llm.Item{
		llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "question"}}},
		llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "looking"}}},
		llm.Reference{URI: "https://example.com/first-source"},
		llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"sydney"}`)},
		llm.Reference{URI: "https://example.com/second-source"},
		llm.ToolResult{CallID: "call-1", Name: "lookup", Content: []llm.Part{llm.TextPart{Text: "found"}}},
	}}
	params, err := lowerRequest(request, testProfile(), "")
	if err != nil {
		t.Fatalf("lower replayed references: %v", err)
	}
	wire := marshalWire(t, params)
	messages := wire["messages"].([]any)
	if len(messages) != 3 {
		t.Fatalf("messages = %#v", messages)
	}
	assistant := messages[1].(map[string]any)
	if calls, _ := assistant["tool_calls"].([]any); assistant["role"] != "assistant" || len(calls) != 1 {
		t.Fatalf("assistant message = %#v", assistant)
	}
	if tool := messages[2].(map[string]any); tool["role"] != "tool" || tool["tool_call_id"] != "call-1" {
		t.Fatalf("tool message = %#v", tool)
	}
	if encoded := mustMarshal(t, wire); strings.Contains(encoded, "example.com") {
		t.Fatalf("reference reached the wire: %s", encoded)
	}
}
