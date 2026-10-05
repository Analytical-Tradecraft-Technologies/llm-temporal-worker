package openaichat

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// wireAuditProfile is one Chat Completions route whose real client, profile
// and adapter are composed the way route construction composes them.
type wireAuditProfile struct {
	id        string
	model     string
	profile   Profile
	newClient func(*http.Client) (*Client, error)
}

// wireAuditExtensions is a configured extension namespace that reaches both
// fields the SDK parameter type models and compatible fields it does not.
func wireAuditExtensions() map[string]ExtensionSpec {
	return map[string]ExtensionSpec{"audit": {Fields: map[string]string{
		"end_user":           "user",
		"metadata":           "metadata",
		"logit_bias":         "logit_bias",
		"prediction":         "prediction",
		"function_call":      "function_call",
		"web_search_options": "web_search_options",
		"stream_options":     "stream_options",
		"modalities":         "modalities",
		"audio":              "audio",
		"logprobs":           "logprobs",
		"top_logprobs":       "top_logprobs",
		"store":              "store",
		"n":                  "n",
		"verbosity":          "verbosity",
		"prompt_cache_key":   "prompt_cache_key",
		"transforms":         "transforms",
		"plugins":            "plugins",
		"safe":               "safe_prompt",
	}}}
}

func wireAuditProfiles(t *testing.T) []wireAuditProfile {
	t.Helper()
	direct := testProfile()
	direct.AllowedExtensions = wireAuditExtensions()
	openAIProfile, err := NewProfile(direct)
	if err != nil {
		t.Fatal(err)
	}
	azureEndpoint := "https://127.0.0.1"
	azureProfile, err := NewAzureProfile(AzureProfileConfig{
		ID:                "azure-audit",
		CapabilityVersion: "azure-audit/v1",
		BaseURL:           azureEndpoint,
		Deployment:        "deployment-a",
		Capabilities:      profileTestCapabilities("azure-audit/v1"),
		ServiceTiers: map[llm.ServiceClass]string{
			llm.ServiceClassEconomy:  "",
			llm.ServiceClassStandard: "default",
			llm.ServiceClassPriority: "priority",
		},
		ActualServiceClasses: map[string]llm.ServiceClass{
			"default":  llm.ServiceClassStandard,
			"priority": llm.ServiceClassPriority,
		},
		AllowedExtensions: wireAuditExtensions(),
	})
	if err != nil {
		t.Fatal(err)
	}
	standardOnly := map[llm.ServiceClass]string{
		llm.ServiceClassEconomy:  "",
		llm.ServiceClassStandard: "standard",
		llm.ServiceClassPriority: "",
	}
	openRouterProfile, err := NewOpenRouterProfile(OpenRouterProfileConfig{
		ID:                        "openrouter-audit",
		CapabilityVersion:         "openrouter-audit/v1",
		BaseURL:                   openRouterBaseURL,
		Model:                     "router-model",
		Capabilities:              profileTestCapabilities("openrouter-audit/v1"),
		ServiceTiers:              standardOnly,
		ActualServiceClasses:      map[string]llm.ServiceClass{"standard": llm.ServiceClassStandard},
		MissingActualServiceClass: llm.ServiceClassStandard,
		ProviderOrder:             []string{"ProviderA", "ProviderB"},
		RequireParameters:         true,
		AllowedExtensions:         wireAuditExtensions(),
	})
	if err != nil {
		t.Fatal(err)
	}
	exaProfile, err := NewExaProfile(ExaProfileConfig{
		ID:                        "exa-audit",
		CapabilityVersion:         "exa-audit/v1",
		BaseURL:                   exaBaseURL,
		Capabilities:              profileTestCapabilities("exa-audit/v1"),
		ServiceTiers:              standardOnly,
		ActualServiceClasses:      map[string]llm.ServiceClass{"standard": llm.ServiceClassStandard},
		MissingActualServiceClass: llm.ServiceClassStandard,
		AllowedExtensions:         wireAuditExtensions(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return []wireAuditProfile{
		{id: "openai", model: "chat-model", profile: openAIProfile, newClient: func(httpClient *http.Client) (*Client, error) {
			return NewClient(ClientConfig{BaseURL: "https://127.0.0.1/v1", APIKey: "audit-key", HTTPClient: httpClient})
		}},
		{id: "azure", model: "deployment-a", profile: azureProfile, newClient: func(httpClient *http.Client) (*Client, error) {
			return NewAzureClient(AzureClientConfig{Endpoint: azureEndpoint, APIVersion: "2025-01-01", APIKey: "audit-azure-key", HTTPClient: httpClient})
		}},
		{id: "openrouter", model: "router-model", profile: openRouterProfile, newClient: func(httpClient *http.Client) (*Client, error) {
			return NewOpenRouterClient(OpenRouterClientConfig{BaseURL: openRouterBaseURL, APIKey: "audit-openrouter-key", HTTPClient: httpClient})
		}},
		{id: "exa", model: "exa", profile: exaProfile, newClient: func(httpClient *http.Client) (*Client, error) {
			return NewExaClient(ExaClientConfig{BaseURL: exaBaseURL, APIKey: "audit-exa-key", HTTPClient: httpClient})
		}},
	}
}

// captureWireBody compiles and invokes request through the real adapter and
// returns the HTTP request body the SDK actually wrote.
func captureWireBody(t *testing.T, profile wireAuditProfile, request llm.Request) map[string]any {
	t.Helper()
	var captured []byte
	client, err := profile.newClient(&http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, readErr := io.ReadAll(request.Body)
		if readErr != nil {
			return nil, readErr
		}
		captured = body
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "X-Request-Id": []string{"req-wire-audit"}},
			Body:       io.NopCloser(strings.NewReader(`{"id":"chatcmpl-audit","object":"chat.completion","created":1700000000,"model":"` + profile.model + `","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)),
			Request:    request,
		}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "wire-audit", profile.profile)
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: "wire-audit", Family: provider.FamilyOpenAIChat, Model: profile.model},
		Strict:  true,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	// Only the request is under audit; the canned response need not satisfy
	// the lift for every request shape.
	_, invokeErr := adapter.Invoke(context.Background(), call, provider.NopObserver{})
	if captured == nil {
		t.Fatalf("request body was not captured: Invoke() error = %v", invokeErr)
	}
	var wire map[string]any
	if err := json.Unmarshal(captured, &wire); err != nil {
		t.Fatalf("captured body is not a JSON object: %v: %s", err, captured)
	}
	return wire
}

// intendedWireBody is the request map the lowerer built before it was carried
// into the SDK parameter type.
func intendedWireBody(t *testing.T, profile wireAuditProfile, request llm.Request) map[string]any {
	t.Helper()
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	serviceClass, err := llm.NormalizeServiceClass(normalized.ServiceClass)
	if err != nil {
		t.Fatal(err)
	}
	tier, err := profile.profile.providerTier(serviceClass)
	if err != nil {
		t.Fatal(err)
	}
	requestMap, err := lowerRequestMap(normalized, profile.profile, tier)
	if err != nil {
		t.Fatal(err)
	}
	return marshalWire(t, requestMap)
}

// wireDifferences lists every JSON path at which the captured body lost,
// gained or altered a value relative to the intended body.
func wireDifferences(path string, intended, captured any) []string {
	switch want := intended.(type) {
	case map[string]any:
		got, ok := captured.(map[string]any)
		if !ok {
			return []string{path + ": intended object, captured " + describeWireValue(captured)}
		}
		keys := make(map[string]struct{}, len(want)+len(got))
		for key := range want {
			keys[key] = struct{}{}
		}
		for key := range got {
			keys[key] = struct{}{}
		}
		sorted := make([]string, 0, len(keys))
		for key := range keys {
			sorted = append(sorted, key)
		}
		sort.Strings(sorted)
		var differences []string
		for _, key := range sorted {
			wantValue, wanted := want[key]
			gotValue, present := got[key]
			child := path + "." + key
			switch {
			case !present:
				differences = append(differences, child+": dropped, intended "+describeWireValue(wantValue))
			case !wanted:
				differences = append(differences, child+": added "+describeWireValue(gotValue))
			default:
				differences = append(differences, wireDifferences(child, wantValue, gotValue)...)
			}
		}
		return differences
	case []any:
		got, ok := captured.([]any)
		if !ok || len(got) != len(want) {
			return []string{path + ": intended " + describeWireValue(intended) + ", captured " + describeWireValue(captured)}
		}
		var differences []string
		for index := range want {
			differences = append(differences, wireDifferences(path+"["+strconv.Itoa(index)+"]", want[index], got[index])...)
		}
		return differences
	default:
		if !reflect.DeepEqual(intended, captured) {
			return []string{path + ": intended " + describeWireValue(intended) + ", captured " + describeWireValue(captured)}
		}
		return nil
	}
}

func describeWireValue(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "<unencodable>"
	}
	return string(encoded)
}

func wireAuditTool(name string) llm.Tool {
	return llm.Tool{Name: name, Description: "look up " + name, InputSchema: json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"},"limit":{"type":"integer","minimum":1}},"required":["q"],"additionalProperties":false}`)}
}

func wireAuditUserText(text string) []llm.Item {
	return []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}}}
}

type wireAuditCase struct {
	name    string
	request llm.Request
}

func wireAuditCases() []wireAuditCase {
	maxTokens := 256
	temperature := 0.25
	topP := 0.9
	seed := int64(7)
	presence := 0.5
	frequency := -0.5
	zero := 0.0
	zeroSeed := int64(0)
	zeroTokens := 0
	tools := []llm.Tool{wireAuditTool("lookup"), wireAuditTool("search")}
	schema := json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"},"score":{"type":"number"}},"required":["answer"],"additionalProperties":false}`)
	cases := []wireAuditCase{
		{name: "minimal", request: llm.Request{Input: wireAuditUserText("hello")}},
		{name: "instructions", request: llm.Request{
			Instructions: []llm.Instruction{
				{Level: llm.InstructionLevelPolicy, Kind: llm.InstructionKindText, Text: "policy"},
				{Level: llm.InstructionLevelPolicy, Kind: llm.InstructionKindParts, Content: []llm.Part{llm.TextPart{Text: "more policy"}, llm.JSONPart{Value: json.RawMessage(`{"rule":1}`)}}},
			},
			Input: wireAuditUserText("hello"),
		}},
		{name: "tool policy none without tools", request: llm.Request{Input: wireAuditUserText("hello"), ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNone}}},
		{name: "tools default policy", request: llm.Request{Input: wireAuditUserText("hello"), Tools: tools}},
		{name: "tools auto parallel", request: llm.Request{Input: wireAuditUserText("hello"), Tools: tools, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceAuto, Parallel: true}}},
		{name: "tools none", request: llm.Request{Input: wireAuditUserText("hello"), Tools: tools, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNone}}},
		{name: "tools required", request: llm.Request{Input: wireAuditUserText("hello"), Tools: tools, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceRequired}}},
		{name: "tools named", request: llm.Request{Input: wireAuditUserText("hello"), Tools: tools, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNamed, Name: "search"}}},
		{name: "tools named parallel", request: llm.Request{Input: wireAuditUserText("hello"), Tools: tools, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNamed, Name: "lookup", Parallel: true}}},
		{name: "tool history", request: llm.Request{
			Input: []llm.Item{
				llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "find sydney"}}},
				llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "looking"}}},
				llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"sydney"}`)},
				llm.ToolCall{ID: "call-2", Name: "search", Arguments: json.RawMessage(`{"q":"harbour","limit":2}`)},
				llm.ToolResult{CallID: "call-1", Content: []llm.Part{llm.JSONPart{Value: json.RawMessage(`{"ok":true}`)}}},
				llm.ToolResult{CallID: "call-2", Content: []llm.Part{llm.TextPart{Text: "two results"}}},
				llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.RefusalPart{Text: "cannot continue"}}},
				llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "try again"}}},
				llm.ToolCall{ID: "call-3", Name: "lookup", Arguments: json.RawMessage(`{"q":"perth"}`)},
				llm.ToolResult{CallID: "call-3", Content: []llm.Part{llm.TextPart{Text: ""}}},
			},
			Tools: tools,
		}},
		{name: "max tokens", request: llm.Request{Input: wireAuditUserText("hello"), Output: &llm.OutputSpec{MaxTokens: &maxTokens}}},
		{name: "json object output", request: llm.Request{Input: wireAuditUserText("hello"), Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSON}}}},
		{name: "json schema strict", request: llm.Request{Input: wireAuditUserText("hello"), Output: &llm.OutputSpec{MaxTokens: &maxTokens, Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Description: "answer object", Strict: true, Schema: schema}}}},
		{name: "json schema lenient", request: llm.Request{Input: wireAuditUserText("hello"), Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Schema: schema}}}},
		{name: "reasoning disabled", request: llm.Request{Input: wireAuditUserText("hello"), Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeDisabled}}},
		{name: "reasoning provider default", request: llm.Request{Input: wireAuditUserText("hello"), Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeProviderDefault}}},
		{name: "image url", request: llm.Request{Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
			llm.TextPart{Text: "describe"},
			llm.ImagePart{URL: "https://example.test/image.png", MediaType: "image/png", Detail: "high"},
			llm.ImagePart{URL: "https://example.test/other.png", MediaType: "image/png"},
		}}}}},
		{name: "image bytes", request: llm.Request{Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
			llm.ImagePart{Bytes: []byte{0x89, 'P', 'N', 'G'}, MediaType: "image/png", Detail: "low"},
			llm.JSONPart{Value: json.RawMessage(`{"caption":true}`)},
		}}}}},
		{name: "sampling", request: llm.Request{Input: wireAuditUserText("hello"), Sampling: &llm.SamplingSpec{Temperature: &temperature, TopP: &topP, Seed: &seed, PresencePenalty: &presence, FrequencyPenalty: &frequency}}},
		{name: "zero values", request: llm.Request{Input: wireAuditUserText("hello"), Tools: []llm.Tool{{Name: "bare", InputSchema: json.RawMessage(`{"type":"object"}`)}}, Output: &llm.OutputSpec{MaxTokens: &zeroTokens}, Sampling: &llm.SamplingSpec{Temperature: &zero, TopP: &zero, Seed: &zeroSeed, PresencePenalty: &zero, FrequencyPenalty: &zero}}},
		{name: "single stop sequence", request: llm.Request{Input: wireAuditUserText("hello"), Sampling: &llm.SamplingSpec{StopSequences: []string{"END"}}}},
		{name: "stop sequences", request: llm.Request{Input: wireAuditUserText("hello"), Sampling: &llm.SamplingSpec{StopSequences: []string{"\n", "END", "STOP"}}}},
		{name: "modelled extensions", request: llm.Request{Input: wireAuditUserText("hello"), Extensions: map[string]json.RawMessage{
			"audit": json.RawMessage(`{"end_user":"pinned","metadata":{"trace":"abc","tenant":"t1"},"logit_bias":{"50256":-100},"prediction":{"type":"content","content":"predicted"}}`),
		}}},
		{name: "modelled union extensions", request: llm.Request{Input: wireAuditUserText("hello"), Extensions: map[string]json.RawMessage{
			"audit": json.RawMessage(`{"function_call":{"name":"lookup"},"web_search_options":{"search_context_size":"low","user_location":{"type":"approximate","approximate":{"country":"AU","city":"Sydney"}}},"stream_options":{"include_usage":true},"modalities":["text","audio"],"audio":{"format":"wav","voice":"alloy"},"logprobs":true,"top_logprobs":0,"store":false,"n":1,"verbosity":"low","prompt_cache_key":"cache-a"}`),
		}}},
		{name: "compatible extensions", request: llm.Request{Input: wireAuditUserText("hello"), Extensions: map[string]json.RawMessage{
			"audit": json.RawMessage(`{"transforms":["middle-out"],"plugins":[{"id":"web","max_results":3}],"safe":false}`),
		}}},
		{name: "everything", request: llm.Request{
			Instructions: []llm.Instruction{{Level: llm.InstructionLevelPolicy, Kind: llm.InstructionKindText, Text: "policy"}},
			Input: []llm.Item{
				llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}, llm.ImagePart{URL: "https://example.test/image.png", MediaType: "image/png", Detail: "high"}}},
				llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "thinking"}}},
				llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"sydney"}`)},
				llm.ToolResult{CallID: "call-1", Content: []llm.Part{llm.JSONPart{Value: json.RawMessage(`{"ok":true}`)}}},
			},
			Tools:      tools,
			ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNamed, Name: "lookup", Parallel: true},
			Output:     &llm.OutputSpec{MaxTokens: &maxTokens, Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Description: "answer object", Strict: true, Schema: schema}},
			Sampling:   &llm.SamplingSpec{Temperature: &temperature, StopSequences: []string{"\n", "END"}},
			Reasoning:  &llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, Effort: llm.ReasoningEffortHigh},
			Extensions: map[string]json.RawMessage{"audit": json.RawMessage(`{"end_user":"pinned","transforms":["middle-out"]}`)},
		}},
	}
	for _, effort := range []llm.ReasoningEffort{llm.ReasoningEffortMinimal, llm.ReasoningEffortLow, llm.ReasoningEffortMedium, llm.ReasoningEffortHigh, llm.ReasoningEffortMaximum} {
		cases = append(cases, wireAuditCase{name: "reasoning " + string(effort), request: llm.Request{Input: wireAuditUserText("hello"), Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, Effort: effort}}})
	}
	return cases
}

// The lowerer builds a request map and carries it into the SDK parameter type
// by a JSON round trip, which silently drops whatever the SDK decodes into the
// wrong union variant. Every field the map intends must reach the wire.
func TestCapturedWireBodyMatchesLoweredRequestMap(t *testing.T) {
	for _, profile := range wireAuditProfiles(t) {
		t.Run(profile.id, func(t *testing.T) {
			audit := func(t *testing.T, request llm.Request) {
				t.Helper()
				request.Model = profile.model
				request.OperationKey = "wire-audit"
				intended := intendedWireBody(t, profile, request)
				captured := captureWireBody(t, profile, request)
				if differences := wireDifferences("$", intended, captured); len(differences) > 0 {
					t.Fatalf("captured wire body differs from the lowered request map:\n%s", strings.Join(differences, "\n"))
				}
			}
			for _, test := range wireAuditCases() {
				t.Run(test.name, func(t *testing.T) { audit(t, test.request) })
			}
			for class, tier := range profile.profile.ServiceTiers {
				if tier == "" {
					continue
				}
				t.Run("service class "+string(class), func(t *testing.T) {
					audit(t, llm.Request{Input: wireAuditUserText("hello"), ServiceClass: class})
				})
			}
		})
	}
}

func TestCapturedWireBodyKeepsNamedToolChoiceFunctionName(t *testing.T) {
	for _, profile := range wireAuditProfiles(t) {
		t.Run(profile.id, func(t *testing.T) {
			wire := captureWireBody(t, profile, llm.Request{
				OperationKey: "wire-named-tool",
				Model:        profile.model,
				Input:        wireAuditUserText("hello"),
				Tools:        []llm.Tool{wireAuditTool("lookup"), wireAuditTool("search")},
				ToolPolicy:   llm.ToolPolicy{Mode: llm.ToolChoiceNamed, Name: "search"},
			})
			choice, _ := wire["tool_choice"].(map[string]any)
			function, _ := choice["function"].(map[string]any)
			if choice["type"] != "function" || function["name"] != "search" || len(choice) != 2 || len(function) != 1 {
				t.Fatalf("tool_choice = %s, want the named function choice for search", describeWireValue(wire["tool_choice"]))
			}
		})
	}
}

// Profile-owned root fields are not modelled by the SDK parameter type; they
// must still reach the wire unchanged alongside a full request.
func TestCapturedWireBodyKeepsProfileWireDefaults(t *testing.T) {
	want := map[string]map[string]any{
		"openrouter": {"provider": map[string]any{"order": []any{"ProviderA", "ProviderB"}, "allow_fallbacks": false, "require_parameters": true}},
		"exa":        {"extra_body": map[string]any{"text": true}},
	}
	for _, profile := range wireAuditProfiles(t) {
		defaults, ok := want[profile.id]
		if !ok {
			continue
		}
		t.Run(profile.id, func(t *testing.T) {
			wire := captureWireBody(t, profile, llm.Request{
				OperationKey: "wire-defaults",
				Model:        profile.model,
				Input:        wireAuditUserText("hello"),
				Tools:        []llm.Tool{wireAuditTool("lookup")},
				ToolPolicy:   llm.ToolPolicy{Mode: llm.ToolChoiceNamed, Name: "lookup"},
			})
			for field, value := range defaults {
				if !reflect.DeepEqual(wire[field], value) {
					t.Fatalf("%s = %s, want %s", field, describeWireValue(wire[field]), describeWireValue(value))
				}
			}
		})
	}
}
