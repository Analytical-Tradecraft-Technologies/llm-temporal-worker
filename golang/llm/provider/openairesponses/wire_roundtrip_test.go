package openairesponses

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

// wireAuditProfile is one Responses route whose real client and adapter are
// composed the way route construction composes them.
type wireAuditProfile struct {
	id         string
	newAdapter func(*http.Client, ...AdapterOption) (*Adapter, error)
}

func wireAuditProfiles() []wireAuditProfile {
	return []wireAuditProfile{
		{id: "openai", newAdapter: func(httpClient *http.Client, options ...AdapterOption) (*Adapter, error) {
			client, err := NewClient(ClientConfig{BaseURL: "https://127.0.0.1/v1", APIKey: "audit-key", HTTPClient: httpClient})
			if err != nil {
				return nil, err
			}
			return New(client, "wire-audit", "wire-audit/v1", options...)
		}},
		{id: "azure", newAdapter: func(httpClient *http.Client, options ...AdapterOption) (*Adapter, error) {
			client, err := NewAzureClient(AzureClientConfig{Endpoint: "https://127.0.0.1", APIVersion: "v1", APIKey: "audit-azure-key", HTTPClient: httpClient})
			if err != nil {
				return nil, err
			}
			return NewAzureAdapter(client, "wire-audit", "wire-audit/v1", options...)
		}},
	}
}

// captureWireBody compiles and invokes request through the real adapter and
// returns the HTTP request body the SDK actually wrote.
func captureWireBody(t *testing.T, profile wireAuditProfile, request llm.Request, options ...AdapterOption) map[string]any {
	t.Helper()
	var captured []byte
	adapter, err := profile.newAdapter(&http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, readErr := io.ReadAll(request.Body)
		if readErr != nil {
			return nil, readErr
		}
		captured = body
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "X-Request-Id": []string{"req-wire-audit"}},
			Body:       io.NopCloser(strings.NewReader(`{"id":"resp-audit","object":"response","status":"completed","model":"gpt-audit","output":[]}`)),
			Request:    request,
		}, nil
	})}, options...)
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: "wire-audit", Family: provider.FamilyOpenAIResponses, Model: request.Model},
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

// intendedWireBody is the request map the profile's adapter built before it
// was carried into the SDK parameter type.
func intendedWireBody(t *testing.T, profile wireAuditProfile, request llm.Request) map[string]any {
	t.Helper()
	adapter, err := profile.newAdapter(http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	serviceClass, err := llm.NormalizeServiceClass(normalized.ServiceClass)
	if err != nil {
		t.Fatal(err)
	}
	requestMap, _, err := adapter.lowerRequestMap(normalized, serviceClass)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(requestMap)
	if err != nil {
		t.Fatal(err)
	}
	var intended map[string]any
	if err := json.Unmarshal(encoded, &intended); err != nil {
		t.Fatal(err)
	}
	return intended
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
	zeroTokens := 0
	temperature := 0.25
	topP := 0.9
	zero := 0.0
	tools := []llm.Tool{wireAuditTool("lookup"), wireAuditTool("search")}
	withOutputSchema := wireAuditTool("typed")
	withOutputSchema.OutputSchema = json.RawMessage(`{"type":"object","properties":{"ok":{"type":"boolean"}}}`)
	schema := json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"},"score":{"type":"number"}},"required":["answer"],"additionalProperties":false}`)
	reasoningState := llm.ProviderState{Provider: "openai", EndpointFamily: "responses", MediaType: reasoningStateMediaType, Opaque: json.RawMessage(`{"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"thought"}],"encrypted_content":"opaque-bytes"}`)}
	cases := []wireAuditCase{
		{name: "minimal", request: llm.Request{Input: wireAuditUserText("hello")}},
		{name: "instructions", request: llm.Request{
			Instructions: []llm.Instruction{
				{Level: llm.InstructionLevelPolicy, Kind: llm.InstructionKindText, Text: "policy"},
				{Level: llm.InstructionLevelApplication, Kind: llm.InstructionKindParts, Content: []llm.Part{llm.TextPart{Text: "developer"}, llm.JSONPart{Value: json.RawMessage(`{"rule":1}`)}}},
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
		{name: "tool output schema and bare tool", request: llm.Request{Input: wireAuditUserText("hello"), Tools: []llm.Tool{withOutputSchema, {Name: "bare", InputSchema: json.RawMessage(`{"type":"object"}`)}}}},
		{name: "tool history", request: llm.Request{
			Input: []llm.Item{
				llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "find sydney"}}},
				llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "looking"}, llm.JSONPart{Value: json.RawMessage(`{"step":1}`)}}},
				reasoningState,
				llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"sydney"}`)},
				llm.ToolCall{ID: "call-2", Name: "search", Arguments: json.RawMessage(`{"q":"harbour","limit":2}`)},
				llm.ToolResult{CallID: "call-1", Content: []llm.Part{llm.JSONPart{Value: json.RawMessage(`{"ok":true}`)}}},
				llm.ToolResult{CallID: "call-2", Content: []llm.Part{llm.TextPart{Text: ""}}},
				llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.RefusalPart{Text: "cannot continue"}}},
				llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "try again"}}},
			},
			Tools: tools,
		}},
		{name: "max tokens", request: llm.Request{Input: wireAuditUserText("hello"), Output: &llm.OutputSpec{MaxTokens: &maxTokens}}},
		{name: "json object output", request: llm.Request{Input: wireAuditUserText("hello"), Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSON}}}},
		{name: "json schema strict", request: llm.Request{Input: wireAuditUserText("hello"), Output: &llm.OutputSpec{MaxTokens: &maxTokens, Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Description: "answer object", Strict: true, Schema: schema}}}},
		{name: "json schema lenient", request: llm.Request{Input: wireAuditUserText("hello"), Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Schema: schema}}}},
		{name: "reasoning disabled", request: llm.Request{Input: wireAuditUserText("hello"), Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeDisabled}}},
		{name: "reasoning provider default", request: llm.Request{Input: wireAuditUserText("hello"), Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeProviderDefault}}},
		{name: "reasoning enabled", request: llm.Request{Input: wireAuditUserText("hello"), Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled}}},
		{name: "reasoning adaptive summary none", request: llm.Request{Input: wireAuditUserText("hello"), Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeAdaptive, Effort: llm.ReasoningEffortLow, Summary: llm.ReasoningSummaryNone}}},
		{name: "image url", request: llm.Request{Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
			llm.TextPart{Text: "describe"},
			llm.ImagePart{URL: "https://example.test/image.png", MediaType: "image/png", Detail: "high"},
			llm.ImagePart{URL: "https://example.test/other.png", MediaType: "image/png"},
		}}}}},
		{name: "image bytes", request: llm.Request{Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
			llm.ImagePart{Bytes: []byte{0x89, 'P', 'N', 'G'}, MediaType: "image/png", Detail: "low"},
			llm.JSONPart{Value: json.RawMessage(`{"caption":true}`)},
		}}}}},
		{name: "documents", request: llm.Request{Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
			llm.DocumentPart{URL: "https://example.test/contract.pdf", MediaType: "application/pdf"},
			llm.DocumentPart{URL: "https://example.test/titled.pdf", MediaType: "application/pdf", Title: "titled.pdf"},
			llm.DocumentPart{Bytes: []byte("%PDF-1.7"), MediaType: "application/pdf", Title: "inline.pdf"},
			llm.DocumentPart{Bytes: []byte("%PDF-1.7"), MediaType: "application/pdf"},
		}}}}},
		{name: "sampling", request: llm.Request{Input: wireAuditUserText("hello"), Sampling: &llm.SamplingSpec{Temperature: &temperature, TopP: &topP}}},
		{name: "zero values", request: llm.Request{Input: wireAuditUserText("hello"), Output: &llm.OutputSpec{MaxTokens: &zeroTokens}, Sampling: &llm.SamplingSpec{Temperature: &zero, TopP: &zero}}},
		{name: "continuation handle", request: llm.Request{Input: wireAuditUserText("hello"), Continuation: &llm.Continuation{Handle: "openai-responses:resp-prev"}}},
		{name: "extensions", request: llm.Request{Input: wireAuditUserText("hello"), Extensions: map[string]json.RawMessage{
			"openai.responses": json.RawMessage(`{"include":["reasoning.encrypted_content","message.output_text.logprobs"],"store":false,"background":false,"truncation":"auto"}`),
		}}},
		{name: "stored background extensions", request: llm.Request{Input: wireAuditUserText("hello"), Extensions: map[string]json.RawMessage{
			"openai.responses": json.RawMessage(`{"include":[],"store":true,"background":true,"truncation":"disabled"}`),
		}}},
		{name: "everything", request: llm.Request{
			Instructions: []llm.Instruction{{Level: llm.InstructionLevelPolicy, Kind: llm.InstructionKindText, Text: "policy"}},
			Input: []llm.Item{
				llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}, llm.ImagePart{URL: "https://example.test/image.png", MediaType: "image/png", Detail: "high"}, llm.DocumentPart{Bytes: []byte("pdf"), MediaType: "application/pdf", Title: "contract.pdf"}}},
				llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"sydney"}`)},
				llm.ToolResult{CallID: "call-1", Content: []llm.Part{llm.JSONPart{Value: json.RawMessage(`{"ok":true}`)}}},
			},
			Tools:        append([]llm.Tool{withOutputSchema}, tools...),
			ToolPolicy:   llm.ToolPolicy{Mode: llm.ToolChoiceNamed, Name: "lookup", Parallel: true},
			Output:       &llm.OutputSpec{MaxTokens: &maxTokens, Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Description: "answer object", Strict: true, Schema: schema}},
			Sampling:     &llm.SamplingSpec{Temperature: &temperature, TopP: &topP},
			Reasoning:    &llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, Effort: llm.ReasoningEffortHigh, Summary: llm.ReasoningSummaryDetailed},
			Continuation: &llm.Continuation{Handle: "openai-responses:resp-prev"},
			Extensions:   map[string]json.RawMessage{"openai.responses": json.RawMessage(`{"include":["reasoning.encrypted_content"],"store":true}`)},
		}},
	}
	for _, effort := range []llm.ReasoningEffort{llm.ReasoningEffortMinimal, llm.ReasoningEffortLow, llm.ReasoningEffortMedium, llm.ReasoningEffortHigh, llm.ReasoningEffortMaximum} {
		cases = append(cases, wireAuditCase{name: "reasoning effort " + string(effort), request: llm.Request{Input: wireAuditUserText("hello"), Reasoning: &llm.ReasoningSpec{Effort: effort}}})
	}
	for _, summary := range []llm.ReasoningSummary{llm.ReasoningSummaryAuto, llm.ReasoningSummaryConcise, llm.ReasoningSummaryDetailed} {
		cases = append(cases, wireAuditCase{name: "reasoning summary " + string(summary), request: llm.Request{Input: wireAuditUserText("hello"), Reasoning: &llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, Effort: llm.ReasoningEffortMedium, Summary: summary}}})
	}
	for _, class := range []llm.ServiceClass{llm.ServiceClassEconomy, llm.ServiceClassStandard, llm.ServiceClassPriority} {
		cases = append(cases, wireAuditCase{name: "service class " + string(class), request: llm.Request{Input: wireAuditUserText("hello"), ServiceClass: class}})
	}
	return cases
}

// The lowerer builds a request map and carries it into the SDK parameter type
// by a JSON round trip, which silently drops whatever the SDK decodes into the
// wrong union variant. Every field the map intends must reach the wire.
func TestCapturedWireBodyMatchesLoweredRequestMap(t *testing.T) {
	for _, profile := range wireAuditProfiles() {
		t.Run(profile.id, func(t *testing.T) {
			for _, test := range wireAuditCases() {
				t.Run(test.name, func(t *testing.T) {
					request := test.request
					request.Model = "gpt-audit"
					request.OperationKey = "wire-audit"
					intended := intendedWireBody(t, profile, request)
					captured := captureWireBody(t, profile, request)
					if differences := wireDifferences("$", intended, captured); len(differences) > 0 {
						t.Fatalf("captured wire body differs from the lowered request map:\n%s", strings.Join(differences, "\n"))
					}
				})
			}
		})
	}
}

// The direct API receives the tier of the requested class. The Azure OpenAI
// Responses specification defines no service_tier, so that route sends none,
// and the omission is part of the intended body rather than applied after it.
func TestCapturedWireBodyServiceTierFollowsTheRoute(t *testing.T) {
	for _, profile := range wireAuditProfiles() {
		for class, tier := range map[llm.ServiceClass]string{llm.ServiceClassEconomy: "flex", llm.ServiceClassStandard: "default", llm.ServiceClassPriority: "priority"} {
			t.Run(profile.id+"/"+string(class), func(t *testing.T) {
				request := llm.Request{OperationKey: "wire-tier", Model: "gpt-audit", Input: wireAuditUserText("hello"), ServiceClass: class}
				intended := intendedWireBody(t, profile, request)
				captured := captureWireBody(t, profile, request)
				if differences := wireDifferences("$", intended, captured); len(differences) > 0 {
					t.Fatalf("captured wire body differs from the lowered request map:\n%s", strings.Join(differences, "\n"))
				}
				sent, present := captured["service_tier"]
				if profile.id == "azure" {
					if present {
						t.Fatalf("azure request sent service_tier %v", sent)
					}
					return
				}
				if sent != tier {
					t.Fatalf("service_tier = %v, want %q", sent, tier)
				}
			})
		}
	}
}

// A storage-denied endpoint adds exactly store:false to the lowered body.
func TestCapturedWireBodyAddsOnlyStoreFalseWhenStorageIsDenied(t *testing.T) {
	for _, profile := range wireAuditProfiles() {
		t.Run(profile.id, func(t *testing.T) {
			request := llm.Request{
				OperationKey: "wire-storage-denied",
				Model:        "gpt-audit",
				Input:        wireAuditUserText("hello"),
				Tools:        []llm.Tool{wireAuditTool("lookup")},
				ToolPolicy:   llm.ToolPolicy{Mode: llm.ToolChoiceNamed, Name: "lookup"},
			}
			intended := intendedWireBody(t, profile, request)
			intended["store"] = false
			captured := captureWireBody(t, profile, request, WithProviderStoragePermitted(false))
			if differences := wireDifferences("$", intended, captured); len(differences) > 0 {
				t.Fatalf("captured wire body differs from the lowered request map:\n%s", strings.Join(differences, "\n"))
			}
		})
	}
}
