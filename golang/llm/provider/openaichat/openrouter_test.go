package openaichat

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// openRouterTestConfig is a valid default-tier-only OpenRouter profile pinned
// like the production Opus endpoint.
func openRouterTestConfig() OpenRouterProfileConfig {
	return OpenRouterProfileConfig{
		ID:                        "openrouter-pinned",
		CapabilityVersion:         "openrouter/v1",
		BaseURL:                   openRouterBaseURL,
		Model:                     "anthropic/claude-opus-5.5",
		Capabilities:              profileTestCapabilities("openrouter/v1"),
		ServiceTiers:              map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "default", llm.ServiceClassPriority: ""},
		ActualServiceClasses:      map[string]llm.ServiceClass{"default": llm.ServiceClassStandard},
		MissingActualServiceClass: llm.ServiceClassStandard,
		ProviderOrder:             []string{"anthropic"},
		RequireParameters:         true,
		SupportedParameters:       []string{"max_tokens", "stop", "reasoning", "include_reasoning", "tool_choice", "tools", "structured_outputs", "response_format", "verbosity"},
		ReasoningEfforts:          map[llm.ReasoningEffort]string{llm.ReasoningEffortLow: "low", llm.ReasoningEffortMedium: "medium", llm.ReasoningEffortHigh: "high", llm.ReasoningEffortMaximum: "max"},
		ModelAliases:              []string{"anthropic/claude-opus-5.5-20260921"},
	}
}

func openRouterTestProfile(t *testing.T, mutate func(*OpenRouterProfileConfig)) Profile {
	t.Helper()
	config := openRouterTestConfig()
	if mutate != nil {
		mutate(&config)
	}
	profile, err := NewOpenRouterProfile(config)
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func TestOpenRouterPinsProviderRoutingAndPricing(t *testing.T) {
	var got *http.Request
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		got = request
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "X-Request-Id": []string{"or-req-1"}},
			Body:       io.NopCloser(strings.NewReader(`{"id":"gen-1","model":"anthropic/claude-opus-5.5","service_tier":"default","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6,"cost":0.00000123,"cost_details":{"upstream_inference_cost":0.00000123}}}`)),
			Request:    request,
		}, nil
	})
	client, err := NewOpenRouterClient(OpenRouterClientConfig{BaseURL: openRouterBaseURL, APIKey: "or-key", HTTPReferer: "https://client.example", Title: "contract", HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "openrouter-a", openRouterTestProfile(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: llm.Request{OperationKey: "or-op", Model: "anthropic/claude-opus-5.5"}, Query: provider.CapabilityQuery{EndpointID: "openrouter-a", Family: provider.FamilyOpenAIChat, Model: "anthropic/claude-opus-5.5"}, Strict: true})
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Invoke(context.Background(), call, provider.NopObserver{})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(body, &wire); err != nil {
		t.Fatal(err)
	}
	providerBody, ok := wire["provider"].(map[string]any)
	if !ok || providerBody["allow_fallbacks"] != false || providerBody["require_parameters"] != true || providerBody["data_collection"] != "deny" {
		t.Fatalf("provider routing body = %#v", wire["provider"])
	}
	if got.Header.Get("Authorization") != "Bearer or-key" || got.Header.Get("HTTP-Referer") != "https://client.example" || got.Header.Get("X-OpenRouter-Title") != "contract" {
		t.Fatalf("openrouter headers = %#v", got.Header)
	}
	if result.Response.Provider.GenerationID != "gen-1" || result.Response.Cost.ActualCostUSD == nil || result.Response.Cost.ActualCostUSD.String() != "0.000001230000000000" {
		t.Fatalf("openrouter metadata = %#v", result.Response)
	}
}

func TestParseDecimalUSDAcceptsExponentForm(t *testing.T) {
	amount, err := parseDecimalUSD(json.RawMessage(`1.23e-7`), "openrouter_cost")
	if err != nil {
		t.Fatal(err)
	}
	if got := amount.String(); got != "0.000000123000000000" {
		t.Fatalf("exponent cost = %s, want 0.000000123000000000", got)
	}
}

func TestOpenRouterRejectsCallerProviderOverride(t *testing.T) {
	profile := openRouterTestProfile(t, nil)
	_, err := lowerRequest(llm.Request{Model: "anthropic/claude-opus-5.5", Extensions: map[string]json.RawMessage{"openrouter": json.RawMessage(`{"provider_order":["Caller"]}`)}}, profile, "default")
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("provider override error = %v", err)
	}
}

// A request shaped like the FutureEval forecast calls: developer
// instructions, one user message, a strict JSON schema, an output cap, tool
// choice none and effort high.
func openRouterForecastRequest(model string, effort llm.ReasoningEffort) llm.Request {
	maxTokens := 4000
	return llm.Request{
		OperationKey: "forecast-1103",
		Model:        model,
		Instructions: []llm.Instruction{{Kind: llm.InstructionKindText, Level: llm.InstructionLevelApplication, Text: "Forecast the question."}},
		Input:        []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "Will it happen?"}}}},
		ToolPolicy:   llm.ToolPolicy{Mode: llm.ToolChoiceNone},
		Output: &llm.OutputSpec{MaxTokens: &maxTokens, Format: llm.OutputFormat{
			Kind: llm.OutputKindJSONSchema, Name: "forecast", Strict: true,
			Schema: json.RawMessage(`{"type":"object","properties":{"probability":{"type":"number"}},"required":["probability"],"additionalProperties":false}`),
		}},
		Reasoning: &llm.ReasoningSpec{Effort: effort},
	}
}

func TestOpenRouterProfileDerivesWireShapeFromSupportedParameters(t *testing.T) {
	params, err := lowerRequest(openRouterForecastRequest("anthropic/claude-opus-5.5", llm.ReasoningEffortHigh), openRouterTestProfile(t, nil), "default")
	if err != nil {
		t.Fatal(err)
	}
	wire := marshalWire(t, params)
	if wire["max_tokens"] != float64(4000) || wire["tool_choice"] != "none" {
		t.Fatalf("wire = %#v", wire)
	}
	if reasoning, ok := wire["reasoning"].(map[string]any); !ok || len(reasoning) != 1 || reasoning["effort"] != "high" {
		t.Fatalf("reasoning = %#v, want only effort high", wire["reasoning"])
	}
	// Opus lists neither max_completion_tokens nor parallel_tool_calls, and
	// store is not an OpenRouter parameter; a default-only endpoint sends no
	// service_tier.
	for _, field := range []string{"max_completion_tokens", "store", "parallel_tool_calls", "service_tier", "reasoning_effort"} {
		if _, exists := wire[field]; exists {
			t.Fatalf("unsupported field %q was emitted: %#v", field, wire)
		}
	}
	// A pinned endpoint that lists max_completion_tokens but not max_tokens
	// receives the field it lists.
	azureLike := openRouterTestProfile(t, func(config *OpenRouterProfileConfig) {
		config.SupportedParameters = []string{"max_completion_tokens", "reasoning", "response_format", "structured_outputs", "tools", "tool_choice", "parallel_tool_calls"}
	})
	params, err = lowerRequest(openRouterForecastRequest("anthropic/claude-opus-5.5", llm.ReasoningEffortMaximum), azureLike, "default")
	if err != nil {
		t.Fatal(err)
	}
	wire = marshalWire(t, params)
	if wire["max_completion_tokens"] != float64(4000) || wire["parallel_tool_calls"] != false || wire["reasoning"].(map[string]any)["effort"] != "max" {
		t.Fatalf("listed-field wire = %#v", wire)
	}
	if _, exists := wire["max_tokens"]; exists {
		t.Fatalf("unlisted max_tokens emitted: %#v", wire)
	}
}

func TestOpenRouterRefusesUnsupportedRequestsBeforeDispatch(t *testing.T) {
	temperature := 0.2
	gemini := func(config *OpenRouterProfileConfig) {
		config.ReasoningEfforts = map[llm.ReasoningEffort]string{llm.ReasoningEffortLow: "low", llm.ReasoningEffortMedium: "medium", llm.ReasoningEffortHigh: "high"}
	}
	for _, test := range []struct {
		name    string
		mutate  func(*OpenRouterProfileConfig)
		request func(*llm.Request)
		want    string
	}{
		{name: "unlisted sampling parameter", request: func(request *llm.Request) { request.Sampling = &llm.SamplingSpec{Temperature: &temperature} }, want: "does not support request parameters temperature"},
		{name: "schema without structured outputs", mutate: func(config *OpenRouterProfileConfig) {
			config.SupportedParameters = []string{"max_tokens", "reasoning", "response_format", "tool_choice"}
		}, want: "does not support request parameters structured_outputs"},
		{name: "unlisted output cap", mutate: func(config *OpenRouterProfileConfig) {
			config.SupportedParameters = []string{"reasoning", "response_format", "structured_outputs", "tool_choice"}
		}, want: "does not support request parameters max_completion_tokens"},
		{name: "sequential callable tools", request: func(request *llm.Request) {
			request.Tools = []llm.Tool{{Name: "search", InputSchema: json.RawMessage(`{"type":"object"}`)}}
			request.ToolPolicy = llm.ToolPolicy{Mode: llm.ToolChoiceAuto, Parallel: false}
		}, want: "cannot request sequential tool calls"},
		{name: "unmapped maximum effort", mutate: gemini, request: func(request *llm.Request) { request.Reasoning.Effort = llm.ReasoningEffortMaximum }, want: `reasoning effort "maximum" is not mapped`},
		{name: "unmapped minimal effort", request: func(request *llm.Request) { request.Reasoning.Effort = llm.ReasoningEffortMinimal }, want: `reasoning effort "minimal" is not mapped`},
		{name: "disabled reasoning", request: func(request *llm.Request) { request.Reasoning.Mode = llm.ReasoningModeDisabled }, want: "cannot be disabled"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := openRouterForecastRequest("anthropic/claude-opus-5.5", llm.ReasoningEffortHigh)
			if test.request != nil {
				test.request(&request)
			}
			_, err := lowerRequest(request, openRouterTestProfile(t, test.mutate), "default")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

// OpenRouter defaults parallel_tool_calls to true and the pinned endpoints do
// not list it: parallel tool use is the omitted default, and a request that
// cannot call tools needs no parallel policy.
func TestOpenRouterParallelToolPolicyUsesUpstreamDefault(t *testing.T) {
	tools := []llm.Tool{{Name: "search", InputSchema: json.RawMessage(`{"type":"object"}`)}}
	for _, test := range []struct {
		name   string
		tools  []llm.Tool
		policy llm.ToolPolicy
	}{
		{name: "parallel auto", tools: tools, policy: llm.ToolPolicy{Mode: llm.ToolChoiceAuto, Parallel: true}},
		{name: "tools disabled", tools: tools, policy: llm.ToolPolicy{Mode: llm.ToolChoiceNone}},
		{name: "no tools", policy: llm.ToolPolicy{Mode: llm.ToolChoiceNone}},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := openRouterForecastRequest("anthropic/claude-opus-5.5", llm.ReasoningEffortHigh)
			request.Tools, request.ToolPolicy = test.tools, test.policy
			params, err := lowerRequest(request, openRouterTestProfile(t, nil), "default")
			if err != nil {
				t.Fatal(err)
			}
			if _, exists := marshalWire(t, params)["parallel_tool_calls"]; exists {
				t.Fatalf("unsupported parallel_tool_calls emitted")
			}
		})
	}
}

func TestOpenRouterProfileRejectsInvalidConfiguration(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*OpenRouterProfileConfig)
		want   string
	}{
		{name: "hidden fallback", mutate: func(config *OpenRouterProfileConfig) { config.AllowFallbacks = true }, want: "allow_fallbacks must be false"},
		{name: "parameters not required", mutate: func(config *OpenRouterProfileConfig) { config.RequireParameters = false }, want: "require_parameters must be true"},
		{name: "no supported parameters", mutate: func(config *OpenRouterProfileConfig) { config.SupportedParameters = nil }, want: "supported_parameters of the pinned endpoint are required"},
		{name: "store is not an OpenRouter parameter", mutate: func(config *OpenRouterProfileConfig) {
			config.SupportedParameters = append(config.SupportedParameters, "store")
		}, want: `"store" is not a documented OpenRouter parameter`},
		{name: "repeated parameter", mutate: func(config *OpenRouterProfileConfig) {
			config.SupportedParameters = append(config.SupportedParameters, "max_tokens")
		}, want: `"max_tokens" is repeated`},
		{name: "efforts without reasoning parameter", mutate: func(config *OpenRouterProfileConfig) {
			config.SupportedParameters = []string{"max_tokens", "reasoning_effort"}
		}, want: `require the pinned endpoint to support "reasoning"`},
		{name: "public effort name as value", mutate: func(config *OpenRouterProfileConfig) {
			config.ReasoningEfforts[llm.ReasoningEffortMaximum] = "maximum"
		}, want: `maps reasoning effort "maximum" to "maximum"`},
		{name: "none effort", mutate: func(config *OpenRouterProfileConfig) {
			config.ReasoningEfforts[llm.ReasoningEffortLow] = "none"
		}, want: `maps reasoning effort "low" to "none"`},
		{name: "provider default key", mutate: func(config *OpenRouterProfileConfig) {
			config.ReasoningEfforts[llm.ReasoningEffortProviderDefault] = "medium"
		}, want: `maps unsupported public reasoning effort "provider_default"`},
		{name: "undocumented tier", mutate: func(config *OpenRouterProfileConfig) {
			config.ServiceTiers[llm.ServiceClassStandard] = "standard"
		}, want: `provider tier "standard" is not a documented OpenRouter tier`},
		{name: "fast alias tier", mutate: func(config *OpenRouterProfileConfig) {
			config.ServiceTiers[llm.ServiceClassPriority] = "fast"
		}, want: `provider tier "fast" is not a documented OpenRouter tier`},
		{name: "missing tier policy absent", mutate: func(config *OpenRouterProfileConfig) { config.MissingActualServiceClass = "" }, want: `must declare missing_service_tier "standard"`},
		{name: "missing tier mapped to non-default class", mutate: func(config *OpenRouterProfileConfig) {
			config.ServiceTiers[llm.ServiceClassEconomy] = "flex"
			config.MissingActualServiceClass = llm.ServiceClassEconomy
		}, want: `missing service tier class "economy" must be the class mapped to the "default" tier`},
		{name: "blank alias", mutate: func(config *OpenRouterProfileConfig) { config.ModelAliases = []string{"anthropic/claude opus"} }, want: "response model alias"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := openRouterTestConfig()
			test.mutate(&config)
			_, err := NewOpenRouterProfile(config)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestParseOpenRouterEndpointRefusesUnsafeOrUnknownFields(t *testing.T) {
	base := func() map[string]any {
		return map[string]any{
			"provider_order":       []any{"anthropic"},
			"allow_fallbacks":      false,
			"require_parameters":   true,
			"supported_parameters": []any{"max_tokens", "reasoning"},
			"reasoning_efforts":    map[string]any{"high": "high", "maximum": "max"},
			"missing_service_tier": "standard",
			"model_aliases":        []any{"anthropic/claude-opus-5.5-20260921"},
		}
	}
	endpoint, err := ParseOpenRouterEndpoint(base())
	if err != nil {
		t.Fatal(err)
	}
	if endpoint.MissingServiceTier != llm.ServiceClassStandard || endpoint.ReasoningEfforts[llm.ReasoningEffortMaximum] != "max" || endpoint.ModelAliases[0] != "anthropic/claude-opus-5.5-20260921" {
		t.Fatalf("endpoint = %#v", endpoint)
	}
	for _, test := range []struct {
		field string
		value any
		want  string
	}{
		{field: "allow_fallbacks", value: true, want: "allow_fallbacks must be false"},
		{field: "require_parameters", value: false, want: "require_parameters must be true"},
		{field: "store", value: false, want: `field "store" is not supported`},
		{field: "reasoning_efforts", value: map[string]any{"high": 3}, want: "reasoning_efforts.high must be a string"},
		{field: "missing_service_tier", value: "default", want: "missing_service_tier must be economy, standard or priority"},
		{field: "supported_parameters", value: "max_tokens", want: "supported_parameters must be an array of strings"},
	} {
		t.Run(test.field, func(t *testing.T) {
			values := base()
			values[test.field] = test.value
			if _, err := ParseOpenRouterEndpoint(values); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

// openRouterResponse is a realistic non-streaming OpenRouter Chat response
// for a reasoning model: reasoning tokens are part of completion_tokens,
// cached prompt tokens are part of prompt_tokens, and cost is USD credits.
func openRouterResponse(model, serviceTier, cost string) *http.Response {
	body := `{"id":"gen-1758900000-Abc","provider":"Anthropic","model":` + model + `,"object":"chat.completion","created":1758900000,"service_tier":` + serviceTier + `,"system_fingerprint":null,` +
		`"choices":[{"index":0,"finish_reason":"stop","native_finish_reason":"end_turn","message":{"role":"assistant","content":"{\"probability\":0.62}","reasoning":"Base rates first."}}],` +
		`"usage":{"prompt_tokens":1200,"completion_tokens":900,"total_tokens":2100,"prompt_tokens_details":{"cached_tokens":200,"audio_tokens":0},"completion_tokens_details":{"reasoning_tokens":700,"image_tokens":0},"cost":` + cost + `,"is_byok":false,"cost_details":{"upstream_inference_cost":null,"upstream_inference_prompt_cost":0.00404,"upstream_inference_completions_cost":0.018}}}`
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}, "X-Request-Id": []string{"or-req-2"}}, Body: io.NopCloser(strings.NewReader(body))}
}

func invokeOpenRouter(t *testing.T, response *http.Response) (provider.Result, error) {
	t.Helper()
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		response.Request = request
		return response, nil
	})
	client, err := NewOpenRouterClient(OpenRouterClientConfig{BaseURL: openRouterBaseURL, APIKey: "or-key", HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "openrouter-anthropic-opus55", openRouterTestProfile(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: openRouterForecastRequest("anthropic/claude-opus-5.5", llm.ReasoningEffortHigh), Query: provider.CapabilityQuery{EndpointID: "openrouter-anthropic-opus55", Family: provider.FamilyOpenAIChat, Model: "anthropic/claude-opus-5.5"}, Strict: true})
	if err != nil {
		t.Fatal(err)
	}
	return adapter.Invoke(context.Background(), call, provider.NopObserver{})
}

func TestOpenRouterLiftsUsageCostReasoningAndServiceTier(t *testing.T) {
	result, err := invokeOpenRouter(t, openRouterResponse(`"anthropic/claude-opus-5.5"`, `"default"`, `0.02204`))
	if err != nil {
		t.Fatal(err)
	}
	response := result.Response
	usage := response.Usage
	if usage.InputTokens != 1000 || usage.CacheReadTokens != 200 || usage.CacheWriteTokens != 0 || usage.OutputTokens != 900 || usage.ReasoningTokens != 700 {
		t.Fatalf("usage = %+v, want disjoint input and completion including reasoning", usage)
	}
	if response.Cost.ActualCostUSD == nil || response.Cost.ActualCostUSD.String() != "0.022040000000000000" || response.Cost.Method != "openrouter_reported" {
		t.Fatalf("cost = %#v", response.Cost)
	}
	if _, ok := response.Provider.Raw["openrouter_cost_details"]; !ok || response.Provider.GenerationID != "gen-1758900000-Abc" {
		t.Fatalf("provider facts = %#v", response.Provider)
	}
	if response.Service.Actual == nil || *response.Service.Actual != llm.ServiceClassStandard || response.Service.ProviderValue != "default" {
		t.Fatalf("service = %#v", response.Service)
	}

	// OpenRouter reports null when the upstream exposes no tier; the declared
	// missing-tier policy records the default-tier class. A null cost is an
	// unreported receipt rather than an invalid response.
	result, err = invokeOpenRouter(t, openRouterResponse(`"anthropic/claude-opus-5.5"`, `null`, `null`))
	if err != nil {
		t.Fatal(err)
	}
	if result.Response.Service.Actual == nil || *result.Response.Service.Actual != llm.ServiceClassStandard || result.Response.Cost.ActualCostUSD != nil {
		t.Fatalf("null tier/cost response = %#v / %#v", result.Response.Service, result.Response.Cost)
	}

	// A tier the endpoint does not map (the request never opted into it)
	// fails closed instead of being accounted as standard.
	_, err = invokeOpenRouter(t, openRouterResponse(`"anthropic/claude-opus-5.5"`, `"priority"`, `0.04408`))
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeProviderInvalidResponse || !strings.Contains(err.Error(), `unsupported service tier "priority"`) {
		t.Fatalf("priority tier error = %v", err)
	}
}

func TestOpenRouterPinsResponseModelEcho(t *testing.T) {
	for _, test := range []struct {
		name  string
		echo  string
		valid bool
	}{
		{name: "pinned slug", echo: `"anthropic/claude-opus-5.5"`, valid: true},
		{name: "configured dated revision", echo: `"anthropic/claude-opus-5.5-20260921"`, valid: true},
		{name: "unconfigured dated revision", echo: `"anthropic/claude-opus-5.5-20261015"`},
		{name: "routing variant suffix", echo: `"anthropic/claude-opus-5.5:floor"`},
		{name: "provider model id", echo: `"claude-opus-5-5"`},
		{name: "different model", echo: `"anthropic/claude-opus-5"`},
		{name: "missing echo", echo: `""`},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := invokeOpenRouter(t, openRouterResponse(test.echo, `"default"`, `0.02204`))
			if !test.valid {
				var mapped *provider.Error
				if !errors.As(err, &mapped) || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Dispatch != provider.DispatchAccepted || !strings.Contains(err.Error(), "pinned model") {
					t.Fatalf("error = %v, want accepted invalid response", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			route := result.Response.Route
			if route.ResolvedModel != "anthropic/claude-opus-5.5" || route.ObservedModelRevision != "anthropic/claude-opus-5.5" || route.ModelIdentityBasis != llm.ModelIdentityBasisProviderReported {
				t.Fatalf("route = %#v, want the pinned slug as resolved and observed revision", route)
			}
			if raw := string(result.Response.Provider.Raw["response_model"]); raw != test.echo {
				t.Fatalf("raw response model = %s, want %s", raw, test.echo)
			}
		})
	}
}
