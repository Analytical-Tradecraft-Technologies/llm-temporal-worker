package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/openairesponses"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

// samplingSettingsRoutes adds the Responses adapter to the four routes the
// instruction hierarchy tests already compile with real adapters.
func samplingSettingsRoutes() []hierarchyRoute {
	routes := hierarchyRoutes()
	return append(routes, hierarchyRoute{name: "openai_responses", family: provider.FamilyOpenAIResponses, tier: "default", adapter: func(t *testing.T) provider.Adapter {
		client, err := openairesponses.NewClient(openairesponses.ClientConfig{BaseURL: "https://api.openai.com/v1/", APIKey: "test-key", HTTPClient: &http.Client{Transport: planningTransportFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("unexpected network call")
		})}})
		if err != nil {
			t.Fatal(err)
		}
		adapter, err := openairesponses.New(client, "endpoint", "profile/v1")
		if err != nil {
			t.Fatal(err)
		}
		return adapter
	}})
}

// compileSettingsPatch decodes a v1 settings_patch from its wire form, prepares
// a root Generate request with it and compiles the result with adapter.
func compileSettingsPatch(t *testing.T, adapter provider.Adapter, family provider.Family, patch string, strict bool) (map[string]any, error) {
	t.Helper()
	var settings llm.SettingsPatchV1
	if err := json.Unmarshal([]byte(patch), &settings); err != nil {
		t.Fatalf("decode settings_patch: %v", err)
	}
	settings.Model.Set = preparationPointer("provider-model")
	portability := llm.PortabilityBestEffort
	if strict {
		portability = llm.PortabilityStrict
	}
	settings.Portability.Set = &portability
	request := llm.GenerateRequestV1{OperationKey: "operation", Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
		Append: []llm.Item{preparationMessage("hello")}, SettingsPatch: settings}
	prepared, err := PrepareGenerateInput(context.Background(), request, durable.GenerateReplay{})
	if err != nil {
		t.Fatalf("PrepareGenerateInput: %v", err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: prepared.Request, Strict: strict,
		Query: provider.CapabilityQuery{EndpointID: "endpoint", Family: family, Model: "provider-model", ServiceClass: llm.ServiceClassStandard}})
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(call.SDKParams)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatal(err)
	}
	return wire, nil
}

func wireValue(wire map[string]any, path ...string) any {
	var value any = wire
	for _, name := range path {
		object, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		value = object[name]
	}
	return value
}

// TestSettingsPatchSamplingAndReasoningReachEveryAdapter follows each new v1
// leaf from its wire form through request preparation into every provider
// adapter, and checks the adapters that cannot honour a leaf reject it rather
// than drop it.
func TestSettingsPatchSamplingAndReasoningReachEveryAdapter(t *testing.T) {
	const (
		topPAndStop = `{"top_p":{"set":"0.9"},"stop_sequences":{"set":["END","STOP"]}}`
		topPOnly    = `{"top_p":{"set":"0.9"}}`
		stopOnly    = `{"stop_sequences":{"set":["END"]}}`
		seed        = `{"seed":{"set":42}}`
		budget      = `{"reasoning_mode":{"set":"enabled"},"reasoning_token_budget":{"set":2048}}`
		disabled    = `{"reasoning_mode":{"set":"disabled"}}`
	)
	type check struct {
		patch  string
		strict bool
		// reject, when set, is a substring of the expected compile error.
		reject string
		path   []string
		want   any
	}
	stop := []any{"END", "STOP"}
	cases := map[string][]check{
		"anthropic_messages": {
			{patch: topPAndStop, path: []string{"top_p"}, want: 0.9},
			{patch: topPAndStop, path: []string{"stop_sequences"}, want: stop},
			{patch: seed, reject: "sampling field is not supported"},
			{patch: budget, path: []string{"thinking"}, want: map[string]any{"type": "enabled", "budget_tokens": float64(2048)}},
			{patch: disabled, path: []string{"thinking"}, want: map[string]any{"type": "disabled"}},
		},
		"bedrock_anthropic_messages": {
			{patch: topPAndStop, path: []string{"top_p"}, want: 0.9},
			{patch: topPAndStop, path: []string{"stop_sequences"}, want: stop},
			{patch: seed, reject: "sampling field is not supported"},
			{patch: budget, path: []string{"thinking"}, want: map[string]any{"type": "enabled", "budget_tokens": float64(2048)}},
			{patch: disabled, path: []string{"thinking"}, want: map[string]any{"type": "disabled"}},
		},
		"bedrock_converse": {
			{patch: topPAndStop, path: []string{"InferenceConfig", "TopP"}, want: 0.9},
			{patch: topPAndStop, path: []string{"InferenceConfig", "StopSequences"}, want: stop},
			{patch: seed, reject: "seed"},
			{patch: budget, strict: true, reject: "strict portability"},
		},
		"azure_chat": {
			{patch: topPAndStop, path: []string{"top_p"}, want: 0.9},
			{patch: topPAndStop, path: []string{"stop"}, want: stop},
			{patch: seed, path: []string{"seed"}, want: float64(42)},
			{patch: budget, reject: "token_budget"},
			{patch: disabled, path: []string{"reasoning_effort"}, want: "none"},
		},
		"openai_responses": {
			{patch: topPOnly, path: []string{"top_p"}, want: 0.9},
			{patch: stopOnly, reject: "sampling field is not supported"},
			{patch: seed, reject: "sampling field is not supported"},
			{patch: budget, reject: "token_budget"},
			{patch: disabled, path: []string{"reasoning", "effort"}, want: "none"},
		},
	}
	for _, route := range samplingSettingsRoutes() {
		checks, ok := cases[route.name]
		if !ok {
			t.Fatalf("no expectations for route %s", route.name)
		}
		t.Run(route.name, func(t *testing.T) {
			adapter := route.adapter(t)
			for _, check := range checks {
				wire, err := compileSettingsPatch(t, adapter, route.family, check.patch, check.strict)
				if check.reject != "" {
					if err == nil || !strings.Contains(err.Error(), check.reject) {
						t.Fatalf("%s: error = %v; want a rejection mentioning %q", check.patch, err, check.reject)
					}
					continue
				}
				if err != nil {
					t.Fatalf("%s: %v", check.patch, err)
				}
				if got := wireValue(wire, check.path...); !reflect.DeepEqual(got, check.want) {
					t.Fatalf("%s: %v = %#v; want %#v", check.patch, check.path, got, check.want)
				}
			}
		})
	}
}

// TestPrepareGenerateInputInheritsSamplingAndReasoningLeaves checks the new
// leaves inherit from the parent, replace and clear independently, and
// project into the provider-neutral request.
func TestPrepareGenerateInputInheritsSamplingAndReasoningLeaves(t *testing.T) {
	request, replay, _ := preparationFixture()
	settings := &replay.State.Settings
	settings.TopP = preparationPointer(llm.DecimalV1("0.25"))
	settings.StopSequences = []string{"END"}
	settings.Seed = preparationPointer(int64(7))
	settings.ReasoningMode = llm.ReasoningModeEnabled
	settings.ReasoningTokenBudget = preparationPointer(4096)

	prepared, err := PrepareGenerateInput(context.Background(), request, replay)
	if err != nil {
		t.Fatal(err)
	}
	sampling, reasoning := prepared.Request.Sampling, prepared.Request.Reasoning
	if sampling == nil || *sampling.TopP != 0.25 || !reflect.DeepEqual(sampling.StopSequences, []string{"END"}) || *sampling.Seed != 7 || *sampling.Temperature != 0.2 {
		t.Fatalf("inherited sampling = %+v", sampling)
	}
	if reasoning == nil || reasoning.Mode != llm.ReasoningModeEnabled || *reasoning.TokenBudget != 4096 || reasoning.Effort != llm.ReasoningEffortHigh {
		t.Fatalf("inherited reasoning = %+v", reasoning)
	}

	request.SettingsPatch.TopP.Set = preparationPointer(llm.DecimalV1("0.500"))
	request.SettingsPatch.StopSequences.Clear = true
	request.SettingsPatch.Seed.Set = preparationPointer(int64(9))
	request.SettingsPatch.ReasoningMode.Set = preparationPointer(llm.ReasoningModeAdaptive)
	request.SettingsPatch.ReasoningTokenBudget.Clear = true
	prepared, err = PrepareGenerateInput(context.Background(), request, replay)
	if err != nil {
		t.Fatal(err)
	}
	sampling, reasoning = prepared.Request.Sampling, prepared.Request.Reasoning
	if *sampling.TopP != 0.5 || prepared.Settings.TopP.String() != "0.5" || sampling.StopSequences != nil || *sampling.Seed != 9 {
		t.Fatalf("patched sampling = %+v", sampling)
	}
	if reasoning.Mode != llm.ReasoningModeAdaptive || reasoning.TokenBudget != nil {
		t.Fatalf("patched reasoning = %+v", reasoning)
	}
	if len(replay.State.Settings.StopSequences) != 1 || *replay.State.Settings.ReasoningTokenBudget != 4096 {
		t.Fatal("preparation changed the parent checkpoint")
	}

	// Leaves the caller did not set never invent a sampling or reasoning spec.
	request, _, _ = preparationFixture()
	request.Parent, request.Cache = nil, nil
	request.SettingsPatch.Model.Set = preparationPointer("root-model")
	request.SettingsPatch.Seed.Set = preparationPointer(int64(0))
	prepared, err = PrepareGenerateInput(context.Background(), request, durable.GenerateReplay{})
	if err != nil || prepared.Request.Reasoning != nil || prepared.Request.Sampling == nil || *prepared.Request.Sampling.Seed != 0 ||
		prepared.Request.Sampling.Temperature != nil || prepared.Request.Sampling.TopP != nil || prepared.Request.Sampling.StopSequences != nil {
		t.Fatalf("root seed-only request = %+v, %v", prepared.Request, err)
	}
}
