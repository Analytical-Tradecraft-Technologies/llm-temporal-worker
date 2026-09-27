package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	yaml "go.yaml.in/yaml/v4"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/openaichat"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

// TestOpenRouterFrontierDeploymentLowersAndLiftsPinnedModels proves the
// shipped deploy/openrouter endpoints and catalogs through the production
// path: configuration validation, catalog binding, factory adapter
// construction, the exact request body per model and the lifted receipt.
func TestOpenRouterFrontierDeploymentLowersAndLiftsPinnedModels(t *testing.T) {
	compiled := compileOpenRouterDeployment(t, nil)
	now := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	snapshot, err := (CatalogSnapshotLoader{Clock: func() time.Time { return now }}).Load(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
		Resolver: secrets.ResolverFunc(func(_ context.Context, ref config.SecretRef) ([]byte, error) {
			if ref.Kind != config.SecretFile || ref.Path != "/var/run/secrets/providers/openrouter-api-key" {
				t.Fatalf("resolved unexpected secret reference: %#v", ref)
			}
			return []byte("test-openrouter-key"), nil
		}),
		SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) { return snapshot, nil }),
		HTTPClient:     &http.Client{},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		endpoint, logical, model, echo, providerValue string
		input, cacheRead, output, reasoning           int64
		cost                                          string
	}{
		{endpoint: "openrouter-anthropic-opus55", logical: "claude-opus-5-5", model: "anthropic/claude-opus-5.5", echo: "anthropic/claude-opus-5.5-20260921", providerValue: "default", input: 1850, output: 2410, reasoning: 2210, cost: "0.055600000000000000"},
		{endpoint: "openrouter-openai-gpt6-astra", logical: "gpt-6-astra", model: "openai/gpt-6-astra", echo: "openai/gpt-6-astra", providerValue: "default", input: 314, cacheRead: 1536, output: 1900, reasoning: 1650, cost: "0.099676000000000000"},
		{endpoint: "openrouter-google-gemini31-pro", logical: "gemini-3-1-pro-preview", model: "google/gemini-3.1-pro-preview", echo: "google/gemini-3.1-pro-preview", input: 1850, output: 1200, reasoning: 950, cost: "0.018100000000000000"},
	} {
		t.Run(test.endpoint, func(t *testing.T) {
			routes := snapshot.Routes.Models[test.logical].Routes
			if len(routes) != 1 || routes[0].Model != test.model || routes[0].Provider != "openrouter" || !routes[0].PriceAvailable || routes[0].PriceVersion != "catalog-2026-09-26" {
				t.Fatalf("route snapshot = %#v", routes)
			}
			built, err := factory.buildAdapter(context.Background(), compiled.Config(), snapshot, test.endpoint)
			if err != nil {
				t.Fatal(err)
			}
			production, ok := built.(*openaichat.Adapter)
			if !ok || production.Name() != "openai.chat/"+test.endpoint {
				t.Fatalf("adapter = %T %v, want the OpenRouter chat adapter", built, built)
			}
			var body []byte
			responseBody := readOpenRouterFixture(t, test.endpoint+".response.json")
			client, err := openaichat.NewOpenRouterClient(openaichat.OpenRouterClientConfig{BaseURL: "https://openrouter.ai/api/v1", APIKey: "test-openrouter-key", HTTPClient: &http.Client{Transport: frontierRoundTrip(func(request *http.Request) (*http.Response, error) {
				body, _ = io.ReadAll(request.Body)
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(responseBody)), Request: request}, nil
			})}})
			if err != nil {
				t.Fatal(err)
			}
			adapter, err := openaichat.New(client, test.endpoint, production.Profile())
			if err != nil {
				t.Fatal(err)
			}
			call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: frontierForecastRequest(test.model), Query: provider.CapabilityQuery{EndpointID: test.endpoint, Family: provider.FamilyOpenAIChat, Model: test.model}, Strict: true})
			if err != nil {
				t.Fatal(err)
			}
			result, err := adapter.Invoke(context.Background(), call, provider.NopObserver{})
			if err != nil {
				t.Fatal(err)
			}
			var got, want any
			if err := json.Unmarshal(body, &got); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(readOpenRouterFixture(t, test.endpoint+".request.json"), &want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("request body = %s", body)
			}
			response := result.Response
			if response.Route.ResolvedModel != test.model || response.Route.ObservedModelRevision != test.model || response.Route.ModelIdentityBasis != llm.ModelIdentityBasisProviderReported || string(response.Provider.Raw["response_model"]) != `"`+test.echo+`"` {
				t.Fatalf("route = %#v, raw model %s", response.Route, response.Provider.Raw["response_model"])
			}
			usage := response.Usage
			if usage.InputTokens != test.input || usage.CacheReadTokens != test.cacheRead || usage.CacheWriteTokens != 0 || usage.OutputTokens != test.output || usage.ReasoningTokens != test.reasoning {
				t.Fatalf("usage = %+v", usage)
			}
			if response.Cost.ActualCostUSD == nil || response.Cost.ActualCostUSD.String() != test.cost || response.Cost.Method != "openrouter_reported" {
				t.Fatalf("cost = %#v", response.Cost)
			}
			if response.Service.Actual == nil || *response.Service.Actual != llm.ServiceClassStandard || response.Service.ProviderValue != test.providerValue {
				t.Fatalf("service = %#v", response.Service)
			}
		})
	}
}

type frontierRoundTrip func(*http.Request) (*http.Response, error)

func (function frontierRoundTrip) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func frontierForecastRequest(model string) llm.Request {
	maxTokens := 4000
	return llm.Request{
		OperationKey: "futureeval-forecast-1103",
		Model:        model,
		ServiceClass: llm.ServiceClassStandard,
		Instructions: []llm.Instruction{{Kind: llm.InstructionKindText, Level: llm.InstructionLevelApplication, Text: "You are a calibrated forecaster. Return a probability and a short rationale."}},
		Input:        []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "Will the event resolve Yes before 2026-12-31?"}}}},
		ToolPolicy:   llm.ToolPolicy{Mode: llm.ToolChoiceNone},
		Output: &llm.OutputSpec{MaxTokens: &maxTokens, Format: llm.OutputFormat{
			Kind: llm.OutputKindJSONSchema, Name: "forecast", Strict: true,
			Schema: json.RawMessage(`{"type":"object","properties":{"probability":{"type":"number","minimum":0,"maximum":1},"rationale":{"type":"string"}},"required":["probability","rationale"],"additionalProperties":false}`),
		}},
		Reasoning: &llm.ReasoningSpec{Effort: llm.ReasoningEffortHigh},
	}
}

func readOpenRouterFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "openrouter", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// compileOpenRouterDeployment merges the shipped endpoints/models fragment and
// catalogs into the local fixture configuration and compiles it with full
// configuration validation.
func compileOpenRouterDeployment(t *testing.T, limits map[string]any) *config.Snapshot {
	t.Helper()
	read := func(path string) []byte {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	var document, fragment map[string]any
	if err := yaml.Unmarshal(read("../../deploy/local/config.yaml"), &document); err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(read("../../deploy/openrouter/endpoints.yaml"), &fragment); err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	catalogRef := func(name string) []any {
		data := read("../../deploy/openrouter/" + name)
		path := filepath.Join(directory, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(data)
		return []any{map[string]any{"file": path, "sha256": hex.EncodeToString(digest[:])}}
	}
	document["endpoints"] = fragment["endpoints"]
	document["models"] = fragment["models"]
	document["limits"].(map[string]any)["provider_timeout"] = "120s"
	for key, value := range limits {
		document["limits"].(map[string]any)[key] = value
	}
	document["capabilities"].(map[string]any)["catalogs"] = catalogRef("capabilities.yaml")
	document["pricing"].(map[string]any)["catalogs"] = catalogRef("prices.yaml")
	data, err := yaml.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := config.Compile(context.Background(), data, nil)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

// The shipped Gemini and GPT-6 quotes are valid only below OpenRouter's
// prompt-size tier thresholds. A configuration or reservation that could admit
// a larger prompt is refused before any provider call.
func TestOpenRouterPriceTierBoundaryIsEnforced(t *testing.T) {
	now := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	loader := CatalogSnapshotLoader{Clock: func() time.Time { return now }}
	if _, err := loader.Load(context.Background(), compileOpenRouterDeployment(t, map[string]any{"max_input_tokens": 200000})); err != nil {
		t.Fatalf("input limit at the lowest tier boundary rejected: %v", err)
	}
	_, err := loader.Load(context.Background(), compileOpenRouterDeployment(t, map[string]any{"max_input_tokens": 200001}))
	if err == nil || !strings.Contains(err.Error(), "limits.max_input_tokens 200001 exceeds max_prompt_tokens 200000") || !strings.Contains(err.Error(), "google/gemini-3.1-pro-preview") {
		t.Fatalf("input limit above the Gemini tier boundary error = %v", err)
	}

	snapshot, err := loader.Load(context.Background(), compileOpenRouterDeployment(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	quote, err := snapshot.Prices.Resolve(pricing.Query{Provider: "openrouter", Family: "openai_chat", EndpointID: "openrouter-openai-gpt6-astra", Region: "global", Model: "openai/gpt-6-astra", ProviderTier: "default", At: now})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := llm.ReserveBatchOperationV1{Model: "gpt-6-astra", ServiceClass: llm.ServiceClassStandard, MaxInputTokens: 272000, MaxOutputTokens: 4000, MaxReasoningTokens: 8000, MaxCacheReadTokens: 272000, MaxCacheWriteTokens: 272000}
	if _, err := priceDescriptorMaximum(descriptor, quote.Entry); err != nil {
		t.Fatalf("descriptor at the GPT-6 tier boundary rejected: %v", err)
	}
	descriptor.MaxInputTokens = 272001
	if _, err := priceDescriptorMaximum(descriptor, quote.Entry); err == nil || !strings.Contains(err.Error(), "exceeds the price entry's max_prompt_tokens 272000") {
		t.Fatalf("descriptor above the GPT-6 tier boundary error = %v", err)
	}
	restored, err := persistPricingEntry(quote.Entry).restore()
	if err != nil || restored.MaxPromptTokens != 272000 {
		t.Fatalf("persisted price lost its prompt ceiling: %#v %v", restored, err)
	}
}

// OpenRouter bills GPT-6 automatic cache reads and writes and Gemini implicit
// cache reads on any eligible prompt. Reserve-batch refuses signed cache caps
// that cannot cover the prompt instead of admitting a call that is billed and
// then refused in finalizeGenerate; Opus is cached only on request.
func TestOpenRouterAutomaticCacheNeedsCoveringCaps(t *testing.T) {
	now := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	snapshot, err := (CatalogSnapshotLoader{Clock: func() time.Time { return now }}).Load(context.Background(), compileOpenRouterDeployment(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		endpoint, model   string
		read, write, want string
	}{
		{endpoint: "openrouter-openai-gpt6-astra", model: "openai/gpt-6-astra", want: "bills automatic prompt-cache reads: max_cache_read_tokens 0 must be at least max_input_tokens 131072"},
		{endpoint: "openrouter-openai-gpt6-astra", model: "openai/gpt-6-astra", read: "full", want: "bills automatic prompt-cache writes: max_cache_write_tokens 0"},
		{endpoint: "openrouter-openai-gpt6-astra", model: "openai/gpt-6-astra", read: "full", write: "full"},
		{endpoint: "openrouter-google-gemini31-pro", model: "google/gemini-3.1-pro-preview", want: "bills automatic prompt-cache reads"},
		{endpoint: "openrouter-google-gemini31-pro", model: "google/gemini-3.1-pro-preview", read: "full"},
		{endpoint: "openrouter-anthropic-opus55", model: "anthropic/claude-opus-5.5"},
	} {
		t.Run(test.endpoint+"/"+test.read+"-"+test.write, func(t *testing.T) {
			quote, err := snapshot.Prices.Resolve(pricing.Query{Provider: "openrouter", Family: "openai_chat", EndpointID: test.endpoint, Region: "global", Model: test.model, ProviderTier: "default", At: now})
			if err != nil {
				t.Fatal(err)
			}
			descriptor := llm.ReserveBatchOperationV1{ServiceClass: llm.ServiceClassStandard, MaxInputTokens: 131072, MaxOutputTokens: 4000, MaxReasoningTokens: 8000}
			if test.read == "full" {
				descriptor.MaxCacheReadTokens = descriptor.MaxInputTokens
			}
			if test.write == "full" {
				descriptor.MaxCacheWriteTokens = descriptor.MaxInputTokens
			}
			_, err = priceDescriptorMaximum(descriptor, quote.Entry)
			if test.want == "" && err != nil || test.want != "" && (err == nil || !strings.Contains(err.Error(), test.want)) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}
