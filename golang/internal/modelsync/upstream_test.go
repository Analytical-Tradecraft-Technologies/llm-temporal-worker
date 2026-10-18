package modelsync

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestCountryListsNest(t *testing.T) {
	sizes := map[string]int{CountryListUS: 1, CountryListEU: 27, CountryListCore1: 5, CountryListCore2: 9, CountryListCore3: 14, CountryListCore4: 34, CountryListCore5: 36}
	for name, size := range sizes {
		countries, err := CountryList(name)
		if err != nil || len(countries) != size {
			t.Fatalf("CountryList(%q) = %d countries, %v; want %d", name, len(countries), err, size)
		}
	}
	order := []string{CountryListCore1, CountryListCore2, CountryListCore3, CountryListCore4, CountryListCore5}
	for index := 1; index < len(order); index++ {
		smaller, _ := CountryList(order[index-1])
		larger, _ := CountryList(order[index])
		for country := range smaller {
			if _, ok := larger[country]; !ok {
				t.Fatalf("%s has %s but %s does not", order[index-1], country, order[index])
			}
		}
	}
	core1, _ := CountryList(CountryListCore1)
	for _, country := range []string{"US", "GB", "AU", "NZ", "CA"} {
		if _, ok := core1[country]; !ok {
			t.Fatalf("Core1 lacks %s", country)
		}
	}
	core5, _ := CountryList(CountryListCore5)
	for _, country := range []string{"JP", "KR", "TR", "DE"} {
		if _, ok := core5[country]; !ok {
			t.Fatalf("Core5 lacks %s", country)
		}
	}
	for _, country := range []string{"CN", "SG", "IL"} {
		if _, ok := core5[country]; ok {
			t.Fatalf("Core5 includes %s", country)
		}
	}
	if _, err := CountryList("core5"); err == nil {
		t.Fatal("list names are case-sensitive")
	}
	if normalizeCountry("uk") != "GB" || normalizeCountry("EL") != "GR" || normalizeCountry("USA") != "" {
		t.Fatal("country codes are not normalized")
	}
}

func upstreamDocument() *Document {
	endpoint := func(tag, provider, quantization string, retention bool, prompt string) Endpoint {
		return Endpoint{Tag: tag, Provider: provider, Quantization: quantization, ZeroRetention: retention, Pricing: Pricing{Prompt: prompt, Completion: "0.000002"}}
	}
	document := &Document{Schema: DocumentSchema, FetchedAt: time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC),
		Providers: []Provider{
			{Slug: "abroad-dc", Headquarters: "US", Datacenters: []string{"CN", "US"}},
			{Slug: "eu-host", Headquarters: "FR", Datacenters: []string{"FR"}},
			{Slug: "no-dc", Headquarters: "US"},
			{Slug: "no-hq", Datacenters: []string{"US"}},
			{Slug: "us-host", Headquarters: "US", Datacenters: []string{"US"}},
		},
		Models: []Model{{ID: "deepseek/deepseek-v3.2", ContextLength: 1000, Endpoints: []Endpoint{
			endpoint("abroad-dc/fp8", "abroad-dc", "fp8", true, "0.000009"),
			endpoint("eu-host/fp8", "eu-host", "fp8", false, "0.000003"),
			endpoint("no-dc", "no-dc", "fp8", true, "0.000009"),
			endpoint("no-hq", "no-hq", "fp8", true, "0.000009"),
			endpoint("unattributed", "", "fp8", true, "0.000009"),
			endpoint("us-host/fp4", "us-host", "fp4", true, "0.000001"),
			endpoint("us-host/fp8", "us-host", "fp8", true, "0.000002"),
			endpoint("us-host/unknown", "us-host", "", true, "0.000001"),
		}}, {ID: "vendor/unlisted-precision", Endpoints: []Endpoint{endpoint("us-host", "us-host", "bf16", true, "0.000001")}}},
	}
	document.Normalize()
	return document
}

func TestUpstreamPolicyRequiresHeadquartersAndEveryDatacenterInTheList(t *testing.T) {
	document := upstreamDocument()
	if err := document.Validate(); err != nil {
		t.Fatal(err)
	}
	rules := mergedPrecisionRules(t)
	core5, _ := NewUpstreamPolicy(CountryListCore5, false, false, false)
	got := core5.Upstreams(document, rules)["deepseek/deepseek-v3.2"].Tags
	want := []string{"eu-host/fp8", "us-host/fp4", "us-host/fp8", "us-host/unknown"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Core5 tags = %v, want %v", got, want)
	}
	us, _ := NewUpstreamPolicy(CountryListUS, false, false, false)
	if got := us.Upstreams(document, rules)["deepseek/deepseek-v3.2"].Tags; slices.Contains(got, "eu-host/fp8") || len(got) != 3 {
		t.Fatalf("US tags = %v, want only us-host endpoints", got)
	}
	eu, _ := NewUpstreamPolicy(CountryListEU, false, false, false)
	if got := eu.Upstreams(document, rules)["deepseek/deepseek-v3.2"].Tags; !reflect.DeepEqual(got, []string{"eu-host/fp8"}) {
		t.Fatalf("EU tags = %v", got)
	}
	if _, ok := eu.Upstreams(document, rules)["vendor/unlisted-precision"]; ok {
		t.Fatal("a model with no endpoint in the list has upstreams")
	}
}

func TestUpstreamPolicyFiltersPrecisionAndRetention(t *testing.T) {
	document := upstreamDocument()
	rules := mergedPrecisionRules(t)
	precise, _ := NewUpstreamPolicy(CountryListCore5, true, false, false)
	upstreams := precise.Upstreams(document, rules)
	got := upstreams["deepseek/deepseek-v3.2"]
	if !reflect.DeepEqual(got.Tags, []string{"eu-host/fp8", "us-host/fp8"}) || !reflect.DeepEqual(got.Quantizations, []string{"bf16", "fp16", "fp32", "fp8"}) {
		t.Fatalf("native precision upstream = %+v; want fp8 endpoints and no fp4 or unpublished quantization", got)
	}
	if _, ok := upstreams["vendor/unlisted-precision"]; ok {
		t.Fatal("a model without a native precision rule is routable when native precision is required")
	}
	retained, _ := NewUpstreamPolicy(CountryListCore5, true, true, false)
	if got := retained.Upstreams(document, rules)["deepseek/deepseek-v3.2"].Tags; !reflect.DeepEqual(got, []string{"us-host/fp8"}) {
		t.Fatalf("zero-retention tags = %v", got)
	}
}

func TestCompilePricesOpenRouterAtTheMostExpensiveAllowedEndpoint(t *testing.T) {
	document := upstreamDocument()
	openrouter := testSource("openrouter", "", "openai_chat", map[llm.ServiceClass]string{llm.ServiceClassStandard: "default", llm.ServiceClassPriority: "default"})
	policy, _ := NewUpstreamPolicy(CountryListCore5, false, false, false)
	compiled, err := Compile(Input{Document: document, Rules: mergedPrecisionRules(t), OpenRouter: &openrouter, Policy: policy,
		Reserved: map[string]struct{}{"vendor/unlisted-precision": {}}})
	if err != nil {
		t.Fatal(err)
	}
	var input string
	for _, entry := range compiled.Prices {
		if entry.Model == "deepseek/deepseek-v3.2" {
			input = entry.Prices.InputPerMillion.CanonicalString()
		}
	}
	// The disallowed endpoints charge 9; the most expensive allowed one 3.
	if input != "3" {
		t.Fatalf("input price = %q, want 3", input)
	}
	// A configured model is not synced, but a pinned OpenRouter endpoint
	// still needs its allowed upstreams.
	if _, ok := compiled.Models["vendor/unlisted-precision"]; ok {
		t.Fatal("reserved model was synced")
	}
	if _, ok := compiled.Upstreams["vendor/unlisted-precision"]; !ok {
		t.Fatal("reserved model has no upstreams")
	}
	none, _ := NewUpstreamPolicy(CountryListUS, true, true, false)
	compiled, err = Compile(Input{Document: document, Rules: Rules{Digest: "x"}, OpenRouter: &openrouter, Policy: none})
	if err != nil {
		t.Fatal(err)
	}
	if len(compiled.Models) != 0 || len(compiled.Upstreams) != 0 {
		t.Fatalf("models %v upstreams %v, want none without native precision rules", compiled.Models, compiled.Upstreams)
	}
}

func TestFetchRecordsProviderQuantizationAndRetention(t *testing.T) {
	document := fetchFixture(t)
	var model Model
	for _, candidate := range document.Models {
		if candidate.ID == "openai/gpt-5.4" {
			model = candidate
		}
	}
	byTag := map[string]Endpoint{}
	for _, endpoint := range model.Endpoints {
		byTag[endpoint.Tag] = endpoint
	}
	if endpoint := byTag["openai"]; endpoint.Provider != "openai" || !endpoint.ZeroRetention || endpoint.Quantization != "" {
		t.Fatalf("openai endpoint = %+v", endpoint)
	}
	if endpoint := byTag["azure/eu"]; endpoint.Provider != "azure" || endpoint.ZeroRetention {
		t.Fatalf("azure/eu endpoint = %+v", endpoint)
	}
	if endpoint := byTag["amazon-bedrock/us-east-1"]; endpoint.Provider != "amazon-bedrock" {
		t.Fatalf("bedrock endpoint = %+v", endpoint)
	}
	var unlisted Provider
	for _, provider := range document.Providers {
		if provider.Slug == "unlisted" {
			unlisted = provider
		}
	}
	if unlisted.Slug == "" || unlisted.Headquarters != "" || unlisted.Datacenters != nil {
		t.Fatalf("provider without countries = %+v", unlisted)
	}
}

func TestNativeQuantizationsPrefersTheLongestPattern(t *testing.T) {
	rules := mergedPrecisionRules(t, `version: model-sync-rules/v1
native_precision:
  "vendor/*": [bf16]
  "vendor/model-*": [fp8]
`)
	if got, ok := rules.NativeQuantizations("vendor/model-a"); !ok || !reflect.DeepEqual(got, []string{"fp8"}) {
		t.Fatalf("vendor/model-a = %v %v", got, ok)
	}
	if got, ok := rules.NativeQuantizations("vendor/other"); !ok || !reflect.DeepEqual(got, []string{"bf16"}) {
		t.Fatalf("vendor/other = %v %v", got, ok)
	}
	if _, ok := rules.NativeQuantizations("elsewhere/model"); ok {
		t.Fatal("unmatched model has native quantizations")
	}
	if got, ok := rules.NativeQuantizations("openai/gpt-oss-120b"); !ok || slices.Contains(got, "fp4") {
		t.Fatalf("gpt-oss native quantizations = %v %v", got, ok)
	}
	if _, err := ParseRules([]byte("version: model-sync-rules/v1\nnative_precision:\n  vendor/x: [unknown]\n")); err != nil {
		t.Fatal(err)
	} else if _, err := MergeRules(mustParse(t, "version: model-sync-rules/v1\nnative_precision:\n  vendor/x: [unknown]\n")); err == nil {
		t.Fatal("unknown is accepted as a native quantization")
	}
}

func mustParse(t *testing.T, data string) RulesDocument {
	t.Helper()
	document, err := ParseRules([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	return document
}

// mergedPrecisionRules merges the built-in rules, the built-in native
// precision rules and any layers.
func mergedPrecisionRules(t *testing.T, layers ...string) Rules {
	t.Helper()
	precision, err := ParseRules(NativePrecisionRulesDocument())
	if err != nil {
		t.Fatal(err)
	}
	builtIn, err := ParseRules(DefaultRulesDocument())
	if err != nil {
		t.Fatal(err)
	}
	documents := []RulesDocument{builtIn, precision}
	for _, layer := range layers {
		documents = append(documents, mustParse(t, layer))
	}
	rules, err := MergeRules(documents...)
	if err != nil {
		t.Fatal(err)
	}
	return rules
}

func TestDropUnlistedProvidersClearsCarriedForwardProviders(t *testing.T) {
	document := upstreamDocument()
	document.Providers = document.Providers[1:] // abroad-dc is no longer listed
	dropUnlistedProviders(document)
	if err := document.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range document.Models[0].Endpoints {
		if endpoint.Tag == "abroad-dc/fp8" && endpoint.Provider != "" {
			t.Fatalf("endpoint %q kept unlisted provider %q", endpoint.Tag, endpoint.Provider)
		}
		if endpoint.Tag == "us-host/fp8" && endpoint.Provider != "us-host" {
			t.Fatalf("endpoint %q lost listed provider", endpoint.Tag)
		}
	}
}

func TestCompileTakesRouteLimitsFromAllowedEndpoints(t *testing.T) {
	document := upstreamDocument()
	for index := range document.Models[0].Endpoints {
		endpoint := &document.Models[0].Endpoints[index]
		endpoint.ContextLength, endpoint.MaxCompletionTokens = 64000, 8000
		if endpoint.Tag == "abroad-dc/fp8" {
			// Only an excluded endpoint serves the largest window.
			endpoint.ContextLength, endpoint.MaxCompletionTokens = 1000000, 100000
		}
	}
	document.Models[0].ContextLength, document.Models[0].MaxCompletionTokens = 1000000, 100000
	openrouter := testSource("openrouter", "", "openai_chat", map[llm.ServiceClass]string{llm.ServiceClassStandard: "default"})
	policy, _ := NewUpstreamPolicy(CountryListCore5, false, false, false)
	compiled, err := Compile(Input{Document: document, Rules: mergedPrecisionRules(t), OpenRouter: &openrouter, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	route := compiled.Models["deepseek/deepseek-v3.2"].Routes[0]
	if route.ContextTokens != 64000 || route.OutputTokens != 8000 {
		t.Fatalf("route limits = %d/%d, want the allowed endpoints' 64000/8000", route.ContextTokens, route.OutputTokens)
	}
}
