package catalog

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func TestLoadCapabilitiesConvertsStrictEntries(t *testing.T) {
	ref := writeCatalog(t, `version: llmtw-capabilities/v1
entries:
  - id: openai-prod
    family: openai_responses
    model:
      exact: gpt-example
    verified_at: 2026-07-13T00:00:00Z
    features:
      input.text:
        level: native
      tools.auto:
        level: native
      tools.required:
        level: native
      tools.parallel:
        level: native
      output.json_schema:
        level: emulated
        transform: json-schema-tool
      service.economy:
        level: native
      service.standard:
        level: native
      service.priority:
        level: native
      continuation.response_id:
        level: unsupported
        reason: provider ids are not stable
    limits:
      context_tokens: 400000
      output_tokens: 32768
`)

	catalog, err := LoadCapabilities(ref)
	if err != nil {
		t.Fatalf("LoadCapabilities() error = %v", err)
	}
	profile, ok := catalog.Profiles["openai-prod"]
	if !ok {
		t.Fatalf("profiles = %#v", catalog.Profiles)
	}
	if profile.Family != provider.FamilyOpenAIResponses || profile.Model != "gpt-example" {
		t.Fatalf("profile identity = %#v", profile)
	}
	if profile.Set.Version != "llmtw-capabilities/v1" {
		t.Fatalf("capability version = %q", profile.Set.Version)
	}
	if got := profile.Set.Features[provider.FeatureStructuredOutput]; got.State != provider.CapabilityEmulated || got.Transform != "json-schema-tool" {
		t.Fatalf("structured output = %#v", got)
	}
	if got := profile.Set.Features[provider.FeatureContinuation]; got.State != provider.CapabilityUnsupported {
		t.Fatalf("continuation = %#v", got)
	}
	if !profile.ServiceClassesDeclared || !reflect.DeepEqual(profile.ServiceClasses, []llm.ServiceClass{llm.ServiceClassEconomy, llm.ServiceClassPriority, llm.ServiceClassStandard}) {
		t.Fatalf("service classes = %#v (declared=%v)", profile.ServiceClasses, profile.ServiceClassesDeclared)
	}
	if catalog.Digest == ([32]byte{}) {
		t.Fatal("catalog digest is empty")
	}
}

func TestLoadCapabilitiesAcceptsLocalProfilesAndClosedClasses(t *testing.T) {
	ref := writeCatalog(t, `version: local-mock-v1
profiles:
  local-mock-v1:
    family: openai_chat
    model: demo-model
    input: [text, reference]
    output: [text, tool_call]
    service_classes: [economy, standard, priority]
    max_context_tokens: 32768
    max_output_tokens: 4096
`)
	catalog, err := LoadCapabilities(ref)
	if err != nil {
		t.Fatalf("LoadCapabilities() error = %v", err)
	}
	profile := catalog.Profiles["local-mock-v1"]
	if !profile.Set.Supports(provider.FeatureText, true) || !profile.Set.Supports(provider.FeatureToolCall, true) {
		t.Fatalf("compiled local features = %#v", profile.Set.Features)
	}
	if !profile.ServiceClassesDeclared || !reflect.DeepEqual(profile.ServiceClasses, []llm.ServiceClass{llm.ServiceClassEconomy, llm.ServiceClassPriority, llm.ServiceClassStandard}) {
		t.Fatalf("compiled local service classes = %#v (declared=%v)", profile.ServiceClasses, profile.ServiceClassesDeclared)
	}
}

func TestLoadCapabilitiesPreservesAbsentAndExplicitlyEmptyServiceClasses(t *testing.T) {
	ref := writeCatalog(t, `version: local-mock-v1
profiles:
  absent:
    family: openai_chat
    model: absent-model
    input: [text]
  empty:
    family: openai_chat
    model: empty-model
    input: [text]
    service_classes: []
`)
	catalog, err := LoadCapabilities(ref)
	if err != nil {
		t.Fatalf("LoadCapabilities() error = %v", err)
	}
	if catalog.Profiles["absent"].ServiceClassesDeclared {
		t.Fatal("omitted service_classes should remain undeclared")
	}
	if !catalog.Profiles["empty"].ServiceClassesDeclared || len(catalog.Profiles["empty"].ServiceClasses) != 0 {
		t.Fatalf("explicit empty service_classes = %#v (declared=%v)", catalog.Profiles["empty"].ServiceClasses, catalog.Profiles["empty"].ServiceClassesDeclared)
	}
}

func TestLoadPricingCompilesExactDecimalEntries(t *testing.T) {
	ref := writeCatalog(t, `version: llmtw-prices/v1
id: catalog-2026-07-13
entries:
  - provider: openai
    endpoint_id: openai-production
    endpoint_family: openai_responses
    region: global
    model: gpt-example
    provider_tier: standard
    input_per_million: "1.250000"
    output_per_million: "10.000000"
    cache_read_per_million: "0.125000"
    source: operator-verified
`)

	catalog, err := LoadPricing(ref)
	if err != nil {
		t.Fatalf("LoadPricing() error = %v", err)
	}
	if catalog.ID != "catalog-2026-07-13" {
		t.Fatalf("catalog identity = %#v", catalog)
	}
	if len(catalog.Catalog.Entries) != 1 {
		t.Fatalf("entries = %#v", catalog.Catalog.Entries)
	}
	entry := catalog.Catalog.Entries[0]
	if entry.Provider != "openai" || entry.EndpointID != "openai-production" || entry.ProviderTier != "standard" {
		t.Fatalf("entry identity = %#v", entry)
	}
	if entry.Prices.InputPerMillion.String() != "1.250000" || entry.Prices.OutputPerMillion.String() != "10.000000" {
		t.Fatalf("entry prices = %#v", entry.Prices)
	}
}

func TestLoadPricingRejectsNonUSDSource(t *testing.T) {
	ref := writeCatalog(t, `version: llmtw-prices/v1
id: catalog-non-usd
currency: EUR
entries:
  - provider: openai
    endpoint_id: openai-production
    endpoint_family: openai_responses
    region: global
    model: gpt-example
    provider_tier: standard
    input_per_million: "1.250000"
    output_per_million: "10.000000"
`)
	if _, err := LoadPricing(ref); err == nil || !strings.Contains(strings.ToLower(err.Error()), "currency") {
		t.Fatalf("LoadPricing() error = %v, want a non-USD rejection", err)
	}
}

func TestLoadPricingPreservesOmittedComponentsAsUnknown(t *testing.T) {
	ref := writeCatalog(t, `version: llmtw-prices/v1
id: catalog-partial
entries:
  - provider: openai
    endpoint_id: openai-production
    endpoint_family: openai_responses
    region: global
    model: gpt-example
    provider_tier: standard
    input_per_million: "1.250000"
    output_per_million: "10.000000"
`)
	catalog, err := LoadPricing(ref)
	if err != nil {
		t.Fatal(err)
	}
	entry := catalog.Catalog.Entries[0]
	want := []pricing.PriceComponent{pricing.PriceComponentCacheRead, pricing.PriceComponentCacheWrite, pricing.PriceComponentPerRequest, pricing.PriceComponentReasoning}
	if !reflect.DeepEqual(entry.UnknownComponents, want) {
		t.Fatalf("unknown components = %#v, want %#v", entry.UnknownComponents, want)
	}
	if _, err := pricing.CostFromUsage(entry, pricing.Usage{CacheReadTokens: 1}); err == nil {
		t.Fatal("CostFromUsage accepted omitted cache price as known zero")
	}
}

func TestLoadPricingPreservesAuditLinkageAcrossRotation(t *testing.T) {
	body := `version: llmtw-prices/v2
id: catalog-rotating
entries:
  - provider: openai
    endpoint_id: openai-production
    endpoint_family: openai_responses
    region: global
    model: gpt-example
    provider_tier: standard
    effective_from: 2026-01-01T00:00:00Z
    effective_until: 2026-02-01T00:00:00Z
    input_per_million: "1.250000"
    output_per_million: "10.000000"
    source: provider-price-sheet-2026-01
  - provider: openai
    endpoint_id: openai-production
    endpoint_family: openai_responses
    region: global
    model: gpt-example
    provider_tier: standard
    effective_from: 2026-02-01T00:00:00Z
    input_per_million: "1.500000"
    output_per_million: "12.000000"
    provenance: provider-price-sheet-2026-02
`
	loaded, err := LoadPricing(writeCatalog(t, body))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Digest == ([32]byte{}) || loaded.Catalog.Digest == ([32]byte{}) {
		t.Fatal("source and compiled digests must be recorded")
	}
	if got := len(loaded.Catalog.Entries); got != 2 {
		t.Fatalf("rotated entries = %d, want 2", got)
	}
	for index, want := range []string{"provider-price-sheet-2026-01", "provider-price-sheet-2026-02"} {
		if got := loaded.Catalog.Entries[index].Provenance; got != want {
			t.Fatalf("entry %d provenance = %q, want %q", index, got, want)
		}
	}
	for _, test := range []struct {
		name string
		at   string
		want string
		cost string
	}{
		{name: "before rotation", at: "2026-01-31T23:59:59Z", want: "provider-price-sheet-2026-01", cost: "1.250000"},
		{name: "after rotation", at: "2026-02-01T00:00:00Z", want: "provider-price-sheet-2026-02", cost: "1.500000"},
	} {
		t.Run(test.name, func(t *testing.T) {
			at, err := time.Parse(time.RFC3339, test.at)
			if err != nil {
				t.Fatal(err)
			}
			quote, err := loaded.Catalog.Resolve(pricing.Query{Provider: "openai", Family: "openai_responses", EndpointID: "openai-production", Region: "global", Model: "gpt-example", ProviderTier: "standard", At: at})
			if err != nil {
				t.Fatal(err)
			}
			if quote.Entry.Provenance != test.want || quote.Entry.Prices.InputPerMillion.String() != test.cost {
				t.Fatalf("quote = (%q, %s), want (%q, %s)", quote.Entry.Provenance, quote.Entry.Prices.InputPerMillion.String(), test.want, test.cost)
			}
		})
	}
}

func TestLoadPricingRejectsAuditLinkageMismatch(t *testing.T) {
	ref := writeCatalog(t, `version: llmtw-prices/v1
id: catalog-audit-mismatch
entries:
  - provider: openai
    endpoint_id: openai-production
    endpoint_family: openai_responses
    region: global
    model: gpt-example
    provider_tier: standard
    input_per_million: "1"
    output_per_million: "2"
    source: source-a
    provenance: source-b
`)
	if _, err := LoadPricing(ref); err == nil || !strings.Contains(err.Error(), "provenance and source disagree") {
		t.Fatalf("audit linkage error = %v, want source/provenance mismatch", err)
	}
}

func TestLoadPricingFailsClosedWhenSourceIsUnavailable(t *testing.T) {
	ref := config.CatalogRef{File: filepath.Join(t.TempDir(), "unavailable.yaml"), SHA256: strings.Repeat("0", 64)}
	if _, err := LoadPricing(ref); err == nil || !strings.Contains(err.Error(), "open catalog") {
		t.Fatalf("source outage error = %v, want bounded open failure", err)
	}
}

func TestLoadRejectsUnknownFieldsDuplicateIDsDigestAndUnsafeSize(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "unknown capability field",
			body: `version: v1
entries:
  - id: p
    family: openai_chat
    model: {exact: m}
    features: {text: {level: native}}
    typo: true
`,
			want: "field typo not found",
		},
		{
			name: "duplicate capability id",
			body: `version: v1
entries:
  - id: p
    family: openai_chat
    model: {exact: m}
    features: {text: {level: native}}
  - id: p
    family: openai_chat
    model: {exact: m2}
    features: {text: {level: native}}
`,
			want: "duplicate capability profile ID",
		},
		{
			name: "unknown price field",
			body: `version: v1
id: prices
entries:
  - provider: p
    endpoint_id: e
    family: openai_chat
    region: r
    model: m
    provider_tier: standard
    input_per_million: "0"
    output_per_million: "0"
    typo: true
`,
			want: "field typo not found",
		},
		{
			name: "provider default class",
			body: `version: v1
profiles:
  p:
    family: openai_chat
    model: m
    input: [text]
    service_classes: [provider_default]
`,
			want: "economy, standard, or priority",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ref := writeCatalog(t, test.body)
			var err error
			if strings.Contains(test.name, "price") {
				_, err = LoadPricing(ref)
			} else {
				_, err = LoadCapabilities(ref)
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}

	ref := writeCatalog(t, "version: v1\nprofiles: {}\n")
	ref.SHA256 = strings.Repeat("0", 64)
	if _, err := LoadCapabilities(ref); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("digest mismatch error = %v", err)
	}

	large := writeCatalog(t, "version: v1\nprofiles: {}\n")
	if _, err := LoadCapabilitiesWithOptions(large, Options{MaxBytes: 8}); err == nil || !strings.Contains(err.Error(), "exceeds 8 bytes") {
		t.Fatalf("size error = %v", err)
	}
}

func TestLoadRejectsMissingEndpointReferences(t *testing.T) {
	capRef := writeCatalog(t, `version: v1
profiles:
  profile:
    family: openai_chat
    model: model
    input: [text]
`)
	priceRef := writeCatalog(t, `version: v1
id: prices
entries:
  - provider: mock
    endpoint_id: endpoint
    family: openai_chat
    region: local
    model: model
    provider_tier: standard
    input_per_million: "0"
    output_per_million: "0"
`)
	cfg := config.Config{
		Capabilities: config.CapabilityConfig{Catalogs: []config.CatalogRef{capRef}},
		Pricing:      config.PricingConfig{Catalogs: []config.CatalogRef{priceRef}},
		Endpoints: map[string]config.EndpointConfig{
			"endpoint": {Family: "openai_chat", CapabilityProfile: "missing", PriceCatalog: "prices"},
		},
	}
	if _, err := Load(cfg); err == nil || !strings.Contains(err.Error(), "missing capability profile") {
		t.Fatalf("missing capability reference error = %v", err)
	}
	cfg.Endpoints["endpoint"] = config.EndpointConfig{Family: "openai_chat", CapabilityProfile: "profile", PriceCatalog: "missing"}
	if _, err := Load(cfg); err == nil || !strings.Contains(err.Error(), "missing price catalog") {
		t.Fatalf("missing price reference error = %v", err)
	}
}

func TestEndpointFamilyMapsAnthropicAWSWithoutConflatingBedrock(t *testing.T) {
	if got := endpointFamily("azure_openai_chat"); got != provider.FamilyOpenAIChat {
		t.Fatalf("Azure Chat family = %q, want %q", got, provider.FamilyOpenAIChat)
	}
	if got := endpointFamily("anthropic_aws_messages"); got != provider.FamilyAnthropicMessages {
		t.Fatalf("Anthropic AWS family = %q, want %q", got, provider.FamilyAnthropicMessages)
	}
	if got := endpointFamily("bedrock_anthropic_messages"); got != provider.FamilyBedrockMessages {
		t.Fatalf("Bedrock family = %q, want %q", got, provider.FamilyBedrockMessages)
	}
	if got := endpointFamily("bedrock_converse"); got != provider.FamilyBedrockConverse {
		t.Fatalf("Bedrock Converse family = %q, want %q", got, provider.FamilyBedrockConverse)
	}
}

func writeCatalog(t *testing.T, body string) config.CatalogRef {
	t.Helper()
	path := filepath.Join(t.TempDir(), "catalog.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(body))
	return config.CatalogRef{File: path, SHA256: hex.EncodeToString(digest[:])}
}
