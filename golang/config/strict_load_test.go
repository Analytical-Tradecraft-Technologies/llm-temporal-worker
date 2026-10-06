package config_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/schema"
)

// replaceExample returns the shipped example with one exact replacement and
// fails the test when the example no longer contains the text.
func replaceExample(t *testing.T, old, replacement string) []byte {
	t.Helper()
	example := string(exampleYAML(t))
	if !strings.Contains(example, old) {
		t.Fatalf("example configuration no longer contains %q", old)
	}
	return []byte(strings.Replace(example, old, replacement, 1))
}

func requireLoadError(t *testing.T, data []byte, fragments ...string) {
	t.Helper()
	_, err := config.Load(data)
	if err == nil {
		t.Fatalf("configuration was accepted, want an error naming %q", fragments)
	}
	for _, fragment := range fragments {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("error = %v, want it to name %q", err, fragment)
		}
	}
}

func TestLoadRejectsContentAfterTheFirstDocument(t *testing.T) {
	example := string(exampleYAML(t))
	for name, trailing := range map[string]string{
		"second document":           "\n---\nbudgets:\n  require_match: false\n",
		"second empty mapping":      "\n---\n{}\n",
		"document after end marker": "\n...\nenvironment: staging\n",
		"garbage after end marker":  "\n...\n}} not yaml [\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := config.Load([]byte(example + trailing)); err == nil {
				t.Fatal("content after the first YAML document was silently ignored")
			}
		})
	}
	// A bare end marker and trailing comments add no content.
	if _, err := config.Load([]byte(example + "\n...\n# end of file\n")); err != nil {
		t.Fatalf("document end marker rejected: %v", err)
	}
}

func TestLoadRejectsBudgetBoundsThatFailAfterValidation(t *testing.T) {
	tests := []struct {
		name, old, replacement string
		want                   []string
	}{
		{
			name: "window bucket count",
			old:  "bucket: 1h", replacement: "bucket: 1s",
			want: []string{"budgets.policies[0].windows[2]", "2592002 buckets", "limits.max_budget_buckets_per_window"},
		},
		{
			name: "limit above the Redis range",
			old:  `limit_usd: "25.000000000000000000"`, replacement: `limit_usd: "99999999999999999999"`,
			want: []string{"budgets.policies[0].windows[0].limit_usd", "must not exceed"},
		},
		{
			name: "limit below one nano-USD",
			old:  `limit_usd: "250.000000000000000000"`, replacement: `limit_usd: "0.0000000001"`,
			want: []string{"budgets.policies[0].windows[1].limit_usd", "rounds down to zero"},
		},
		{
			name: "policy ID leaves no room for the window id",
			old:  "id: acme-production", replacement: "id: " + strings.Repeat("p", 127),
			want: []string{"budgets.policies[0].windows[0] identity", "must be at most 128 bytes"},
		},
		{
			name: "admission hash tag with a brace",
			old:  "admission_hash_tag: admission", replacement: `admission_hash_tag: "ad{mission"`,
			want: []string{"state.redis.admission_hash_tag"},
		},
		{
			name: "admission hash tag too long",
			old:  "admission_hash_tag: admission", replacement: "admission_hash_tag: " + strings.Repeat("t", 65),
			want: []string{"state.redis.admission_hash_tag"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requireLoadError(t, replaceExample(t, test.old, test.replacement), test.want...)
		})
	}
	// A long policy ID whose window identities still fit is accepted.
	if _, err := config.Load(replaceExample(t, "id: acme-production", "id: "+strings.Repeat("p", 120))); err != nil {
		t.Fatalf("120-byte policy ID rejected: %v", err)
	}
}

func TestLoadRejectsLegacyBudgetLimitOutsideTheRedisRange(t *testing.T) {
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	window := &loaded.Budgets.Policies[0].Windows[0]
	*window = config.BudgetWindow{Duration: window.Duration, Bucket: window.Bucket, LimitMicroUSD: 1 << 53}
	err = loaded.Validate()
	if err == nil || !strings.Contains(err.Error(), "budgets.policies[0].windows[0].limit_micro_usd") {
		t.Fatalf("legacy limit error = %v", err)
	}
}

func TestLoadRejectsBudgetMatchersThatCanNeverMatch(t *testing.T) {
	const match = "match:\n        tenant: acme\n        environment: production"
	tests := []struct {
		name, replacement string
		want              string
	}{
		{"unknown logical model", "match:\n        logical_model: invoice-sumarizer", "budgets.policies[0].match.logical_model"},
		{"unknown endpoint", "match:\n        endpoint: openai-prd", "budgets.policies[0].match.endpoint"},
		{"endpoint no model routes to", "match:\n        endpoint: exa-answer", "budgets.policies[0].match.endpoint \"exa-answer\" is not used by any model route"},
		{"another environment", "match:\n        tenant: acme\n        environment: staging", "budgets.policies[0].match.environment"},
		{"tenant with different case", "match:\n        tenant: Acme", "budgets.policies[0].match.tenant"},
		{"tenant outside the allowlist", "match:\n        tenant: globex", "budgets.policies[0].match.tenant"},
		{"project outside the allowlist", "match:\n        project: payroll", "budgets.policies[0].match.project"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requireLoadError(t, replaceExample(t, match, test.replacement), test.want, "budgets.policies[0]")
		})
	}

	t.Run("tenant and project from different scopes", func(t *testing.T) {
		data := string(replaceExample(t, match, "match:\n        tenant: acme\n        project: payroll"))
		const scope = "    - tenant: acme\n      project: invoice-processing\n"
		if !strings.Contains(data, scope) {
			t.Fatal("example authorization scope missing")
		}
		data = strings.Replace(data, scope, scope+"    - tenant: globex\n      project: payroll\n", 1)
		requireLoadError(t, []byte(data), "budgets.policies[0].match", "not paired")
	})

	t.Run("wildcards and declared values stay valid", func(t *testing.T) {
		replacement := "match:\n        tenant: acme\n        project: \"*\"\n        environment: production\n        logical_model: invoice-summarizer\n        endpoint: openai-prod"
		if _, err := config.Load(replaceExample(t, match, replacement)); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("scopes are not checked without an allowlist", func(t *testing.T) {
		loaded, err := config.Load(exampleYAML(t))
		if err != nil {
			t.Fatal(err)
		}
		loaded.Authorization = nil
		loaded.Budgets.Policies[0].Match.Tenant = "embedding-tenant"
		if err := loaded.Validate(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestLoadRejectsNullWhereAValueWouldBeSubstituted(t *testing.T) {
	tests := []struct {
		name, old, replacement, want string
	}{
		{"budget switch", "require_match: true", "require_match: ~", "budgets.require_match must not be null"},
		{"empty boolean", "require_price_when_budgeted: true", "require_price_when_budgeted:", "pricing.require_price_when_budgeted must not be null"},
		{"tenant list element", "allowed_tenants: [acme]", "allowed_tenants: [~]", "models.invoice-summarizer.allowed_tenants[0] must not be null"},
		{"tenant list element beside a tenant", "allowed_tenants: [acme]", "allowed_tenants: [acme, null]", "models.invoice-summarizer.allowed_tenants[1] must not be null"},
		{"string setting", "environment: production", "environment: null", "environment must not be null"},
		{"duration", "duration: 1h", "duration: ~", "budgets.policies[0].windows[0].duration must not be null"},
		{"exact limit", `limit_usd: "25.000000000000000000"`, "limit_usd: ~", "budgets.policies[0].windows[0].limit_usd must not be null"},
		{"section", "match:\n        tenant: acme\n        environment: production", "match: ~", "budgets.policies[0].match must not be null"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requireLoadError(t, replaceExample(t, test.old, test.replacement), test.want)
		})
	}
}

func TestBudgetsJSONRejectsNullSwitchAndPolicy(t *testing.T) {
	for document, want := range map[string]string{
		`{"require_match":null,"policies":[]}`:      "budgets.require_match must not be null",
		`{"require_match":false,"policies":[null]}`: "budgets.policies[0] must not be null",
	} {
		if _, err := config.ParseBudgetsJSON([]byte(document)); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("budgets_json %s error = %v, want %q", document, err, want)
		}
	}
}

// A null or omitted list is the documented spelling of "no restriction", and
// the rendered effective configuration writes unset lists and maps as null.
func TestLoadAcceptsNullListsAndTheRenderedEffectiveConfiguration(t *testing.T) {
	loaded, err := config.Load(replaceExample(t, "allowed_tenants: [acme]", "allowed_tenants: ~"))
	if err != nil {
		t.Fatal(err)
	}
	if tenants := loaded.Models["invoice-summarizer"].AllowedTenants; len(tenants) != 0 {
		t.Fatalf("allowed_tenants = %v, want no restriction", tenants)
	}
	rendered, err := json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rendered), `"allowed_tenants":null`) {
		t.Fatal("fixture no longer renders an unset list as null")
	}
	if _, err := config.Load(rendered); err != nil {
		t.Fatalf("rendered effective configuration no longer loads: %v", err)
	}
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiled.Validate(rendered); err != nil {
		t.Fatalf("schema rejects a model without a tenant restriction: %v", err)
	}
}

// An endpoint reaches budget matching only through a route of the requested
// model, so a matcher pairing a model with another model's endpoint is inert.
func TestValidateRejectsBudgetEndpointOutsideTheMatchedModel(t *testing.T) {
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	var class llm.ServiceClass
	for class = range loaded.Endpoints["exa-answer"].ServiceClasses {
		break
	}
	loaded.Models["search"] = config.ModelConfig{Routes: []config.RouteConfig{{ID: "exa", Endpoint: "exa-answer", Model: "exa", Classes: []llm.ServiceClass{class}}}}
	match := &loaded.Budgets.Policies[0].Match
	*match = config.BudgetMatch{EndpointID: "exa-answer"}
	if err := loaded.Validate(); err != nil {
		t.Fatalf("endpoint routed by a model rejected: %v", err)
	}
	match.LogicalModel = "search"
	if err := loaded.Validate(); err != nil {
		t.Fatalf("endpoint routed by the matched model rejected: %v", err)
	}
	match.LogicalModel = "invoice-summarizer"
	err = loaded.Validate()
	if err == nil || !strings.Contains(err.Error(), `budgets.policies[0].match.endpoint "exa-answer" is not used by a route of model "invoice-summarizer"`) {
		t.Fatalf("endpoint of another model error = %v", err)
	}
}
