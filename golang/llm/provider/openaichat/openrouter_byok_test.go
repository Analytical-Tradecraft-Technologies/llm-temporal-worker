package openaichat

import (
	"encoding/json"
	"testing"

	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func liftOpenRouterUsage(t *testing.T, usage string) *llm.Response {
	t.Helper()
	var response openai.ChatCompletion
	body := `{"id":"gen-1","model":"router-model","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}],"usage":` + usage + `}`
	if err := json.Unmarshal([]byte(body), &response); err != nil {
		t.Fatal(err)
	}
	lifted := &llm.Response{}
	if err := augmentOpenRouter(provider.Call{}, &response, lifted); err != nil {
		t.Fatal(err)
	}
	return lifted
}

// A BYOK call settles at OpenRouter's fee plus the upstream inference cost the
// operator's provider key is billed (#1314).
func TestOpenRouterBYOKSettlesUpstreamInferenceCost(t *testing.T) {
	for name, test := range map[string]struct {
		usage string
		want  string
	}{
		"zero fee":     {`{"prompt_tokens":4,"completion_tokens":2,"cost":0,"is_byok":true,"cost_details":{"upstream_inference_cost":0.00010675}}`, "0.000106750000000000"},
		"platform fee": {`{"prompt_tokens":4,"completion_tokens":2,"cost":0.000005,"is_byok":true,"cost_details":{"upstream_inference_cost":0.0001}}`, "0.000105000000000000"},
		"non-BYOK":     {`{"prompt_tokens":4,"completion_tokens":2,"cost":0.00000123,"cost_details":{"upstream_inference_cost":0.00000123}}`, "0.000001230000000000"},
		"BYOK false":   {`{"prompt_tokens":4,"completion_tokens":2,"cost":0.00000123,"is_byok":false,"cost_details":{"upstream_inference_cost":0.00000123}}`, "0.000001230000000000"},
	} {
		t.Run(name, func(t *testing.T) {
			lifted := liftOpenRouterUsage(t, test.usage)
			if lifted.Cost.Status != "" || lifted.Cost.ActualCostUSD == nil || lifted.Cost.ActualCostUSD.String() != test.want || lifted.Cost.Method != "openrouter_reported" {
				t.Fatalf("cost = %#v, want actual %s", lifted.Cost, test.want)
			}
		})
	}
}

// A BYOK call whose upstream cost or fee is missing must not settle at the fee
// alone: the cost stays unknown so the reservation is charged.
func TestOpenRouterBYOKWithoutUpstreamCostIsUnknown(t *testing.T) {
	for name, usage := range map[string]string{
		"no cost_details":   `{"prompt_tokens":4,"completion_tokens":2,"cost":0,"is_byok":true}`,
		"null upstream":     `{"prompt_tokens":4,"completion_tokens":2,"cost":0,"is_byok":true,"cost_details":{"upstream_inference_cost":null}}`,
		"negative upstream": `{"prompt_tokens":4,"completion_tokens":2,"cost":0,"is_byok":true,"cost_details":{"upstream_inference_cost":-1}}`,
		"no OpenRouter fee": `{"prompt_tokens":4,"completion_tokens":2,"is_byok":true,"cost_details":{"upstream_inference_cost":0.0001}}`,
	} {
		t.Run(name, func(t *testing.T) {
			lifted := liftOpenRouterUsage(t, usage)
			if lifted.Cost.Status != llm.CostStatusUnknown || lifted.Cost.ActualCostUSD != nil {
				t.Fatalf("cost = %#v, want unknown", lifted.Cost)
			}
		})
	}
}
