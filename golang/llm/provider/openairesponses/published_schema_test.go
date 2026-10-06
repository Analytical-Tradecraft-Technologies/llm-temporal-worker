package openairesponses

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/openai/openai-go/v3/responses"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	llmschema "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/schema"
)

// Checkpoint publication copies lifted output and usage into the public v1
// response unfiltered, so what this lifter emits (reasoning provider_state
// items, coded refusals, usage.provider_raw) must satisfy the published schema.
func TestLiftedResponseSatisfiesPublishedGenerateResponseSchema(t *testing.T) {
	schemaData, err := os.ReadFile("../../../api/schema/v1/generate-response.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	call := provider.Call{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract", OperationKey: "op-lift", ServiceClass: llm.ServiceClassEconomy}
	completed := loadResponseFixture(t, "response.completed.json")
	refused := minimalResponse(responses.ResponseServiceTierDefault, responses.ResponseStatusCompleted)
	refused.Output = decodeOutputItems(t, `[{"type":"message","id":"msg","role":"assistant","status":"completed","content":[{"type":"refusal","refusal":"no"}]}]`)
	for name, response := range map[string]responses.Response{"completed": completed, "refused": refused} {
		t.Run(name, func(t *testing.T) {
			lifted, err := liftResponse(call, &response, "req-1")
			if err != nil {
				t.Fatal(err)
			}
			if name == "completed" && len(lifted.Usage.ProviderRaw) == 0 {
				t.Fatal("fixture no longer lifts provider_raw usage")
			}
			amount := "0"
			encoded, err := json.Marshal(llm.GenerateResponseV1{OperationKey: lifted.OperationKey, OperationID: "op-id", Status: lifted.Status, Output: lifted.Output,
				Checkpoint: llm.CheckpointMetadata{Handle: "ckp_v1.child", Kind: "generation"}, Cache: llm.CacheDispositionV1{Disposition: "disabled"},
				Route: &lifted.Route, Usage: &lifted.Usage, Cost: llm.CostV1{Status: "exact", ActualCostUSD: &amount, Method: "provider_reported"}, Diagnostics: lifted.Diagnostics})
			if err != nil {
				t.Fatal(err)
			}
			if err := llmschema.Validate(schemaData, encoded); err != nil {
				t.Fatalf("published schema rejects the lifted response: %v\n%s", err, encoded)
			}
		})
	}
}
