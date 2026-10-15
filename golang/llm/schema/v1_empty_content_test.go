package schema_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// TestV1RequestsRejectEmptyContent pins that the schema and the Go codec both
// reject request content every provider family rejects after admission:
// empty message content, empty text parts and empty instructions. Responses
// keep accepting empty model output.
func TestV1RequestsRejectEmptyContent(t *testing.T) {
	generateRequest := readV1Schema(t, "generate-request.schema.json")
	generateResponse := readV1Schema(t, "generate-response.schema.json")
	const prefix = `{"api_version":"llm.temporal/v1","operation_key":"op","context":{"tenant":"t","project":"p","actor":"a"},`
	for name, body := range map[string]string{
		"empty-message":            `"append":[{"kind":"message","actor":"human","content":[]}]`,
		"empty-model-message":      `"append":[{"kind":"message","actor":"model","content":[]}]`,
		"empty-text-part":          `"append":[{"kind":"message","actor":"human","content":[{"kind":"text","text":""}]}]`,
		"empty-text-after-image":   `"append":[{"kind":"message","actor":"human","content":[{"kind":"image","media_type":"image/png","url":"https://example.com/a.png"},{"kind":"text","text":""}]}]`,
		"empty-text-instruction":   `"append":[],"settings_patch":{"instructions":{"set":[{"kind":"text","text":""}]}}`,
		"empty-parts-instruction":  `"append":[],"settings_patch":{"instructions":{"set":[{"kind":"parts","content":[]}]}}`,
		"empty-text-in-parts-inst": `"append":[],"settings_patch":{"instructions":{"set":[{"kind":"parts","content":[{"kind":"text","text":""}]}]}}`,
	} {
		t.Run(name, func(t *testing.T) {
			document := []byte(prefix + body + "}")
			if err := generateRequest.Validate(document); err == nil {
				t.Fatalf("schema accepts %s", document)
			}
			var request llm.GenerateRequestV1
			if err := json.Unmarshal(document, &request); err == nil || !strings.Contains(err.Error(), "must not be empty") {
				t.Fatalf("Go codec error = %v, want a must-not-be-empty rejection", err)
			}
		})
	}

	context := llm.RequestContext{Tenant: "t", Project: "p", Actor: "a"}
	textInstruction := []llm.Instruction{{Kind: llm.InstructionKindText}}
	for name, request := range map[string]llm.GenerateRequestV1{
		"empty-message":          {Append: []llm.Item{llm.Message{Actor: llm.ActorHuman}}},
		"empty-text-part":        {Append: []llm.Item{&llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{}}}}},
		"empty-text-instruction": {SettingsPatch: llm.SettingsPatchV1{Instructions: llm.Patch[[]llm.Instruction]{Set: &textInstruction}}},
	} {
		t.Run("marshal/"+name, func(t *testing.T) {
			request.OperationKey, request.Context = "op", context
			if _, err := json.Marshal(request); err == nil {
				t.Fatal("Go codec marshalled a request with empty content")
			}
		})
	}

	// Allowed request content and response output are unaffected.
	for _, document := range []string{
		prefix + `"append":[{"kind":"message","actor":"human","content":[{"kind":"text","text":"hi"}]},{"kind":"tool_result","call_id":"c","content":[{"kind":"text","text":""}],"is_error":false}]}`,
		prefix + `"append":[],"settings_patch":{"instructions":{"set":[{"kind":"text","text":"be brief"},{"kind":"parts","content":[{"kind":"text","text":"x"}]}]}}}`,
	} {
		if err := generateRequest.Validate([]byte(document)); err != nil {
			t.Fatalf("schema rejects %s: %v", document, err)
		}
		var request llm.GenerateRequestV1
		if err := json.Unmarshal([]byte(document), &request); err != nil {
			t.Fatalf("Go codec rejects %s: %v", document, err)
		}
	}
	response, err := json.Marshal(llm.GenerateResponseV1{OperationKey: "op", OperationID: "op-id", Status: llm.ResponseStatusCompleted,
		Checkpoint: llm.CheckpointMetadata{Handle: "ckp_v1.child", Kind: "generation"}, Cache: llm.CacheDispositionV1{Disposition: "disabled"},
		Cost:   llm.CostV1{Status: "unknown", UnknownReason: "provider_did_not_report_cost"},
		Output: []llm.Item{llm.Message{Actor: llm.ActorModel}, llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := generateResponse.Validate(response); err != nil {
		t.Fatalf("response schema rejects empty model output: %v", err)
	}
}
