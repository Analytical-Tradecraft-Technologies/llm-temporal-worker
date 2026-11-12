package llm_test

import (
	"encoding/json"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestV1ContractsRejectAnUnsupportedAPIVersion(t *testing.T) {
	for name, target := range map[string]any{
		"generate request":  &llm.GenerateRequestV1{},
		"generate response": &llm.GenerateResponseV1{},
		"compact request":   &llm.CompactRequestV1{},
		"compact response":  &llm.CompactResponseV1{},
		"query request":     &llm.QueryRequestV1{},
		"query response":    &llm.QueryResponseV1{},
	} {
		err := json.Unmarshal([]byte(`{"api_version":"v0","operation_key":"k"}`), target)
		if err == nil || err.Error() != `api_version "v0" is unsupported` {
			t.Fatalf("%s: error = %v", name, err)
		}
		err = json.Unmarshal([]byte(`{"api_version":7}`), target)
		if err == nil || err.Error() != `api_version "" is unsupported` {
			t.Fatalf("%s: non-string version error = %v", name, err)
		}
	}
}
