package llm

import (
	"bytes"
	"encoding/json"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"os"
	"testing"
)

// The v1 schema requires service.fallback_index, so the Go contract decoder
// must reject its omission like the OCaml decoder does.
func TestGenerateResponseV1RequiresServiceFallbackIndex(t *testing.T) {
	data, err := os.ReadFile("testdata/v1/generate-response-all-kinds.json")
	testassert.NoError(t, err)
	mustUnmarshalJSON[GenerateResponseV1](t, data, "decode fixture")
	missing := bytes.Replace(data, []byte(`, "fallback_index": 0}`), []byte(`}`), 1)
	if bytes.Equal(missing, data) {
		t.Fatal("fixture no longer contains the service fallback_index")
	}
	var response GenerateResponseV1
	testassert.ErrorContains(t, json.Unmarshal(missing, &response), "fallback_index")
}
