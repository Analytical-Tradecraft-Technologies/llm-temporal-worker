package llm

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// The v1 schema requires service.fallback_index, so the Go contract decoder
// must reject its omission like the OCaml decoder does.
func TestGenerateResponseV1RequiresServiceFallbackIndex(t *testing.T) {
	data, err := os.ReadFile("testdata/v1/generate-response-all-kinds.json")
	if err != nil {
		t.Fatal(err)
	}
	var valid GenerateResponseV1
	if err := json.Unmarshal(data, &valid); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	missing := bytes.Replace(data, []byte(`, "fallback_index": 0}`), []byte(`}`), 1)
	if bytes.Equal(missing, data) {
		t.Fatal("fixture no longer contains the service fallback_index")
	}
	var response GenerateResponseV1
	if err := json.Unmarshal(missing, &response); err == nil || !strings.Contains(err.Error(), "fallback_index") {
		t.Fatalf("decode without fallback_index = %v, want a fallback_index error", err)
	}
}
