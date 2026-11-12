package llm

import (
	"encoding/json"
	"testing"
)

func TestDecodeRequiredJSONRequiresTheField(t *testing.T) {
	fields := map[string]json.RawMessage{"cost": json.RawMessage(`{"status":"unknown"}`)}
	var target map[string]string
	if err := decodeRequiredJSON(fields, "cost", &target); err != nil || target["status"] != "unknown" {
		t.Fatalf("decodeRequiredJSON = %v, %v", target, err)
	}
	if err := decodeRequiredJSON(fields, "cache", &target); err == nil {
		t.Fatal("decodeRequiredJSON accepted a missing field")
	}
	if err := decodeRequiredJSON(map[string]json.RawMessage{"cost": json.RawMessage(`[1]`)}, "cost", &target); err == nil {
		t.Fatal("decodeRequiredJSON accepted a mistyped field")
	}
}
