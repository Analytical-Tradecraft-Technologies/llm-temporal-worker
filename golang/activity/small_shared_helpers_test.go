package activity

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDecodeRequiredFieldNamesTheField(t *testing.T) {
	fields := map[string]json.RawMessage{"api_version": json.RawMessage(`"v1"`), "request": json.RawMessage(`7`)}
	var version string
	if err := decodeRequiredField(fields, "api_version", &version); err != nil || version != "v1" {
		t.Fatalf("decode = %q, %v", version, err)
	}
	if err := decodeRequiredField(fields, "missing", &version); err == nil || err.Error() != `missing required JSON field "missing"` {
		t.Fatalf("missing = %v", err)
	}
	if err := decodeRequiredField(fields, "request", &version); err == nil || !strings.HasPrefix(err.Error(), "request: ") {
		t.Fatalf("wrong type = %v", err)
	}
}

func TestSafeIdentifierBoundsLengthAndControlCharacters(t *testing.T) {
	for value, want := range map[string]string{"": "", "route-1": "route-1", "abcd": "", "a\nb": ""} {
		limit := 8
		if value == "abcd" {
			limit = 3
		}
		if got := safeIdentifier(value, limit); got != want {
			t.Errorf("safeIdentifier(%q, %d) = %q, want %q", value, limit, got, want)
		}
	}
}
