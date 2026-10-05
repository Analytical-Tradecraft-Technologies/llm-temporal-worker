package llm

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestGenerateRequestV1ValidatesOutputFormatName(t *testing.T) {
	negative, err := os.ReadFile("testdata/v1/negative-generate-output-name.json")
	if err != nil {
		t.Fatal(err)
	}
	const invalid = `"name":"claim summary",`
	if !strings.Contains(string(negative), invalid) {
		t.Fatalf("fixture does not contain %s", invalid)
	}
	for name, test := range map[string]struct {
		replacement string
		valid       bool
	}{
		"fixture":            {replacement: invalid},
		"omitted":            {replacement: ``, valid: true},
		"provider charset":   {replacement: `"name":"Claim_summary-2",`, valid: true},
		"64 characters":      {replacement: `"name":"` + strings.Repeat("a", 64) + `",`, valid: true},
		"65 characters":      {replacement: `"name":"` + strings.Repeat("a", 65) + `",`},
		"dot":                {replacement: `"name":"claim.summary",`},
		"non-ASCII letter":   {replacement: `"name":"résumé",`},
		"control characters": {replacement: `"name":"claim\nsummary",`},
	} {
		t.Run(name, func(t *testing.T) {
			var request GenerateRequestV1
			err := request.UnmarshalJSON([]byte(strings.Replace(string(negative), invalid, test.replacement, 1)))
			if test.valid != (err == nil) {
				t.Fatalf("decode error = %v, want valid=%t", err, test.valid)
			}
		})
	}
}

func TestOutputFormatMarshalRejectsInvalidName(t *testing.T) {
	format := OutputFormat{Kind: OutputKindJSONSchema, Name: "claim summary", Schema: json.RawMessage(`{"type":"object"}`)}
	if _, err := json.Marshal(format); err == nil {
		t.Fatal("invalid output format name was marshalled")
	}
	format.Name = ""
	if _, err := json.Marshal(format); err != nil {
		t.Fatalf("nameless output format = %v", err)
	}
}
