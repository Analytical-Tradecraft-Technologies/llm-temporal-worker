package openaiwire

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/openaischema"
)

func TestMediaURLPrefersURLThenInlineBytes(t *testing.T) {
	if got, err := MediaURL("https://example.test/a.png", []byte("x"), "image/png"); err != nil || got != "https://example.test/a.png" {
		t.Fatalf("MediaURL with URL = %q, %v", got, err)
	}
	if got, err := MediaURL("", []byte("hi"), "text/plain"); err != nil || got != "data:text/plain;base64,aGk=" {
		t.Fatalf("MediaURL with bytes = %q, %v", got, err)
	}
	if got, err := MediaURL("", nil, "image/png"); err == nil || got != "" || err.Error() != "blob-backed media is not available to adapter" {
		t.Fatalf("MediaURL without source = %q, %v", got, err)
	}
}

func TestToolResultTextJoinsTextAndJSON(t *testing.T) {
	got, err := ToolResultText([]llm.Part{llm.TextPart{Text: "a"}, llm.JSONPart{Value: json.RawMessage(`{"b":1}`)}})
	if err != nil || got != `a{"b":1}` {
		t.Fatalf("ToolResultText = %q, %v", got, err)
	}
	if _, err := ToolResultText([]llm.Part{llm.JSONPart{Value: json.RawMessage(`{`)}}); err == nil || err.Error() != "tool result part 0 is invalid JSON" {
		t.Fatalf("invalid JSON error = %v", err)
	}
	if _, err := ToolResultText([]llm.Part{llm.TextPart{}, llm.ImagePart{URL: "https://example.test/a.png"}}); err == nil || err.Error() != `tool result part 1 kind "image" cannot be represented as text` {
		t.Fatalf("image error = %v", err)
	}
}

func TestExaClientOptionsValidatesTheEndpoint(t *testing.T) {
	client := &http.Client{}
	baseURL, options, err := ExaClientOptions("exa test", ExaBaseURL, "key", client)
	if err != nil || baseURL != ExaBaseURL || len(options) != 5 {
		t.Fatalf("ExaClientOptions = %q, %d options, %v", baseURL, len(options), err)
	}
	for _, test := range []struct {
		name    string
		baseURL string
		key     string
		client  *http.Client
		want    string
	}{
		{"other host", "https://example.test/", "key", client, `exa test: base URL must be exactly "https://api.exa.ai/"`},
		{"missing client", ExaBaseURL, "key", nil, "exa test: HTTP client is required"},
		{"missing key", ExaBaseURL, "", client, "exa test: "},
	} {
		_, _, err := ExaClientOptions("exa test", test.baseURL, test.key, test.client)
		if err == nil || !strings.HasPrefix(err.Error(), test.want) {
			t.Fatalf("%s: error = %v, want prefix %q", test.name, err, test.want)
		}
	}
}

func TestPartFeatureAndLowerError(t *testing.T) {
	if PartFeature(llm.ImagePart{}) != provider.FeatureImage || PartFeature(llm.DocumentPart{}) != provider.FeatureDocument || PartFeature(llm.JSONPart{}) != provider.FeatureText {
		t.Fatal("PartFeature mapped a part to the wrong feature")
	}
	if mapped := LowerError(&openaischema.UnsupportedError{}); mapped.Code != provider.CodeUnsupportedCapability {
		t.Fatalf("unsupported schema error code = %q", mapped.Code)
	}
	if mapped := LowerError(errors.New("bad request")); mapped.Phase != provider.PhaseCompile || mapped.Code == provider.CodeUnsupportedCapability {
		t.Fatalf("compile error = %+v", mapped)
	}
}
