package openaiwire

import (
	"testing"

	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestImageSourceDefaultsAndChecksDetail(t *testing.T) {
	url, detail, err := ImageSource(llm.ImagePart{URL: "https://example.test/a.png"})
	if err != nil || url != "https://example.test/a.png" || detail != "auto" {
		t.Fatalf("ImageSource() = %q, %q, %v; want the URL with auto detail", url, detail, err)
	}
	if _, detail, err := ImageSource(llm.ImagePart{Bytes: []byte{1}, MediaType: "image/png", Detail: "original"}); err != nil || detail != "original" {
		t.Fatalf("ImageSource(original) = %q, %v", detail, err)
	}
	for _, test := range []struct {
		image llm.ImagePart
		want  string
	}{
		{image: llm.ImagePart{MediaType: "image/png"}, want: "image: blob-backed media is not available to adapter"},
		{image: llm.ImagePart{URL: "https://example.test/a.png", Detail: "max"}, want: `image detail "max" is not one of low, high, auto or original`},
	} {
		if _, _, err := ImageSource(test.image); err == nil || err.Error() != test.want {
			t.Errorf("ImageSource(%#v) = %v, want %q", test.image, err, test.want)
		}
	}
}

func TestSchemaOrderOptionsWithoutSchemasIsEmpty(t *testing.T) {
	options, err := SchemaOrderOptions(openai.ChatCompletionNewParams{Model: "m"}, false)
	if err != nil || len(options) != 0 {
		t.Fatalf("SchemaOrderOptions() = %d options, %v; want none", len(options), err)
	}
}
