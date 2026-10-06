package openairesponses

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestLowerPartRejectsAnUnknownImageDetail(t *testing.T) {
	image := llm.ImagePart{URL: "https://example.com/a.png", MediaType: "image/png", Detail: "ultra"}
	if _, err := lowerPart(image); err == nil {
		t.Fatal("image detail ultra was accepted")
	}
	image.Detail = "low"
	if _, err := lowerPart(image); err != nil {
		t.Fatalf("image detail low rejected: %v", err)
	}
}
