package openaichat

import (
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func TestLowerSamplingRejectsMoreThanFourStopSequences(t *testing.T) {
	if err := lowerSampling(llm.SamplingSpec{StopSequences: []string{"a", "b", "c", "d", "e"}}, map[string]any{}); err == nil {
		t.Fatal("five stop sequences were accepted")
	}
	target := map[string]any{}
	if err := lowerSampling(llm.SamplingSpec{StopSequences: []string{"a", "b", "c", "d"}}, target); err != nil || target["stop"] == nil {
		t.Fatalf("four stop sequences = %v, %v; want them sent", target["stop"], err)
	}
}

func TestLowerPartsRejectsAnUnknownImageDetail(t *testing.T) {
	image := llm.ImagePart{URL: "https://example.com/a.png", MediaType: "image/png", Detail: "ultra"}
	if _, err := lowerParts([]llm.Part{image}); err == nil {
		t.Fatal("image detail ultra was accepted")
	}
	image.Detail = "original"
	if _, err := lowerParts([]llm.Part{image}); err != nil {
		t.Fatalf("image detail original rejected: %v", err)
	}
}
