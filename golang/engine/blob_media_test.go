package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestGenerateRejectsBlobReferencedMediaBeforeAdmission(t *testing.T) {
	adapter := &fakeAdapter{name: "blob-media", response: successfulResponse()}
	harness := newHarness(t, adapter)
	request := baseRequest("blob-media")
	request.Input = []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.ImagePart{Blob: &llm.BlobRef{Digest: strings.Repeat("a", 64), ByteLength: 1024, MediaType: "image/png", Locator: "blobs/tenant/image"}, MediaType: "image/png"}}}}
	for name, run := range map[string]func() error{
		"generate": func() error { _, err := harness.engine.Generate(context.Background(), request); return err },
		"stream":   func() error { _, err := harness.engine.Stream(context.Background(), request); return err },
	} {
		err := run()
		testkit.ProviderError(t, err, testkit.ProviderClass{Code: provider.CodeUnsupportedCapability, Dispatch: provider.DispatchNotDispatched}, fmt.Sprintf("%s error", name))
	}
	if adapter.compiles != 0 || adapter.invokes != 0 {
		t.Fatalf("adapter compiled %d and invoked %d times, want neither", adapter.compiles, adapter.invokes)
	}
}
