package engine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
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
		var mapped *provider.Error
		if !errors.As(err, &mapped) || mapped.Code != provider.CodeUnsupportedCapability || mapped.Dispatch != provider.DispatchNotDispatched {
			t.Fatalf("%s error = %v, want unsupported_capability before dispatch", name, err)
		}
	}
	if adapter.compiles != 0 || adapter.invokes != 0 {
		t.Fatalf("adapter compiled %d and invoked %d times, want neither", adapter.compiles, adapter.invokes)
	}
}
