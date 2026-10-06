package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// A library caller passes a Go value that never crosses a request decoder, so
// the engine entry points apply the remote-media URL policy themselves.
func TestEngineRejectsBlockedMediaURLBeforeDispatch(t *testing.T) {
	for _, raw := range []string{"http://localhost/a.png", "http://127.1/a.png", "http://[2002:a9fe:a9fe::1]/a.png", "http://metadata.google.internal/a.png"} {
		adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
		harness := newHarness(t, adapter)
		request := baseRequest("blocked-media-url")
		request.Input = append(request.Input, llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.ImagePart{URL: raw, MediaType: "image/png"}}})
		_, generateErr := harness.engine.Generate(context.Background(), request)
		_, streamErr := harness.engine.Stream(context.Background(), request)
		for entry, err := range map[string]error{"Generate": generateErr, "Stream": streamErr} {
			var mapped *provider.Error
			if !errors.As(err, &mapped) || mapped.Code != provider.CodeInvalidArgument || mapped.Phase != provider.PhaseNormalize || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever {
				t.Errorf("%s with media URL %q: error = %#v, want invalid_argument before dispatch", entry, raw, err)
			}
		}
		if len(adapter.calls) != 0 {
			t.Errorf("media URL %q reached the provider", raw)
		}
	}
}
