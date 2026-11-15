package openaichat

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestInvokeRecoversAnSDKPanicWhileDecodingTheResponse(t *testing.T) {
	client, err := NewClient(ClientConfig{
		BaseURL:    "https://127.0.0.1/contract",
		APIKey:     "test-key",
		HTTPClient: &http.Client{Transport: adaptertest.PanickingTransport(http.StatusOK)},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, call := compileChatProdCall(t, client, "op-panic")
	_, err = adapter.Invoke(context.Background(), call, nil)
	adaptertest.RecoveredDecodePanic(t, err, "op-panic")
}

func TestInvokeRecoversAnSDKPanicWhileReadingAnErrorResponse(t *testing.T) {
	client, err := NewClient(ClientConfig{
		BaseURL:    "https://127.0.0.1/contract",
		APIKey:     "test-key",
		HTTPClient: &http.Client{Transport: adaptertest.PanickingTransport(http.StatusServiceUnavailable)},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, call := compileChatProdCall(t, client, "op-panic")
	_, err = adapter.Invoke(context.Background(), call, nil)
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Dispatch != provider.DispatchRejected || mapped.Retry != provider.RetrySameOperation || mapped.Code != provider.CodeProviderUnavailable {
		t.Fatalf("Invoke() error = %#v, want the rejected, retryable 503 classification", err)
	}
}
