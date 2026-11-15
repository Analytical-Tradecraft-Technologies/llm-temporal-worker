package openaichat

import (
	"context"
	"net/http"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestInvokeRecoversAnSDKPanicWhileDecodingTheResponse(t *testing.T) {
	client, err := NewClient(ClientConfig{
		BaseURL:    "https://127.0.0.1/contract",
		APIKey:     "test-key",
		HTTPClient: &http.Client{Transport: adaptertest.PanickingTransport(http.StatusOK)},
	})
	testassert.NoError(t, err)
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
	testassert.NoError(t, err)
	adapter, call := compileChatProdCall(t, client, "op-panic")
	_, err = adapter.Invoke(context.Background(), call, nil)
	testkit.ProviderError(t, err, testkit.ProviderClass{Code: provider.CodeProviderUnavailable, Dispatch: provider.DispatchRejected, Retry: provider.RetrySameOperation}, "Invoke() error")
}
