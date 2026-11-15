package openairesponses

import (
	"context"
	"net/http"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestInvokeRecoversAnSDKPanicWhileDecodingTheResponse(t *testing.T) {
	client, err := NewClient(ClientConfig{
		BaseURL:    "https://provider.example/v1",
		APIKey:     "test-key",
		HTTPClient: &http.Client{Transport: adaptertest.PanickingTransport(http.StatusOK)},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "openai-prod", "cap-test")
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: llm.Request{OperationKey: "op-panic", Model: "gpt-contract"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Invoke(context.Background(), call, nil)
	adaptertest.RecoveredDecodePanic(t, err, "op-panic")
}
